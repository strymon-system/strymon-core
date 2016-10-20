/// Fork of the log analysis driver which has been extensively tidied up for benchmarking purposes.
/// It only performs sessionization and does not collect histograms or run further applications.
///
/// (NOTE: this sadly had to be copied-and-pasted because the Timely glue is not easily modular, in
/// particular because we do not have a query interface and Rust does not support abstract return
/// types which limits code reuse.)

extern crate libc;
extern crate logparse;
extern crate time;
extern crate timely;
extern crate timely_contrib;
extern crate walkdir;
#[macro_use] extern crate abomonation;
#[macro_use] extern crate sessionize;

use logparse::input::LazyArchiveChain;
use logparse::parser2::GetDcxidTrxnb;
use logparse::reorder::RecordReorder;
use sessionize::sessionize::{MessagesForSession, Sessionize, SessionizableMessage};

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use abomonation::Abomonation;
use timely::dataflow::Scope;
use timely::dataflow::operators::{Concatenate, Filter, Input, Inspect, Probe, Map};
use timely::progress::timestamp::RootTimestamp;
use timely_contrib::operators::Accumulate;
use walkdir::{DirEntry, WalkDir, WalkDirIterator};

#[derive(Debug, Clone)]
struct Message {
   session_id: String,
   trxnb: String,
   time: u64,
}

unsafe_abomonate!(Message: session_id, trxnb, time);

impl SessionizableMessage for Message {
    fn time(&self) -> u64 {
        self.time
    }

    fn session(&self) -> &str {
        &self.session_id
    }
}

impl Message {
    fn new(session_id: String, trxnb: String, time: u64) -> Message {
        Message{session_id: session_id, trxnb: trxnb, time: time}
    }
}

fn convert_trxnb(s: &str) -> Vec<u32> {
    s.split('-').filter_map(|num| {
        if num.is_empty() {
            None  // discard empty segments (e.g. "2-1--8")
        } else {
            Some(::logparse::date_time::parse_positive_decimal(num))
        }
    }).collect()

    // TODO: this is a good use case for SmallVec<[u32; 16]>
    // https://github.com/servo/rust-smallvec
}

fn main() {

    let prefix = ::std::env::args().nth(1).unwrap();
    let window = ::std::env::args().nth(2).unwrap().parse::<u64>().unwrap();

    println!("starting analysis with prefix: {}", prefix);
    let inputs = locate_log_runs(prefix);
    if let Some(limit) = get_max_fd_limit() {
        if (inputs.len() as u64) > limit {
            println!("WARNING: file descriptor limit is too low ({} inputs but max is {}); \
                    run `ulimit -n 2048` to increase it", inputs.len(), limit);
        }
    }

    timely::execute_from_args(::std::env::args(), move |computation| {
        let peers = computation.peers();
        let worker_index = computation.index();
        let (mut input, probe) = computation.scoped::<u64,_,_>(|scope| {
            let (input, stream) = scope.new_input();
            let sessions = stream.sessionize(1_000_000, 5_000_000);

            let streams_to_tie = vec![
                stream.count_by_epoch()
                        .inspect(|&(t, c)| println!("logs,{},{}", t.inner, c))
                        .filter(|_| false).map(|_| 0),

                sessions.count_by_epoch()
                        .inspect(|&(t, c)| println!("sessions,{},{}", t.inner, c))
                        .filter(|_| false).map(|_| 0),

                sessions.map(|session| {
                    session.messages.iter()
                                    .map(|message: &Message| convert_trxnb(&message.trxnb))
                                    .collect::<HashSet<_>>()
                                    .len()
                })
				.accumulate_by_epoch(0, |sum, data| { for &x in data.iter() { *sum += x; } })
				.inspect(|&(t, c)| println!("trx,{},{}", t.inner, c))
				.filter(|_| false).map(|_| 0),

                sessions.map(|session| {
                    session.messages.iter()
                                    .map(|message: &Message| convert_trxnb(&message.trxnb))
                                    .filter(|trxnb| trxnb.len() == 1)
                                    .collect::<HashSet<_>>()
                                    .len()
                })
				.accumulate_by_epoch(0, |sum, data| { for &x in data.iter() { *sum += x; } })
				.inspect(|&(t, c)| println!("root_trx,{},{}", t.inner, c))
				.filter(|_| false).map(|_| 0),
            ];

            let concatenated_stream = scope.concatenate(streams_to_tie);
            let probe = concatenated_stream.probe().0;
            (input, probe)
        });

        let worker_inputs = open_file_readers_for_worker(&inputs, worker_index, peers);
        let file_count = worker_inputs.iter().map(|chain| chain.path_count).fold(0, |acc, c| acc + c);
        println!("Worker {}: {} log runs across {} files", worker_index, worker_inputs.len(), file_count);

        let mut ordered = worker_inputs.into_iter()
            .map(|x| RecordReorder::new(x, window, |rec| rec.timestamp.to_epoch_seconds() as u64).peekable())
            .collect::<Vec<_>>();

        let mut index = 0;
        while index < ordered.len() {
            if ordered[index].peek().is_none() {
                drop(ordered.remove(index));
            }
            else {
                index += 1;
            }
        }

        let mut epochs_processed = 0u64;
        while ordered.len() > 0 {
            let begin_ts = time::precise_time_ns();

            // determine next smallest time to play
            let min_time = ordered.iter_mut().map(|x| x.peek().unwrap().timestamp.to_epoch_seconds()).min().unwrap() as u64;

            // advance input time
            if input.epoch() < &min_time {    // NOTE: Asserts otherwise
                input.advance_to(min_time);
            }

            assert!(input.epoch() == &min_time);

            // drain records with this time from each iterator
            for iterator in &mut ordered {
                while iterator.peek().is_some() && iterator.peek().unwrap().timestamp.to_epoch_seconds() as u64 == min_time {
                    let record = iterator.next().unwrap();
                    if let Some((dcx, trxnb)) = record.get_dcxid_trxnb() {
                        input.send(Message::new(dcx.to_owned(), trxnb.to_owned(), record.timestamp.micros as u64));
                    }
                }
            }

            // discard iterators with no more records
            let mut index = 0;
            while index < ordered.len() {
                if ordered[index].peek().is_none() {
                    drop(ordered.remove(index));
                }
                else {
                    index += 1;
                }
            }

            let process_ts = time::precise_time_ns();

            // advance input time
            if min_time > 0 {
                input.advance_to(min_time + 1);
            }
            while probe.le(&RootTimestamp::new(min_time)) {
                computation.step();
            }
            let end_ts = time::precise_time_ns();

            //println!("{},{},{},{},{}", worker_index, min_time, begin_ts, process_ts, end_ts);

            epochs_processed += 1;
/*
            if epochs_processed > 600 {
                println_stderr!("WARNING: early exit -> only processed the first 10 minutes of log data [!]");
                break
            }
*/
        }
    }).unwrap();
}

/// Returns the limit on the number of file descriptors this process may allocate.
fn get_max_fd_limit() -> Option<u64> {
    let mut rlim = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    let ret = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) };
    if ret == 0 { Some(rlim.rlim_cur) } else { None }
}

fn is_gzip(entry: &DirEntry) -> bool {
    assert!(entry.file_type().is_file());
    match entry.path().extension() {
        None => false,
        Some(ext) => ext == "gz",
    }
}

// Takes a prefix for a collection of logs and produces a vector of paths for each time-ordered log run.
fn locate_log_runs<P: AsRef<Path>>(prefix: P) -> Vec<Vec<PathBuf>> {
    let mut walker = WalkDir::new(prefix.as_ref())
        .min_depth(3)
        .max_depth(3)
        .into_iter()
        .filter_entry(|e| e.file_type().is_dir())
        .map(|e| e.unwrap())
        .collect::<Vec<_>>();

    // Iteration order is unspecified and depends on file system.  Sorting ensures that round-robin
    // assignment is consistent even when running across several machines.
    walker.sort_by(|x, y| Path::cmp(x.path(), y.path()));

    let mut log_run_paths = Vec::new();
    for log_entry in walker.into_iter() {
        let log_dir = log_entry.path();

        // Within each log run we need the files in lexographic order.  Each file has some portion
        // of the records and the naming convention includes the date/time.
        let mut entries = WalkDir::new(&log_dir)
            .min_depth(1)
            .max_depth(1)
            .into_iter()
            .filter_entry(|e| is_gzip(e))
            .map(|e| e.unwrap().path().to_owned())
            .collect::<Vec<_>>();
        entries.sort_by(|p1, p2| PathBuf::cmp(p1, p2));

        // NOTE: there are ~100 log runs where this does not hold true (for example 'lgssp105/sicif/01')
        //assert!(!entries.is_empty(), "No files match '*.gz' in {}", log_dir.display());

        if !entries.is_empty() {
            log_run_paths.push(entries);
        }
    }

    log_run_paths
}

// Strided access: each worker reads from a subset of the log servers
fn open_file_readers_for_worker(all_inputs: &Vec<Vec<PathBuf>>, index: usize, peers: usize) -> Vec<LazyArchiveChain> {
    let mut count = 0;
    let mut record_iters = Vec::new();
    for paths in all_inputs {
        if count % peers == index {
            record_iters.push(LazyArchiveChain::new(paths.to_owned()));
        }
        count += 1;
    }
    record_iters
}
