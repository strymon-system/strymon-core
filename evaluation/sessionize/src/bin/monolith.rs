/// Fork of the log analysis driver which has been extensively tidied up for benchmarking purposes.
/// It only performs sessionization and does not collect histograms or run further applications.
///
/// (NOTE: this sadly had to be copied-and-pasted because the Timely glue is not easily modular, in
/// particular because we do not have a query interface and Rust does not support abstract return
/// types which limits code reuse.)

extern crate logparse;
extern crate timely;

#[macro_use] extern crate sessionize;
extern crate sessionize_shared;

use logparse::parser2::{GetDcxidTrxnb, GetMsgTypeIP};
use logparse::reorder::RecordReorder;

use sessionize::sessionize::{MessagesForSession, Sessionize};
use sessionize::sessionize::{Histogram, TopK};

use timely::dataflow::Scope;
use timely::dataflow::operators::{Filter, Input, Inspect, Probe, Map};
use timely::progress::timestamp::RootTimestamp;

use sessionize_shared::Message;
use sessionize_shared::reconstruction;
use sessionize_shared::util::{log_discretize, convert_trxnb, dump_histogram_hash_map};
use sessionize_shared::reader::{locate_log_runs, get_max_fd_limit, open_file_readers_for_worker};

/// Window for the reorder buffer
const MAX_ALLOWED_OUT_OF_ORDERNESS: u64 = 10;  // unit: seconds

/// Interval spanned by each dataflow epoch, specified in terms of log timestamps
const EPOCH_DURATION: u64 = 1_000_000;  // unit: microseconds

/// Duration after which an inactive session is terminated
const SESSION_INACTIVITY_LIMIT: u64 = 5_000_000;  // unit: microseconds

fn main() {
    let prefix = ::std::env::args().nth(1)
        .expect("need to pass the prefix to the logs as the first argument");

    println!("starting analysis with prefix: {}", prefix);
    let inputs = locate_log_runs(prefix);
    if let Some(limit) = get_max_fd_limit() {
        if (inputs.len() as u64) > limit {
            println!("WARNING: file descriptor limit is too low ({} inputs but max is {}); \
                    run `ulimit -n 2048` to increase it", inputs.len(), limit);
        }
    }

    timely::execute_from_args(::std::env::args().skip(2), move |computation| {
        let peers = computation.peers();
        let worker_index = computation.index();
        let (mut input, probe) = computation.scoped::<u64,_,_>(|scope| {
            let (input, stream) = scope.new_input();
            // Root Query: Messages per ssession
            let sessionize = stream.sessionize(EPOCH_DURATION, SESSION_INACTIVITY_LIMIT);
            
            // we currently only probe after sessionization, in order to be
            // comparable with the modular approach, i.e. in pub/sub we cannot
            // know if the subscribers are catching up
            let probe = sessionize.probe().0;

            ///////////////////////////////////////////////////////////////////////////////////////////////////////////

            // Leaf Query: Message count per session
            let histogram_message_count = sessionize.map(|messages_for_session| messages_for_session.messages.len()).histogram(|x| x.clone());
            histogram_message_count.inspect(move |x| {
                let epoch = x.0;
                let values = x.1.clone();
                dump_histogram_hash_map("MessageCountLog", worker_index, epoch, values, Some(|x| log_discretize(x as u64) as usize), false);
            });

            ///////////////////////////////////////////////////////////////////////////////////////////////////////////

            // Leaf Query: Session duration 
            let histogram_log_span = sessionize.filter(|messages_for_session| messages_for_session.messages.len() >= 2)
                                                .map(|messages_for_session : MessagesForSession<Message>| messages_for_session.messages.iter()
                                                .map(|m| m.time).max().unwrap() - messages_for_session.messages.iter()
                                                .map(|m| m.time).min().unwrap()).histogram(|x| log_discretize(x.clone()));
            histogram_log_span.inspect(move |x| {
                let epoch = x.0;
                let values = x.1.clone();
                dump_histogram_hash_map("LogMessageSpan", worker_index, epoch, values, Some(|x| x), true);
            });

            ///////////////////////////////////////////////////////////////////////////////////////////////////////////

            // Intermediate Query: Converted Transaction Ids
            let txns_for_each_session_in_message = sessionize.map(|messages_for_session : MessagesForSession<Message>| messages_for_session.messages.iter()
                                                            .map(|message| convert_trxnb(&message.trxnb)).collect::<Vec<_>>());

            ///////////////////////////////////////////////////////////////////////////////////////////////////////////

            // Leaf Query: Transaction tree depth
            let histogram_txn_depth = txns_for_each_session_in_message.map(|txns_in_messages| txns_in_messages.iter().map(|x| x.len()).max().unwrap())
                                                                        .histogram(|x| x.clone());
            histogram_txn_depth.inspect(move |x| {
                let epoch = x.0;
                let values = x.1.clone();
                dump_histogram_hash_map("TrxnDepth", worker_index, epoch, values, Some(|x| x), true);
            });

            ///////////////////////////////////////////////////////////////////////////////////////////////////////////

            // Leaf Query: Top-k transaction tree patterns per epoch
            let histogram_txn_type = txns_for_each_session_in_message.map(|txns_in_messages| reconstruction::reconstruct(&txns_in_messages))
                                        .filter(|txn_shape| txn_shape.len() <= 25)
                                        .topk(|x| x.clone(), 10);
            histogram_txn_type.inspect(move |x| {
                let epoch = x.0;
                let values = x.1.clone();
                dump_histogram_hash_map("TrxnTypeTop10", worker_index, epoch, values, Some(|x| x), true);
            });

            ///////////////////////////////////////////////////////////////////////////////////////////////////////////

            // Leaf Query: Top-k communicating pairs of services per epoch
            let short_messages_for_each_session = sessionize.map(|messages_for_session : MessagesForSession<Message>|
                    messages_for_session.messages.iter().map(|message| (convert_trxnb(&message.trxnb.clone()), message.msg_tag.clone(), message.ip.clone())).collect::<Vec<_>>());

            let service_call_patterns = short_messages_for_each_session.flat_map(|mut short_messages| reconstruction::service_calls(&mut short_messages).into_iter())
                                                                        .topk(|pairs| pairs.clone(), 10);
            service_call_patterns.inspect(move |x| {
                let epoch = x.0;
                let values = x.1.clone();
                dump_histogram_hash_map("ServiceCallsTop10", worker_index, epoch, values, Some(|x| x), true);
            });

            ///////////////////////////////////////////////////////////////////////////////////////////////////////////

            (input, probe)
        });

        let worker_inputs = open_file_readers_for_worker(&inputs, worker_index, peers);
        let file_count = worker_inputs.iter().map(|chain| chain.path_count).fold(0, |acc, c| acc + c);
        println!("Worker {}: {} log runs across {} files", worker_index, worker_inputs.len(), file_count);

        let mut ordered = worker_inputs.into_iter()
            .map(|x| RecordReorder::new(x, MAX_ALLOWED_OUT_OF_ORDERNESS, |rec| rec.timestamp.to_epoch_seconds() as u64).peekable())
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

        while ordered.len() > 0 {
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
                        if let Some((msg_tag, ip, _, _)) = record.get_msg_type_ip_cor() {
                            input.send(Message::new(dcx.to_owned(), trxnb.to_owned(), 
                                        msg_tag.to_string(), ip.to_string(), 
                                        record.timestamp.micros as u64));
                        }
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

            // advance input time
            if min_time > 0 {
                input.advance_to(min_time + 1);
            }

            // note: the probe is after sessionization, not the complete computation
            // but we need to interleave somehow
            while probe.le(&RootTimestamp::new(min_time)) {
                computation.step();
            }

        }
    }).unwrap();
}
