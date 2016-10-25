use std::path::{Path, PathBuf};

use libc;

use logparse::input::LazyArchiveChain;

use walkdir::{DirEntry, WalkDir, WalkDirIterator};

/// Returns the limit on the number of file descriptors this process may allocate.
pub fn get_max_fd_limit() -> Option<u64> {
    let mut rlim = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    let ret = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) };
    if ret == 0 { Some(rlim.rlim_cur) } else { None }
}

pub fn is_gzip(entry: &DirEntry) -> bool {
    assert!(entry.file_type().is_file());
    match entry.path().extension() {
        None => false,
        Some(ext) => ext == "gz",
    }
}

// Takes a prefix for a collection of logs and produces a vector of paths for each time-ordered log run.
pub fn locate_log_runs<P: AsRef<Path>>(prefix: P, follow_links: bool) -> Vec<Vec<PathBuf>> {
    let mut walker = WalkDir::new(prefix.as_ref()).follow_links(follow_links)
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
        let mut entries = WalkDir::new(&log_dir).follow_links(follow_links)
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
pub fn open_file_readers_for_worker(all_inputs: &Vec<Vec<PathBuf>>, index: usize, peers: usize) -> Vec<LazyArchiveChain> {
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
