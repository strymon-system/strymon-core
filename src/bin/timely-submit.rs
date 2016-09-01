extern crate timely_query;
extern crate env_logger;

use std::env;
use std::fs;
use std::io;

use timely_query::coordinator::request::Submission as QuerySubmission;
use timely_query::submitter::Submission;
use timely_query::executor::ExecutorType;

fn main() {
    drop(env_logger::init());

    let binary = env::args().nth(1).expect("missing binary path");
    let addr = env::args().nth(2).unwrap_or("localhost:9189".to_string());

    let fetch = fs::canonicalize(binary)
                    .and_then(|path| {
                        path.to_str()
                            .map(|s| s.to_string())
                            .ok_or(io::Error::new(io::ErrorKind::Other, "invalid path"))
                    })
                    .expect("binary not found");

    let query = QuerySubmission {
        fetch: fetch,
        binary: ExecutorType::Executable,
        num_executors: 1,
        num_workers: 4, // per executor
    };

    let submission = Submission::connect(&addr).expect("failed to connect to coordinator");
    let id = submission.query(query).expect("failed to spawn query");
    
    println!("spawend query: {:?}", id);
}
