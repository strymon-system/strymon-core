use super::SubmissionPromise;
use query::{QueryId, QueryConfig};

pub struct Pending {
    id: QueryId,
    config: QueryConfig,

    submission: SubmissionPromise,
}

impl Pending {
    pub fn new(id: QueryId, config: QueryConfig, promise: SubmissionPromise) -> Self {
        Pending {
            id: id,
            config: config,
            submission: promise,
        }
    }
}
