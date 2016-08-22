use abomonation::Abomonation;

use query::{QueryId, QueryConfig};
use messaging::response::Promise;

pub enum Request {
    Submission(Submission, SubmissionPromise),
}

#[derive(Clone)]
pub struct Submission {
    pub config: QueryConfig,
}

pub type SubmissionPromise = Promise<QueryId, SubmissionError>;

#[derive(Clone, Debug)]
pub struct SubmissionError;

unsafe_abomonate!(Submission: config);
unsafe_abomonate!(SubmissionError);
