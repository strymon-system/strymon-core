use model::*;
use network::reqrep::Request;

mod imp;

#[derive(Debug, Clone)]
pub enum Placement {
    Random(usize, usize), // (num executors, num workers)
    Fixed(Vec<ExecutorId>, usize), // (executors, num workers)
}

#[derive(Debug, Clone)]
pub struct Submission {
    pub query: QueryProgram,
    pub name: Option<String>,
    pub placement: Placement,
}

#[derive(Clone, Debug)]
pub enum SubmissionError {
    ExecutorsNotFound,
    SpawnError,
}

impl Request for Submission {
    type Success = QueryId;
    type Error = SubmissionError;

    fn name() -> &'static str {
        "Submission"
    }
}

#[derive(Clone, Debug)]
pub struct AddExecutor {
    pub host: String,
    pub ports: (u16, u16),
    pub format: ExecutionFormat,
}

#[derive(Clone, Debug)]
pub struct ExecutorError;

impl Request for AddExecutor {
    type Success = ExecutorId;
    type Error = ExecutorError;

    fn name() -> &'static str {
        "AddExecutor"
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueryToken {
    pub id: QueryId,
    pub auth: u64,
}

#[derive(Clone, Debug)]
pub struct AddWorkerGroup {
    pub query: QueryId,
    pub group: usize,
}

#[derive(Clone, Debug)]
pub enum WorkerGroupError {
    InvalidWorkerGroup,
    SpawningAborted,
    PeerFailed,
}

impl Request for AddWorkerGroup {
    type Success = QueryToken;
    type Error = WorkerGroupError;

    fn name() -> &'static str {
        "AddWorkerGroup"
    }
}

#[derive(Clone, Debug)]
pub struct Subscribe {
    pub name: String,
    pub blocking: bool,
    pub token: QueryToken,
}

#[derive(Clone, Debug)]
pub enum SubscribeError {
    TopicNotFound,
    AuthenticationFailure,
}

impl Request for Subscribe {
    type Success = Topic;
    type Error = SubscribeError;

    fn name() -> &'static str {
        "Subscribe"
    }
}

#[derive(Clone, Debug)]
pub struct Unsubscribe {
    pub topic: TopicId,
    pub token: QueryToken,
}

#[derive(Clone, Debug)]
pub enum UnsubscribeError {
    InvalidTopicId,
    AuthenticationFailure,
}

impl Request for Unsubscribe {
    type Success = ();
    type Error = UnsubscribeError;
    
    fn name() -> &'static str {
        "Unsubscribe"
    }
}

#[derive(Clone, Debug)]
pub struct Publish {
    pub name: String,
    pub addr: (String, u16),
    pub schema: TopicSchema,
    pub token: QueryToken,
}

#[derive(Clone, Debug)]
pub enum PublishError {
    TopicAlreadyExists,
    AuthenticationFailure,
}

impl Request for Publish {
    type Success = Topic;
    type Error = PublishError;
    
    fn name() -> &'static str {
        "Publish"
    }
}

#[derive(Clone, Debug)]
pub struct Unpublish {
    pub topic: TopicId,
    pub token: QueryToken,
}

#[derive(Clone, Debug)]
pub enum UnpublishError {
    InvalidTopicId,
    AuthenticationFailure,
}

impl Request for Unpublish {
    type Success = ();
    type Error = UnpublishError;
    
    fn name() -> &'static str {
        "Unpublish"
    }
}

#[derive(Clone, Debug)]
pub struct Lookup {
    pub name: String,
}

impl Request for Lookup {
    type Success = Topic;
    type Error = ();

    fn name() -> &'static str {
        "Lookup"
    }
}
