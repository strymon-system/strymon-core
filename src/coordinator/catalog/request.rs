use abomonation::Abomonation;

use topic::{Topic, TopicId, TypeId};

use messaging::request::Request;

#[derive(Clone, Debug)]
pub struct Publish {
    pub name: String,
    pub addr: String,
    pub dtype: TypeId,
}

unsafe_abomonate!(Publish: name, addr);

#[derive(Clone, Debug)]
pub enum PublishError {
    TopicAlreadyExists,
}

unsafe_abomonate!(PublishError);

impl Request for Publish {
    type Success = Topic;
    type Error = PublishError;
}

#[derive(Clone, Debug)]
pub struct Subscribe {
    pub name: String,
    pub blocking: bool,
}

unsafe_abomonate!(Subscribe: name);

#[derive(Clone, Debug)]
pub enum SubscribeError {
    TopicNotFound,
}

unsafe_abomonate!(SubscribeError);

impl Request for Subscribe {
    type Success = Topic;
    type Error = SubscribeError;
}

#[derive(Clone, Debug)]
pub struct Unsubscribe {
    pub topic: TopicId,
}

#[derive(Copy, Clone, Debug)]
pub enum UnsubscribeError {
    InvalidTopicId,
    InvalidQueryId,
}

unsafe_abomonate!(Unsubscribe: topic);
unsafe_abomonate!(UnsubscribeError);

impl Request for Unsubscribe {
    type Success = ();
    type Error = UnsubscribeError;
}

#[derive(Clone, Debug)]
pub struct Unpublish {
    pub topic: TopicId,
}

#[derive(Copy, Clone, Debug)]
pub enum UnpublishError {
    InvalidTopicId,
    InvalidQueryId,
}

unsafe_abomonate!(Unpublish: topic);
unsafe_abomonate!(UnpublishError);

impl Request for Unpublish {
    type Success = ();
    type Error = UnpublishError;
}
