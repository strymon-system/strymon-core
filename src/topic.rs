pub use std::any::TypeId;

use abomonation::Abomonation;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicId(pub u64);

impl From<u64> for TopicId {
    fn from(id: u64) -> TopicId {
        TopicId(id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Topic {
    pub id: TopicId,
    pub name: String,
    pub addr: String,
    pub dtype: TypeId,
}

unsafe_abomonate!(TopicId);
unsafe_abomonate!(Topic: id, name, addr);
