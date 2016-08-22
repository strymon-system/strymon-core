use abomonation::Abomonation;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExecutorId(pub u64);

impl From<u64> for ExecutorId {
    fn from(id: u64) -> ExecutorId {
        ExecutorId(id)
    }
}

#[derive(Copy, Clone, Debug)]
pub enum ExecutorType {
    Executable,
    DynamicSharedObject,
}

unsafe_abomonate!(ExecutorId);
unsafe_abomonate!(ExecutorType);
