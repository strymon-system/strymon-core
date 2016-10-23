use std::any::Any;
use std::intrinsics::type_name;

use abomonation::Abomonation;

use super::*;

impl Abomonation for TopicSchema {
    #[inline]
    unsafe fn embalm(&mut self) {
        match *self {
            TopicSchema::Item(ref mut d) => d.embalm(),
            TopicSchema::Collection(ref mut d) => d.embalm(),
            TopicSchema::Timely(ref mut t, ref mut d) => {
                t.embalm();
                d.embalm();
            }
        }
    }
    #[inline]
    unsafe fn entomb(&self, bytes: &mut Vec<u8>) {
        match *self {
            TopicSchema::Item(ref d) => d.entomb(bytes),
            TopicSchema::Collection(ref d) => d.entomb(bytes),
            TopicSchema::Timely(ref t, ref d) => {
                t.entomb(bytes);
                d.entomb(bytes);
            }
        }
    }
    #[inline]
    unsafe fn exhume<'a, 'b>(&'a mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        match *self {
            TopicSchema::Item(ref mut d) => d.exhume(bytes),
            TopicSchema::Collection(ref mut d) => d.exhume(bytes),
            TopicSchema::Timely(ref mut t, ref mut d) => {
                Some(bytes)
                    .and_then(|bytes| t.exhume(bytes))
                    .and_then(|bytes| d.exhume(bytes))
            }
        }
    }
}

unsafe_abomonate!(TopicId);
unsafe_abomonate!(TopicType: name);
unsafe_abomonate!(Topic: id, name, addr, schema);

impl TopicType {
    pub fn of<T: Any>() -> Self {
        TopicType {
            id: TypeId::of::<T>(),
            name: unsafe { type_name::<T>() }.to_string(),
        }
    }
}

unsafe_abomonate!(QueryId);
unsafe_abomonate!(Query: id, name, program, workers, executors);
unsafe_abomonate!(QueryProgram: format, source, args);

unsafe_abomonate!(ExecutorId);
unsafe_abomonate!(Executor: id, host, format);
unsafe_abomonate!(ExecutionFormat);

impl From<u64> for TopicId {
    fn from(id: u64) -> TopicId {
        TopicId(id)
    }
}

impl From<u64> for QueryId {
    fn from(id: u64) -> QueryId {
        QueryId(id)
    }
}

impl From<u64> for ExecutorId {
    fn from(id: u64) -> ExecutorId {
        ExecutorId(id)
    }
}

unsafe_abomonate!(Publication);
unsafe_abomonate!(Subscription);
