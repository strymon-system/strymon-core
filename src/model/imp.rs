use std::fmt;
use std::any::Any;
use std::intrinsics::type_name;

use abomonation::Abomonation;

use super::*;

impl Abomonation for TopicSchema {
    #[inline]
    unsafe fn embalm(&mut self) {
        match *self {
            TopicSchema::Collection(ref mut d) => d.embalm(),
            TopicSchema::Stream(ref mut t, ref mut d) => {
                t.embalm();
                d.embalm();
            }
        }
    }
    #[inline]
    unsafe fn entomb(&self, bytes: &mut Vec<u8>) {
        match *self {
            TopicSchema::Collection(ref d) => d.entomb(bytes),
            TopicSchema::Stream(ref t, ref d) => {
                t.entomb(bytes);
                d.entomb(bytes);
            }
        }
    }
    #[inline]
    unsafe fn exhume<'a, 'b>(&'a mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        match *self {
            TopicSchema::Collection(ref mut d) => d.exhume(bytes),
            TopicSchema::Stream(ref mut t, ref mut d) => {
                Some(bytes)
                    .and_then(|bytes| t.exhume(bytes))
                    .and_then(|bytes| d.exhume(bytes))
            }
        }
    }
}

impl fmt::Display for TopicSchema {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TopicSchema::Collection(ref d) => write!(f, "Collection(item={:?})", d.name),
            TopicSchema::Stream(ref d, ref t) => {
                write!(f, "Stream(timestamp={:?}, data={:?})", t.name, d.name)
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

impl TopicSchema {
    pub fn is_collection<D: Any>(&self) -> bool {
        match *self {
            TopicSchema::Collection(ref d) => d.id == TypeId::of::<D>(),
            _ => false,
        }
    }

    pub fn is_stream<T: Any, D: Any>(&self) -> bool {
        match *self {
            TopicSchema::Stream(ref d, ref t) => {
                d.id == TypeId::of::<D>() && t.id == TypeId::of::<T>()
            }
            _ => false,
        }
    }
}
