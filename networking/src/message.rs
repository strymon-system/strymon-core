use std::sync::Arc;
use std::collections::VecDeque;
use std::mem;

use byteorder::{ByteOrder, NetworkEndian};

pub trait Encode {
    type Error;

    fn encode(&self, &mut Vec<u8>) -> Result<(), Self::Error>;
}

pub trait Decode: Sized {
    type Error;

    fn decode(&mut [u8]) -> Result<Self, Self::Error>;
}

#[derive(Clone, Default)]
pub struct MessageBuf {
    buf: Arc<Buf>,
}

impl MessageBuf {
    pub fn empty() -> Self {
        Default::default()
    }
        
    pub fn push<T: Encode>(&mut self, payload: &T) -> Result<(), T::Error> {
        Arc::make_mut(&mut self.buf).push(payload)
    }
    
    pub fn pop<T: Decode>(&mut self) -> Result<T, T::Error> {
        Arc::make_mut(&mut self.buf).pop()
    }
}


#[derive(Clone, Default)]
struct Buf {
    buf: Vec<u8>,
    pos: VecDeque<(u32, u32)>, // (start, end)
}

impl Buf {
    pub fn push<T: Encode>(&mut self, payload: &T) -> Result<(), T::Error> {
        let align = mem::align_of::<T>();
        let start = self.buf.len();
        // we assume the alignment is always a power of two
        let aligned = (start + align - 1) & !(align - 1);
        self.buf.resize(aligned, 0);

        match payload.encode(&mut self.buf) {
            Ok(()) => {
                self.pos.push_back((aligned as u32, self.buf.len() as u32));
                Ok(())
            }
            Err(err) => {
                self.buf.truncate(start);
                Err(err)
            }
        }
    }
    
    pub fn pop<T: Decode>(&mut self) -> Result<T, T::Error> {
        let (start, end) = self.pos.pop_front().expect("cannot pop from empty buffer");
        match T::decode(&mut self.buf[start as usize..end as usize]) {
            Ok(t) => Ok(t),
            Err(err) => {
                // put offset back            
                self.pos.push_front((start, end));
                Err(err)
            }
        }
     
    }
}

impl<T: Encode<Error=::void::Void>> From<T> for MessageBuf {
    fn from(t: T) -> Self {
        let mut buf = MessageBuf::empty();
        buf.push(&t).unwrap();
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use abomonate::Vault;

    #[test]
    fn pop_msg() {
        let mut buf = MessageBuf::from(Vault(&vec!["foo", "bar"]));
        let Vault(vec) = buf.pop::<Vault<Vec<&'static str>>>().unwrap();
        assert_eq!(vec, vec!["foo", "bar"]);
    }
    
    #[test]
    fn push_many_msg() {
        let string = String::from("hi");
        let vector = vec![1u8,2,3];
        let integer = 42i32;

        let mut buf = MessageBuf::empty();
        buf.push(&Vault(&string));

        assert_eq!(string, buf.pop::<Vault<String>>().unwrap().0);

        buf.push(&Vault(&vector));
        buf.push(&Vault(&integer));
        assert_eq!(vector, buf.pop::<Vault<Vec<u8>>>().unwrap().0);
        assert_eq!(integer, buf.pop::<Vault<i32>>().unwrap().0);
    }

    #[test]
    #[should_panic]
    fn pop_empty() {
        let mut buf = MessageBuf::empty();
        buf.pop::<Vault<i32>>().unwrap();
    }
    
    #[test]
    fn type_mismatch() {
        use ::abomonate::DecodeError::*;

        let mut buf = MessageBuf::from(Vault(&0i32));
        assert_eq!(TypeMismatch, buf.pop::<Vault<String>>().unwrap_err());
    }
}
