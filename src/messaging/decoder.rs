use std::io::{Error, Result};

use messaging::{Frame, Decode};

#[must_use]
pub struct Decoder {
    inner: Option<Result<Frame>>,
}

impl From<Result<Frame>> for Decoder {
    fn from(result: Result<Frame>) -> Decoder {
        Decoder {
            inner: Some(result)
        }
    }
}

impl Decoder {
    pub fn done() -> Self {
        Decoder { inner: None }
    }

    pub fn when<T: Decode, F>(mut self, f: F) -> Self
        where F: FnOnce(T) {

        let can_decode = match self.inner {
            Some(Ok(ref frame)) if frame.is::<T>() => true,
            _ => false
        };
        
        if can_decode {
            let frame = self.inner.unwrap().unwrap();
            f(frame.decode::<T>().expect("unexpected decoder error"));

            Decoder::done()
        } else {
            self
        }
    }
    
    pub fn when_err<F>(self, f: F) -> Self
        where F: FnOnce(Error) {
        if let Some(Err(err)) = self.inner {
            f(err);
            Decoder::done()
        } else {
            self
        }
    }
    
    pub fn forward<F>(self, f: F) -> Self
        where F: FnOnce(Result<Frame>) -> Self {
        
        if let Some(res) = self.inner {
            f(res)
        } else {
            self
        }
    }

    pub fn else_panic(self, message: &str) {
        match self.inner {
            None => (),
            Some(Ok(_)) => panic!("{:?}", message),
            Some(Err(err)) => panic!("{:?}: {:?}", message, err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error, ErrorKind};
    use messaging::Frame;

    #[test]
    fn test_bytes_message() {
        let s = "Hello, World".to_string();
        let msg = Frame::encode(s.clone());
        
        Decoder::from(Ok(msg))
            .when::<String, _>(|_| ())
            .when_err(|err| panic!("unexpected error"))
            .else_panic("failed to decode string");

        Decoder::from(Err(Error::new(ErrorKind::Other, "okay")))
            .when_err(|_| ())
            .else_panic("failed to handle error")
    }
}

/*


Decoder::from(result)
    .when::<Foo, _>(|foo| {
        
    })
    .forward()
*/
