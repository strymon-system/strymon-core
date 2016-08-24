use std::io::{Result, Error};

use messaging::{Frame, Decode};

enum Inner<T> {
    Frame(Frame),
    Error(Error),
    Decoded(T),
}

#[must_use]
pub struct Decoder<T = ()> {
    inner: Inner<T>,
}

impl<T> From<Result<Frame>> for Decoder<T> {
    fn from(result: Result<Frame>) -> Decoder<T> {
        let inner = match result {
            Ok(frame) => Inner::Frame(frame),
            Err(err) => Inner::Error(err),
        };

        Decoder {
            inner: inner,
        }
    }
}

impl<T> Decoder<T> {
    pub fn done(t: T) -> Self {
        Decoder {
            inner: Inner::Decoded(t),
        }
    }

    pub fn when<D: Decode, F>(self, f: F) -> Self
        where F: FnOnce(D) -> T {

        let valid = if let Inner::Frame(ref frame) = self.inner {
            frame.is::<D>()
        } else {
            false
        };

        if valid {
            if let Inner::Frame(frame) = self.inner {
                let decoded = frame.decode::<D>().expect("decoder failed unexpectedly");
                Decoder::done(f(decoded))
            } else {
                unreachable!()
            }
        } else {
            self
        }

    }
    
    pub fn forward<F>(self, f: F) -> Self
        where F: FnOnce(Frame) -> Decoder<T>
    {
        if let Inner::Frame(frame) = self.inner {
            f(frame)
        } else {
            self
        }
    }
    
    pub fn when_err<F>(self, f: F) -> Self
        where F: FnOnce(Error) -> T {
        if let Inner::Error(err) = self.inner {
            Decoder::done(f(err))
        } else {
            self
        }
    }

    pub fn expect(self, message: &str) -> T {
        match self.inner {
            Inner::Frame(_) => panic!("{:?}", message),
            Inner::Error(err) => panic!("{:?}: {:?}", message, err),
            Inner::Decoded(t) => t
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
        let s1 = "Hello, World".to_string();
        let msg = Frame::encode(s1.clone());

        let s2 = Decoder::from(Ok(msg))
            .when::<String, _>(|s| s)
            .when_err(|err| panic!("unexpected error"))
            .expect("failed to decode string");
        assert_eq!(s1, s2);

        let err = Decoder::<Error>::from(Err(Error::new(ErrorKind::Other, "okay")))
            .when_err(|e| e)
            .expect("failed to handle error");
        assert_eq!(err.kind(), ErrorKind::Other);
    }
}
