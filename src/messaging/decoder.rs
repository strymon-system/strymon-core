use std::io::{Error, Result};

use messaging::{Message, Transfer};

enum Inner<T> {
    Message(Message),
    Error(Error),
    Decoded(T),
}

#[must_use]
pub struct Decoder<T = ()> {
    inner: Inner<T>,
}

impl<T> From<Result<Message>> for Decoder<T> {
    fn from(result: Result<Message>) -> Decoder<T> {
        let inner = match result {
            Ok(msg) => Inner::Message(msg),
            Err(err) => Inner::Error(err),
        };

        Decoder { inner: inner }
    }
}

impl<T> Decoder<T> {
    pub fn done(t: T) -> Self {
        Decoder { inner: Inner::Decoded(t) }
    }

    pub fn when<D: Transfer, F>(self, f: F) -> Self
        where F: FnOnce(D) -> T
    {

        if let Inner::Message(msg) = self.inner {
            match msg.downcast::<D>() {
                Ok(decoded) => Decoder::done(f(decoded)),
                Err(msg) => Decoder::from(Ok(msg)),
            }
        } else {
            self
        }

    }

    pub fn forward<F>(self, f: F) -> Self
        where F: FnOnce(Result<Message>) -> Decoder<T>
    {
        match self.inner {
            Inner::Message(msg) => f(Ok(msg)),
            Inner::Error(err) => f(Err(err)),
            Inner::Decoded(done) => Decoder::done(done),
        }
    }

    pub fn when_err<F>(self, f: F) -> Self
        where F: FnOnce(Error) -> T
    {
        if let Inner::Error(err) = self.inner {
            Decoder::done(f(err))
        } else {
            self
        }
    }

    pub fn expect(self, message: &str) -> T {
        match self.inner {
            Inner::Message(_) => panic!("{:?}", message),
            Inner::Error(err) => panic!("{:?}: {:?}", message, err),
            Inner::Decoded(t) => t,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error, ErrorKind};
    use messaging::Message;
    use messaging::bytes::Encode;

    #[test]
    fn test_bytes_message() {
        let s1 = "Hello, World".to_string();
        let msg = Message::Bytes(s1.clone().encode());

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
