use network::{Encode, Decode};

pub mod server;

pub trait Request: Encode + Decode {
    type Success: Encode + Decode;
    type Error: Encode + Decode;
    
    fn method() -> &'static str;
}

pub struct Complete<R: Request> {
    inner: Box<Invoke<Result<R::Success, <R as Request>::Error>, ()>>,
}

impl<R: Request> Complete<R> {
    pub fn new<F>(f: F) -> Self
        where F: FnOnce(Result<R::Success, <R as Request>::Error>),
              F: 'static + Send
    {
        Complete { inner: Box::new(f) }
    }
    
    pub fn resolve(self, res: Result<R::Success, <R as Request>::Error>) {
        self.inner.invoke(res)
    }
    
    pub fn reject(self, err: <R as Request>::Error) {
        self.resolve(Err(err))
    }
    
    pub fn complete(self, succ: R::Success) {
        self.resolve(Ok(succ))
    }
}

// helper trait replacement for FnBox
trait Invoke<A = (), R = ()> {
    fn invoke(self: Box<Self>, arg: A) -> R;
}

impl<A, R, F> Invoke<A, R> for F
    where F: FnOnce(A) -> R
{
    fn invoke(self: Box<F>, arg: A) -> R {
        let f = *self;
        f(arg)
    }
}
