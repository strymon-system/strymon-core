use std::sync::mpsc;

pub struct Sender<T> {
    tx: Box<Fn(T) -> Result<(), Disconnected> + Send>,
}

#[derive(Debug)]
pub struct Disconnected;

impl<T> Sender<T> {
    pub fn new<S, F>(tx: mpsc::Sender<S>, f: F) -> Self
        where F: Fn(T) -> S,
              F: Send + 'static,
              S: Send + 'static,
    {
        Sender {
            tx: Box::new(move |t: T| {
                tx.send(f(t)).map_err(|_| Disconnected)
            })
        }
    }
    
    pub fn send(&self, t: T) {
        (self.tx)(t).expect("event receiver dropped")
    }
    
    pub fn try_send(&self, t: T) -> Result<(), Disconnected> {
        (self.tx)(t)
    }
}

impl<T, F: Fn(T) + Send + 'static> From<F> for Sender<T> {
    fn from(f: F) -> Self {
        Sender {
            tx: Box::new(move |t: T| Ok(f(t)))
        }
    }
}

fn _assert() {
    fn _is_send<T: Send>() {}
    _is_send::<Sender<()>>();
}
