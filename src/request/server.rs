use std::hash::Hash;
use std::collections::HashMap;
use std::error::Error;

use super::{Request, Complete};

use network::{Encode, Decode};
use network::message::MessageBuf;

type HandlerFn = Fn(MessageBuf) -> Result<(), ServerError>;

pub struct Server {
    handler: HashMap<String, Box<HandlerFn>>,
}

#[derive(Debug)]
pub enum ServerError {
    UnknownMethod,
    DecodeError, // TODO(swicki) DecodeError(Option<Box<Error>>),
}


impl Server {
    fn new() -> Self {
        Server {
            handler: HashMap::new(),
        }
    }
    
    pub fn request<R: Request, F>(&mut self, handler: F)
        where F: Fn(R, Complete<R>) + 'static {

        let method = String::from(R::method());
        let outer = Box::new(move |mut msg: MessageBuf| {
            let request = msg.pop::<R>().map_err(|_| ServerError::DecodeError)?;
            /*let cont = Complete::new(move |res| {
                // TODO token, message type?
                // TODO need to require certain format here
            });*/
            Ok(())
        });

        self.handler.insert(method, outer);
    }
    
    pub fn handle<F: FnOnce(MessageBuf)>(msg: MessageBuf, cont: F) -> Result<(), ServerError> {
        unimplemented!()
    }
}
