extern crate libc;
extern crate walkdir;
#[macro_use] extern crate abomonation;
#[macro_use] extern crate sessionize;
#[macro_use] extern crate log;
extern crate logparse;
extern crate timely;

use abomonation::Abomonation;
use sessionize::sessionize::{SessionizableMessage};

pub mod reader;
pub mod reconstruction;
pub mod util;
pub mod monitor;

#[derive(Debug, Clone)]
pub struct Message {
   pub session_id: String,
   pub trxnb: String,
   pub msg_tag: String,
   pub ip: String,
   pub time: u64, // microseconds
}

unsafe_abomonate!(Message: session_id, trxnb, msg_tag, ip, time);

impl SessionizableMessage for Message {
    fn time(&self) -> u64 {
        self.time
    }

    fn session(&self) -> &str {
        &self.session_id
    }
}

impl Message {
    pub fn new(session_id: String, trxnb: String, msg_tag: String, ip: String, time: u64) -> Message {
        Message{session_id: session_id, trxnb: trxnb, msg_tag: msg_tag, ip: ip, time: time}
    }
}
