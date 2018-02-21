// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate futures;
extern crate timely;

extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate typename;

extern crate strymon_job;
extern crate strymon_communication;

use std::thread;

use futures::Stream;
use timely::dataflow::operators::{Input, Inspect};

use strymon_job::operators::service::Service;
use strymon_communication::rpc::{Name, Request};

// === Service Interface ===
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize, TypeName)]
pub enum PingService {
    Ping,
}

impl Name for PingService {
    type Discriminant = u8;

    fn discriminant(&self) -> Self::Discriminant {
        0
    }

    fn from_discriminant(value: &Self::Discriminant) -> Option<Self> {
        match *value {
            0 => Some(PingService::Ping),
            _ => None,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Ping(String);

impl Request<PingService> for Ping {
    const NAME: PingService = PingService::Ping;
    type Success = String;
    type Error = ();
}
// === End of Interface ===

fn handle_requests(server: Service<PingService>) {
    thread::spawn(move || {
        let blocking = server.wait();
        for req in blocking {
            let req = req.unwrap();
            assert_eq!(req.name(), &PingService::Ping);
            let (Ping(s), resp) = req.decode::<Ping>().unwrap();
            resp.respond(Ok(s));
        }
    });
}

fn main() {
    strymon_job::execute(|root, coord| {
        let mut input = root.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input();
            stream.inspect(|x| println!("got response {:?}", x));
            input
        });

        let server = coord.announce_service("test_ping_service").unwrap();
        let client = coord.bind_service("test_ping_service", false).unwrap();

        handle_requests(server);

        let resp = client.request(&Ping(String::from("Hello, world!")));
        input.send(resp.wait_unwrap());
    }).unwrap();
}
