/// Demo of 'incremental' session tree construction.  In no way is this intended as a real
/// application but just an exploration of what that term means and its implications.

#[macro_use]
extern crate abomonation;
extern crate timely;
extern crate timely_contrib;
extern crate timely_query;
extern crate sessionize;

use timely::dataflow::Scope;
use abomonation::{Abomonation};
use sessionize::sessionize::SessionizableMessage;
use sessionize::sessionize::Sessionize;
use timely_contrib::operators::InspectTs;
use timely::dataflow::operators::Input;

/// Maximum interval a session can be inactive (specified in terms of epochs)
const GRANULARITY: u64 = 1000;
const EXPIRY_DELAY: u64 = 5000;

#[derive(Debug, Clone)]
struct Message {
   session_id: String, 
   time: u64,
   addr: Vec<i32>,
}

unsafe_abomonate!(Message: session_id, time, addr);

impl SessionizableMessage for Message {
    fn time(&self) -> u64 {
        self.time
    }

    fn session(&self) -> &str {
        &self.session_id
    }
}

impl Message {
    fn new(session_id: String, time: u64, addr: Vec<i32>) -> Message {
        Message{session_id: session_id, time: time, addr: addr}
    }
}

fn main() {
    timely_query::execute(move |computation, coord| {
        let index = computation.index();
        let log_data = vec![
            Message::new("A".into(), 1000, vec![0]),
            Message::new("A".into(), 2100, vec![0, 1]),
            Message::new("B".into(), 2500, vec![0]),
            Message::new("A".into(), 6100, vec![0, 2]),
            Message::new("A".into(), 6890, vec![0, 1, 1]),
            Message::new("B".into(), 12100, vec![1]),
            Message::new("B".into(), 13500, vec![2]),
        ];

        let mut input = computation.scoped(move |scope| {
            let (input, stream) = scope.new_input::<Message>();
            let output = stream.sessionize(GRANULARITY, EXPIRY_DELAY);
            
            coord.publish("sessionalize", &output).unwrap();
            input
        });

        // The choice of epoch here plays an important role: if we use per-second granularity we
        // cannot hope to see all tree fragments within a shorter interval.  We could use
        // fine-grained epochs but this depends on what the application demands and would also
        // generate more progress traffic.
        let mut last_epoch = 0;
        for msg in log_data {
            let epoch = msg.time/GRANULARITY;
            if epoch != last_epoch {
                assert!(epoch > last_epoch);
                input.advance_to(epoch);
                last_epoch = epoch;
            }
            input.send(msg);
        }
        input.close();
    }).unwrap();
}
