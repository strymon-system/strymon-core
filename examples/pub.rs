extern crate timely;
extern crate timely_query;
extern crate env_logger;

use std::time::Duration;
use std::thread;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;


fn main() {
    drop(env_logger::init());

    timely_query::execute(|root, catalog| {
        use std::{thread, time};
        use std::any::TypeId;
    
        let topic = if root.index() == 0 {
            catalog.publish("foo".to_string(), "nope".to_string(), TypeId::of::<()>())
                .await()
                .expect("failed to publish topic")
        } else {
            catalog.subscribe("foo".to_string(), true)
                .await()
                .expect("failed to subscribe")
        };
        
        println!("worker {:?}, topic: {:?}", root.index(), topic)
    }).unwrap();
}
