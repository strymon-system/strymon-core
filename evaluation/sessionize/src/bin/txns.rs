extern crate time;
extern crate timely;
extern crate timely_query;

#[macro_use] extern crate sessionize;
extern crate sessionize_shared;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;
use timely_query::publish::Topics;

use sessionize::sessionize::*;
use sessionize_shared::Message;
use sessionize_shared::util::convert_trxnb;

fn main() {
    let start = time::precise_time_ns();

    timely_query::execute(|computation, coord| {
        let worker_index = computation.index();
    
        type Msg = MessagesForSession<Message>;
    
        let (mut input, cap) = computation.scoped::<u64, _, _>(|scope| {
            let (input, sessionize) = scope.new_unordered_input::<Msg>();

            // Intermediate Query: Converted Transaction Ids
            let txns_for_each_session_in_message = sessionize.map(|messages_for_session : MessagesForSession<Message>| messages_for_session.messages.iter()
                                                            .map(|message| convert_trxnb(&message.trxnb)).collect::<Vec<_>>());
            coord.publish("transactions", &txns_for_each_session_in_message, Topics::PerWorker).unwrap();

            input
        });

        let name = format!("sessionize.{}", worker_index);
        let messages = coord.subscribe::<_, Msg>(name, cap).unwrap();
        for (time, data) in messages {
            input.session(time).give_iterator(data.into_iter());
            computation.step();
        }
    }).unwrap();

    let end = time::precise_time_ns();
    println!("txns,{},{}", start, end);
}
