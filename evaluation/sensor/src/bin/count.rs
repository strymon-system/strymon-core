extern crate timely;
extern crate timely_query;

use timely::dataflow::Scope;
use timely::dataflow::operators::{Inspect, Accumulate, UnorderedInput};

use timely_query::publish::Partition;
use timely::dataflow::channels::message::Content;

type SensorData = (f32, i32, i32, i32, f32);

fn main() {
    timely_query::execute(|root, coord| {
        let (mut input, mut cap) = root.scoped::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_unordered_input();

            let count = stream.inspect_batch(|t, xs| {
                                  for x in xs {
                                      println!("sensor data @ {:?}: {:?}", t, x);
                                  }
                               })
                              .count();

            coord.publish("count", &count, Partition::Merge).unwrap();

            input
        });

        let topic = coord.subscribe::<_, SensorData>("sensor", cap).unwrap();
        for (time, data) in topic {
            input.session(time).give_content(&mut Content::Typed(data));
            root.step();
        }
    }).unwrap();
}
