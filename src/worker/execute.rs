use timely_communication::{Allocator, WorkerGuards};
use timely;
use timely::dataflow::scopes::Root;

use worker::coordinator::Catalog;

pub fn execute<T, F>(func: F) -> Result<WorkerGuards<T>, String> 
    where T: Send + 'static,
          F: Fn(&mut Root<Allocator>, Catalog) -> T,
          F: Send + Sync + 'static
{
    let conf = unimplemented!();
    timely::execute(conf, move |a| {
        let c = unimplemented!();
        func(a, c)
    })
}
