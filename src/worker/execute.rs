use std::sync::Arc;

use timely_communication::{Allocator, WorkerGuards};
use timely::{self, Configuration};
use timely::dataflow::scopes::Root;

use worker::catalog::{Coordinator, Catalog};
use executor::executable::ExecutableConfig;

pub fn execute<T, F>(func: F) -> Result<WorkerGuards<T>, String> 
    where T: Send + 'static,
          F: Fn(&mut Root<Allocator>, Catalog) -> T,
          F: Send + Sync + 'static
{
    let exec_conf = ExecutableConfig::from_env().expect("unable to parse config");
    let query = exec_conf.query;
    let query_id = query.id;

    let timely_conf = if query.processes > 1 {
        Configuration::Cluster(query.threads, query.processes, query.hostlist, true)
    } else if query.threads > 1 {
        Configuration::Process(query.threads)
    } else {
        Configuration::Thread
    };

    let host = exec_conf.host;
    let coord = exec_conf.coord;

    timely::execute(timely_conf, move |root| {
        let index = root.index();
        let coord = Coordinator::announce(&*coord, &*host, query_id, root.index())
                        .expect("failed to connect to coordinator");

        func(root, coord.catalog())
    })
}
