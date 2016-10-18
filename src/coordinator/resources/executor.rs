use network::reqresp::*;
use executor::requests::*;

pub struct ExecutorState {
    tx: Outgoing,
    port_min: u16,
    port_max: u16,
}

impl ExecutorState {
    pub fn new(tx: Outgoing, ports: (u16, u16)) -> Self {
        ExecutorState {
            tx: tx,
            port_min: ports.0,
            port_max: ports.1,
        }
    }

    pub fn has_ports(&self) -> bool {
        true
    }

    pub fn allocate_port(&mut self) -> u16 {
        self.port_min
    }

    pub fn spawn(&self, req: &SpawnQuery) -> Response<SpawnQuery> {
        debug!("issue spawn request for {:?}", req.query.id);
        self.tx.request(req)
    }
}
