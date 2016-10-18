use coordinator::requests::*;
use network::reqresp::Outgoing;

#[derive(Clone)]
pub struct Coordinator {
    token: QueryToken,
    tx: Outgoing,
}

pub fn new(tx: Outgoing, token: QueryToken) -> Coordinator {
    Coordinator {
        tx: tx,
        token: token,
    }
}

impl Coordinator {

}


