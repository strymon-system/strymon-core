use rand;

use async::promise::{promise, Complete, Promise};
use network::reqresp::Outgoing;

use model::{QueryId, Query};
use coordinator::requests::*;

pub struct QueryState {
    token: QueryToken,
}


impl QueryState {
    pub fn new(id: QueryId) -> Self {
        QueryState {
            token: QueryToken(id, rand::random::<u64>())
        }
    }
    
    pub fn token(&self) -> QueryToken {
        self.token
    }
}
