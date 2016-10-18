use rand;

use async::promise::{promise, Complete, Promise};
use network::reqresp::Outgoing;

use model::{QueryId, Query};
use coordinator::requests::*;

pub struct QueryState {
    token: QueryToken,
}


impl QueryState {
    pub fn new() -> Self {
        QueryState {
            token: QueryToken(rand::random::<u64>())
        }
    }
    
    pub fn token(&self) -> QueryToken {
        self.token
    }
}
