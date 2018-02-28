extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate strymon_communication;
extern crate futures;

use std::io;
use std::thread;

use futures::prelude::*;

use strymon_communication::Network;
use strymon_communication::rpc::*;

// A service with the methods `Subtract` and `Divide`.
#[derive(Clone, Copy)]
pub enum CalcRPC {
   Subtract,
   Divide,
}

#[derive(Serialize, Deserialize)]
pub struct SubtractArgs {
    pub minuend: i32,
    pub subtrahend: i32,
}

impl Request<CalcRPC> for SubtractArgs {
    const NAME: CalcRPC = CalcRPC::Subtract;
    type Success = i32; // difference
    type Error = ();    // cannot fail
}

#[derive(Serialize, Deserialize)]
pub struct DivideArgs {
    pub dividend: i32,
    pub divisor: i32,
}

// TODO(swicki): because of https://github.com/3Hren/msgpack-rust/issues/159,
// we currently cannot use a unit struct here.
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DivideError {
    DivisionByZero
}

impl Request<CalcRPC> for DivideArgs {
    const NAME: CalcRPC = CalcRPC::Divide;
    type Success = i32; // quotient
    type Error = DivideError;
}

impl Name for CalcRPC {
    type Discriminant = u8;

    fn discriminant(&self) -> Self::Discriminant {
        match *self {
            CalcRPC::Subtract => 1,
            CalcRPC::Divide => 2,
        }
    }

    fn from_discriminant(value: &Self::Discriminant) -> Option<Self> {
        match *value {
            1 => Some(CalcRPC::Subtract),
            2 => Some(CalcRPC::Divide),
            _ => None,
        }
    }
}

fn serve_client(incoming: Incoming<CalcRPC>) -> io::Result<()> {
    for request in incoming.wait() {
        let request = request?;
        match request.name() {
            &CalcRPC::Subtract => {
                let (args, resp) = request.decode::<SubtractArgs>()?;
                let result = Ok(args.minuend - args.subtrahend);
                resp.respond(result)
            },
            &CalcRPC::Divide => {
                let (args, resp) = request.decode::<DivideArgs>()?;
                let result = if args.divisor == 0 {
                    Err(DivideError::DivisionByZero)
                } else {
                    Ok(args.dividend / args.divisor)
                };
                resp.respond(result)
            }
        };
    }

    Ok(())
}

/// Spawns a server in a new thread and returns its local port
fn calc_server(network: &Network) -> io::Result<u16> {
    let server = network.server::<CalcRPC, _>(None)?;
    let (_, port) = server.external_addr();
    thread::spawn(|| {
        let mut clients = server.wait();
        // this server is acting as a receiver only
        while let Some(Ok((_, rx))) = clients.next() {
            thread::spawn(move || {
                serve_client(rx).expect("failed to serve client")
            });
        }
    });

    Ok(port)
}

fn run_client() -> io::Result<()> {
    let network = Network::new(String::from("localhost"))?;
    let port = calc_server(&network)?;

    // this client only acts as a sender
    let (tx, _) = network.client::<CalcRPC, _>(("localhost", port))?;

    let ten_minus_one = tx.request(&SubtractArgs {
        minuend: 10,
        subtrahend: 1,
    }).wait_unwrap();
    assert_eq!(ten_minus_one, Ok(9));

    let twenty_divided_by_five = tx.request(&DivideArgs {
        dividend: 20,
        divisor: 5,
    }).wait_unwrap();
    assert_eq!(twenty_divided_by_five, Ok(4));

    let division_by_zero = tx.request(&DivideArgs {
        dividend: 100,
        divisor: 0,
    }).wait_unwrap();
    assert_eq!(division_by_zero, Err(DivideError::DivisionByZero));

    Ok(())
}

#[test]
fn rpc_calculator() {
    run_client().expect("I/O failure")
}
