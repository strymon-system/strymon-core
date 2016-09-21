use std::io::Result;
use std::net::{TcpStream, TcpListener};

pub struct Service {
    external: String,
    port: u16,
    listener: TcpListener,
}

impl Service {
    pub fn init(external: Option<String>, port: Option<u16>) -> Result<Self> {
        let external = external.unwrap_or(String::from("localhost"));
        let sockaddr = format!("[::]:{}", port.unwrap_or(0));
        let listener = try!(TcpListener::bind(&*sockaddr));
        let port = try!(listener.local_addr()).port();
        Ok(Service {
            external: external,
            port: port,
            listener: listener,
            queue: 0,
        })
    }

    pub fn listen
}

service.listen("executors", |
