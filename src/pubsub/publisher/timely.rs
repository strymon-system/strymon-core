impl<T, D> TimelyPublisher<T, D>
    where: T: Abomonation + Any + Clone + NonStatic,
           D: Abomonation + Any + Clone + NonStatic
{
    pub fn new(network: &Network) -> Result<((String, u16), Self)> {
        let server = PublisherServer::new(network)?;
        let addr = {
            let (host, port) = server.external_addr();
            (host.to_string(), port)
        };

        Ok((addr, StreamPublisher {
            server: PollServer::from(server),
            subscribers: BTreeMap::new(),
            frontier: CountMap<
            marker: PhantomData,
        }))
    }
}
