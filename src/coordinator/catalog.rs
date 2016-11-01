use std::any::Any;
use std::io::Result as IoResult;
use std::collections::hash_map::{HashMap, Entry as HashEntry};
use std::hash::Hash;

use abomonation::Abomonation;

use model::*;
use coordinator::requests::*;

use network::Network;
use network::message::abomonate::{Abomonate, NonStatic};
use pubsub::publisher::collection::CollectionPublisher;
pub use pubsub::publisher::collection::Iter;

use super::util::Generator;

pub struct Catalog {
    generator: Generator<TopicId>,
    directory: HashMap<String, TopicId>,

    topics: Collection<Topic>,
    executors: Collection<Executor>,
    queries: Collection<Query>,
    publications: Collection<Publication>,
    subscriptions: Collection<Subscription>,
}

impl Catalog {
    pub fn new(network: &Network) -> IoResult<Self> {
        let mut generator = Generator::<TopicId>::new();
        let mut directory = HashMap::<String, TopicId>::new();

        let id = generator.generate();
        let (topic, mut topics) = Collection::<Topic>::new(network, id, "$topics")?;
        directory.insert(topic.name.clone(), topic.id);
        topics.insert(topic);

        let id = generator.generate();
        let (topic, executors) = Collection::<Executor>::new(network, id, "$executors")?;
        directory.insert(topic.name.clone(), topic.id);
        topics.insert(topic);

        let id = generator.generate();
        let (topic, queries) = Collection::<Query>::new(network, id, "$queries")?;
        directory.insert(topic.name.clone(), topic.id);
        topics.insert(topic);

        let id = generator.generate();
        let (topic, pubs) = Collection::<Publication>::new(network, id, "$publications")?;
        directory.insert(topic.name.clone(), topic.id);
        topics.insert(topic);

        let id = generator.generate();
        let (topic, subs) = Collection::<Subscription>::new(network, id, "$subscription")?;
        directory.insert(topic.name.clone(), topic.id);
        topics.insert(topic);

        Ok(Catalog {
            generator: generator,
            directory: directory,
            topics: topics,
            executors: executors,
            queries: queries,
            publications: pubs,
            subscriptions: subs,
        })
    }

    pub fn add_executor(&mut self, executor: Executor) {
        debug!("add_executor: {:?}", executor);
        self.executors.insert(executor);
    }

    pub fn remove_executor(&mut self, id: ExecutorId) {
        debug!("remove_executor: {:?}", id);
        // TODO(swicki): this is unnecessarily slow
        let executor = self.executors.iter().find(|e| e.id == id).cloned();
        if let Some(executor) = executor {
            self.executors.remove(executor)
        } else {
            warn!("tried to remove inexisting executor: {:?}", id)
        }
    }

    pub fn executors<'a>(&'a self) -> Executors<'a> {
        Executors { inner: self.executors.iter() }
    }

    pub fn add_query(&mut self, query: Query) {
        debug!("add_query: {:?}", query);
        self.queries.insert(query);
    }

    pub fn remove_query(&mut self, id: QueryId) {
        debug!("remove_query: {:?}", id);
        // TODO(swicki): this is unnecessarily slow
        let query = self.queries.iter().find(|q| q.id == id).cloned();
        if let Some(query) = query {
            self.queries.remove(query)
        } else {
            warn!("tried to remove inexisting query: {:?}", id)
        }
    }

    pub fn publish(&mut self,
                   query: QueryId,
                   name: String,
                   addr: (String, u16),
                   schema: TopicSchema)
                   -> Result<Topic, PublishError> {
        // TODO(swicki): Check if query actually exists
        match self.directory.entry(name.clone()) {
            HashEntry::Occupied(_) => Err(PublishError::TopicAlreadyExists),
            HashEntry::Vacant(entry) => {
                let id = self.generator.generate();
                let publication = Publication(query, id);
                let topic = Topic {
                    id: id,
                    name: name,
                    addr: addr,
                    schema: schema,
                };

                debug!("publish: {:?}", publication);

                self.topics.insert(topic.clone());
                self.publications.insert(publication);
                entry.insert(id);

                Ok(topic)
            }
        }
    }
    
    pub fn unpublish(&mut self, query_id: QueryId, topic: TopicId) -> Result<(), UnpublishError> {
        // TODO(swicki): Check if topic and query actually exist
        let publication = Publication(query_id, topic);
        debug!("unpublish: {:?}", publication);
        self.publications.remove(publication);
        Ok(())
    }

    pub fn lookup(&mut self, name: &str) -> Option<Topic> {
        if let Some(id) = self.directory.get(name) {
            // TODO(swicki): The point of the directory was to avoid linear search
            self.topics.iter().find(|topic| topic.id == *id).cloned()
        } else {
            None
        }
    }

    pub fn subscribe(&mut self, query_id: QueryId, topic: TopicId) {
        // TODO(swicki): Check if topic and query actually exist
        let subscription = Subscription(query_id, topic);
        debug!("subscribe: {:?}", subscription);
        self.subscriptions.insert(subscription);
    }

    pub fn unsubscribe(&mut self, query_id: QueryId, topic: TopicId) -> Result<(), UnsubscribeError> {
        // TODO(swicki): Check if topic and query actually exist
        let subscription = Subscription(query_id, topic);
        debug!("unsubscribe: {:?}", subscription);
        self.subscriptions.remove(subscription);
        Ok(())
    }
}

struct Collection<T> {
    publisher: CollectionPublisher<T>,
}

impl<T: Abomonation + Any + Clone + Eq + NonStatic> Collection<T> {
    fn new(network: &Network, id: TopicId, name: &str) -> IoResult<(Topic, Self)> {
        let (addr, publisher) = CollectionPublisher::<T>::new(network)?;

        let elem = TopicType::of::<T>();
        let schema = TopicSchema::Collection(elem);
        let topic = Topic {
            id: id,
            name: String::from(name),
            addr: addr,
            schema: schema,
        };

        Ok((topic, Collection { publisher: publisher }))
    }

    fn insert(&mut self, elem: T) {
        if let Err(err) = self.publisher.publish(&vec![(elem, 1)]) {
            error!("I/O failure in catalog: {:?}", err)
        }
    }

    fn remove(&mut self, elem: T) {
        if let Err(err) = self.publisher.publish(&vec![(elem, -1)]) {
            error!("I/O failure in catalog: {:?}", err)
        }
    }

    fn iter(&self) -> Iter<T> {
        self.publisher.into_iter()
    }
}

pub struct Executors<'a> {
    inner: Iter<'a, Executor>,
}

impl<'a> Iterator for Executors<'a> {
    type Item = &'a Executor;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
