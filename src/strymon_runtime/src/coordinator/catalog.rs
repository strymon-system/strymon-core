// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::io;
use std::collections::hash_map::{HashMap, Entry as HashEntry};
use std::collections::btree_map::{BTreeMap, Values};
use std::hash::Hash;

use futures::Future;
use tokio_core::reactor::Handle;
use strymon_communication::Network;
use serde::ser::Serialize;

use model::*;
use coordinator::requests::*;

use pubsub::publisher::collection::{CollectionPublisher, Mutator};

use super::util::Generator;

pub struct Catalog {
    generator: Generator<TopicId>,
    directory: HashMap<String, TopicId>,

    topics: MapCollection<TopicId, Topic>,
    executors: MapCollection<ExecutorId, Executor>,
    queries: MapCollection<QueryId, Query>,

    publications: Collection<Publication>,
    subscriptions: Collection<Subscription>,

    keepers: MapCollection<KeeperId, Keeper>,
}

impl Catalog {
    pub fn new(network: &Network, handle: &Handle) -> io::Result<Self> {
        let mut generator = Generator::<TopicId>::new();
        let mut directory = HashMap::<String, TopicId>::new();

        let id = generator.generate();
        let (topic, mut topics) = MapCollection::new(network, handle, id, "$topics")?;
        directory.insert(topic.name.clone(), topic.id);
        topics.insert(topic.id, topic);

        let id = generator.generate();
        let (topic, executors) = MapCollection::new(network, handle, id, "$executors")?;
        directory.insert(topic.name.clone(), topic.id);
        topics.insert(topic.id, topic);

        let id = generator.generate();
        let (topic, queries) = MapCollection::new(network, handle, id, "$queries")?;
        directory.insert(topic.name.clone(), topic.id);
        topics.insert(topic.id, topic);

        let id = generator.generate();
        let (topic, pubs) = Collection::<Publication>::new(network, handle, id, "$publications")?;
        directory.insert(topic.name.clone(), topic.id);
        topics.insert(topic.id, topic);

        let id = generator.generate();
        let (topic, subs) =
            Collection::<Subscription>::new(network, handle, id, "$subscriptions")?;
        directory.insert(topic.name.clone(), topic.id);
        topics.insert(topic.id, topic);

        let id = generator.generate();
        let (topic, keepers) = MapCollection::new(network, handle, id, "$keepers")?;
        directory.insert(topic.name.clone(), topic.id);
        topics.insert(topic.id, topic);

        Ok(Catalog {
               generator: generator,
               directory: directory,
               topics: topics,
               executors: executors,
               queries: queries,
               publications: pubs,
               subscriptions: subs,
               keepers: keepers,
           })
    }

    pub fn add_executor(&mut self, executor: Executor) {
        debug!("add_executor: {:?}", executor);
        self.executors.insert(executor.id, executor);
    }

    pub fn remove_executor(&mut self, id: ExecutorId) {
        debug!("remove_executor: {:?}", id);
        self.executors.remove(&id);
    }

    pub fn executors<'a>(&'a self) -> Executors<'a> {
        Executors { inner: self.executors.values() }
    }

    pub fn add_query(&mut self, query: Query) {
        debug!("add_query: {:?}", query);
        self.queries.insert(query.id, query);
    }

    pub fn remove_query(&mut self, id: QueryId) {
        debug!("remove_query: {:?}", id);
        self.queries.remove(&id);
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

                self.topics.insert(id, topic.clone());
                self.publications.insert(publication);
                entry.insert(id);

                Ok(topic)
            }
        }
    }

    pub fn unpublish(&mut self,
                     query_id: QueryId,
                     topic: TopicId)
                     -> Result<(), UnpublishError> {
        let publication = Publication(query_id, topic);
        debug!("unpublish: {:?}", publication);

        if let Some(name) = self.topics.get(&topic).map(|t| &*t.name) {
            self.directory.remove(name);
        }

        self.topics.remove(&topic);
        self.publications.remove(publication);
        Ok(())
    }

    pub fn lookup(&self, name: &str) -> Option<Topic> {
        if let Some(id) = self.directory.get(name) {
            self.topics.get(&id).cloned()
        } else {
            None
        }
    }

    pub fn subscribe(&mut self, query_id: QueryId, topic: TopicId) {
        let subscription = Subscription(query_id, topic);
        debug!("subscribe: {:?}", subscription);
        self.subscriptions.insert(subscription);
    }

    pub fn unsubscribe(&mut self,
                       query_id: QueryId,
                       topic: TopicId)
                       -> Result<(), UnsubscribeError> {
        let subscription = Subscription(query_id, topic);
        debug!("unsubscribe: {:?}", subscription);
        self.subscriptions.remove(subscription);
        Ok(())
    }

    pub fn add_keeper(&mut self, keeper: Keeper) {
        self.keepers.insert(keeper.id, keeper);
    }

    pub fn add_keeper_worker(&mut self,
                             keeper_id: &KeeperId,
                             worker_num: usize,
                             addr: (String, u16)) -> Result<(), String> {
        let mut keeper = match self.keepers.remove(keeper_id) {
            Some(keeper) => keeper,
            None => return Err("No such Keeper".to_string()),
        };
        keeper.workers.push((worker_num, addr));
        self.keepers.insert(keeper.id, keeper);
        Ok(())
    }

    pub fn remove_keeper(&mut self, id: &KeeperId) -> Option<Keeper> {
        self.keepers.remove(id)
    }
}

struct MapCollection<K, V> {
    inner: BTreeMap<K, V>,
    mutator: Mutator<V>,
}

impl<K: Ord, V: Serialize + Eq + Clone + 'static> MapCollection<K, V> {
    fn new(network: &Network,
           handle: &Handle,
           topic_id: TopicId,
           name: &'static str)
           -> io::Result<(Topic, Self)> {
        let (addr, mutator, publisher) = CollectionPublisher::new(network)?;
        let topic = Topic {
            id: topic_id,
            name: String::from(name),
            addr: addr,
            schema: TopicSchema::Collection(TopicType::of::<V>()),
        };

        handle.spawn(publisher.map_err(|err| {
            error!("failure in catalog publisher: {:?}", err)
        }));

        Ok((topic,
            MapCollection {
                inner: BTreeMap::new(),
                mutator: mutator,
            }))
    }

    fn insert(&mut self, key: K, value: V) {
        self.inner.insert(key, value.clone());
        self.mutator.insert(value);
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(value) = self.inner.remove(key) {
            self.mutator.remove(value.clone());
            return Some(value);
        }
        None
    }

    fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    fn values<'a>(&'a self) -> Values<'a, K, V> {
        self.inner.values()
    }
}

struct Collection<T> {
    inner: HashMap<T, usize>,
    mutator: Mutator<T>,
}

impl<T: Serialize + Clone + Eq + Hash + 'static> Collection<T> {
    fn new(network: &Network,
           handle: &Handle,
           topic_id: TopicId,
           name: &'static str)
           -> io::Result<(Topic, Self)> {
        let (addr, mutator, publisher) = CollectionPublisher::new(network)?;
        let topic = Topic {
            id: topic_id,
            name: String::from(name),
            addr: addr,
            schema: TopicSchema::Collection(TopicType::of::<T>()),
        };

        handle.spawn(publisher.map_err(|err| {
            error!("failure in catalog publisher: {:?}", err)
        }));

        Ok((topic,
            Collection {
                inner: HashMap::new(),
                mutator: mutator,
            }))
    }

    fn insert(&mut self, item: T) {
        self.mutator.insert(item.clone());
        *self.inner.entry(item).or_insert(0) += 1;
    }

    fn remove(&mut self, item: T) {
        if let HashEntry::Occupied(mut entry) = self.inner.entry(item) {
            self.mutator.remove(entry.key().clone());
            if *entry.get() == 1 {
                entry.remove();
            } else {
                *entry.get_mut() -= 1;
            }
        }
    }
}

pub struct Executors<'a> {
    inner: Values<'a, ExecutorId, Executor>,
}

impl<'a> Iterator for Executors<'a> {
    type Item = &'a Executor;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
