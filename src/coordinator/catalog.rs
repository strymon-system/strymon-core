use std::collections::btree_map::{BTreeMap, Values};
use std::collections::hash_map::{HashMap, Entry as HashEntry};
use std::collections::btree_set::BTreeSet;

use super::util::Generator;

use model::*;
use coordinator::requests::*;

pub struct Catalog {
    topic_id: Generator<TopicId>,
    directory: HashMap<String, TopicId>,

    // TODO wrap these btreemaps into indexed collections
    topics: BTreeMap<TopicId, Topic>,
    executors: BTreeMap<ExecutorId, Executor>,
    queries: BTreeMap<QueryId, Query>,
    publications: BTreeSet<Publication>,
    subscriptions: BTreeSet<Subscription>, // TODO(swicki): this should be a mulitset
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            topic_id: Generator::new(),
            directory: HashMap::new(),
            topics: BTreeMap::new(),
            executors: BTreeMap::new(),
            queries: BTreeMap::new(),
            publications: BTreeSet::new(),
            subscriptions: BTreeSet::new(),
        }
    }

    pub fn add_executor(&mut self, executor: Executor) {
        self.executors.insert(executor.id, executor);
    }

    pub fn remove_executor(&mut self, id: ExecutorId) {
        self.executors.remove(&id);
    }

    pub fn executors<'a>(&'a self) -> Executors<'a> {
        Executors { values: self.executors.values() }
    }

    pub fn publish(&mut self,
                   query: QueryId,
                   name: String,
                   addr: (String, u16),
                   kind: TopicType)
                   -> Result<Topic, PublishError> {

        // TODO(swicki): Check if query actually exists
        match self.directory.entry(name.clone()) {
            HashEntry::Occupied(_) => Err(PublishError::TopicAlreadyExists),
            HashEntry::Vacant(directory) => {
                let id = self.topic_id.generate();
                let publication = Publication(query, id);
                let topic = Topic {
                    id: id,
                    name: name,
                    addr: addr,
                    kind: kind,
                };

                self.topics.insert(id, topic.clone());
                self.publications.insert(publication);
                directory.insert(id);
                
                Ok(topic)
            }
        }
    }
    
    pub fn unpublish(&mut self, query_id: QueryId, topic: TopicId) -> Result<(), UnpublishError> {
        if self.publications.remove(&Publication(query_id, topic)) {
            Ok(())
        } else {
            Err(UnpublishError::InvalidTopicId)
        }
    }

    pub fn lookup(&mut self, name: &str) -> Option<Topic> {
        if let Some(id) = self.directory.get(name) {
            self.topics.get(id).cloned()
        } else {
            None
        }
    }

    pub fn subscribe(&mut self, query_id: QueryId, topic: TopicId) {
        // TODO(swicki): Check if topic and query acutally exist
        self.subscriptions.insert(Subscription(query_id, topic));
    }

    pub fn unsubscribe(&mut self, query_id: QueryId, topic: TopicId) -> Result<(), UnsubscribeError> {
        if self.subscriptions.remove(&Subscription(query_id, topic)) {
            Ok(())
        } else {
            Err(UnsubscribeError::InvalidTopicId)
        }
    }
}

pub struct Executors<'a> {
    values: Values<'a, ExecutorId, Executor>,
}

impl<'a> Iterator for Executors<'a> {
    type Item = &'a Executor;

    fn next(&mut self) -> Option<Self::Item> {
        self.values.next()
    }
}
