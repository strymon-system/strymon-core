use std::collections::BTreeSet;
use std::collections::hash_map::{Entry as HashEntry, HashMap};
use std::collections::btree_map::{BTreeMap, Entry as BTreeEntry};

use query::QueryId;
use model::{Topic as TopicDesc, TopicId, TopicType};

use messaging::request::handler::Handoff;

use util::Generator;

use coordinator::catalog::TopicRequest;
use coordinator::catalog::request::*;

pub struct Topics {
    topic_id: Generator<TopicId>,
    directory: HashMap<String, TopicId>,
    topics: BTreeMap<TopicId, Topic>,
    blocked: HashMap<String, Vec<Handoff<Subscribe>>>,
}

pub struct Topic {
    desc: TopicDesc,
    publisher: QueryId,
    subscriber: BTreeSet<QueryId>,
}

impl Topics {
    pub fn new() -> Self {
        Topics {
            topic_id: Generator::new(),
            directory: HashMap::new(),
            topics: BTreeMap::new(),
            blocked: HashMap::new(),
        }
    }

    pub fn publish(&mut self,
                   query: QueryId,
                   name: String,
                   addr: (String, u16),
                   kind: TopicType)
                   -> Result<TopicDesc, PublishError> {
        match self.directory.entry(name.clone()) {
            HashEntry::Occupied(_) => Err(PublishError::TopicAlreadyExists),
            HashEntry::Vacant(directory) => {
                let id = self.topic_id.generate();
                let desc = TopicDesc {
                    id: id,
                    name: name,
                    addr: addr,
                    kind: kind,
                };

                // create meta-topic
                self.topics.insert(id,
                                   Topic {
                                       desc: desc.clone(),
                                       publisher: query,
                                       subscriber: BTreeSet::new(),
                                   });

                // finally insert new topic into name directory
                directory.insert(id);

                // notify blocked subscribers (if any)
                if let Some(waiting) = self.blocked.remove(&desc.name) {
                    for promise in waiting {
                        promise.result(Ok(desc.clone()));
                    }
                }

                Ok(desc)
            }
        }
    }


    pub fn subscribe(&mut self, query: QueryId, name: &str) -> Result<TopicDesc, SubscribeError> {
        if let Some(id) = self.directory.get(name) {
            let topic = self.topics.get_mut(id).expect("invalid topic id in directory");
            topic.subscriber.insert(query);
            Ok(topic.desc.clone())
        } else {
            Err(SubscribeError::TopicNotFound)
        }
    }

    pub fn unsubscribe(&mut self,
                       query: QueryId,
                       topic_id: TopicId)
                       -> Result<(), UnsubscribeError> {
        if let Some(topic) = self.topics.get_mut(&topic_id) {
            if topic.subscriber.remove(&query) {
                Ok(())
            } else {
                Err(UnsubscribeError::InvalidQueryId)
            }
        } else {
            Err(UnsubscribeError::InvalidTopicId)
        }
    }

    pub fn unpublish(&mut self, query: QueryId, topic_id: TopicId) -> Result<(), UnpublishError> {
        match self.topics.entry(topic_id) {
            BTreeEntry::Occupied(topic) => {
                if topic.get().publisher == query {
                    self.directory.remove(&topic.get().desc.name);
                    topic.remove();
                    Ok(())
                } else {
                    Err(UnpublishError::InvalidQueryId)
                }
            }
            BTreeEntry::Vacant(_) => Err(UnpublishError::InvalidTopicId),
        }
    }

    pub fn request(&mut self, req: TopicRequest) {
        match req {
            TopicRequest::Publish(query, Publish { name, addr, kind }, promise) => {
                promise.result(self.publish(query, name, addr, kind));
            }
            TopicRequest::Subscribe(query, Subscribe { name, blocking }, promise) => {
                let result = self.subscribe(query, &name);
                if let (&Err(SubscribeError::TopicNotFound), true) = (&result, blocking) {
                    // TODO probably want to add timeout somewhere here
                    self.blocked
                        .entry(name)
                        .or_insert(Vec::new())
                        .push(promise);
                } else {
                    promise.result(result);
                }
            }
            TopicRequest::Unpublish(query, Unpublish { topic }, promise) => {
                promise.result(self.unpublish(query, topic));
            }
            TopicRequest::Unsubscribe(query, Unsubscribe { topic }, promise) => {
                promise.result(self.unsubscribe(query, topic));
            }
        }
    }
}
