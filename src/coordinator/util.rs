use std::ops::RangeFrom;
use std::marker::PhantomData;
use std::collections::HashMap;
use std::collections::hash_map::{Entry, Iter as HashMapIter};
use std::hash::{Hash, SipHasher};

pub struct Generator<T> {
    generator: RangeFrom<u64>,
    marker: PhantomData<T>,
}

impl<T: From<u64>> Generator<T> {
    pub fn new() -> Self {
        Generator {
            generator: 0..,
            marker: PhantomData,
        }
    }

    pub fn generate(&mut self) -> T {
        From::from(self.generator.next().unwrap())
    }
}

pub struct HashBag<T> {
    map: HashMap<T, usize>,
}

impl<T: Hash + Eq> HashBag<T> {
    pub fn new() -> Self {
        HashBag {
            map: HashMap::new(),
        }
    }
    
    pub fn insert(&mut self, value: T) {
        self.map.entry(value).or_insert(1);
    }

    pub fn contains(&self, value: &T) -> bool {
        self.map.contains_key(value)
    }

    pub fn remove(&mut self, value: T) {
        if let Entry::Occupied(mut entry) = self.map.entry(value) {
            *entry.get_mut() -= 1;
            if *entry.get() == 0 {
                entry.remove();
            }
        }
    }
}

impl<'a, T: Hash + Eq> IntoIterator for &'a HashBag<T> {
    type Item = (&'a T, usize);
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        Iter {
            inner: self.map.iter(),
        }
    }
}

pub struct Iter<'a, T: 'a> {
    inner: HashMapIter<'a, T, usize>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = (&'a T, usize);
    
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(t, c)| (t, *c))
    }
}
