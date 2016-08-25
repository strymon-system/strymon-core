use std::ops::RangeFrom;
use std::marker::PhantomData;

pub mod promise;

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
