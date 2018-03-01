// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Wrapper types around Timely's antichain types for tracking the contents
//! of the upper and lower frontier of a Timely stream.

use std::slice;

use timely::order::PartialOrder;
use timely::progress::timestamp::Timestamp;
use timely::progress::frontier::{Antichain, MutableAntichain};

/// A representation of all active epochs.
/// An epoch T is "active" if there has been a tuple on the stream with
/// timestamp T'  where T' >= T. Forms an antichain.
#[derive(Debug)]
pub struct UpperFrontier<T: Timestamp> {
    antichain: Antichain<Rev<T>>,
}

impl<T: Timestamp> UpperFrontier<T> {
    /// Creates a new empty upper frontier.
    pub fn empty() -> Self {
        UpperFrontier { antichain: Antichain::new() }
    }

    #[cfg(test)]
    /// Returns true if any item in the antichain is greater or equal than the argument.
    pub fn greater_equal(&self, other: &T) -> bool {
        // SAFETY: This assumes that Rev<T> and T use the same memory layout
        debug_assert_eq!(
            ::std::mem::size_of::<&Rev<T>>(),
            ::std::mem::size_of::<&T>()
        );
        debug_assert_eq!(
            ::std::mem::align_of::<&Rev<T>>(),
            ::std::mem::align_of::<&T>()
        );

        let rev: &Rev<T> = unsafe { ::std::mem::transmute(other) };

        self.antichain.less_equal(rev)
    }

    /// Marks a timestamp as observed.
    ///
    /// This can indicate the start of a new epoch, if the timestamp has not
    /// been observed before. Returns `true` if the `upper` frontier changed.
    pub fn insert(&mut self, t: T) -> bool {
        self.antichain.insert(Rev(t))
    }

    /// Reveals the maximal elements in the frontier.
    pub fn elements(&self) -> &[T] {
        // SAFETY: This assumes that Rev<T> and T use the same memory layout
        debug_assert_eq!(::std::mem::size_of::<Rev<T>>(), ::std::mem::size_of::<T>());
        debug_assert_eq!(
            ::std::mem::align_of::<Rev<T>>(),
            ::std::mem::align_of::<T>()
        );

        let ptr = self.antichain.elements().as_ptr() as *const T;
        let len = self.antichain.elements().len();

        unsafe { slice::from_raw_parts(ptr, len) }
    }
}

/// A representation of all closed epochs.
/// An epoch T is "closed" if it cannot be observed in the stream anymore.
/// This attribute is an antichain equvalent to the Timely frontier.
#[derive(Debug)]
pub struct LowerFrontier<T: Timestamp> {
    antichain: MutableAntichain<T>,
}

impl<T: Timestamp> LowerFrontier<T> {
    /// Updates the frontier and compacts the argument to reflect only the externally
    /// visible changes.
    pub fn update(&mut self, updates: &mut Vec<(T, i64)>) {
        for (t, delta) in updates.drain(..) {
            self.antichain.update_dirty(t, delta);
        }

        self.antichain.update_iter_and(
            None,
            |t, i| updates.push((t.clone(), i)),
        );
    }

    /// Reveals the minimal elements in the frontier.
    pub fn elements(&self) -> &[T] {
        self.antichain.frontier()
    }
}

impl<T: Timestamp> Default for LowerFrontier<T> {
    fn default() -> Self {
        LowerFrontier { antichain: MutableAntichain::new_bottom(Default::default()) }
    }
}

/// A wrapper type implementing the *reverse* partial order of `T`
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
// TODO(swicki): use #[repr(transparent)] once it's stable
struct Rev<T>(T);

impl<T: PartialOrder> PartialOrder for Rev<T> {
    fn less_equal(&self, other: &Self) -> bool {
        other.0.less_equal(&self.0)
    }

    fn less_than(&self, other: &Self) -> bool {
        other.0.less_than(&self.0)
    }
}

impl<T: PartialOrd> PartialOrd for Rev<T> {
    fn partial_cmp(&self, other: &Self) -> Option<::std::cmp::Ordering> {
        other.0.partial_cmp(&self.0)
    }
}

impl<T: Ord> Ord for Rev<T> {
    fn cmp(&self, other: &Self) -> ::std::cmp::Ordering {
        other.0.cmp(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{UpperFrontier, LowerFrontier};

    #[test]
    fn upper_frontier() {
        let mut state = UpperFrontier::empty();
        assert_eq!(state.elements(), &[]);
        assert!(!state.greater_equal(&0));
        assert!(state.insert(0));
        assert_eq!(state.elements(), &[0]);
        assert!(state.greater_equal(&0));
        assert!(!state.greater_equal(&1));
        assert!(state.insert(1));
        assert_eq!(state.elements(), &[1]);
        assert!(state.greater_equal(&1));
        assert!(state.greater_equal(&0));
        assert!(!state.insert(0));
        assert_eq!(state.elements(), &[1]);
        assert!(!state.greater_equal(&6));
        assert!(state.insert(6));
        assert_eq!(state.elements(), &[6]);
        assert!(state.greater_equal(&6));
    }

    #[test]
    fn lower_frontier() {
        let mut updates;
        let mut state = LowerFrontier::<i32>::default();
        assert_eq!(state.elements(), &[0]);

        updates = vec![(0, 1)];
        state.update(&mut updates);
        assert_eq!(&updates, &[]);
        assert_eq!(state.elements(), &[0]);

        updates = vec![(1, 1), (0, -2)];
        state.update(&mut updates);
        updates.sort();
        assert_eq!(&updates, &[(0, -1), (1, 1)]);
        assert_eq!(state.elements(), &[1]);
    }
}
