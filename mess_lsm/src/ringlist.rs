use std::{
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::error::{Error, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Item<T> {
    version: usize,
    item: Option<T>,
}

impl<T> Default for Item<T> {
    fn default() -> Self {
        Self { version: 0, item: None }
    }
}

impl<T> Item<T> {
    pub fn new(item: T) -> Self {
        Self { version: 1, item: Some(item) }
    }
    pub fn update(&mut self, item: T) {
        self.version += 1;
        self.item = Some(item)
    }
    pub fn clear(&mut self) {
        self.item = None
    }
    // pub fn is_some(&self) -> bool {
    //     self.item.is_some()
    // }
    // pub fn is_none(&self) -> bool {
    //     self.item.is_none()
    // }
    // pub fn take(&self) -> Option<T> {
    //     self.item.take()
    // }
}

impl<T> Deref for Item<T> {
    type Target = Option<T>;

    fn deref(&self) -> &Self::Target {
        &self.item
    }
}

impl<T> DerefMut for Item<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.item
    }
}

pub struct RingList<T, const N: usize> {
    items: Vec<Item<T>>,
    /// Head is the beginning of the list.
    head: AtomicUsize,
    /// Tail is the next free position.
    tail: AtomicUsize,
}

impl<T, const N: usize> RingList<T, N>
where
    T: Clone,
{
    pub fn new() -> Self {
        Self {
            items: vec![Item::default(); N],
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }

    pub fn head(&self) -> usize {
        self.head.load(Ordering::Acquire)
    }
    pub fn tail(&self) -> usize {
        self.tail.load(Ordering::Acquire)
    }

    pub fn is_full(&self) -> bool {
        let head = self.head();
        let tail = self.tail();
        head == tail && self.items[head].is_some()
    }

    pub fn is_empty(&self) -> bool {
        let head = self.head();
        let tail = self.tail();
        head == tail && self.items[head].is_none()
    }

    pub fn next(&self, list_idx: usize) -> usize {
        match list_idx + 1 {
            x if x < N => x,
            _ => 0,
        }
    }

    pub fn prev(&self, list_idx: usize) -> usize {
        match list_idx {
            i if i > 0 => i - 1,
            _ => N - 1,
        }
    }

    pub fn push(&mut self, item: T) -> Result<()> {
        if self.is_full() {
            return Err(Error::ListFull);
        }
        let tail = self.tail();
        match self.items.len() {
            x if x < N - 1 => self.items.push(Item::new(item)),
            _ => self.items[tail].update(item),
        }
        self.tail.store(self.next(tail), Ordering::Release);
        Ok(())
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }
        let head = self.head();
        let item = self.items[head].clone().item?;
        self.items[head].clear();
        self.head.store(self.next(head), Ordering::Release);
        Some(item)
    }

    pub fn iter(&self) -> RingIter<'_, T, N> {
        let head = self.head();
        let version = self.items[head].version;
        RingIter { list: self, list_idx: Some(head), version }
    }
}

pub struct RingIter<'a, T, const N: usize> {
    list: &'a RingList<T, N>,
    list_idx: Option<usize>,
    version: usize,
}

impl<'a, T, const N: usize> Iterator for RingIter<'a, T, N>
where
    T: Clone,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let list_idx = self.list_idx?;
        let wrapper = self.list.items[list_idx].clone();
        let item = wrapper.item?;
        // Stop if no longer reading same version.
        if wrapper.version != self.version {
            self.list_idx = None;
            return None;
        }
        self.list_idx = if self.list.next(list_idx) == self.list.tail() {
            None
        } else {
            Some(self.list.next(list_idx))
        };
        Some(item)
    }
}

#[cfg(test)]
mod test_index_list {
    use super::*;
    use assert2::assert;
    use rstest::*;

    #[rstest]
    fn next_wraps() {
        let list = RingList::<char, 10>::new();
        assert!(list.next(0) == 1);
        assert!(list.next(9) == 0);
    }

    #[rstest]
    fn prev_wraps() {
        let list = RingList::<char, 10>::new();
        assert!(list.prev(1) == 0);
        assert!(list.prev(0) == 9);
    }

    #[rstest]
    fn it_pushes_from_the_beginning() {
        let mut list = RingList::<char, 10>::new();
        list.push('a').unwrap();
        list.push('b').unwrap();
        let mut expected = vec![Item::default(); 10];
        expected[0] = Item::new('a');
        expected[1] = Item::new('b');
        assert!(list.items == expected);
    }

    #[rstest]
    fn push_wraps_around() {
        let mut list = RingList::<u32, 10>::new();
        list.head.store(7, Ordering::Release);
        list.tail.store(7, Ordering::Release);
        for i in 0..10 {
            list.push(i as u32).unwrap();
        }
        let expected: Vec<_> = (3..10)
            .chain(0..3)
            .enumerate()
            .map(|(_i, j)| Item::new(j as u32))
            .collect();
        assert!(list.items == expected);
    }

    #[rstest]
    fn is_empty_works() {
        let mut list = RingList::<char, 10>::new();
        assert!(list.is_empty() == true);
        list.push('a').unwrap();
        assert!(list.is_empty() == false);
        list.pop();
        assert!(list.is_empty() == true);
    }

    #[rstest]
    fn is_full_works() {
        let mut list = RingList::<u32, 10>::new();
        assert!(list.is_full() == false);
        (0..10).for_each(|i| list.push(i as u32).unwrap());
        assert!(list.is_full() == true);
    }

    #[rstest]
    fn pop_returns_none_if_empty() {
        let mut list = RingList::<char, 10>::new();
        assert!(list.pop() == None);
    }

    #[rstest]
    fn pop_returns_entry_index() {
        let mut list = RingList::<char, 10>::new();
        list.push('a').unwrap();
        list.push('b').unwrap();
        assert!(list.pop() == Some('a'));
        assert!(list.pop() == Some('b'));
        assert!(list.pop() == None);
    }

    #[rstest]
    fn pop_wraps_around() {
        let mut list = RingList::<_, 10>::new();
        list.head.store(8, Ordering::Release);
        list.tail.store(8, Ordering::Release);
        list.push('h').unwrap();
        list.push('i').unwrap();
        list.push('a').unwrap();
        assert!(list.pop() == Some('h'));
        assert!(list.pop() == Some('i'));
        assert!(list.pop() == Some('a'));
        assert!(list.pop() == None);
    }

    #[rstest]
    fn pop_clears_idx() {
        let mut list = RingList::<char, 10>::new();
        list.push('a').unwrap();
        assert!(list.pop() == Some('a'));
        let mut expected = vec![Item::default(); 10];
        expected[0].version = 1;
        assert!(list.items == expected);
    }

    #[rstest]
    fn iterator_iterates() {
        let mut list = RingList::<u32, 10>::new();
        let items: Vec<_> = (0..5).map(|i| i as u32).collect();
        for item in items.clone() {
            list.push(item).unwrap();
        }
        let idxs: Vec<_> = list.iter().collect();
        assert!(idxs == vec![0, 1, 2, 3, 4]);
    }

    #[rstest]
    fn iterator_wraps_around() {
        let mut list = RingList::<u32, 10>::new();
        list.head.store(7, Ordering::Release);
        list.tail.store(7, Ordering::Release);
        let items: Vec<_> = (7u32..10).chain(0..7).collect();
        for item in items.clone() {
            list.push(item).unwrap();
        }
        let idxs: Vec<_> = list.iter().collect();
        assert!(idxs == items);
    }
}
