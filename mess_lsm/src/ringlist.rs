use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use crate::error::{Error, Result};

#[derive(Clone, Debug)]
pub struct Item<T> {
    version: usize,
    item: Option<T>,
}

impl<T: Default> Default for Item<T> {
    fn default() -> Self {
        Self { version: 0, item: None }
    }
}

impl<T> Item<T> {
    pub fn new(item: Option<T>) -> Self {
        Self { version: 0, item }
    }
    pub fn update(&mut self, item: Option<T>) {
        self.version += 1;
        self.item = item
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
    cap: usize,
    /// Head is the beginning of the list.
    head: usize,
    /// Tail is the next free position.
    tail: usize,
    _mark: PhantomData<N>,
}

impl<T> RingList<T>
where
    T: Clone + Default,
{
    pub fn with_capacity(cap: usize) -> Self {
        Self { items: vec![Item::default(); cap], cap, head: 0, tail: 0 }
    }

    pub fn is_full(&self) -> bool {
        self.head == self.tail && self.items[self.head].is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.head == self.tail && self.items[self.head].is_none()
    }
    pub fn tail(&self) -> Option<usize> {}

    pub fn next(&self, list_idx: usize) -> usize {
        match list_idx + 1 {
            x if x < self.cap => x,
            _ => 0,
        }
    }

    pub fn prev(&self, list_idx: usize) -> usize {
        match list_idx {
            i if i > 0 => i - 1,
            _ => self.cap - 1,
        }
    }

    pub fn push(&mut self, item: T) -> Result<()> {
        if self.is_full() {
            return Err(Error::ListFull);
        }
        self.items[self.tail] = Some(item);
        self.tail = self.next(self.tail);
        Ok(())
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }
        let Some(item) = self.items[self.head] else {
            return None;
        };
        self.items[self.head] = None;
        self.head = self.next(self.head);
        Some(item)
    }

    pub fn iter(&self) -> IndexIter<'_, T> {
        IndexIter { list: self, list_idx: Some(self.head) }
    }
}

pub struct IndexIter<'a, T> {
    list: &'a RingList<T>,
    list_idx: Option<usize>,
}

impl<'a, T> Iterator for IndexIter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let list_idx = self.list_idx?;
        let item = self.list.items[list_idx]?;
        self.list_idx = if self.list.next(list_idx) == self.list.tail {
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
        let list = RingList::with_capacity(10);
        assert!(list.next(0) == 1);
        assert!(list.next(9) == 0);
    }

    #[rstest]
    fn prev_wraps() {
        let list = RingList::with_capacity(10);
        assert!(list.prev(1) == 0);
        assert!(list.prev(0) == 9);
    }

    #[rstest]
    fn it_pushes_from_the_beginning() {
        let mut list = RingList::with_capacity(10);
        list.push(1, 1, 1).unwrap();
        list.push(2, 2, 2).unwrap();
        let mut expected = vec![None; 10];
        expected[0] = Some(Item::new(1, 1, 1));
        expected[1] = Some(Item::new(2, 2, 2));
        assert!(list.items == expected);
    }

    #[rstest]
    fn push_wraps_around() {
        let mut list = RingList::with_capacity(10);
        list.head = 7;
        list.tail = 7;
        for i in 0..10 {
            list.push(i, i as u32, i as u32).unwrap();
        }
        let expected: Vec<_> = (3..10)
            .chain(0..3)
            .enumerate()
            .map(|(_i, j)| Some(Item::new(j as u64, j as u32, j as u32)))
            .collect();
        assert!(list.items == expected);
    }

    #[rstest]
    fn is_empty_works() {
        let mut list = RingList::with_capacity(10);
        assert!(list.is_empty() == true);
        list.push(1, 1, 1).unwrap();
        assert!(list.is_empty() == false);
        list.pop();
        assert!(list.is_empty() == true);
    }

    #[rstest]
    fn is_full_works() {
        let mut list = RingList::with_capacity(10);
        assert!(list.is_full() == false);
        (0..10).for_each(|i| list.push(i, i as u32, i as u32).unwrap());
        assert!(list.is_full() == true);
    }

    #[rstest]
    fn pop_returns_none_if_empty() {
        let mut list = RingList::with_capacity(10);
        assert!(list.pop() == None);
    }

    #[rstest]
    fn pop_returns_entry_index() {
        let mut list = RingList::with_capacity(10);
        list.push(1, 1, 0).unwrap();
        list.push(2, 2, 0).unwrap();
        assert!(list.pop() == Some(Item::new(1, 1, 0)));
        assert!(list.pop() == Some(Item::new(2, 2, 0)));
        assert!(list.pop() == None);
    }

    #[rstest]
    fn pop_wraps_around() {
        let mut list = RingList::with_capacity(10);
        list.head = 8;
        list.tail = 8;
        list.push(8, 8, 0).unwrap();
        list.push(9, 9, 0).unwrap();
        list.push(0, 0, 0).unwrap();
        assert!(list.pop() == Some(Item::new(8, 8, 0)));
        assert!(list.pop() == Some(Item::new(9, 9, 0)));
        assert!(list.pop() == Some(Item::new(0, 0, 0)));
        assert!(list.pop() == None);
    }

    #[rstest]
    fn pop_clears_idx() {
        let mut list = RingList::with_capacity(10);
        list.push(1, 1, 0).unwrap();
        assert!(list.pop() == Some(Item::new(1, 1, 0)));
        assert!(list.items == vec![None; 10]);
    }

    #[rstest]
    fn iterator_iterates() {
        let mut list = RingList::with_capacity(10);
        let items: Vec<_> =
            (0..5).map(|i| Item::new(i as u64, i as u32, i as u32)).collect();
        for item in items.clone() {
            list.push(item.global_position, item.entry_idx, item.next_idx)
                .unwrap();
        }
        let idxs: Vec<_> = list.iter().collect();
        assert!(idxs == items);
    }

    #[rstest]
    fn iterator_wraps_around() {
        let mut list = RingList::with_capacity(10);
        list.head = 7;
        list.tail = 7;
        let items: Vec<_> = (7..10)
            .chain(0..7)
            .enumerate()
            .map(|(ord, _idx)| Item::new(ord as u64, ord as u32, ord as u32))
            .collect();
        for item in items.clone() {
            list.push(item.global_position, item.entry_idx, item.next_idx)
                .unwrap();
        }
        let idxs: Vec<_> = list.iter().collect();
        assert!(idxs == items);
    }
}
