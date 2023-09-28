use crate::error::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Item {
    global_position: u64,
    idx: u32,
}

impl Item {
    pub fn new(global_position: u64, idx: u32) -> Self {
        Self { global_position, idx }
    }

    pub fn index(&self) -> usize {
        self.idx as usize
    }
}

pub struct IndexList {
    idxs: Vec<Option<Item>>,
    cap: usize,
    /// Head is the beginning of the list.
    head: usize,
    /// Tail is the next free position.
    tail: usize,
}

impl IndexList {
    pub fn with_capacity(cap: usize) -> Self {
        Self { idxs: vec![None; cap], cap, head: 0, tail: 0 }
    }

    pub fn is_full(&self) -> bool {
        self.head == self.tail && self.idxs[self.head].is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.head == self.tail && self.idxs[self.head].is_none()
    }

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

    pub fn push(&mut self, item: Item) -> Result<()> {
        if self.is_full() {
            return Err(Error::ListFull);
        }
        self.idxs[self.tail] = Some(item);
        self.tail = self.next(self.tail);
        Ok(())
    }

    pub fn pop(&mut self) -> Option<Item> {
        if self.is_empty() {
            return None;
        }
        let Some(item) = self.idxs[self.head] else {
            return None;
        };
        self.idxs[self.head] = None;
        self.head = self.next(self.head);
        Some(item)
    }

    pub fn iter(&self) -> IndexIter<'_> {
        IndexIter { list: self, list_idx: Some(self.head) }
    }
}

pub struct IndexIter<'a> {
    list: &'a IndexList,
    list_idx: Option<usize>,
}

impl<'a> Iterator for IndexIter<'a> {
    type Item = Item;

    fn next(&mut self) -> Option<Self::Item> {
        let list_idx = self.list_idx?;
        let item = self.list.idxs[list_idx]?;
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
        let list = IndexList::with_capacity(10);
        assert!(list.next(0) == 1);
        assert!(list.next(9) == 0);
    }

    #[rstest]
    fn prev_wraps() {
        let list = IndexList::with_capacity(10);
        assert!(list.prev(1) == 0);
        assert!(list.prev(0) == 9);
    }

    #[rstest]
    fn it_pushes_from_the_beginning() {
        let mut list = IndexList::with_capacity(10);
        list.push(Item::new(1, 1)).unwrap();
        list.push(Item::new(2, 2)).unwrap();
        let mut expected = vec![None; 10];
        expected[0] = Some(Item::new(1, 1));
        expected[1] = Some(Item::new(2, 2));
        assert!(list.idxs == expected);
    }

    #[rstest]
    fn push_wraps_around() {
        let mut list = IndexList::with_capacity(10);
        list.head = 7;
        list.tail = 7;
        for i in 0..10 {
            list.push(Item::new(i, i as u32)).unwrap();
        }
        let expected: Vec<_> =
            (3..10).chain(0..3).map(|i| Some(Item::new(i, i as u32))).collect();
        assert!(list.idxs == expected);
    }

    #[rstest]
    fn is_empty_works() {
        let mut list = IndexList::with_capacity(10);
        assert!(list.is_empty() == true);
        list.push(Item::new(1, 1)).unwrap();
        assert!(list.is_empty() == false);
        list.pop();
        assert!(list.is_empty() == true);
    }

    #[rstest]
    fn is_full_works() {
        let mut list = IndexList::with_capacity(10);
        assert!(list.is_full() == false);
        (0..10).for_each(|i| list.push(Item::new(i, i as u32)).unwrap());
        assert!(list.is_full() == true);
    }

    #[rstest]
    fn pop_returns_none_if_empty() {
        let mut list = IndexList::with_capacity(10);
        assert!(list.pop() == None);
    }

    #[rstest]
    fn pop_returns_entry_index() {
        let mut list = IndexList::with_capacity(10);
        list.push(Item::new(1, 1)).unwrap();
        list.push(Item::new(2, 2)).unwrap();
        assert!(list.pop() == Some(Item::new(1, 1)));
        assert!(list.pop() == Some(Item::new(2, 2)));
        assert!(list.pop() == None);
    }

    #[rstest]
    fn pop_wraps_around() {
        let mut list = IndexList::with_capacity(10);
        list.head = 8;
        list.tail = 8;
        list.push(Item::new(8, 8)).unwrap();
        list.push(Item::new(9, 9)).unwrap();
        list.push(Item::new(1, 1)).unwrap();
        assert!(list.pop() == Some(Item::new(8, 8)));
        assert!(list.pop() == Some(Item::new(9, 9)));
        assert!(list.pop() == Some(Item::new(1, 1)));
        assert!(list.pop() == None);
    }

    #[rstest]
    fn pop_clears_idx() {
        let mut list = IndexList::with_capacity(10);
        list.push(Item::new(1, 1)).unwrap();
        assert!(list.pop() == Some(Item::new(1, 1)));
        assert!(list.idxs == vec![None; 10]);
    }

    #[rstest]
    fn iterator_iterates() {
        let mut list = IndexList::with_capacity(10);
        let items: Vec<_> = (0..5).map(|i| Item::new(i, i as u32)).collect();
        for item in items.clone() {
            list.push(item).unwrap();
        }
        let idxs: Vec<_> = list.iter().collect();
        assert!(idxs == items);
    }

    #[rstest]
    fn iterator_wraps_around() {
        let mut list = IndexList::with_capacity(10);
        list.head = 7;
        list.tail = 7;
        let items: Vec<_> = (0..10).map(|i| Item::new(i, i as u32)).collect();
        for item in items.clone() {
            list.push(item).unwrap();
        }
        let idxs: Vec<_> = list.iter().collect();
        assert!(idxs == items);
    }
}
