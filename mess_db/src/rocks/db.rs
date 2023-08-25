use std::{
    cell::{RefCell, RefMut},
    ops::{Deref, DerefMut},
    path::Path,
};

use rocksdb::{ColumnFamilyDescriptor, ColumnFamilyRef, Options};

use crate::error::MessResult;

const DEFAULT_CAPACITY: usize = 1024;

pub struct DB {
    db: ::rocksdb::DB,
    data_buf: RefCell<Vec<u8>>,
    meta_buf: RefCell<Vec<u8>>,
}

impl DB {
    pub fn new(path: impl AsRef<Path>) -> MessResult<Self> {
        println!("DEBUG: open db at {:?}", path.as_ref());

        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let cf_opts = Options::default();
        let db = ::rocksdb::DB::open_cf_descriptors(
            &db_opts,
            path,
            vec![
                ColumnFamilyDescriptor::new("global", cf_opts.clone()),
                ColumnFamilyDescriptor::new("stream", cf_opts.clone()),
            ],
        )?;
        Ok(Self {
            db,
            data_buf: Vec::with_capacity(DEFAULT_CAPACITY).into(),
            meta_buf: Vec::with_capacity(DEFAULT_CAPACITY).into(),
        })
    }

    pub fn global(&self) -> ColumnFamilyRef<'_> {
        self.db.cf_handle("global").expect("no global column family")
    }

    pub fn stream(&self) -> ColumnFamilyRef<'_> {
        self.db.cf_handle("stream").expect("no stream column family")
    }

    pub fn data_buffer(&self) -> RefMut<'_, Vec<u8>> {
        self.data_buf.borrow_mut()
    }

    pub fn meta_buffer(&self) -> RefMut<'_, Vec<u8>> {
        self.meta_buf.borrow_mut()
    }

    pub fn clear_serialization_buffers(&self) {
        self.data_buf.borrow_mut().clear();
        self.meta_buf.borrow_mut().clear();
    }
}

impl Deref for DB {
    type Target = ::rocksdb::DB;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl DerefMut for DB {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.db
    }
}

#[cfg(test)]
pub(crate) mod test {
    use ident::Id;

    use super::DB;

    pub(crate) struct SelfDestructingDB(Option<DB>);

    impl SelfDestructingDB {
        pub(crate) fn new_tmp() -> Self {
            let path = std::env::temp_dir();
            let path = path.join(Id::new().to_string());
            SelfDestructingDB(Some(DB::new(path).unwrap()))
        }
    }

    impl std::ops::Deref for SelfDestructingDB {
        type Target = DB;

        fn deref(&self) -> &Self::Target {
            self.0.as_ref().unwrap()
        }
    }

    impl std::ops::DerefMut for SelfDestructingDB {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.0.as_mut().unwrap()
        }
    }

    impl Drop for SelfDestructingDB {
        fn drop(&mut self) {
            let path = self.path().to_owned();
            drop(std::mem::take(&mut self.0));
            ::rocksdb::DB::destroy(&rocksdb::Options::default(), path).unwrap();
        }
    }
}
