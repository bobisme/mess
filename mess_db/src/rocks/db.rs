use std::{
    ops::{Deref, DerefMut},
    path::Path,
};

use rocksdb::{ColumnFamilyDescriptor, ColumnFamilyRef, Options};

use crate::error::MessResult;

pub struct DB {
    db: ::rocksdb::DB,
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
        Ok(Self { db })
    }

    pub fn global(&self) -> ColumnFamilyRef<'_> {
        self.db.cf_handle("global").expect("no global column family")
    }

    pub fn stream(&self) -> ColumnFamilyRef<'_> {
        self.db.cf_handle("stream").expect("no stream column family")
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
