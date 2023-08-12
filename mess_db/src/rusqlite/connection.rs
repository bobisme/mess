use core::ops::Deref;
use core::ops::DerefMut;
use std::cell::RefCell;

use rusqlite::Statement;

use crate::error::MessResult;

const CACHE_SIZE: usize = 16;

#[derive(Clone, Copy, Debug)]
pub enum Stmt {
    Write,
}

#[derive(Default, Debug)]
pub struct StatementCache<'conn>(RefCell<Vec<Option<Statement<'conn>>>>);

impl<'conn> StatementCache<'conn> {
    pub fn get(&'conn self, key: usize) -> Option<CachedStatement<'conn>> {
        let mut cache = self.0.borrow_mut();
        let stmt = cache.get_mut(key)?;
        let stmt = stmt.take()?;
        Some(CachedStatement { key, stmt: Some(stmt), cache: self })
    }

    pub fn set(&'conn self, key: usize, stmt: Statement<'conn>) {
        if key > (CACHE_SIZE - 1) {
            return;
        }
        self.0.borrow_mut()[key] = Some(stmt);
    }
}

#[derive(Debug)]
pub struct CachedStatement<'conn> {
    key: usize,
    stmt: Option<Statement<'conn>>,
    cache: &'conn StatementCache<'conn>,
}

impl<'conn> Deref for CachedStatement<'conn> {
    type Target = Statement<'conn>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.stmt.as_ref().unwrap()
    }
}

impl DerefMut for CachedStatement<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.stmt.as_mut().unwrap()
    }
}

impl Drop for CachedStatement<'_> {
    fn drop(&mut self) {
        if let Some(stmt) = self.stmt.take() {
            self.cache.set(self.key, stmt);
        }
    }
}

#[derive(Debug)]
pub struct Conn<'conn> {
    pub(crate) conn: rusqlite::Connection,
    // stmts: SlotMap<Stmt, rusqlite::Statement<'conn>>,
    // write_stmt: Option<rusqlite::Statement<'conn>>,
    stmt_cache: StatementCache<'conn>,
}

impl Deref for Conn<'_> {
    type Target = rusqlite::Connection;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl<'conn> Conn<'conn> {
    pub fn new(conn: rusqlite::Connection) -> Self {
        Self { conn, stmt_cache: Default::default() }
    }

    pub fn write_stmt(&'conn self) -> MessResult<CachedStatement<'conn>> {
        const KEY: usize = 0;
        if let Some(stmt) = self.stmt_cache.get(KEY) {
            Ok(stmt)
        } else {
            let write_stmt = self.conn.prepare(
                r#"
                INSERT INTO messages (
                    id,
                    stream_name,
                    position,
                    message_type,
                    data,
                    metadata
                ) VALUES (?, ?, ?, ?, ?, ?)
                RETURNING global_position, position"#,
            )?;
            Ok(CachedStatement {
                key: KEY,
                stmt: Some(write_stmt),
                cache: &self.stmt_cache,
            })
        }
    }
}
