use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

use crate::protector::{BorrowedProtector, ProtectorPool};

pub struct BBPPIterator<'a> {
    protector: BorrowedProtector<'a, Arc<(Mutex<bool>, Condvar)>>,
}

pub struct BBPP {
    protectors: ProtectorPool<Arc<(Mutex<bool>, Condvar)>, 64>,
}

impl BBPP {
    pub fn new() -> Self {
        let released = Arc::new((Mutex::new(false), Condvar::new()));
        Self { protectors: ProtectorPool::new(released) }
    }

    pub fn iter(&self) -> BBPPIterator {
        let protector = self.protectors.blocking_get();

        BBPPIterator { protector }
    }
}

impl Default for BBPP {
    fn default() -> Self {
        Self::new()
    }
}
