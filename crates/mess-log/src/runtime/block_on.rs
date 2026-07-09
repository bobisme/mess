//! A dependency-free `block_on`: park the current thread until the future
//! resolves, re-polling whenever a waker unparks us. This is the classic
//! `pollster` shape (~30 lines) — enough for the real runtime, whose only
//! `Pending` sources are join handles and thread-blocking sleeps.

use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

/// A waker that unparks the blocked thread.
struct ThreadWaker {
    thread: Thread,
}

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.thread.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.thread.unpark();
    }
}

/// Poll `fut` to completion on the current thread, parking between polls.
pub fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    let waker: Waker =
        Arc::new(ThreadWaker { thread: thread::current() }).into();
    let mut cx = Context::from_waker(&waker);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(out) => return out,
            Poll::Pending => thread::park(),
        }
    }
}
