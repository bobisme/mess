//! Handler tests driven through the real router with [`tower`]'s `oneshot`,
//! against the seedable [`FakeReadModels`] and a programmable [`FakeWriteOps`].
//! Every route gets a happy path and at least one rejection-rendering check,
//! plus an HTML smoke test asserting timeline order and post bodies.

use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use ident::Id;
use tower::ServiceExt;

use crate::contracts::{FakeReadModels, WriteError, WriteOps};
use crate::domain::post::PostError;
use crate::domain::user::UserError;

use super::{AppState, router};

// ---------------------------------------------------------------------------
// A programmable fake writer: returns a fixed result and records call names.
// ---------------------------------------------------------------------------

struct FakeWriteOps {
    response: Result<u64, WriteError>,
    calls: Mutex<Vec<String>>,
}

impl FakeWriteOps {
    fn ok() -> Self {
        Self { response: Ok(1), calls: Mutex::new(Vec::new()) }
    }
    fn err(e: WriteError) -> Self {
        Self { response: Err(e), calls: Mutex::new(Vec::new()) }
    }
    fn record(&self, what: &str) -> Result<u64, WriteError> {
        self.calls.lock().unwrap().push(what.to_string());
        self.response.clone()
    }
    fn called(&self, what: &str) -> bool {
        self.calls.lock().unwrap().iter().any(|c| c == what)
    }
}

impl WriteOps for FakeWriteOps {
    async fn register(
        &self,
        _u: Id,
        _h: String,
        _d: String,
    ) -> Result<u64, WriteError> {
        self.record("register")
    }
    async fn set_display_name(
        &self,
        _u: Id,
        _d: String,
    ) -> Result<u64, WriteError> {
        self.record("set_display_name")
    }
    async fn follow(&self, _f: Id, _t: Id) -> Result<u64, WriteError> {
        self.record("follow")
    }
    async fn unfollow(&self, _f: Id, _t: Id) -> Result<u64, WriteError> {
        self.record("unfollow")
    }
    async fn create_post(
        &self,
        _p: Id,
        _a: Id,
        _b: String,
    ) -> Result<u64, WriteError> {
        self.record("create_post")
    }
    async fn delete_post(&self, _p: Id, _by: Id) -> Result<u64, WriteError> {
        self.record("delete_post")
    }
    async fn like(&self, _p: Id, _u: Id) -> Result<u64, WriteError> {
        self.record("like")
    }
    async fn unlike(&self, _p: Id, _u: Id) -> Result<u64, WriteError> {
        self.record("unlike")
    }
}

// ---------------------------------------------------------------------------
// Fixtures & request helpers
// ---------------------------------------------------------------------------

/// alice follows bob (not carol); bob has p1,p3; carol has p2; alice liked p1.
/// Post ids are real `Id`s (as in production) so the `/p/{id}` routes that
/// parse the path param back into an `Id` work. Trailing fields let callers
/// that only need the ids destructure with `..`.
type World = (Arc<FakeWriteOps>, Router, Id, Id, Id, Id, Id, Id);

fn world(write: FakeWriteOps) -> World {
    let alice = Id::new();
    let bob = Id::new();
    let carol = Id::new();
    let p1 = Id::new();
    let p2 = Id::new();
    let p3 = Id::new();
    let rm = FakeReadModels::new()
        .with_user(alice, "alice", "Alice")
        .with_user(bob, "bob", "Bob")
        .with_user(carol, "carol", "Carol")
        .with_follow(alice, bob)
        .with_post(p1, bob, "bob first")
        .with_post(p2, carol, "carol first")
        .with_post(p3, bob, "bob second")
        .with_like(p1, alice);
    let write = Arc::new(write);
    let state = AppState { read: Arc::new(rm), write: write.clone() };
    (write, router(state), alice, bob, carol, p1, p2, p3)
}

/// The acting-user cookie holds a **handle**, not an id (see the `web` module
/// docs), so tests sign in with the fixtures' known handles directly.
fn cookie_for(handle: &str) -> String {
    format!("{}={handle}", super::ACTING_COOKIE)
}

async fn send(app: &Router, req: Request<Body>) -> (StatusCode, String, Option<String>) {
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let location = resp
        .headers()
        .get(header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&body).to_string(), location)
}

fn get(uri: &str, cookie: Option<&str>) -> Request<Body> {
    let mut b = Request::builder().method("GET").uri(uri);
    if let Some(handle) = cookie {
        b = b.header(header::COOKIE, cookie_for(handle));
    }
    b.body(Body::empty()).unwrap()
}

fn post_form(uri: &str, form: &str, cookie: Option<&str>) -> Request<Body> {
    let mut b = Request::builder()
        .method("POST")
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded");
    if let Some(handle) = cookie {
        b = b.header(header::COOKIE, cookie_for(handle));
    }
    b.body(Body::from(form.to_string())).unwrap()
}

// ---------------------------------------------------------------------------
// GET routes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn home_shows_followed_posts_newest_first() {
    let (_w, app, _alice, ..) = world(FakeWriteOps::ok());
    let (status, body, _) = send(&app, get("/", Some("alice"))).await;
    assert_eq!(status, StatusCode::OK);
    // alice sees bob's p1 & p3, not carol's p2. Newest first: "bob second"
    // before "bob first".
    let second = body.find("bob second").expect("p3 body present");
    let first = body.find("bob first").expect("p1 body present");
    assert!(second < first, "newest-first order in the timeline");
    assert!(!body.contains("carol first"), "unfollowed author excluded");
    // Compose box present for a signed-in user.
    assert!(body.contains("What's happening?"));
    assert!(body.contains("@alice"));
}

#[tokio::test]
async fn home_anonymous_shows_explore_and_no_compose() {
    let (_w, app, ..) = world(FakeWriteOps::ok());
    let (status, body, _) = send(&app, get("/", None)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Explore"));
    assert!(!body.contains("What's happening?"), "no compose when anonymous");
    assert!(body.contains("No real authentication"), "demo banner present");
}

#[tokio::test]
async fn firehose_shows_all_posts() {
    let (_w, app, ..) = world(FakeWriteOps::ok());
    let (status, body, _) = send(&app, get("/firehose", None)).await;
    assert_eq!(status, StatusCode::OK);
    for expected in ["bob first", "carol first", "bob second"] {
        assert!(body.contains(expected), "firehose missing {expected}");
    }
}

#[tokio::test]
async fn profile_happy_and_not_found() {
    let (_w, app, _alice, ..) = world(FakeWriteOps::ok());
    let (status, body, _) =
        send(&app, get("/u/bob", Some("alice"))).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("@bob"));
    // alice follows bob → the button reads "Following".
    assert!(body.contains("Following"));

    let (status, _, _) = send(&app, get("/u/nobody", Some("alice"))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn single_post_happy_and_not_found() {
    let (_w, app, _alice, _b, _c, p1, ..) = world(FakeWriteOps::ok());
    let (status, body, _) =
        send(&app, get(&format!("/p/{p1}"), Some("alice"))).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("bob first"));

    let (status, _, _) = send(&app, get("/p/does-not-exist", None)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn stylesheet_is_css() {
    let (_w, app, ..) = world(FakeWriteOps::ok());
    let resp = app
        .oneshot(get("/style.css", None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp.headers().get(header::CONTENT_TYPE).unwrap();
    assert!(ct.to_str().unwrap().starts_with("text/css"));
}

#[tokio::test]
async fn whoami_shows_picker() {
    let (_w, app, ..) = world(FakeWriteOps::ok());
    let (status, body, _) = send(&app, get("/whoami", None)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("no authentication"));
    assert!(body.contains("name=\"handle\""));
}

// ---------------------------------------------------------------------------
// POST routes: happy path (303 + ok flash) and one rejection each
// ---------------------------------------------------------------------------

fn is_ok_redirect(status: StatusCode, loc: &Option<String>) -> bool {
    status == StatusCode::SEE_OTHER
        && loc.as_ref().is_some_and(|l| l.contains("kind=ok"))
}
fn is_err_redirect(status: StatusCode, loc: &Option<String>) -> bool {
    status == StatusCode::SEE_OTHER
        && loc.as_ref().is_some_and(|l| l.contains("kind=err"))
}

#[tokio::test]
async fn post_happy_then_rejection() {
    // Happy: writer returns Ok → 303 to / with ok flash, create_post called.
    let (w, app, _alice, ..) = world(FakeWriteOps::ok());
    let (status, _, loc) =
        send(&app, post_form("/post", "body=hello+world", Some("alice"))).await;
    assert!(is_ok_redirect(status, &loc), "loc={loc:?}");
    assert!(w.called("create_post"));

    // Rejection: writer returns EmptyBody → 303 with err flash.
    let (_w, app, _alice, ..) =
        world(FakeWriteOps::err(WriteError::Post(PostError::EmptyBody)));
    let (status, _, loc) =
        send(&app, post_form("/post", "body=", Some("alice"))).await;
    assert!(is_err_redirect(status, &loc), "loc={loc:?}");
    assert!(loc.unwrap().to_lowercase().contains("empty"));
}

#[tokio::test]
async fn post_requires_login() {
    let (_w, app, ..) = world(FakeWriteOps::ok());
    let (status, _, loc) =
        send(&app, post_form("/post", "body=hi", None)).await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    let loc = loc.unwrap();
    assert!(loc.starts_with("/whoami"));
    assert!(loc.contains("kind=err"));
}

#[tokio::test]
async fn like_happy_then_already_liked() {
    let (w, app, _alice, _b, _c, _p1, _p2, p3) = world(FakeWriteOps::ok());
    let (status, _, loc) =
        send(&app, post_form(&format!("/p/{p3}/like"), "", Some("alice"))).await;
    assert!(is_ok_redirect(status, &loc));
    assert!(w.called("like"));

    let (_w, app, _alice, _b, _c, p1, ..) =
        world(FakeWriteOps::err(WriteError::Post(PostError::AlreadyLiked)));
    let (status, _, loc) =
        send(&app, post_form(&format!("/p/{p1}/like"), "", Some("alice"))).await;
    assert!(is_err_redirect(status, &loc));
}

#[tokio::test]
async fn unlike_happy() {
    let (w, app, _alice, _b, _c, p1, ..) = world(FakeWriteOps::ok());
    let (status, _, loc) =
        send(&app, post_form(&format!("/p/{p1}/unlike"), "", Some("alice")))
            .await;
    assert!(is_ok_redirect(status, &loc));
    assert!(w.called("unlike"));
}

#[tokio::test]
async fn delete_happy_then_not_author() {
    let (w, app, _alice, _bob, _c, p1, ..) = world(FakeWriteOps::ok());
    let (status, _, loc) =
        send(&app, post_form(&format!("/p/{p1}/delete"), "", Some("bob"))).await;
    assert!(is_ok_redirect(status, &loc));
    assert!(w.called("delete_post"));

    let (_w, app, _alice, _b, _c, p1, ..) =
        world(FakeWriteOps::err(WriteError::Post(PostError::NotAuthor)));
    let (status, _, loc) =
        send(&app, post_form(&format!("/p/{p1}/delete"), "", Some("alice")))
            .await;
    assert!(is_err_redirect(status, &loc));
    assert!(loc.unwrap().to_lowercase().contains("own"));
}

#[tokio::test]
async fn follow_happy_reject_and_unknown_handle() {
    // Happy.
    let (w, app, _alice, ..) = world(FakeWriteOps::ok());
    let (status, _, loc) =
        send(&app, post_form("/u/carol/follow", "", Some("alice"))).await;
    assert!(is_ok_redirect(status, &loc));
    assert!(w.called("follow"));

    // Rejection: self-follow.
    let (_w, app, _alice, ..) =
        world(FakeWriteOps::err(WriteError::User(UserError::SelfFollow)));
    let (status, _, loc) =
        send(&app, post_form("/u/alice/follow", "", Some("alice"))).await;
    assert!(is_err_redirect(status, &loc));

    // Unknown handle never reaches the writer.
    let (w, app, _alice, ..) = world(FakeWriteOps::ok());
    let (status, _, loc) =
        send(&app, post_form("/u/ghost/follow", "", Some("alice"))).await;
    assert!(is_err_redirect(status, &loc));
    assert!(!w.called("follow"));
}

#[tokio::test]
async fn unfollow_happy() {
    let (w, app, _alice, ..) = world(FakeWriteOps::ok());
    let (status, _, loc) =
        send(&app, post_form("/u/bob/unfollow", "", Some("alice"))).await;
    assert!(is_ok_redirect(status, &loc));
    assert!(w.called("unfollow"));
}

#[tokio::test]
async fn whoami_switch_existing_and_register_new() {
    // Existing handle → cookie switch, no register call.
    let (w, app, ..) = world(FakeWriteOps::ok());
    let (status, _, loc) =
        send(&app, post_form("/whoami", "handle=bob", None)).await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    assert!(is_ok_redirect(status, &loc));
    assert!(!w.called("register"), "existing handle must not register");

    // New handle → register-on-first-use.
    let (w, app, ..) = world(FakeWriteOps::ok());
    let resp = app
        .oneshot(post_form(
            "/whoami",
            "handle=dave&display_name=Dave",
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SEE_OTHER);
    let set_cookie = resp
        .headers()
        .get(header::SET_COOKIE)
        .expect("cookie set on register")
        .to_str()
        .unwrap()
        .to_string();
    assert!(set_cookie.starts_with(super::ACTING_COOKIE));
    assert!(w.called("register"));
}

#[tokio::test]
async fn whoami_register_invalid_handle_renders_error() {
    let (_w, app, ..) = world(FakeWriteOps::err(WriteError::User(
        UserError::InvalidHandle { handle: "Bad Handle".into() },
    )));
    let (status, _, loc) =
        send(&app, post_form("/whoami", "handle=BadHandle", None)).await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    let loc = loc.unwrap();
    assert!(loc.starts_with("/whoami"));
    assert!(loc.contains("kind=err"));
}

// ---------------------------------------------------------------------------
// HTML smoke test: full page structure, order, and escaping.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn html_smoke_timeline_structure_order_and_escaping() {
    let alice = Id::new();
    let (q1, q2, q3) = (Id::new(), Id::new(), Id::new());
    let rm = FakeReadModels::new()
        .with_user(alice, "alice", "Alice")
        .with_post(q1, alice, "first <b>escaped</b> body")
        .with_post(q2, alice, "second body")
        .with_post(q3, alice, "third body");
    let state = AppState {
        read: Arc::new(rm),
        write: Arc::new(FakeWriteOps::ok()),
    };
    let app = router(state);
    let (status, body, _) = send(&app, get("/", Some("alice"))).await;
    assert_eq!(status, StatusCode::OK);

    // Full document.
    assert!(body.starts_with("<!DOCTYPE html>"));
    assert!(body.contains("<link rel=\"stylesheet\" href=\"/style.css\">"));

    // Newest-first order: q3, q2, q1.
    let p3 = body.find("third body").unwrap();
    let p2 = body.find("second body").unwrap();
    let p1 = body.find("first").unwrap();
    assert!(p3 < p2 && p2 < p1, "newest-first ordering");

    // maud escapes the body: raw <b> must not appear, escaped form must.
    assert!(!body.contains("first <b>escaped</b> body"));
    assert!(body.contains("&lt;b&gt;escaped&lt;/b&gt;"));
}
