//! The axum handlers and the router.
//!
//! # Read-your-writes
//!
//! Every POST follows the same shape: call one [`DynWrite`] op, take the
//! returned global log position, `await` [`DynRead::wait_for`] on it, then
//! `303`-redirect to a GET that is now guaranteed to reflect the write. On the
//! `FakeReadModels`/`MemBackend` this barrier is immediate; against real
//! projections it blocks until the tailer catches up.
//!
//! # Rejections never 500
//!
//! A [`WriteError`] (a typed domain rejection, or an exhausted-retry
//! [`WriteError::Conflict`], or a store error) is mapped by
//! [`error::friendly`] to a sentence and carried to the redirect target as a
//! flash query param. See [`error`](super::error) for the exhausted-retry
//! contract.

use axum::{
    Form, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use ident::Id;
use serde::Deserialize;

use super::error::friendly;
use super::views::{self, Flash};
use super::{ACTING_COOKIE, AppState, PAGE_SIZE};

/// Build the application router. Non-generic: [`AppState`] holds trait objects,
/// so the handlers' futures are concretely `Send` and axum accepts them.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(home))
        .route("/firehose", get(firehose))
        .route("/u/{handle}", get(profile))
        .route("/p/{id}", get(single_post))
        .route("/post", post(create_post))
        .route("/p/{id}/delete", post(delete_post))
        .route("/p/{id}/like", post(like))
        .route("/p/{id}/unlike", post(unlike))
        .route("/u/{handle}/follow", post(follow))
        .route("/u/{handle}/unfollow", post(unfollow))
        .route("/whoami", get(whoami).post(whoami_post))
        .route("/style.css", get(stylesheet))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Query / form shapes
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
struct PageQuery {
    cursor: Option<String>,
    flash: Option<String>,
    kind: Option<String>,
}

impl PageQuery {
    fn flash(&self) -> Option<Flash> {
        self.flash.as_ref().map(|m| Flash {
            msg: m.clone(),
            kind: self.kind.clone().unwrap_or_else(|| "ok".into()),
        })
    }
}

#[derive(Debug, Deserialize)]
struct PostForm {
    body: String,
}

#[derive(Debug, Deserialize)]
struct WhoamiForm {
    handle: String,
    #[serde(default)]
    display_name: String,
}

// ---------------------------------------------------------------------------
// Acting-user (cookie) helpers
// ---------------------------------------------------------------------------

/// Read a cookie value out of the `Cookie` request header (hand-rolled; no
/// cookie-jar dependency for a demo that stores one plain id).
fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    let raw = headers.get(header::COOKIE)?.to_str().ok()?;
    raw.split(';').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k.trim() == name).then(|| v.trim().to_string())
    })
}

/// The acting user's id, if the cookie is present and parses.
fn acting_id(headers: &HeaderMap) -> Option<Id> {
    cookie_value(headers, ACTING_COOKIE)?.parse().ok()
}

/// The acting user's `(id, handle)`, if the cookie resolves to a known user.
fn acting(state: &AppState, headers: &HeaderMap) -> Option<(Id, String)> {
    let id = acting_id(headers)?;
    let handle = state.handle_of(id)?;
    Some((id, handle))
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

/// Percent-encode a flash message for a query string (spaces → `%20`, etc.).
fn enc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.'
            | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// A `303 See Other` redirect to `location` (POST → GET, per PRG).
fn see_other(location: &str) -> Response {
    (StatusCode::SEE_OTHER, [(header::LOCATION, location)]).into_response()
}

/// A `303` redirect carrying a flash message in the query string.
fn redirect_flash(path: &str, kind: &str, msg: &str) -> Response {
    let sep = if path.contains('?') { '&' } else { '?' };
    see_other(&format!("{path}{sep}flash={}&kind={kind}", enc(msg)))
}

/// The path to return to after an action: the `Referer` (same-origin path
/// only), else `fallback`.
fn back(headers: &HeaderMap, fallback: &str) -> String {
    headers
        .get(header::REFERER)
        .and_then(|v| v.to_str().ok())
        .map(|r| {
            // Keep only a same-origin absolute path; drop scheme/host and any
            // existing flash query so banners do not stack.
            let path = r.strip_prefix("http://").map_or(r, |rest| {
                rest.split_once('/').map_or("/", |(_, p)| p)
            });
            let path = if path.starts_with('/') { path } else { fallback };
            path.split('?').next().unwrap_or(fallback).to_string()
        })
        .unwrap_or_else(|| fallback.to_string())
}

/// After a successful write: wait for the read model to catch up to `pos`, then
/// redirect with an ok flash.
async fn after_write(
    state: &AppState,
    pos: u64,
    path: &str,
    ok_msg: &str,
) -> Response {
    state.read.wait_for(pos).await;
    redirect_flash(path, "ok", ok_msg)
}

// ---------------------------------------------------------------------------
// GET handlers
// ---------------------------------------------------------------------------

async fn stylesheet() -> Response {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        views::STYLESHEET,
    )
        .into_response()
}

async fn home(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<PageQuery>,
) -> Response {
    match acting(&state, &headers) {
        Some((id, handle)) => {
            let page = state
                .read
                .home_timeline(id, q.cursor.clone(), PAGE_SIZE)
                .await;
            let body = views::feed(
                "Home",
                Some("Posts from you and the people you follow."),
                &page,
                Some(&handle),
                "/",
                true,
            );
            views::page("Home", Some(&handle), q.flash().as_ref(), body)
                .into_response()
        }
        None => {
            // Anonymous: show the firehose as a discovery fallback.
            let page = state.read.firehose(q.cursor.clone(), PAGE_SIZE).await;
            let body = views::feed(
                "Explore",
                Some("Sign in on whoami to get your own home timeline."),
                &page,
                None,
                "/",
                false,
            );
            views::page("Explore", None, q.flash().as_ref(), body)
                .into_response()
        }
    }
}

async fn firehose(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<PageQuery>,
) -> Response {
    let acting = acting(&state, &headers);
    let handle = acting.as_ref().map(|(_, h)| h.as_str());
    let page = state.read.firehose(q.cursor.clone(), PAGE_SIZE).await;
    let body = views::feed(
        "Firehose",
        Some("Every post in the system, newest first."),
        &page,
        handle,
        "/firehose",
        false,
    );
    views::page("Firehose", handle, q.flash().as_ref(), body).into_response()
}

async fn profile(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(handle): Path<String>,
    Query(q): Query<PageQuery>,
) -> Response {
    let acting = acting(&state, &headers);
    let viewer = acting.as_ref().map(|(id, _)| *id);
    let viewer_handle = acting.as_ref().map(|(_, h)| h.as_str());
    let Some(prof) = state.read.profile(&handle, viewer).await else {
        return (
            StatusCode::NOT_FOUND,
            views::page(
                "Not found",
                viewer_handle,
                q.flash().as_ref(),
                views::not_found("No user with that handle."),
            ),
        )
            .into_response();
    };
    let posts =
        state.read.user_posts(&handle, q.cursor.clone(), PAGE_SIZE).await;
    let is_self = viewer_handle == Some(prof.handle.as_str());
    let body = views::profile(
        &prof,
        acting.is_some(),
        is_self,
        &posts,
        viewer_handle,
    );
    views::page(&prof.handle, viewer_handle, q.flash().as_ref(), body)
        .into_response()
}

async fn single_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(q): Query<PageQuery>,
) -> Response {
    let acting = acting(&state, &headers);
    let viewer = acting.as_ref().map(|(id, _)| *id);
    let viewer_handle = acting.as_ref().map(|(_, h)| h.as_str());
    let Some(pv) = state.read.post(&id, viewer).await else {
        return (
            StatusCode::NOT_FOUND,
            views::page(
                "Not found",
                viewer_handle,
                q.flash().as_ref(),
                views::not_found("That post does not exist or was deleted."),
            ),
        )
            .into_response();
    };
    let body = views::single_post(&pv, viewer_handle);
    views::page("Post", viewer_handle, q.flash().as_ref(), body).into_response()
}

async fn whoami(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<PageQuery>,
) -> Response {
    let handle = acting(&state, &headers).map(|(_, h)| h);
    let body = views::whoami(handle.as_deref());
    views::page("Who am I?", handle.as_deref(), q.flash().as_ref(), body)
        .into_response()
}

// ---------------------------------------------------------------------------
// POST handlers
// ---------------------------------------------------------------------------

/// Redirect an unauthenticated POST to the picker with a nudge.
fn require_login() -> Response {
    redirect_flash("/whoami", "err", "Pick a user first — you are not signed in.")
}

async fn create_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<PostForm>,
) -> Response {
    let Some((author, _)) = acting(&state, &headers) else {
        return require_login();
    };
    let post_id = Id::new();
    match state.write.create_post(post_id, author, form.body).await {
        Ok(pos) => after_write(&state, pos, "/", "Posted.").await,
        Err(e) => redirect_flash("/", "err", &friendly(&e)),
    }
}

async fn delete_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let Some((by, _)) = acting(&state, &headers) else {
        return require_login();
    };
    let Ok(post_id) = id.parse::<Id>() else {
        return redirect_flash("/", "err", "Unknown post.");
    };
    match state.write.delete_post(post_id, by).await {
        Ok(pos) => after_write(&state, pos, "/", "Post deleted.").await,
        Err(e) => redirect_flash(&format!("/p/{id}"), "err", &friendly(&e)),
    }
}

async fn like(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    like_or_unlike(state, headers, id, true).await
}

async fn unlike(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    like_or_unlike(state, headers, id, false).await
}

async fn like_or_unlike(
    state: AppState,
    headers: HeaderMap,
    id: String,
    like: bool,
) -> Response {
    let Some((user, _)) = acting(&state, &headers) else {
        return require_login();
    };
    let Ok(post_id) = id.parse::<Id>() else {
        return redirect_flash("/", "err", "Unknown post.");
    };
    let dest = back(&headers, &format!("/p/{id}"));
    let result = if like {
        state.write.like(post_id, user).await
    } else {
        state.write.unlike(post_id, user).await
    };
    match result {
        Ok(pos) => {
            let msg = if like { "Liked." } else { "Like removed." };
            after_write(&state, pos, &dest, msg).await
        }
        Err(e) => redirect_flash(&dest, "err", &friendly(&e)),
    }
}

async fn follow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(handle): Path<String>,
) -> Response {
    follow_or_unfollow(state, headers, handle, true).await
}

async fn unfollow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(handle): Path<String>,
) -> Response {
    follow_or_unfollow(state, headers, handle, false).await
}

async fn follow_or_unfollow(
    state: AppState,
    headers: HeaderMap,
    handle: String,
    follow: bool,
) -> Response {
    let Some((follower, _)) = acting(&state, &headers) else {
        return require_login();
    };
    let dest = back(&headers, &format!("/u/{handle}"));
    let Some(target) = state.id_of(&handle) else {
        return redirect_flash(&dest, "err", "No user with that handle.");
    };
    let result = if follow {
        state.write.follow(follower, target).await
    } else {
        state.write.unfollow(follower, target).await
    };
    match result {
        Ok(pos) => {
            let msg = if follow {
                format!("Now following @{handle}.")
            } else {
                format!("Unfollowed @{handle}.")
            };
            after_write(&state, pos, &dest, &msg).await
        }
        Err(e) => redirect_flash(&dest, "err", &friendly(&e)),
    }
}

async fn whoami_post(
    State(state): State<AppState>,
    Form(form): Form<WhoamiForm>,
) -> Response {
    let handle = form.handle.trim().to_string();
    // Already known? Just switch the cookie to that id — no write.
    if let Some(id) = state.id_of(&handle) {
        return set_acting(id, "/", &format!("You are now @{handle}."));
    }
    // Register-on-first-use: mint an id, register, record in the directory.
    let id = Id::new();
    let display = if form.display_name.trim().is_empty() {
        handle.clone()
    } else {
        form.display_name.trim().to_string()
    };
    match state.write.register(id, handle.clone(), display).await {
        Ok(pos) => {
            state.dir.write().expect("dir lock").insert(id, &handle);
            state.read.wait_for(pos).await;
            set_acting(id, "/", &format!("Welcome, @{handle}!"))
        }
        Err(e) => redirect_flash("/whoami", "err", &friendly(&e)),
    }
}

/// Set the acting-user cookie and redirect with an ok flash.
fn set_acting(id: Id, path: &str, msg: &str) -> Response {
    let cookie = format!(
        "{ACTING_COOKIE}={id}; Path=/; HttpOnly; SameSite=Lax; Max-Age=31536000"
    );
    let sep = if path.contains('?') { '&' } else { '?' };
    let location = format!("{path}{sep}flash={}&kind=ok", enc(msg));
    (
        StatusCode::SEE_OTHER,
        [
            (header::SET_COOKIE, cookie),
            (header::LOCATION, location),
        ],
    )
        .into_response()
}
