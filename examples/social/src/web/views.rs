//! Server-rendered HTML, via [`maud`] compile-time templates.
//!
//! Every template escapes its interpolations by default (maud does this), so
//! user-supplied handles and post bodies cannot inject markup. The single
//! stylesheet is served separately at `/style.css` (see
//! [`STYLESHEET`]).

use ident::Id;
use maud::{DOCTYPE, Markup, html};

use crate::contracts::{PostView, ProfileView, TimelinePage};

/// A one-shot notification carried across a redirect via query params.
#[derive(Debug, Clone)]
pub struct Flash {
    pub msg: String,
    /// `"ok"` or `"err"` — styles the banner.
    pub kind: String,
}

/// The whole-page chrome: `<head>`, the demo/no-auth banner, the nav, an
/// optional flash, and the page `content`.
pub fn page(
    title: &str,
    acting: Option<&str>,
    flash: Option<&Flash>,
    content: Markup,
) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { (title) " · mess social" }
                link rel="stylesheet" href="/style.css";
            }
            body {
                header.topbar {
                    nav.nav {
                        a.brand href="/" { "mess" span.brand-sub { "social" } }
                        a href="/" { "Home" }
                        a href="/firehose" { "Firehose" }
                        span.spacer {}
                        @if let Some(h) = acting {
                            a.acting href="/whoami" { "@" (h) }
                        } @else {
                            a.acting href="/whoami" { "Sign in" }
                        }
                    }
                }
                div.demo-banner {
                    strong { "Demo." }
                    " No real authentication — pick or create any user on "
                    a href="/whoami" { "whoami" }
                    ". The acting user is just a cookie."
                }
                @if let Some(f) = flash {
                    div.flash.(format!("flash-{}", f.kind)) { (f.msg) }
                }
                main.container {
                    (content)
                }
                footer.footer {
                    "mess event-sourcing demo · server-rendered · no JavaScript"
                }
            }
        }
    }
}

/// One post card, shared by feeds and the permalink page. `viewer_id` (the
/// acting user's id, compared against [`PostView::author_id`] — not the
/// handle — for the author-only delete button) and `viewer_handle` (for the
/// author link/compose visibility) both describe the same acting user, if
/// any. `permalink` controls whether the body links to `/p/:id`.
fn post_card(
    p: &PostView,
    viewer_id: Option<Id>,
    viewer_handle: Option<&str>,
    permalink: bool,
) -> Markup {
    let is_author = viewer_id == Some(p.author_id);
    let logged_in = viewer_handle.is_some();
    html! {
        article.post {
            div.post-head {
                a.author href=(format!("/u/{}", p.author_handle)) {
                    span.display { (p.author_display) }
                    span.handle { "@" (p.author_handle) }
                }
                a.permalink href=(format!("/p/{}", p.id)) { "#" }
            }
            @if permalink {
                p.body { (p.body) }
            } @else {
                a.body-link href=(format!("/p/{}", p.id)) {
                    p.body { (p.body) }
                }
            }
            div.post-actions {
                @if logged_in {
                    @if p.liked_by_me {
                        form method="post" action=(format!("/p/{}/unlike", p.id)) {
                            button.btn.liked type="submit" {
                                "♥ Liked " span.count { (p.likes) }
                            }
                        }
                    } @else {
                        form method="post" action=(format!("/p/{}/like", p.id)) {
                            button.btn type="submit" {
                                "♡ Like " span.count { (p.likes) }
                            }
                        }
                    }
                } @else {
                    span.likes-static { "♥ " (p.likes) }
                }
                @if is_author {
                    form method="post" action=(format!("/p/{}/delete", p.id)) {
                        button.btn.danger type="submit" { "Delete" }
                    }
                }
            }
        }
    }
}

/// A feed: an optional compose box, the posts, and a pager.
pub fn feed(
    heading: &str,
    subtitle: Option<&str>,
    page_data: &TimelinePage,
    viewer_id: Option<Id>,
    viewer_handle: Option<&str>,
    base_path: &str,
    show_compose: bool,
) -> Markup {
    html! {
        section.feed {
            h1 { (heading) }
            @if let Some(s) = subtitle { p.subtitle { (s) } }
            @if show_compose {
                form.compose method="post" action="/post" {
                    textarea name="body" rows="3"
                        maxlength="500"
                        placeholder="What's happening?" {}
                    div.compose-actions {
                        button.btn.primary type="submit" { "Post" }
                    }
                }
            }
            @if page_data.entries.is_empty() {
                p.empty { "Nothing here yet." }
            }
            @for p in &page_data.entries {
                (post_card(p, viewer_id, viewer_handle, false))
            }
            @if let Some(cursor) = &page_data.next_cursor {
                a.pager href=(format!("{base_path}?cursor={cursor}")) {
                    "Older posts →"
                }
            }
        }
    }
}

/// A single post permalink page.
pub fn single_post(
    p: &PostView,
    viewer_id: Option<Id>,
    viewer_handle: Option<&str>,
) -> Markup {
    html! {
        section.feed {
            h1 { "Post" }
            (post_card(p, viewer_id, viewer_handle, true))
            p.back { a href="/" { "← Back home" } }
        }
    }
}

/// A profile page: identity, counts, follow button, and the user's posts.
pub fn profile(
    prof: &ProfileView,
    logged_in: bool,
    is_self: bool,
    posts: &TimelinePage,
    viewer_id: Option<Id>,
    viewer_handle: Option<&str>,
) -> Markup {
    html! {
        section.profile {
            div.profile-head {
                div {
                    h1 { (prof.display_name) }
                    p.handle { "@" (prof.handle) }
                }
                @if logged_in && !is_self {
                    @if prof.followed_by_me {
                        form method="post"
                            action=(format!("/u/{}/unfollow", prof.handle)) {
                            button.btn type="submit" { "Following ✓" }
                        }
                    } @else {
                        form method="post"
                            action=(format!("/u/{}/follow", prof.handle)) {
                            button.btn.primary type="submit" { "Follow" }
                        }
                    }
                }
            }
            div.stats {
                span { strong { (prof.post_count) } " posts" }
                span { strong { (prof.follower_count) } " followers" }
                span { strong { (prof.following_count) } " following" }
            }
        }
        (feed(
            "Posts",
            None,
            posts,
            viewer_id,
            viewer_handle,
            &format!("/u/{}", prof.handle),
            false,
        ))
    }
}

/// The act-as-user picker. Explains there is no real auth.
pub fn whoami(acting: Option<&str>) -> Markup {
    html! {
        section.whoami {
            h1 { "Who am I?" }
            div.note {
                p {
                    "This demo has "
                    strong { "no authentication" }
                    ". Enter a handle to act as that user. If the handle is "
                    "new, it is registered on the spot. Your choice is stored "
                    "in a plain cookie — anyone can be anyone."
                }
            }
            @if let Some(h) = acting {
                p.current { "Currently acting as " strong { "@" (h) } "." }
            }
            form.whoami-form method="post" action="/whoami" {
                label {
                    "Handle"
                    input type="text" name="handle" required
                        pattern="[a-z0-9_]{1,30}"
                        placeholder="e.g. alice";
                }
                label {
                    "Display name "
                    span.hint { "(only used when registering a new handle)" }
                    input type="text" name="display_name"
                        placeholder="e.g. Alice";
                }
                button.btn.primary type="submit" { "Act as this user" }
            }
        }
    }
}

/// A minimal not-found body.
pub fn not_found(what: &str) -> Markup {
    html! {
        section.feed {
            h1 { "Not found" }
            p { (what) }
            p.back { a href="/" { "← Back home" } }
        }
    }
}

/// The single static stylesheet, served at `/style.css`.
pub const STYLESHEET: &str = include_str!("style.css");
