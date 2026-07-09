//! Given-When-Then tests for the social-feed domain via `mess-testkit`'s
//! store-free `AggregateTest` — see `examples/bank/tests/gwt.rs` for the
//! first walkthrough of this kit.
//!
//! Run with:
//!
//! ```sh
//! cargo test -p social
//! ```

use ident::Id;
use mess_testkit::AggregateTest;
use social::{
    HideByModerator, HideByPoster, Post, PostError, PostEvent, PostStatus,
    Publish,
};

#[test]
fn publishing_emits_posted() {
    let poster_id = Id::new();
    AggregateTest::<Post>::given_no_events()
        .when(Publish { poster_id, body: "hello".into() })
        .then_events([PostEvent::Posted { poster_id, body: "hello".into() }]);
}

#[test]
fn cannot_publish_twice() {
    let poster_id = Id::new();
    AggregateTest::<Post>::given([PostEvent::Posted {
        poster_id,
        body: "hello".into(),
    }])
    .when(Publish { poster_id, body: "again".into() })
    .then_error(PostError::AlreadyPosted);
}

#[test]
fn poster_can_hide_their_own_post() {
    let poster_id = Id::new();
    AggregateTest::<Post>::given([PostEvent::Posted {
        poster_id,
        body: "hello".into(),
    }])
    .when(HideByPoster { requester: poster_id })
    .then_events([PostEvent::HiddenByPoster]);
}

#[test]
fn a_stranger_cannot_hide_someone_elses_post() {
    let poster_id = Id::new();
    let stranger = Id::new();
    AggregateTest::<Post>::given([PostEvent::Posted {
        poster_id,
        body: "hello".into(),
    }])
    .when(HideByPoster { requester: stranger })
    .then_error(PostError::NotYourPost);
}

#[test]
fn cannot_hide_a_post_that_was_never_published() {
    AggregateTest::<Post>::given_no_events()
        .when(HideByPoster { requester: Id::new() })
        .then_error(PostError::NotPosted);
}

#[test]
fn cannot_hide_an_already_hidden_post_twice() {
    let poster_id = Id::new();
    AggregateTest::<Post>::given([
        PostEvent::Posted { poster_id, body: "hello".into() },
        PostEvent::HiddenByPoster,
    ])
    .when(HideByPoster { requester: poster_id })
    .then_error(PostError::AlreadyHidden);
}

#[test]
fn moderator_can_hide_any_post_with_no_ownership_check() {
    let poster_id = Id::new();
    AggregateTest::<Post>::given([PostEvent::Posted {
        poster_id,
        body: "spam".into(),
    }])
    .when(HideByModerator)
    .then_events([PostEvent::HiddenByModerator]);
}

#[test]
fn full_fold_matches_expected_state() {
    let poster_id = Id::new();
    let mut state = Post::default();
    for event in [
        PostEvent::Posted { poster_id, body: "hello".into() },
        PostEvent::HiddenByModerator,
    ] {
        state.apply(&event);
    }
    assert_eq!(
        state,
        Post {
            poster_id: Some(poster_id),
            body: "hello".into(),
            status: PostStatus::HiddenByModerator,
        }
    );
}
