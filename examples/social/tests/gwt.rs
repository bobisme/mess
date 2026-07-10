//! Given-When-Then specs for the social domain via `mess-testkit`'s
//! store-free [`AggregateTest`] — one test per **accept** and per
//! **rejection** of every command on all four aggregates (the two entities,
//! [`User`]/[`Post`], and the two relationships, [`Like`]/[`Follow`]). Read
//! these as the executable specification of the rules documented in
//! `src/domain/*`.
//!
//! Cross-aggregate / seam-level rules that no single `decide` can express —
//! self-follow refusal (a `follow-X_X` well-formedness check) and self-like
//! *allowance* — are specified in `tests/store_roundtrip.rs`, which drives them
//! through [`WriteOps`], the seam that owns them.
//!
//! See `examples/bank/tests/gwt.rs` for the first walkthrough of this kit.
//!
//! Run with:
//!
//! ```sh
//! cargo test -p social
//! ```

use ident::Id;
use mess_testkit::{AggregateTest, matching};
use social::domain::follow::{
    Follow, FollowError, FollowEvent, PlaceFollow, RemoveFollow,
};
use social::domain::like::{Like, LikeError, LikeEvent, PlaceLike, RemoveLike};
use social::domain::post::{
    CreatePost, DeletePost, Post, PostError, PostEvent,
};
use social::domain::user::{
    RegisterUser, SetDisplayName, User, UserError, UserEvent,
};

// ===========================================================================
// User: RegisterUser
// ===========================================================================

#[test]
fn register_emits_registered() {
    AggregateTest::<User>::given_no_events()
        .when(RegisterUser {
            handle:       "alice".into(),
            display_name: "Alice".into(),
        })
        .then_events([UserEvent::Registered {
            handle:       "alice".into(),
            display_name: "Alice".into(),
        }]);
}

#[test]
fn cannot_register_twice() {
    AggregateTest::<User>::given([UserEvent::Registered {
        handle:       "alice".into(),
        display_name: "Alice".into(),
    }])
    .when(RegisterUser {
        handle:       "alice".into(),
        display_name: "A".into(),
    })
    .then_error(UserError::AlreadyRegistered);
}

#[test]
fn rejects_handle_with_uppercase() {
    AggregateTest::<User>::given_no_events()
        .when(RegisterUser {
            handle:       "Alice".into(),
            display_name: "Alice".into(),
        })
        .then_error(matching("invalid handle", |e| {
            matches!(e, UserError::InvalidHandle { .. })
        }));
}

#[test]
fn rejects_empty_handle() {
    AggregateTest::<User>::given_no_events()
        .when(RegisterUser {
            handle:       String::new(),
            display_name: "A".into(),
        })
        .then_error(matching("invalid handle", |e| {
            matches!(e, UserError::InvalidHandle { .. })
        }));
}

#[test]
fn rejects_handle_over_30_chars() {
    AggregateTest::<User>::given_no_events()
        .when(RegisterUser {
            handle:       "a".repeat(31),
            display_name: "A".into(),
        })
        .then_error(matching("invalid handle", |e| {
            matches!(e, UserError::InvalidHandle { .. })
        }));
}

#[test]
fn rejects_handle_with_spaces_or_symbols() {
    for bad in ["ali ce", "ali-ce", "ali.ce", "aliçe", "@alice"] {
        AggregateTest::<User>::given_no_events()
            .when(RegisterUser {
                handle:       bad.into(),
                display_name: "A".into(),
            })
            .then_error(matching("invalid handle", |e| {
                matches!(e, UserError::InvalidHandle { .. })
            }));
    }
}

#[test]
fn accepts_handle_with_digits_and_underscore() {
    AggregateTest::<User>::given_no_events()
        .when(RegisterUser {
            handle:       "alice_01".into(),
            display_name: "Alice".into(),
        })
        .then_events([UserEvent::Registered {
            handle:       "alice_01".into(),
            display_name: "Alice".into(),
        }]);
}

// ===========================================================================
// User: SetDisplayName
// ===========================================================================

#[test]
fn set_display_name_emits_changed() {
    AggregateTest::<User>::given([UserEvent::Registered {
        handle:       "alice".into(),
        display_name: "Alice".into(),
    }])
    .when(SetDisplayName { display_name: "Alice B.".into() })
    .then_events([UserEvent::DisplayNameChanged {
        display_name: "Alice B.".into(),
    }]);
}

#[test]
fn cannot_set_display_name_when_unregistered() {
    AggregateTest::<User>::given_no_events()
        .when(SetDisplayName { display_name: "Nobody".into() })
        .then_error(UserError::NotRegistered);
}

// ===========================================================================
// Post: CreatePost
// ===========================================================================

#[test]
fn create_post_emits_posted() {
    let author = Id::new();
    AggregateTest::<Post>::given_no_events()
        .when(CreatePost { author, body: "hello world".into() })
        .then_events([PostEvent::Posted {
            author,
            body: "hello world".into(),
        }]);
}

#[test]
fn cannot_create_post_twice() {
    let author = Id::new();
    AggregateTest::<Post>::given([PostEvent::Posted {
        author,
        body: "first".into(),
    }])
    .when(CreatePost { author, body: "second".into() })
    .then_error(PostError::AlreadyCreated);
}

#[test]
fn rejects_empty_body() {
    let author = Id::new();
    AggregateTest::<Post>::given_no_events()
        .when(CreatePost { author, body: String::new() })
        .then_error(PostError::EmptyBody);
}

#[test]
fn accepts_body_at_max_length() {
    let author = Id::new();
    let body = "x".repeat(500);
    AggregateTest::<Post>::given_no_events()
        .when(CreatePost { author, body: body.clone() })
        .then_events([PostEvent::Posted { author, body }]);
}

#[test]
fn rejects_body_over_max_length() {
    let author = Id::new();
    AggregateTest::<Post>::given_no_events()
        .when(CreatePost { author, body: "x".repeat(501) })
        .then_error(PostError::BodyTooLong { len: 501, max: 500 });
}

// ===========================================================================
// Post: DeletePost
// ===========================================================================

#[test]
fn author_can_delete_post() {
    let author = Id::new();
    AggregateTest::<Post>::given([PostEvent::Posted {
        author,
        body: "hello".into(),
    }])
    .when(DeletePost { by: author })
    .then_events([PostEvent::Deleted { by: author }]);
}

#[test]
fn cannot_delete_post_that_was_never_created() {
    AggregateTest::<Post>::given_no_events()
        .when(DeletePost { by: Id::new() })
        .then_error(PostError::NotCreated);
}

#[test]
fn non_author_cannot_delete_post() {
    let author = Id::new();
    let stranger = Id::new();
    AggregateTest::<Post>::given([PostEvent::Posted {
        author,
        body: "hello".into(),
    }])
    .when(DeletePost { by: stranger })
    .then_error(PostError::NotAuthor);
}

#[test]
fn cannot_delete_post_twice() {
    let author = Id::new();
    AggregateTest::<Post>::given([
        PostEvent::Posted { author, body: "hello".into() },
        PostEvent::Deleted { by: author },
    ])
    .when(DeletePost { by: author })
    .then_error(PostError::AlreadyDeleted);
}

// ===========================================================================
// Like relationship: PlaceLike / RemoveLike — the alternating machine
// ===========================================================================

#[test]
fn place_like_emits_liked() {
    AggregateTest::<Like>::given_no_events()
        .when(PlaceLike)
        .then_events([LikeEvent::Liked]);
}

#[test]
fn cannot_like_twice() {
    AggregateTest::<Like>::given([LikeEvent::Liked])
        .when(PlaceLike)
        .then_error(LikeError::AlreadyLiked);
}

#[test]
fn remove_like_emits_unliked() {
    AggregateTest::<Like>::given([LikeEvent::Liked])
        .when(RemoveLike)
        .then_events([LikeEvent::Unliked]);
}

#[test]
fn cannot_unlike_when_not_liked() {
    AggregateTest::<Like>::given_no_events()
        .when(RemoveLike)
        .then_error(LikeError::NotLiked);
}

#[test]
fn cannot_unlike_after_unliking() {
    // Idempotent-rejection: the machine is back to not-liked, so a second
    // RemoveLike is refused just like the first-ever one.
    AggregateTest::<Like>::given([LikeEvent::Liked, LikeEvent::Unliked])
        .when(RemoveLike)
        .then_error(LikeError::NotLiked);
}

#[test]
fn can_relike_after_unliking() {
    // The edge alternates: after unliking, liking again is accepted.
    AggregateTest::<Like>::given([LikeEvent::Liked, LikeEvent::Unliked])
        .when(PlaceLike)
        .then_events([LikeEvent::Liked]);
}

// ===========================================================================
// Follow relationship: PlaceFollow / RemoveFollow — the alternating machine
// ===========================================================================

#[test]
fn place_follow_emits_followed() {
    AggregateTest::<Follow>::given_no_events()
        .when(PlaceFollow)
        .then_events([FollowEvent::Followed]);
}

#[test]
fn cannot_follow_twice() {
    AggregateTest::<Follow>::given([FollowEvent::Followed])
        .when(PlaceFollow)
        .then_error(FollowError::AlreadyFollowing);
}

#[test]
fn remove_follow_emits_unfollowed() {
    AggregateTest::<Follow>::given([FollowEvent::Followed])
        .when(RemoveFollow)
        .then_events([FollowEvent::Unfollowed]);
}

#[test]
fn cannot_unfollow_when_not_following() {
    AggregateTest::<Follow>::given_no_events()
        .when(RemoveFollow)
        .then_error(FollowError::NotFollowing);
}

#[test]
fn cannot_unfollow_after_unfollowing() {
    // Idempotent-rejection: back to not-following, second RemoveFollow refused.
    AggregateTest::<Follow>::given([
        FollowEvent::Followed,
        FollowEvent::Unfollowed,
    ])
    .when(RemoveFollow)
    .then_error(FollowError::NotFollowing);
}

#[test]
fn can_refollow_after_unfollowing() {
    AggregateTest::<Follow>::given([
        FollowEvent::Followed,
        FollowEvent::Unfollowed,
    ])
    .when(PlaceFollow)
    .then_events([FollowEvent::Followed]);
}

// ===========================================================================
// Full-fold sanity: replaying a whole history yields the expected state.
// ===========================================================================

#[test]
fn user_full_fold_matches_expected_state() {
    let mut state = User::default();
    for event in [
        UserEvent::Registered {
            handle:       "alice".into(),
            display_name: "Alice".into(),
        },
        UserEvent::DisplayNameChanged { display_name: "Alice B.".into() },
    ] {
        state.apply(&event);
    }
    assert!(state.registered);
    assert_eq!(state.handle, "alice");
    assert_eq!(state.display_name, "Alice B.");
}

#[test]
fn post_full_fold_matches_expected_state() {
    let author = Id::new();
    let mut state = Post::default();
    for event in [
        PostEvent::Posted { author, body: "hello".into() },
        PostEvent::Deleted { by: author },
    ] {
        state.apply(&event);
    }
    assert!(state.created);
    assert!(state.deleted);
    assert_eq!(state.author, Some(author));
    assert_eq!(state.body, "hello");
}

#[test]
fn like_full_fold_alternates() {
    let mut state = Like::default();
    assert!(!state.liked);
    for (event, expected) in [
        (LikeEvent::Liked, true),
        (LikeEvent::Unliked, false),
        (LikeEvent::Liked, true),
    ] {
        state.apply(&event);
        assert_eq!(state.liked, expected);
    }
}

#[test]
fn follow_full_fold_alternates() {
    let mut state = Follow::default();
    assert!(!state.following);
    for (event, expected) in [
        (FollowEvent::Followed, true),
        (FollowEvent::Unfollowed, false),
        (FollowEvent::Followed, true),
    ] {
        state.apply(&event);
        assert_eq!(state.following, expected);
    }
}
