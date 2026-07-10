//! Given-When-Then specs for the social domain via `mess-testkit`'s
//! store-free [`AggregateTest`] — one test per **accept** and per
//! **rejection** of every command on both aggregates. Read these as the
//! executable specification of the rules documented in `src/domain/*`.
//!
//! See `examples/bank/tests/gwt.rs` for the first walkthrough of this kit.
//!
//! Run with:
//!
//! ```sh
//! cargo test -p social
//! ```

use ident::Id;
use mess_core::Actor;
use mess_testkit::{AggregateTest, matching};
use social::domain::post::{
    CreatePost, DeletePost, Like, Post, PostError, PostEvent, Unlike,
};
use social::domain::user::{
    Follow, RegisterUser, SetDisplayName, Unfollow, User, UserError, UserEvent,
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
// User: Follow
// ===========================================================================

#[test]
fn follow_emits_followed() {
    let alice = Id::new();
    let bob = Id::new();
    AggregateTest::<User>::given([UserEvent::Registered {
        handle:       "alice".into(),
        display_name: "Alice".into(),
    }])
    .when(Follow { follower: alice, target: bob })
    .then_events([UserEvent::Followed { target: bob }]);
}

#[test]
fn cannot_follow_when_unregistered() {
    let alice = Id::new();
    let bob = Id::new();
    AggregateTest::<User>::given_no_events()
        .when(Follow { follower: alice, target: bob })
        .then_error(UserError::NotRegistered);
}

#[test]
fn cannot_follow_self() {
    let alice = Id::new();
    AggregateTest::<User>::given([UserEvent::Registered {
        handle:       "alice".into(),
        display_name: "Alice".into(),
    }])
    .when(Follow { follower: alice, target: alice })
    .then_error(UserError::SelfFollow);
}

#[test]
fn follow_declares_its_follower_stream() {
    // bn-2i3: the echoed `follower` id is now the command's declared *actor
    // stream*, built from the same `user_stream` helper the writer dispatches
    // with — so `EventStore::command_as` can assert the two agree instead of
    // trusting the restated id.
    let alice = Id::new();
    let bob = Id::new();
    assert_eq!(
        Follow { follower: alice, target: bob }.actor_stream(),
        social::user_stream(alice),
    );
    // A divergent restating (some *other* follower) resolves to a different
    // stream — precisely the mismatch the authored command path refuses.
    assert_ne!(
        Follow { follower: bob, target: alice }.actor_stream(),
        social::user_stream(alice),
    );
}

#[test]
fn cannot_follow_twice() {
    let alice = Id::new();
    let bob = Id::new();
    AggregateTest::<User>::given([
        UserEvent::Registered {
            handle:       "alice".into(),
            display_name: "Alice".into(),
        },
        UserEvent::Followed { target: bob },
    ])
    .when(Follow { follower: alice, target: bob })
    .then_error(UserError::AlreadyFollowing);
}

// ===========================================================================
// User: Unfollow
// ===========================================================================

#[test]
fn unfollow_emits_unfollowed() {
    let bob = Id::new();
    AggregateTest::<User>::given([
        UserEvent::Registered {
            handle:       "alice".into(),
            display_name: "Alice".into(),
        },
        UserEvent::Followed { target: bob },
    ])
    .when(Unfollow { target: bob })
    .then_events([UserEvent::Unfollowed { target: bob }]);
}

#[test]
fn cannot_unfollow_when_unregistered() {
    AggregateTest::<User>::given_no_events()
        .when(Unfollow { target: Id::new() })
        .then_error(UserError::NotRegistered);
}

#[test]
fn cannot_unfollow_when_not_following() {
    let bob = Id::new();
    AggregateTest::<User>::given([UserEvent::Registered {
        handle:       "alice".into(),
        display_name: "Alice".into(),
    }])
    .when(Unfollow { target: bob })
    .then_error(UserError::NotFollowing);
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
// Post: Like
// ===========================================================================

#[test]
fn like_emits_liked() {
    let author = Id::new();
    let liker = Id::new();
    AggregateTest::<Post>::given([PostEvent::Posted {
        author,
        body: "hello".into(),
    }])
    .when(Like { user: liker })
    .then_events([PostEvent::Liked { user: liker }]);
}

#[test]
fn author_may_like_own_post() {
    // Decision (see post.rs): self-like IS allowed.
    let author = Id::new();
    AggregateTest::<Post>::given([PostEvent::Posted {
        author,
        body: "hello".into(),
    }])
    .when(Like { user: author })
    .then_events([PostEvent::Liked { user: author }]);
}

#[test]
fn cannot_like_post_that_was_never_created() {
    AggregateTest::<Post>::given_no_events()
        .when(Like { user: Id::new() })
        .then_error(PostError::NotCreated);
}

#[test]
fn cannot_like_deleted_post() {
    let author = Id::new();
    let liker = Id::new();
    AggregateTest::<Post>::given([
        PostEvent::Posted { author, body: "hello".into() },
        PostEvent::Deleted { by: author },
    ])
    .when(Like { user: liker })
    .then_error(PostError::LikeOnDeleted);
}

#[test]
fn cannot_like_twice() {
    let author = Id::new();
    let liker = Id::new();
    AggregateTest::<Post>::given([
        PostEvent::Posted { author, body: "hello".into() },
        PostEvent::Liked { user: liker },
    ])
    .when(Like { user: liker })
    .then_error(PostError::AlreadyLiked);
}

// ===========================================================================
// Post: Unlike
// ===========================================================================

#[test]
fn unlike_emits_unliked() {
    let author = Id::new();
    let liker = Id::new();
    AggregateTest::<Post>::given([
        PostEvent::Posted { author, body: "hello".into() },
        PostEvent::Liked { user: liker },
    ])
    .when(Unlike { user: liker })
    .then_events([PostEvent::Unliked { user: liker }]);
}

#[test]
fn cannot_unlike_post_that_was_never_created() {
    AggregateTest::<Post>::given_no_events()
        .when(Unlike { user: Id::new() })
        .then_error(PostError::NotCreated);
}

#[test]
fn cannot_unlike_when_not_liked() {
    let author = Id::new();
    let liker = Id::new();
    AggregateTest::<Post>::given([PostEvent::Posted {
        author,
        body: "hello".into(),
    }])
    .when(Unlike { user: liker })
    .then_error(PostError::NotLiked);
}

// ===========================================================================
// Full-fold sanity: replaying a whole history yields the expected state.
// ===========================================================================

#[test]
fn user_full_fold_matches_expected_state() {
    let bob = Id::new();
    let carol = Id::new();
    let mut state = User::default();
    for event in [
        UserEvent::Registered {
            handle:       "alice".into(),
            display_name: "Alice".into(),
        },
        UserEvent::DisplayNameChanged { display_name: "Alice B.".into() },
        UserEvent::Followed { target: bob },
        UserEvent::Followed { target: carol },
        UserEvent::Unfollowed { target: bob },
    ] {
        state.apply(&event);
    }
    assert!(state.registered);
    assert_eq!(state.handle, "alice");
    assert_eq!(state.display_name, "Alice B.");
    assert!(state.following.contains(&carol));
    assert!(!state.following.contains(&bob));
    assert_eq!(state.following.len(), 1);
}

#[test]
fn post_full_fold_matches_expected_state() {
    let author = Id::new();
    let liker = Id::new();
    let mut state = Post::default();
    for event in [
        PostEvent::Posted { author, body: "hello".into() },
        PostEvent::Liked { user: liker },
        PostEvent::Liked { user: author },
        PostEvent::Unliked { user: liker },
    ] {
        state.apply(&event);
    }
    assert!(state.created);
    assert!(!state.deleted);
    assert_eq!(state.author, Some(author));
    assert_eq!(state.body, "hello");
    assert!(state.likes.contains(&author));
    assert!(!state.likes.contains(&liker));
    assert_eq!(state.likes.len(), 1);
}
