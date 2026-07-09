use mess_derive::Event;

// `version` must be an integer literal, not a string.
#[derive(Event)]
#[event(name = "account", version = "oops")]
enum AccountEvent {
    Opened { owner: String },
}

fn main() {}
