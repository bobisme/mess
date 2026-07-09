use mess_derive::Event;

// `#[derive(Event)]` is only valid on enums.
#[derive(Event)]
#[event(name = "account", version = 1)]
struct Account {
    owner: String,
}

fn main() {}
