use mess_derive::Event;

// No `#[event(...)]` attribute at all.
#[derive(Event)]
enum AccountEvent {
    Opened { owner: String },
}

fn main() {}
