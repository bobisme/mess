use mess_derive::Event;

// The `#[event(...)]` attribute is present but omits the required `name` key.
#[derive(Event)]
#[event(version = 1)]
enum AccountEvent {
    Opened { owner: String },
}

fn main() {}
