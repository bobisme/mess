use mess_derive::Aggregate;

#[derive(Debug, PartialEq)]
enum AccountEvent {
    Opened,
}

// `snapshot` is not a recognized `#[aggregate(...)]` key.
#[derive(Default, Aggregate)]
#[aggregate(event = AccountEvent, snapshot = true)]
struct Account {
    open: bool,
}

impl Account {
    fn apply(&mut self, _event: &AccountEvent) {}
}

fn main() {}
