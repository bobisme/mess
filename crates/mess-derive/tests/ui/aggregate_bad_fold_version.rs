use mess_derive::Aggregate;

#[derive(Debug, PartialEq)]
enum AccountEvent {
    Opened,
}

// `fold_version` must be an integer literal, not a string.
#[derive(Default, Aggregate)]
#[aggregate(event = AccountEvent, fold_version = "oops")]
struct Account {
    open: bool,
}

impl Account {
    fn apply(&mut self, _event: &AccountEvent) {}
}

fn main() {}
