use mess_derive::Aggregate;

// `#[derive(Aggregate)]` requires `#[aggregate(event = Type)]`.
#[derive(Default, Aggregate)]
struct Account {
    open: bool,
}

fn main() {}
