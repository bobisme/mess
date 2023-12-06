# mess_protector

This crate provides a `Protector` and `ProtectorPool`.
The protector is kind of like a RWLock, RCU, or hazard pointer.
It's used to mark some `usize` (likely an array index) as "protected."
