use std::{
    borrow::Cow,
    num::NonZeroU16,
    sync::atomic::{AtomicU16, Ordering},
};

use ident::Id;

#[derive(Default)]
struct StrPos(AtomicU16);

impl StrPos {
    pub const fn new(val: u16) -> Self {
        Self(AtomicU16::new(val + 1))
    }

    pub const fn none() -> Self {
        Self(AtomicU16::new(0))
    }

    pub fn pos(&self) -> Option<u16> {
        let x = self.0.load(Ordering::Acquire);
        if x == 0 {
            return None;
        }
        Some(x - 1)
    }

    pub fn set(&self, val: u16) {
        self.0.store(val + 1, Ordering::SeqCst);
    }
}
impl Eq for StrPos {}
impl std::cmp::Ord for StrPos {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Clone for StrPos {
    fn clone(&self) -> Self {
        Self(AtomicU16::new(self.0.load(Ordering::SeqCst)))
    }
}

impl std::hash::Hash for StrPos {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.load(Ordering::SeqCst).hash(state);
    }
}

impl PartialOrd for StrPos {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0
            .load(Ordering::SeqCst)
            .partial_cmp(&other.0.load(Ordering::SeqCst))
    }
}

impl PartialEq for StrPos {
    fn eq(&self, other: &Self) -> bool {
        self.0.load(Ordering::SeqCst) == other.0.load(Ordering::SeqCst)
    }
}

// impl StrPos {
//     const fn new(x: u16) -> Self {
//         if x == 0 {
//             return Self::None;
//         }
//         unsafe { Self::Some(NonZeroU16::new_unchecked(x)) }
//     }
//
//     const fn to_option(self) -> Option<NonZeroU16> {
//         match self {
//             StrPos::Unset | StrPos::None => None,
//             StrPos::Some(x) => Some(x),
//         }
//     }
// }

#[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamName<'a> {
    source: Cow<'a, str>,
    id_split: StrPos,
    ex_split: StrPos,
}

impl<'a> std::hash::Hash for StreamName<'a> {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.source.hash(state);
    }
}

const fn splits(stream: &str) -> (Option<usize>, Option<usize>) {
    let id_split = konst::string::find(stream, '-');
    let ex_split = konst::string::find(stream, ':');
    match (id_split, ex_split) {
        (Some(id), Some(ex)) if ex < id => (None, Some(ex)),
        x => x,
    }
}

impl<'a> StreamName<'a> {
    #[inline]
    #[must_use]
    pub const fn new(source: Cow<'a, str>) -> Self {
        Self { source, id_split: StrPos::none(), ex_split: StrPos::none() }
    }

    #[inline]
    #[must_use]
    pub fn from_component_and_id(
        component: &'a str,
        id: Id,
        extras: Option<&'a [&'a str]>,
    ) -> Self {
        if component.is_empty() {
            return Self::new(Cow::Borrowed(component));
        }
        let source = {
            let mut source = format!("{component}-{id}");
            if let Some(extras) = extras {
                source.push_str(&extras.join(","));
            }
            source
        };
        #[allow(clippy::cast_possible_truncation)]
        let id_split = component.len() as u16;
        Self {
            source: source.into(),
            id_split: StrPos::new(id_split),
            ex_split: StrPos::none(),
        }
    }

    fn cache_splits(&self) -> (Option<NonZeroU16>, Option<NonZeroU16>) {
        #![allow(clippy::cast_possible_truncation)]
        let (id_split, ex_split) = splits(&self.source);
        let id_split = id_split.unwrap_or(0) as u16;
        let ex_split = ex_split.unwrap_or(0) as u16;
        self.id_split.set(id_split);
        self.ex_split.set(ex_split);
        (NonZeroU16::new(id_split), NonZeroU16::new(ex_split))
    }

    fn id_split(&self) -> Option<NonZeroU16> {
        match self.id_split.pos() {
            None => self.cache_splits().0,
            Some(x) => NonZeroU16::new(x),
        }
    }

    fn ex_split(&self) -> Option<NonZeroU16> {
        match self.ex_split.pos() {
            None => self.cache_splits().1,
            Some(x) => NonZeroU16::new(x),
        }
    }

    #[inline]
    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }

    #[inline]
    #[must_use]
    pub fn component(&self) -> &str {
        let id_split = self.id_split();
        let ex_split = self.ex_split();
        match (id_split, ex_split) {
            (None, None) => &self.source,
            (None, Some(ex)) => &self.source[..ex.get() as usize],
            (Some(id), _) => &self.source[..id.get() as usize],
        }
    }

    #[inline]
    #[must_use]
    pub fn id(&self) -> Option<&str> {
        let (id_split, ex_split) = (self.id_split(), self.ex_split());
        match (id_split, ex_split) {
            (None, _) => None,
            (Some(id), None) => Some(&self.source[(id.get() as usize + 1)..]),
            (Some(id), Some(ex)) => {
                Some(&self.source[id.get() as usize + 1..ex.get() as usize])
            }
        }
    }

    #[inline]
    #[must_use]
    pub fn extra(&self) -> Option<&str> {
        let ex_split = self.ex_split();
        if let Some(ex_split) = ex_split {
            Some(&self.source[ex_split.get() as usize + 1..])
        } else {
            None
        }
    }
}

// compile time tests
const _: () = {
    use konst::option::unwrap;
    let split = splits("stream-1234");
    assert!(unwrap!(split.0) == 6 && split.1.is_none());
    let split = splits("stream-1234:whatever");
    assert!(unwrap!(split.0) == 6 && unwrap!(split.1) == 11);
    let split = splits("stream");
    assert!(split.0.is_none() && split.1.is_none());
    let split = splits("stream2:whatever");
    assert!(split.0.is_none() && unwrap!(split.1) == 7);
    let split = splits("stream2:what-ever");
    assert!(split.0.is_none() && unwrap!(split.1) == 7);
};

#[cfg(test)]
mod test_streamname {
    use super::*;
    use assert2::assert;
    use rstest::*;

    #[rstest]
    #[case("stream-1234", "stream")]
    #[case("stream-1234:extra", "stream")]
    #[case("stream-12-34", "stream")]
    #[case("stream:ex-tra", "stream")]
    fn component_returns_the_component(
        #[case] input: &str,
        #[case] expected: &str,
    ) {
        let stream = Cow::Borrowed(input);
        let sn = StreamName::new(stream);
        assert!(sn.component() == expected);
    }

    #[rstest]
    #[case("stream-1234", Some("1234"))]
    #[case("stream-1234:extra", Some("1234"))]
    #[case("stream-12-34", Some("12-34"))]
    #[case("str:eam-12-34", None)]
    #[case("str:eam-12-34:ex-tra", None)]
    fn id_returns_id_if_present(
        #[case] input: &str,
        #[case] expected: Option<&str>,
    ) {
        let stream = Cow::Borrowed(input);
        let sn = StreamName::new(stream);
        assert!(sn.id() == expected);
    }

    #[rstest]
    #[case("stream-1234", None)]
    #[case("stream-1234:extra", Some("extra"))]
    #[case("stream-12-34", None)]
    #[case("str:eam-12-34", Some("eam-12-34"))]
    #[case("str:eam-12-34:ex-tra", Some("eam-12-34:ex-tra"))]
    fn extra_returns_extra_if_present(
        #[case] input: &str,
        #[case] expected: Option<&str>,
    ) {
        let stream = Cow::Borrowed(input);
        let sn = StreamName::new(stream);
        assert!(sn.extra() == expected);
    }

    #[rstest]
    #[case(("stream", Id::from_u128((1 << 100) - 1), None), "stream-zzzzzz-zzzzzzzz-zzzzzz")]
    #[case(("streamWithMoreWords", Id::from_u128(1 << 10), None), "streamWithMoreWords-000000-00000000-000100")]
    #[case(("", Id::from_u128(1 << 10), None), "")]
    fn from_component_and_entity_works(
        #[case] input: (&str, Id, Option<&[&str]>),
        #[case] expected: &str,
    ) {
        // from_component_and_entity
        let sn = StreamName::from_component_and_id(input.0, input.1, input.2);
        assert!(sn.source() == expected);
        assert!(sn.component() == input.0);
        if !sn.source().is_empty() {
            assert!(sn.id() == Some(input.1.to_string().as_str()));
        }
    }
}
