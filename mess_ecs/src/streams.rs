use std::{borrow::Cow, cell::Cell, num::NonZeroU16};

#[derive(Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum StrPos {
    #[default]
    Unset,
    None,
    Some(NonZeroU16),
}

impl StrPos {
    const fn new(x: u16) -> Self {
        if x == 0 {
            return Self::None;
        }
        unsafe { Self::Some(NonZeroU16::new_unchecked(x)) }
    }

    const fn to_option(self) -> Option<NonZeroU16> {
        match self {
            StrPos::Unset | StrPos::None => None,
            StrPos::Some(x) => Some(x),
        }
    }
}

#[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamName<'a> {
    source: Cow<'a, str>,
    id_split: Cell<StrPos>,
    ex_split: Cell<StrPos>,
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
        Self {
            source,
            id_split: Cell::new(StrPos::Unset),
            ex_split: Cell::new(StrPos::Unset),
        }
    }

    fn cache_splits(&self) -> (Option<NonZeroU16>, Option<NonZeroU16>) {
        #![allow(clippy::cast_possible_truncation)]
        let (id_split, ex_split) = splits(&self.source);
        let id_split = id_split.map_or(StrPos::None, |x| StrPos::new(x as u16));
        let ex_split = ex_split.map_or(StrPos::None, |x| StrPos::new(x as u16));
        self.id_split.set(id_split);
        self.ex_split.set(ex_split);
        (id_split.to_option(), ex_split.to_option())
    }

    fn id_split(&self) -> Option<NonZeroU16> {
        match self.id_split.get() {
            StrPos::Unset => {
                let (id_split, _) = self.cache_splits();
                id_split
            }
            other => other.to_option(),
        }
    }

    fn ex_split(&self) -> Option<NonZeroU16> {
        match self.ex_split.get() {
            StrPos::Unset => {
                let (_, ex_split) = self.cache_splits();
                ex_split
            }
            x => x.to_option(),
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
}
