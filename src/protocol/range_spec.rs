use std::fmt;

use bao_tree::ChunkNum;
use range_collections::{RangeSet2, RangeSetRef};
use serde::{Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};

/// A chunk range specification.
///
/// A sequence of spans in a blob, encoded as a sequence of bao chunk offsets.
///
/// Each index in this sequence is the chunk offset of a span boundary.  The first span
/// starts at the first offset and ends at the second offset, the second span from the 3rd
/// offset to the 4th offset.  More generally spans range from offset of every odd index in
/// this sequence to offset with the next even index.  Thus an empty sequence is no spans
/// and an unclosed span indicates to the end of the blob.
///
/// Examples:
/// The range 10..33 would be encoded as `[10, 23]`
/// The empty range would be encoded as the empty array `[]`
/// A full interval .. would be encoded as `[0]`
/// A half open interval 15.. would be encoded as `[15]`
///
/// All values except for the first one must be non-zero. The first value may be zero.
/// Values are bao chunk numbers, not byte offsets.
///
/// This is a SmallVec so we can avoid allocations for the very common case of a single
/// chunk range.
#[derive(Deserialize, Serialize, PartialEq, Eq, Clone)]
#[repr(transparent)]
pub struct RangeSpec(SmallVec<[u64; 2]>);

impl RangeSpec {
    /// Creates a new range spec from a range set.
    pub fn new(ranges: impl AsRef<RangeSetRef<ChunkNum>>) -> Self {
        let ranges = ranges.as_ref().boundaries();
        let mut res = SmallVec::new();
        if let Some((start, rest)) = ranges.split_first() {
            let mut prev = start.0;
            res.push(prev);
            for v in rest {
                res.push(v.0 - prev);
                prev = v.0;
            }
        }
        Self(res)
    }

    /// An empty range spec.
    pub const EMPTY: Self = Self(SmallVec::new_const());

    /// Creates a range spec that covers the entire range.
    pub fn all() -> Self {
        Self(smallvec![0])
    }

    /// Checks if this range spec is empty.
    ///
    /// Empty range specs do not contain any spans.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Checks if this range spec is a single span containing the entire blob.
    pub fn is_all(&self) -> bool {
        self.0.len() == 1 && self.0[0] == 0
    }

    /// Converts to a range set from this range spec.
    pub fn to_chunk_ranges(&self) -> RangeSet2<ChunkNum> {
        // this is zero allocation for single ranges
        // todo: optimize this in range collections
        let mut ranges = RangeSet2::empty();
        let mut current = ChunkNum(0);
        let mut on = false;
        for &width in self.0.iter() {
            let next = current + width;
            if on {
                ranges |= RangeSet2::from(current..next);
            }
            current = next;
            on = !on;
        }
        if on {
            ranges |= RangeSet2::from(current..);
        }
        ranges
    }
}

impl fmt::Debug for RangeSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            f.debug_list()
                .entries(self.to_chunk_ranges().iter())
                .finish()
        } else if self.is_all() {
            write!(f, "all")
        } else if self.is_empty() {
            write!(f, "empty")
        } else {
            f.debug_list().entries(self.0.iter()).finish()
        }
    }
}

/// A sequence of [`RangeSpec`]s.
///
/// Like [`RangeSpec`] this is used to specify which chunk spans to retrieve, but for a
/// collection of blobs.  Each entry in the sequence is a tuple of `(count, range_spec)`:
/// for `count` blobs the spans in `range_spec` should be used.
///
/// Examples:
///
/// All child ranges: `[(0, [0])]` starting at offset 0, all offsets (see above)
/// First chunk of all children: `[(0, [0, 1])]` starting at offset 0, chunk range 0..1
/// All of child 1234: `[(1234, [0]), [1, []]]`.
/// First 33 chunks of child 5678: `[(5678, [0, 33]), (1, [])]`.
/// Chunks 10 to 30 of child 6789: `[(6789, [10, 20]), (1, [])]`.
/// No child ranges: `[]`
///
/// This is a smallvec so that we can avoid allocations in the common case of a single child
/// range.
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
#[repr(transparent)]
pub struct RangeSpecSeq(SmallVec<[(u64, RangeSpec); 2]>);

impl RangeSpecSeq {
    #[allow(dead_code)]
    /// An empty range spec sequence.
    ///
    /// When iterated, will return an empty range forever, i.e. no spans.
    pub const fn empty() -> Self {
        Self(SmallVec::new_const())
    }

    /// Returns the blob offset and [`RangeSpec`] for a single item.
    ///
    /// Only if this sequence contains a [`RangeSpec`] for a single blob will this return
    /// something.
    pub fn single(&self) -> Option<(u64, &RangeSpec)> {
        // we got two elements,
        // the first element starts at offset 0,
        // and the second element is empty
        if self.0.len() != 2 {
            return None;
        }
        let (fst_ofs, fst_val) = &self.0[0];
        let (snd_ofs, snd_val) = &self.0[1];
        if *fst_ofs == 0 && *snd_ofs == 1 && snd_val.is_empty() {
            Some((*fst_ofs, fst_val))
        } else {
            None
        }
    }

    /// A complete range spec sequence.
    ///
    /// When iterated, will return a full range forever.
    pub fn all() -> Self {
        Self(smallvec![(0, RangeSpec::all())])
    }

    /// Creates a new range spec sequence from a sequence of range sets.
    pub fn new(children: impl IntoIterator<Item = RangeSet2<ChunkNum>>) -> Self {
        let mut prev = RangeSet2::empty();
        let mut count = 0;
        let mut res = SmallVec::new();
        for v in children
            .into_iter()
            .chain(std::iter::once(RangeSet2::empty()))
        {
            if v == prev {
                count += 1;
            } else {
                res.push((count, RangeSpec::new(&v)));
                prev = v;
                count = 1;
            }
        }
        Self(res)
    }

    /// Returns an iterator of range specs.
    ///
    /// This iterator will yield the [`RangeSpec`] for each item in the sequence, expanding
    /// the compact notation.  For example if the sequence represents the blobs of a
    /// collection each time `.next()` is called the [`RangeSpec`] for the next blob is
    /// returned.
    ///
    /// The iterator does not know the size of the collection this [`RangeSpecSeq`]
    /// describes, hence the last [`RangeSpec`] in the sequence will be repeated forever.
    pub fn iter(&self) -> RangeSpecSeqIter<'_> {
        let before_first = self.0.get(0).map(|(c, _)| *c).unwrap_or_default();
        RangeSpecSeqIter {
            current: &EMPTY_RANGE_SPEC,
            count: before_first,
            remaining: &self.0,
        }
    }

    /// Returns an iterator over non empty range specs.
    ///
    /// This iterator will yield the sequence offset and [`RangeSpec`] for each item in the
    /// sequence containing a non-empty rangespec.  For example when used for a collection
    /// request this will yield each blob index in the sequence for which spans are
    /// requested.
    ///
    /// If the last range spec in the sequence contains spans, it will be repeated forever
    /// since a [`RangeSpecSeq`] does not know how many items are in a collection it
    /// describes.
    pub fn iter_non_empty(&self) -> NonEmptyRangeSpecSeqIter<'_> {
        NonEmptyRangeSpecSeqIter::new(self.iter())
    }
}

static EMPTY_RANGE_SPEC: RangeSpec = RangeSpec::EMPTY;

/// An iterator of range specs for a [`RangeSpecSeq`].
///
/// See [`RangeSpecSeq::iter`] which is used to create this iterator.
#[derive(Debug)]
pub struct RangeSpecSeqIter<'a> {
    /// current value
    current: &'a RangeSpec,
    /// number of times to emit current before grabbing next value
    /// if remaining is empty, this is ignored and current is emitted forever
    count: u64,
    /// remaining ranges
    remaining: &'a [(u64, RangeSpec)],
}

impl<'a> RangeSpecSeqIter<'a> {
    pub fn new(ranges: &'a [(u64, RangeSpec)]) -> Self {
        let before_first = ranges.get(0).map(|(c, _)| *c).unwrap_or_default();
        RangeSpecSeqIter {
            current: &EMPTY_RANGE_SPEC,
            count: before_first,
            remaining: ranges,
        }
    }

    /// True if we are at the end of the iterator
    ///
    /// This does not mean that the iterator is terminated, it just means that
    /// it will repeat the same value forever.
    pub fn is_at_end(&self) -> bool {
        self.count == 0 && self.remaining.is_empty()
    }
}

impl<'a> Iterator for RangeSpecSeqIter<'a> {
    type Item = &'a RangeSpec;

    fn next(&mut self) -> Option<Self::Item> {
        Some(loop {
            break if self.count > 0 {
                // emit current value count times
                self.count -= 1;
                self.current
            } else if let Some(((_, new), rest)) = self.remaining.split_first() {
                // get next current value, new count, and set remaining
                self.current = new;
                self.count = rest.get(0).map(|(c, _)| *c).unwrap_or_default();
                self.remaining = rest;
                continue;
            } else {
                // no more values, just repeat current forever
                self.current
            };
        })
    }
}

/// An iterator of the items of a [`RangeSpecSeq`] containing spans.
///
/// See [`RangeSpecSeq::iter_non_empty`] which is used to create this iterator.
#[derive(Debug)]
pub struct NonEmptyRangeSpecSeqIter<'a> {
    inner: RangeSpecSeqIter<'a>,
    count: u64,
}

impl<'a> NonEmptyRangeSpecSeqIter<'a> {
    fn new(inner: RangeSpecSeqIter<'a>) -> Self {
        Self { inner, count: 0 }
    }
}

impl<'a> Iterator for NonEmptyRangeSpecSeqIter<'a> {
    type Item = (u64, &'a RangeSpec);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // unwrapping is safe because we know that the inner iterator will never terminate
            let curr = self.inner.next().unwrap();
            let count = self.count;
            // increase count in any case until we are at the end of possible u64 values
            // we are unlikely to ever reach this limit.
            self.count = self.count.checked_add(1)?;
            // yield only if the current value is non-empty
            if !curr.is_empty() {
                break Some((count, curr));
            } else if self.inner.is_at_end() {
                // terminate instead of looping until we run out of u64 values
                break None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use super::*;
    use bao_tree::ChunkNum;
    use proptest::prelude::*;
    use range_collections::RangeSet2;

    fn ranges(value_range: Range<u64>) -> impl Strategy<Value = RangeSet2<ChunkNum>> {
        prop::collection::vec((value_range.clone(), value_range), 0..16).prop_map(|v| {
            let mut res = RangeSet2::empty();
            for (a, b) in v {
                let start = a.min(b);
                let end = a.max(b);
                res |= RangeSet2::from(ChunkNum(start)..ChunkNum(end));
            }
            res
        })
    }

    fn range_spec_seq_roundtrip_impl(ranges: &[RangeSet2<ChunkNum>]) -> Vec<RangeSet2<ChunkNum>> {
        let spec = RangeSpecSeq::new(ranges.iter().cloned());
        spec.iter()
            .map(|x| x.to_chunk_ranges())
            .take(ranges.len())
            .collect::<Vec<_>>()
    }

    #[test]
    fn range_spec_seq_roundtrip_cases() {
        for case in [
            vec![0..1, 0..0],
            vec![1..2, 1..2, 1..2],
            vec![1..2, 1..2, 2..3, 2..3],
        ] {
            let case = case
                .iter()
                .map(|x| RangeSet2::from(ChunkNum(x.start)..ChunkNum(x.end)))
                .collect::<Vec<_>>();
            let expected = case.clone();
            let actual = range_spec_seq_roundtrip_impl(&case);
            assert_eq!(expected, actual);
        }
    }

    proptest! {
        #[test]
        fn range_spec_roundtrip(ranges in ranges(0..1000)) {
            let spec = RangeSpec::new(&ranges);
            let ranges2 = spec.to_chunk_ranges();
            prop_assert_eq!(ranges, ranges2);
        }

        #[test]
        fn range_spec_seq_roundtrip(ranges in proptest::collection::vec(ranges(0..100), 0..10)) {
            let expected = ranges.clone();
            let actual = range_spec_seq_roundtrip_impl(&ranges);
            prop_assert_eq!(expected, actual);
        }
    }
}
