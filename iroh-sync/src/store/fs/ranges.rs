//! Ranges and helpers for working with [`redb`] tables

use redb::{Key, Range, ReadOnlyTable, ReadTransaction, Value};

use crate::{store::SortDirection, SignedEntry};

use super::{
    bounds::{ByKeyBounds, RecordsBounds},
    into_entry, RecordsByKeyId, RecordsId, RecordsValue, RECORDS_BY_KEY_TABLE, RECORDS_TABLE,
};

/// An extension trait for [`Range`] that provides methods for mapped retrieval.
pub trait RangeExt<K: Key, V: Value> {
    /// Get the next entry and map the item with a callback function.
    fn next_map<T>(
        &mut self,
        map: impl for<'x> Fn(K::SelfType<'x>, V::SelfType<'x>) -> T,
    ) -> Option<anyhow::Result<T>>;

    /// Get the next entry, but only if the callback function returns Some, otherwise continue.
    ///
    /// With `direction` the range can be either process in forward or backward direction.
    fn next_filter_map<T>(
        &mut self,
        direction: &SortDirection,
        filter_map: impl for<'x> Fn(K::SelfType<'x>, V::SelfType<'x>) -> Option<T>,
    ) -> Option<anyhow::Result<T>>;

    /// Like [`Self::next_filter_map`], but the callback returns a `Result`, and the result is
    /// flattened with the result from the range operation.
    fn next_try_filter_map<T>(
        &mut self,
        direction: &SortDirection,
        filter_map: impl for<'x> Fn(K::SelfType<'x>, V::SelfType<'x>) -> Option<anyhow::Result<T>>,
    ) -> Option<anyhow::Result<T>> {
        Some(self.next_filter_map(direction, filter_map)?.and_then(|r| r))
    }
}

impl<K: Key + 'static, V: Value + 'static> RangeExt<K, V> for Range<'static, K, V> {
    fn next_map<T>(
        &mut self,
        map: impl for<'x> Fn(K::SelfType<'x>, V::SelfType<'x>) -> T,
    ) -> Option<anyhow::Result<T>> {
        self.next()
            .map(|r| r.map_err(Into::into).map(|r| map(r.0.value(), r.1.value())))
    }

    fn next_filter_map<T>(
        &mut self,
        direction: &SortDirection,
        filter_map: impl for<'x> Fn(K::SelfType<'x>, V::SelfType<'x>) -> Option<T>,
    ) -> Option<anyhow::Result<T>> {
        loop {
            let next = match direction {
                SortDirection::Asc => self.next(),
                SortDirection::Desc => self.next_back(),
            };
            match next {
                None => break None,
                Some(Err(err)) => break Some(Err(err.into())),
                Some(Ok(res)) => match filter_map(res.0.value(), res.1.value()) {
                    None => continue,
                    Some(item) => break Some(Ok(item)),
                },
            }
        }
    }
}

/// An iterator over a range of entries from the records table.
#[derive(derive_more::Debug)]
#[debug("RecordsRange")]
pub struct RecordsRange(Range<'static, RecordsId<'static>, RecordsValue<'static>>);

impl RecordsRange {
    pub(super) fn all(read_tx: &ReadTransaction) -> anyhow::Result<Self> {
        let table = read_tx.open_table(RECORDS_TABLE)?;
        let range = table.range::<RecordsId<'static>>(..)?;
        Ok(Self(range))
    }

    pub(super) fn with_bounds(
        read_tx: &ReadTransaction,
        bounds: RecordsBounds,
    ) -> anyhow::Result<Self> {
        let table = read_tx.open_table(RECORDS_TABLE)?;
        let range = table.range(bounds.as_ref())?;
        Ok(Self(range))
    }

    /// Get the next item in the range.
    ///
    /// Omit items for which the `matcher` function returns false.
    pub(super) fn next_filtered(
        &mut self,
        direction: &SortDirection,
        filter: impl for<'x> Fn(RecordsId<'x>, RecordsValue<'x>) -> bool,
    ) -> Option<anyhow::Result<SignedEntry>> {
        self.0
            .next_filter_map(direction, |k, v| filter(k, v).then(|| into_entry(k, v)))
    }

    pub(super) fn next_map<T>(
        &mut self,
        map: impl for<'x> Fn(RecordsId<'x>, RecordsValue<'x>) -> T,
    ) -> Option<anyhow::Result<T>> {
        self.0.next_map(map)
    }
}

impl Iterator for RecordsRange {
    type Item = anyhow::Result<SignedEntry>;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next_map(into_entry)
    }
}

#[derive(derive_more::Debug)]
#[debug("RecordsByKeyRange")]
pub struct RecordsByKeyRange {
    records_table: ReadOnlyTable<RecordsId<'static>, RecordsValue<'static>>,
    by_key_range: Range<'static, RecordsByKeyId<'static>, ()>,
}

impl RecordsByKeyRange {
    pub fn with_bounds(read_tx: &ReadTransaction, bounds: ByKeyBounds) -> anyhow::Result<Self> {
        let records_table = read_tx.open_table(RECORDS_TABLE).map_err(anyhow_err)?;
        let by_key_table = read_tx
            .open_table(RECORDS_BY_KEY_TABLE)
            .map_err(anyhow_err)?;
        let by_key_range = by_key_table.range(bounds.as_ref())?;
        Ok(Self {
            records_table,
            by_key_range,
        })
    }

    /// Get the next item in the range.
    ///
    /// Omit items for which the `filter` function returns false.
    pub fn next_filtered(
        &mut self,
        direction: &SortDirection,
        filter: impl for<'x> Fn(RecordsByKeyId<'x>) -> bool,
    ) -> Option<anyhow::Result<SignedEntry>> {
        let entry = self.by_key_range.next_try_filter_map(direction, |k, _v| {
            if !filter(k) {
                return None;
            };
            let (namespace, key, author) = k;
            let records_id = (namespace, author, key);
            let entry = self.records_table.get(&records_id).transpose()?;
            let entry = entry
                .map(|value| into_entry(records_id, value.value()))
                .map_err(anyhow::Error::from);
            Some(entry)
        });
        entry
    }
}

fn anyhow_err(err: impl Into<anyhow::Error>) -> anyhow::Error {
    err.into()
}
