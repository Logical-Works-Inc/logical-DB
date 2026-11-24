//! Bounded serde decoders that cap length and count before allocation.

use lgwks_std::json::serde::{
    self, Deserialize, Deserializer,
    de::{Error as _, SeqAccess, Visitor},
};
use logical_db_model::{MAX_CHANGE_BYTES, MAX_CHANGES_PER_PAGE, MAX_PAGE_BYTES};

use crate::{ChangeEnvelope, MAX_CHECKPOINT_BYTES, PushOutcome};

/// Decodes opaque checkpoint bytes, bounding length and refusing emptiness.
pub(crate) fn deserialize_checkpoint_bytes<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let bytes = deserialize_bounded_bytes(deserializer, MAX_CHECKPOINT_BYTES, "checkpoint bytes")?;
    if bytes.is_empty() {
        return Err(D::Error::custom("checkpoint must not be empty"));
    }
    Ok(bytes)
}

/// Decodes change bytes, bounding length and refusing emptiness.
pub(crate) fn deserialize_change_bytes<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let bytes = deserialize_bounded_bytes(deserializer, MAX_CHANGE_BYTES, "change bytes")?;
    if bytes.is_empty() {
        return Err(D::Error::custom("change bytes must not be empty"));
    }
    Ok(bytes)
}

/// Decodes a page of change envelopes, capping count and aggregate bytes.
pub(crate) fn deserialize_changes<'de, D>(deserializer: D) -> Result<Vec<ChangeEnvelope>, D::Error>
where
    D: Deserializer<'de>,
{
    /// Visitor that caps the change page at its count and byte bounds.
    struct ChangesVisitor;

    impl<'de> Visitor<'de> for ChangesVisitor {
        type Value = Vec<ChangeEnvelope>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a bounded page of change envelopes")
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut changes = Vec::new();
            if let Some(hint) = sequence.size_hint() {
                changes.reserve(hint.min(MAX_CHANGES_PER_PAGE));
            }
            let mut aggregate_bytes = 0usize;
            while let Some(change) = sequence.next_element::<ChangeEnvelope>()? {
                if changes.len() == MAX_CHANGES_PER_PAGE {
                    return Err(A::Error::custom("change page count exceeds limit"));
                }
                aggregate_bytes = aggregate_bytes
                    .checked_add(change.bytes.len())
                    .ok_or_else(|| A::Error::custom("change page byte count overflow"))?;
                if aggregate_bytes > MAX_PAGE_BYTES {
                    return Err(A::Error::custom("change page bytes exceed limit"));
                }
                changes.push(change);
            }
            Ok(changes)
        }
    }

    deserializer.deserialize_seq(ChangesVisitor)
}

/// Decodes a bounded push-outcome list.
pub(crate) fn deserialize_outcomes<'de, D>(deserializer: D) -> Result<Vec<PushOutcome>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec(deserializer, MAX_CHANGES_PER_PAGE, "push outcomes")
}

/// Decodes byte content through any serde byte representation, bounding it to
/// `maximum`.
pub(crate) fn deserialize_bounded_bytes<'de, D>(
    deserializer: D,
    maximum: usize,
    expecting: &'static str,
) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    /// Visitor that bounds every byte representation to the configured maximum.
    struct BytesVisitor {
        /// Maximum byte length the visitor will accept.
        maximum: usize,
        /// Human-readable description used in decode errors.
        expecting: &'static str,
    }

    impl<'de> Visitor<'de> for BytesVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str(self.expecting)
        }

        fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if value.len() > self.maximum {
                return Err(E::invalid_length(value.len(), &self));
            }
            Ok(value.to_vec())
        }

        fn visit_borrowed_bytes<E>(self, value: &'de [u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.visit_bytes(value)
        }

        fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if value.len() > self.maximum {
                return Err(E::invalid_length(value.len(), &self));
            }
            Ok(value)
        }

        fn visit_seq<A>(self, sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            collect_bounded(sequence, self.maximum, &self)
        }
    }

    let visitor = BytesVisitor { maximum, expecting };
    deserializer.deserialize_byte_buf(visitor)
}

/// Collects at most `maximum` sequence items, refusing the first excess item.
pub(crate) fn deserialize_bounded_vec<'de, D, T>(
    deserializer: D,
    maximum: usize,
    expecting: &'static str,
) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    /// Visitor that caps a sequence at its configured item bound.
    struct BoundedVecVisitor<T> {
        /// Maximum number of items the sequence may yield.
        maximum: usize,
        /// Human-readable description used in decode errors.
        expecting: &'static str,
        /// Type marker for the deserialized element.
        marker: std::marker::PhantomData<T>,
    }

    impl<'de, T> Visitor<'de> for BoundedVecVisitor<T>
    where
        T: Deserialize<'de>,
    {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str(self.expecting)
        }

        fn visit_seq<A>(self, sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            collect_bounded(sequence, self.maximum, &self)
        }
    }

    deserializer.deserialize_seq(BoundedVecVisitor {
        maximum,
        expecting,
        marker: std::marker::PhantomData,
    })
}

/// Collects at most `maximum` items from `sequence`, refusing the first excess
/// item and reserving only what the sender's hint justifies within the bound.
fn collect_bounded<'de, A, T>(
    mut sequence: A,
    maximum: usize,
    expected: &dyn serde::de::Expected,
) -> Result<Vec<T>, A::Error>
where
    A: SeqAccess<'de>,
    T: Deserialize<'de>,
{
    let mut values = Vec::new();
    if let Some(hint) = sequence.size_hint() {
        values.reserve(hint.min(maximum));
    }
    while let Some(value) = sequence.next_element()? {
        if values.len() == maximum {
            return Err(A::Error::invalid_length(
                maximum.saturating_add(1),
                expected,
            ));
        }
        values.push(value);
    }
    Ok(values)
}
