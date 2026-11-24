//! Read-only and editing views over one convergent document.

use automerge::{AutoCommit, ObjType, ROOT, ReadDoc, ScalarValue, transaction::Transactable};
use logical_db_model::MAX_CHANGE_BYTES;

use crate::{
    ConflictValue, CrdtError, Item, Object, ObjectKind, Scalar, TOMBSTONE_PROPERTY, item_from,
    validate_property, validate_scalar, validate_writable_property,
};

/// Read-only document view used by application projections.
///
/// ```compile_fail
/// use logical_db_crdt::Document;
/// let mut document = Document::new();
/// let mut reader = document.reader();
/// reader.edit(|_| Ok(()));
/// ```
pub struct Reader<'document> {
    /// Borrowed document that exposes only read operations.
    pub(crate) document: &'document mut AutoCommit,
}

impl std::fmt::Debug for Reader<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Reader").finish_non_exhaustive()
    }
}

impl Reader<'_> {
    /// Reads one scalar or object from a map property.
    ///
    /// # Errors
    ///
    /// Returns a property, object, or Automerge access failure.
    pub fn get(&mut self, object: &Object, property: &str) -> Result<Option<Item>, CrdtError> {
        validate_property(property)?;
        Ok(self
            .document
            .get(&object.0, property)?
            .map(|(value, object)| item_from(value, object)))
    }

    /// Reads one visible list or text-sequence value by index.
    ///
    /// # Errors
    ///
    /// Returns an object, index, or Automerge access failure.
    pub fn get_index(&mut self, object: &Object, index: usize) -> Result<Option<Item>, CrdtError> {
        Ok(self
            .document
            .get(&object.0, index)?
            .map(|(value, object)| item_from(value, object)))
    }

    /// Returns every causally concurrent value for a map property.
    ///
    /// A non-conflicting property returns zero or one value. Callers that show
    /// user-visible state can surface or reconcile multiple values explicitly.
    ///
    /// # Errors
    ///
    /// Returns a property, object, or Automerge access failure.
    pub fn conflicts(
        &mut self,
        object: &Object,
        property: &str,
    ) -> Result<Vec<ConflictValue>, CrdtError> {
        validate_property(property)?;
        Ok(self
            .document
            .get_all(&object.0, property)?
            .into_iter()
            .map(|(value, operation)| ConflictValue {
                value: item_from(value, operation.clone()),
                causal_id: operation.to_bytes(),
            })
            .collect())
    }

    /// Returns every causally concurrent value at one list or text index.
    ///
    /// # Errors
    ///
    /// Returns an object, index, or Automerge access failure.
    pub fn conflicts_at(
        &mut self,
        object: &Object,
        index: usize,
    ) -> Result<Vec<ConflictValue>, CrdtError> {
        Ok(self
            .document
            .get_all(&object.0, index)?
            .into_iter()
            .map(|(value, operation)| ConflictValue {
                value: item_from(value, operation.clone()),
                causal_id: operation.to_bytes(),
            })
            .collect())
    }

    /// Lists the visible keys of a map or visible element identifiers of a sequence.
    #[must_use]
    pub fn keys(&mut self, object: &Object) -> Vec<String> {
        self.document.keys(&object.0).collect()
    }

    /// Returns the visible length of a map, list, or text object.
    #[must_use]
    pub fn len(&mut self, object: &Object) -> usize {
        self.document.length(&object.0)
    }

    /// Returns whether a map, list, or text object has no visible entries.
    #[must_use]
    pub fn is_empty(&mut self, object: &Object) -> bool {
        self.len(object) == 0
    }

    /// Materializes a collaborative text object as UTF-8.
    ///
    /// # Errors
    ///
    /// Returns an object-kind or Automerge access failure.
    pub fn text(&mut self, object: &Object) -> Result<String, CrdtError> {
        Ok(self.document.text(&object.0)?)
    }

    /// Returns whether a tombstone marker is present on the read document.
    ///
    /// Conflict candidates stay inspectable through [`Reader::conflicts`]
    /// after deletion; this flag tells the caller above the boundary to
    /// report the document absent instead.
    ///
    /// # Errors
    ///
    /// Returns an Automerge access failure.
    pub fn is_deleted(&mut self) -> Result<bool, CrdtError> {
        Ok(self.document.get(&ROOT, TOMBSTONE_PROPERTY)?.is_some())
    }
}

/// Mutable view whose lifetime cannot outlive its document edit.
pub struct Editor<'document> {
    /// Borrowed document receiving the staged mutations.
    pub(crate) document: &'document mut AutoCommit,
}

impl std::fmt::Debug for Editor<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Editor").finish_non_exhaustive()
    }
}

impl Editor<'_> {
    /// Sets a scalar property on a map-like object.
    ///
    /// # Errors
    ///
    /// Returns a property, numeric portability, object, or Automerge failure.
    pub fn put(&mut self, object: &Object, property: &str, value: Scalar) -> Result<(), CrdtError> {
        validate_property(property)?;
        validate_writable_property(property)?;
        validate_scalar(&value)?;
        self.document
            .put(&object.0, property, ScalarValue::from(value))?;
        Ok(())
    }

    /// Deletes a property. Repeating the deletion is harmless.
    ///
    /// # Errors
    ///
    /// Returns a property, object, or Automerge failure.
    pub fn delete(&mut self, object: &Object, property: &str) -> Result<(), CrdtError> {
        validate_property(property)?;
        validate_writable_property(property)?;
        self.document.delete(&object.0, property)?;
        Ok(())
    }

    /// Creates or replaces a nested map, list, or text object.
    ///
    /// # Errors
    ///
    /// Returns a property, object, or Automerge failure.
    pub fn put_object(
        &mut self,
        object: &Object,
        property: &str,
        kind: ObjectKind,
    ) -> Result<Object, CrdtError> {
        validate_property(property)?;
        validate_writable_property(property)?;
        Ok(Object(self.document.put_object(
            &object.0,
            property,
            ObjType::from(kind),
        )?))
    }

    /// Inserts a scalar into a list.
    ///
    /// # Errors
    ///
    /// Returns a numeric portability, list-index, object, or Automerge failure.
    pub fn insert(&mut self, list: &Object, index: usize, value: Scalar) -> Result<(), CrdtError> {
        validate_scalar(&value)?;
        self.document
            .insert(&list.0, index, ScalarValue::from(value))?;
        Ok(())
    }

    /// Replaces a visible scalar at one list index.
    ///
    /// # Errors
    ///
    /// Returns a numeric portability, index, object, or Automerge failure.
    pub fn put_index(
        &mut self,
        list: &Object,
        index: usize,
        value: Scalar,
    ) -> Result<(), CrdtError> {
        validate_scalar(&value)?;
        self.document
            .put(&list.0, index, ScalarValue::from(value))?;
        Ok(())
    }

    /// Inserts a nested map, list, or text object into a list.
    ///
    /// # Errors
    ///
    /// Returns an index, object, or Automerge failure.
    pub fn insert_object(
        &mut self,
        list: &Object,
        index: usize,
        kind: ObjectKind,
    ) -> Result<Object, CrdtError> {
        Ok(Object(self.document.insert_object(
            &list.0,
            index,
            ObjType::from(kind),
        )?))
    }

    /// Deletes one visible list or text-sequence value.
    ///
    /// # Errors
    ///
    /// Returns an index, object, or Automerge failure.
    pub fn delete_index(&mut self, object: &Object, index: usize) -> Result<(), CrdtError> {
        self.document.delete(&object.0, index)?;
        Ok(())
    }

    /// Adds a signed delta to a map counter.
    ///
    /// # Errors
    ///
    /// Returns a property, type, object, or Automerge failure.
    pub fn increment(
        &mut self,
        object: &Object,
        property: &str,
        delta: i64,
    ) -> Result<(), CrdtError> {
        validate_property(property)?;
        validate_writable_property(property)?;
        self.document.increment(&object.0, property, delta)?;
        Ok(())
    }

    /// Adds a signed delta to a list counter.
    ///
    /// # Errors
    ///
    /// Returns an index, type, object, or Automerge failure.
    pub fn increment_index(
        &mut self,
        list: &Object,
        index: usize,
        delta: i64,
    ) -> Result<(), CrdtError> {
        self.document.increment(&list.0, index, delta)?;
        Ok(())
    }

    /// Replaces a bounded range of collaborative text using Unicode code-point offsets.
    ///
    /// # Errors
    ///
    /// Returns a text-size, range, object, or Automerge failure.
    pub fn splice_text(
        &mut self,
        text: &Object,
        position: usize,
        delete: usize,
        replacement: &str,
    ) -> Result<(), CrdtError> {
        if replacement.len() > MAX_CHANGE_BYTES {
            return Err(CrdtError::ScalarTooLarge(replacement.len()));
        }
        let delete = isize::try_from(delete).map_err(CrdtError::SequenceWidth)?;
        self.document
            .splice_text(&text.0, position, delete, replacement)?;
        Ok(())
    }
}
