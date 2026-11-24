//! Document-editing journey: every editor and reader operation of the product
//! document API, driven as a client would drive it.
//!
//! One replica file takes a single edit that uses every `Editor` operation,
//! is reopened, and is read back through every `Reader` operation. Concurrent
//! edits on two forks of one document are then merged, and both conflict
//! views report both candidates with their causal identities.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::{
    fmt, fs,
    io::{self, Write},
    path::Path,
};

use logical_db::{
    AccountId, CrdtError, Database, DatabaseError, DocumentId, Editor, Item, Object, ObjectKind,
    OperationDigest, OperationId, Reader, ReplicaId, Scalar, Scope, SpaceId,
};
use logical_db_crdt::{ConflictValue, Document};

/// Why the document journey failed, one variant per layer it crosses.
#[derive(Debug)]
enum JourneyError {
    /// The replica file or the receipt could not be handled.
    Io(io::Error),
    /// The replica refused an open, edit or read.
    Database(DatabaseError),
    /// The document refused an edit, a read or an exchange of changes.
    Crdt(CrdtError),
    /// An observed value disagreed with the expected one; names the check.
    Mismatch(&'static str),
}

impl fmt::Display for JourneyError {
    /// Names the failing layer and its cause for the journey log.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Io(ref error) => write!(formatter, "journey I/O failed: {error}"),
            Self::Database(ref error) => write!(formatter, "journey replica failed: {error}"),
            Self::Crdt(ref error) => write!(formatter, "journey document failed: {error}"),
            Self::Mismatch(check) => write!(formatter, "journey check failed: {check}"),
        }
    }
}

impl From<io::Error> for JourneyError {
    /// Adapts an I/O failure without discarding its kind.
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<DatabaseError> for JourneyError {
    /// Adapts a replica refusal without discarding its variant.
    fn from(error: DatabaseError) -> Self {
        Self::Database(error)
    }
}

impl From<CrdtError> for JourneyError {
    /// Adapts a document refusal without discarding its variant.
    fn from(error: CrdtError) -> Self {
        Self::Crdt(error)
    }
}

/// Everything the journey reads back from the edited document.
#[derive(Debug, PartialEq)]
struct Readback {
    /// Visible keys of the root map, sorted.
    root_keys: Vec<String>,
    /// Visible keys of the nested profile map.
    profile_keys: Vec<String>,
    /// The profile name.
    name: Option<Item>,
    /// The map counter after its increment.
    visits: Option<Item>,
    /// Visible length of the tag list.
    tag_count: usize,
    /// First and second tag, then the index past the end.
    tags: [Option<Item>; 3],
    /// The list counter after its increment.
    score: Option<Item>,
    /// The integer inside the map nested in the score list.
    level: Option<Item>,
    /// The collaborative text, materialized.
    text: String,
    /// Visible length of the text object.
    text_len: usize,
}

/// A string scalar item.
fn string_item(text: &str) -> Item {
    Item::Scalar(Scalar::String(text.to_owned()))
}

/// The root property `property` as a nested object handle, when it is one.
fn nested(reader: &mut Reader<'_>, property: &str) -> Result<Option<Object>, CrdtError> {
    Ok(match reader.get(&Object::root(), property)? {
        Some(Item::Object { handle, .. }) => Some(handle),
        _ => None,
    })
}

/// Stages one edit that uses every `Editor` operation.
fn write_document(database: &Database, document: DocumentId) -> Result<(), JourneyError> {
    database.edit(
        document,
        OperationId::new(),
        OperationDigest::new([0xd1; 32]),
        |editor: &mut Editor<'_>| {
            let root = Object::root();
            let profile = editor.put_object(&root, "profile", ObjectKind::Map)?;
            editor.put(&profile, "name", Scalar::String("ada".to_owned()))?;
            editor.put(&root, "visits", Scalar::Counter(0))?;
            editor.increment(&root, "visits", 3)?;
            let tags = editor.put_object(&root, "tags", ObjectKind::List)?;
            for (index, tag) in ["alpha", "beta", "gamma"].into_iter().enumerate() {
                editor.insert(&tags, index, Scalar::String(tag.to_owned()))?;
            }
            editor.put_index(&tags, 1, Scalar::String("BETA".to_owned()))?;
            editor.delete_index(&tags, 2)?;
            let scores = editor.put_object(&root, "scores", ObjectKind::List)?;
            editor.insert(&scores, 0, Scalar::Counter(10))?;
            editor.increment_index(&scores, 0, 5)?;
            let level = editor.insert_object(&scores, 1, ObjectKind::Map)?;
            editor.put(&level, "level", Scalar::Integer(2))?;
            let body = editor.put_object(&root, "body", ObjectKind::Text)?;
            editor.splice_text(&body, 0, 0, "hello world")?;
            editor.splice_text(&body, 6, 5, "there")
        },
    )?;
    Ok(())
}

/// Reads the edited document back through every `Reader` operation.
fn read_back(reader: &mut Reader<'_>) -> Result<Option<Readback>, CrdtError> {
    let (Some(profile), Some(tags), Some(scores), Some(body)) = (
        nested(reader, "profile")?,
        nested(reader, "tags")?,
        nested(reader, "scores")?,
        nested(reader, "body")?,
    ) else {
        return Ok(None);
    };
    let level = match reader.get_index(&scores, 1)? {
        Some(Item::Object { handle, .. }) => reader.get(&handle, "level")?,
        _ => None,
    };
    let mut root_keys = reader.keys(&Object::root());
    root_keys.sort();
    Ok(Some(Readback {
        root_keys,
        profile_keys: reader.keys(&profile),
        name: reader.get(&profile, "name")?,
        visits: reader.get(&Object::root(), "visits")?,
        tag_count: reader.len(&tags),
        tags: [
            reader.get_index(&tags, 0)?,
            reader.get_index(&tags, 1)?,
            reader.get_index(&tags, 2)?,
        ],
        score: reader.get_index(&scores, 0)?,
        level,
        text: reader.text(&body)?,
        text_len: reader.len(&body),
    }))
}

/// What `write_document` must leave behind.
fn expected_readback() -> Readback {
    Readback {
        root_keys: ["body", "profile", "scores", "tags", "visits"]
            .map(str::to_owned)
            .to_vec(),
        profile_keys: vec!["name".to_owned()],
        name: Some(string_item("ada")),
        visits: Some(Item::Scalar(Scalar::Counter(3))),
        tag_count: 2,
        tags: [Some(string_item("alpha")), Some(string_item("BETA")), None],
        score: Some(Item::Scalar(Scalar::Counter(15))),
        level: Some(Item::Scalar(Scalar::Integer(2))),
        text: "hello there".to_owned(),
        text_len: 11,
    }
}

/// Edits through every editor operation, reopens the replica and reads it back.
fn edited_replica(scope: Scope, replica: ReplicaId, path: &Path) -> Result<(), JourneyError> {
    let document = DocumentId::new();
    let database = Database::open(path, scope, replica)?;
    write_document(&database, document)?;
    drop(database);
    let reopened = Database::open(path, scope, replica)?;
    let observed = reopened
        .read(document, read_back)?
        .flatten()
        .ok_or(JourneyError::Mismatch("the edited document did not reopen"))?;
    if observed != expected_readback() {
        return Err(JourneyError::Mismatch(
            "the reopened document differs from what was written",
        ));
    }
    Ok(())
}

/// Splits conflict candidates into their values and causal identities.
fn candidates(conflicts: Vec<ConflictValue>) -> (Vec<Item>, Vec<Vec<u8>>) {
    conflicts.into_iter().map(ConflictValue::into_parts).unzip()
}

/// Whether `items` hold exactly the two scalars `first` and `second`.
fn holds_both(items: &[Item], first: &Scalar, second: &Scalar) -> bool {
    items.len() == 2
        && items.contains(&Item::Scalar(first.clone()))
        && items.contains(&Item::Scalar(second.clone()))
}

/// Whether two causal identities are both present and distinct.
fn distinct(identities: &[Vec<u8>]) -> bool {
    match *identities {
        [ref first, ref second] => !first.is_empty() && !second.is_empty() && first != second,
        _ => false,
    }
}

/// Forks one document, writes the same map key and list index on both forks,
/// merges them, and checks both conflict views.
fn concurrent_documents() -> Result<(), JourneyError> {
    let root = Object::root();
    let mut left = Document::new();
    let list = left
        .edit(|editor| {
            editor.put(&root, "title", Scalar::String("base".to_owned()))?;
            let items = editor.put_object(&root, "items", ObjectKind::List)?;
            editor.insert(&items, 0, Scalar::Integer(0))?;
            Ok(items)
        })?
        .0;
    let mut right = Document::load(&left.save()?)?;
    left.edit(|editor| {
        editor.put(&root, "title", Scalar::String("left".to_owned()))?;
        editor.put_index(&list, 0, Scalar::Integer(1))
    })?;
    let right_changes = right
        .edit(|editor| {
            editor.put(&root, "title", Scalar::String("right".to_owned()))?;
            editor.put_index(&list, 0, Scalar::Integer(2))
        })?
        .1;
    left.apply(&right_changes)?;
    let mut reader = left.reader();
    let (titles, title_ids) = candidates(reader.conflicts(&root, "title")?);
    let (slots, slot_ids) = candidates(reader.conflicts_at(&list, 0)?);
    let titled = holds_both(
        &titles,
        &Scalar::String("left".to_owned()),
        &Scalar::String("right".to_owned()),
    );
    if !titled || !distinct(&title_ids) {
        return Err(JourneyError::Mismatch(
            "concurrent map puts must keep both candidates",
        ));
    }
    if !holds_both(&slots, &Scalar::Integer(1), &Scalar::Integer(2)) || !distinct(&slot_ids) {
        return Err(JourneyError::Mismatch(
            "concurrent list puts must keep both candidates",
        ));
    }
    Ok(())
}

/// Runs the replica journey, then the concurrent-edit journey, and reports.
fn main() -> Result<(), JourneyError> {
    let path = std::env::temp_dir().join(format!(
        "logical-db-document-journey-{}.redb",
        std::process::id()
    ));
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let outcome = edited_replica(scope, ReplicaId::new(), &path);
    let removed = fs::remove_file(&path);
    outcome?;
    removed?;
    concurrent_documents()?;
    io::stdout().lock().write_all(
        b"PASS document-journey text=hello there tags=2 map_conflicts=2 index_conflicts=2\n",
    )?;
    Ok(())
}
