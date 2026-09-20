//! Bounded local episode model ported from LOGICAL-EVENT-MEMORY (`src/lib.rs`,
//! operators). It is not canonical ORIGIN persistence and does not implement
//! the locked LEM v0 wire. V1 identity covers scope, clocks, references, and
//! payload; snapshots fail closed on unknown versions and rebuild derived
//! indexes during decoding.
//!
//! Evidence: S02 S03 S04 S05 define opaque progress, ordered changes, and
//! explicit conflict provenance; S09 S10 S11 S12 S13 cover resumable logs,
//! conflict visibility, CRDT causality, local-first ownership, and convergence;
//! S17 S19 S20 S22 cover bounds, atomicity, recovery, and corruption refusal;
//! S23 S24 define idempotent operations and typed errors; S25 S26 define
//! canonical content identities; S34 S35 S36 cover capability-bound records
//! and admission evidence. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;

use lgwks_std::json::serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error as _, SeqAccess, Visitor},
};
use logical_db_cairn::{EpisodeRef, TokenKind, digest_frame};
use thiserror::Error;
use uuid::Uuid;

/// Maximum episode references accepted in one bounded list (mirrors
/// model `MAX_CHANGES_PER_PAGE`).
pub const MAX_REFERENCES: usize = 256;
/// Maximum rendered bytes accepted in one local-world record (mirrors
/// model `MAX_SNAPSHOT_BYTES` at a smaller local budget).
pub const MAX_RENDERED_BYTES: usize = 512 * 1024;
/// Maximum parent references accepted on one episode (mirrors
/// model `MAX_CHANGES_PER_PAGE` at a smaller local budget).
pub const MAX_PARENTS: usize = 64;
/// Maximum payload bytes accepted on one episode (mirrors
/// model `MAX_CHANGE_BYTES` at a smaller local budget).
pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024;
/// Schema tag mixed into every episode content hash.
pub const EPISODE_SCHEMA: &str = "logical-db-episode.v1";
/// Numeric version carried by episode and origin-log snapshots.
pub const EPISODE_SCHEMA_VERSION: u16 = 1;
/// Maximum episodes accepted in one origin-log snapshot.
pub const MAX_EPISODES_PER_SNAPSHOT: usize = 256;

/// Content-derived episode identity; the zero digest is rejected.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[serde(transparent)]
pub struct EpisodeId([u8; 32]);

impl<'de> Deserialize<'de> for EpisodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = <[u8; 32]>::deserialize(deserializer)?;
        Self::new(bytes).map_err(D::Error::custom)
    }
}

impl EpisodeId {
    /// Wraps a non-zero 32-byte digest.
    ///
    /// # Errors
    ///
    /// Returns [`EpisodeError::ZeroId`] for the all-zero digest.
    #[must_use = "the zero-digest rejection must be handled"]
    pub fn new(bytes: [u8; 32]) -> Result<Self, EpisodeError> {
        if bytes == [0; 32] {
            eprintln!("logical-db-episode: rejected zero episode id");
            return Err(EpisodeError::ZeroId);
        }
        Ok(Self(bytes))
    }

    /// Exposes the raw digest bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl TryFrom<EpisodeId> for EpisodeRef {
    type Error = logical_db_cairn::CairnError;

    fn try_from(id: EpisodeId) -> Result<Self, Self::Error> {
        EpisodeRef::new(id.into_bytes())
    }
}

impl TryFrom<EpisodeRef> for EpisodeId {
    type Error = EpisodeError;

    fn try_from(reference: EpisodeRef) -> Result<Self, Self::Error> {
        EpisodeId::new(reference.into_bytes())
    }
}

impl fmt::Display for EpisodeId {
    /// Renders the digest as lowercase hex.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// One of the seven episode clocks; time is first-class and multi-clock.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[non_exhaustive]
pub enum ClockKind {
    /// When ORIGIN encountered the episode.
    Observed,
    /// When the represented thing happened.
    Occurred,
    /// Earliest state for which the proposition was true.
    ValidFrom,
    /// End of validity.
    ValidTo,
    /// Append-log insertion time, assigned by the appender.
    Recorded,
    /// When the inference or consolidation was produced.
    Derived,
    /// Monotonic local sequence clock.
    Sequence,
}

/// Seven-clock episode timestamp in milliseconds since the epoch.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[non_exhaustive]
pub struct EpisodeTime {
    /// When ORIGIN encountered the episode.
    pub observed: i64,
    /// When the represented thing happened.
    pub occurred: i64,
    /// Earliest state for which the proposition was true.
    pub valid_from: i64,
    /// End of validity.
    pub valid_to: i64,
    /// Append-log insertion time, assigned by the appender.
    pub recorded: i64,
    /// When the inference or consolidation was produced.
    pub derived: i64,
    /// Monotonic local sequence clock.
    pub sequence: i64,
}

/// How an episode is known.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[non_exhaustive]
pub enum Epistemic {
    /// Directly observed evidence.
    Explicit,
    /// Derived from prior episodes.
    Inferred,
    /// A forecast about a future state.
    Predicted,
    /// Merged from multiple sources.
    Synthesized,
}

/// One evidence link: the episode plus the role it played.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[non_exhaustive]
pub struct Evidence {
    /// Upstream episode this link rests on.
    pub episode: EpisodeId,
    /// Owned role label for the link, so the aggregate serializes.
    pub role: String,
}

/// Sealed error surface for the episode log.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum EpisodeError {
    /// Zero digest passed to `EpisodeId::new`.
    #[error("episode identity cannot be zero")]
    ZeroId,
    /// Stored identity differs from recomputed content.
    #[error("episode identity does not match content hash")]
    IdMismatch,
    /// Gap, replay, or value beyond the signed range.
    #[error("episode sequence does not extend the log head")]
    BadSequence,
    /// More than 256 references (64 parents).
    #[error("reference list exceeds its bound")]
    TooManyReferences,
    /// Same episode twice in one list.
    #[error("reference list contains a duplicate episode")]
    DuplicateReference,
    /// Trajectory, contradiction, or list without evidence.
    #[error("reference lacks evidence cover")]
    UnprovenReference,
    /// Empty `rendered` bytes.
    #[error("rendered representation cannot be empty")]
    EmptyRepresentation,
    /// `rendered` over 512KiB.
    #[error("rendered representation exceeds the bounded budget")]
    RepresentationTooLarge,
    /// Oversize episode payload.
    #[error("episode payload exceeds 64KiB")]
    PayloadTooLarge,
    /// Identity already present in the log.
    #[error("duplicate episode identity")]
    Duplicate,
    /// Origin log exceeded its snapshot episode budget.
    #[error("origin log exceeds its bounded episode budget")]
    LogTooLarge,
    /// Zero or otherwise unsatisfiable traversal/slice budget.
    #[error("traversal budget is out of bounds")]
    InvalidBounds,
    /// Unknown identity read.
    #[error("episode not found at {context}: {id}")]
    NotFound {
        /// Lookup boundary that detected the missing identity.
        context: &'static str,
        /// Missing identity as lowercase hexadecimal.
        id: String,
    },
}

/// Computes the v1 identity over every immutable episode field except `id`.
#[must_use]
pub fn content_id(episode: &Episode) -> [u8; 32] {
    let seq = episode.seq.to_le_bytes();
    let scope = scope_bytes(episode.scope);
    let time = time_bytes(episode.time);
    let evidence = reference_bytes(&episode.evidence);
    let parents = reference_bytes(&episode.parents);
    digest_frame(&[
        EPISODE_SCHEMA.as_bytes(),
        &seq,
        episode.kind.as_str().as_bytes(),
        &scope,
        &time,
        &evidence,
        &parents,
        &episode.payload,
    ])
}

fn scope_bytes(scope: Option<(Uuid, Uuid)>) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(33);
    bytes.push(u8::from(scope.is_some()));
    if let Some((account, space)) = scope {
        bytes.extend_from_slice(account.as_bytes());
        bytes.extend_from_slice(space.as_bytes());
    }
    bytes
}

fn time_bytes(time: EpisodeTime) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(56);
    for clock in [
        time.observed,
        time.occurred,
        time.valid_from,
        time.valid_to,
        time.recorded,
        time.derived,
        time.sequence,
    ] {
        bytes.extend_from_slice(&clock.to_le_bytes());
    }
    bytes
}

fn reference_bytes(ids: &[EpisodeId]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(ids.len() * 32);
    for id in ids {
        bytes.extend_from_slice(&id.into_bytes());
    }
    bytes
}

/// Request digest for idempotent publish over [`digest_frame`] of `schema`, `op`, `record`.
#[must_use]
pub fn request_digest(schema: &str, op: &[u8], record: &[u8]) -> [u8; 32] {
    digest_frame(&[schema.as_bytes(), op, record])
}

/// Checks length and uniqueness of one reference list against its named bound.
/// The `"parents"` list is capped at [`MAX_PARENTS`]; every other `what`
/// (`"evidence"`, `"source_heads"`, `"trajectories"`, `"contradictions"`)
/// is capped at [`MAX_REFERENCES`]. The empty-list rule stays with the
/// caller: pass-through lists may be empty while provenance cover lists
/// must not be.
///
/// # Errors
///
/// Returns [`EpisodeError::TooManyReferences`] or
/// [`EpisodeError::DuplicateReference`] when the list violates its bound.
fn check_ref_list(ids: &[EpisodeId], what: &'static str) -> Result<(), EpisodeError> {
    let limit = match what {
        "parents" => MAX_PARENTS,
        _ => MAX_REFERENCES,
    };
    if ids.len() > limit {
        eprintln!("logical-db-episode: rejected {what} count {}", ids.len());
        return Err(EpisodeError::TooManyReferences);
    }
    if ids.iter().collect::<BTreeSet<_>>().len() != ids.len() {
        eprintln!("logical-db-episode: rejected duplicate {what} reference");
        return Err(EpisodeError::DuplicateReference);
    }
    Ok(())
}

fn deserialize_evidence<'de, D>(deserializer: D) -> Result<Vec<EpisodeId>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec(deserializer, MAX_REFERENCES, "bounded episode evidence")
}

fn deserialize_parents<'de, D>(deserializer: D) -> Result<Vec<EpisodeId>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec(deserializer, MAX_PARENTS, "bounded episode parents")
}

fn deserialize_payload<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec(deserializer, MAX_PAYLOAD_BYTES, "bounded episode payload")
}

fn deserialize_episodes<'de, D>(deserializer: D) -> Result<Vec<Episode>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec(
        deserializer,
        MAX_EPISODES_PER_SNAPSHOT,
        "bounded origin log",
    )
}

fn deserialize_bounded_vec<'de, D, T>(
    deserializer: D,
    maximum: usize,
    expectation: &'static str,
) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct BoundedVisitor<T> {
        maximum: usize,
        expectation: &'static str,
        marker: std::marker::PhantomData<T>,
    }

    impl<'de, T> Visitor<'de> for BoundedVisitor<T>
    where
        T: Deserialize<'de>,
    {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.expectation)
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let capacity = sequence.size_hint().unwrap_or(0).min(self.maximum);
            let mut values = Vec::with_capacity(capacity);
            while let Some(value) = sequence.next_element()? {
                if values.len() == self.maximum {
                    eprintln!(
                        "logical-db-episode: rejected {} above {} items",
                        self.expectation, self.maximum
                    );
                    return Err(A::Error::invalid_length(self.maximum + 1, &self));
                }
                values.push(value);
            }
            Ok(values)
        }
    }

    deserializer.deserialize_seq(BoundedVisitor {
        maximum,
        expectation,
        marker: std::marker::PhantomData,
    })
}

/// One immutable episode; identity is derived from content at construction.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Episode {
    /// Content-derived identity over every immutable field below.
    pub id: EpisodeId,
    /// Monotonic local sequence, assigned by the appender.
    pub seq: u64,
    /// Episode kind from the CAIRN token vocabulary.
    pub kind: TokenKind,
    /// Account/space scope as an `(account, space)` [`Uuid`] pair mirroring the
    /// bytes of model `AccountId`/`SpaceId`; kept as [`Uuid`]s here so this
    /// layer does not depend on model types.
    pub scope: Option<(Uuid, Uuid)>,
    /// Seven-clock timestamp.
    pub time: EpisodeTime,
    /// Upstream episodes, bounded to 256 and deduped.
    pub evidence: Vec<EpisodeId>,
    /// Superseded or cited episodes, bounded to 64 and deduped.
    pub parents: Vec<EpisodeId>,
    /// Raw payload bytes, bounded to 64KiB.
    pub payload: Vec<u8>,
}

impl Episode {
    /// Builds an episode, deriving `id` from content and enforcing bounds.
    ///
    /// # Errors
    ///
    /// Returns [`EpisodeError::TooManyReferences`], [`EpisodeError::DuplicateReference`],
    /// [`EpisodeError::PayloadTooLarge`], or [`EpisodeError::ZeroId`] when a bound is violated.
    #[must_use = "construction enforces bounds; handle the error"]
    pub fn new(
        seq: u64,
        kind: TokenKind,
        time: EpisodeTime,
        evidence: Vec<EpisodeId>,
        parents: Vec<EpisodeId>,
        payload: Vec<u8>,
        scope: Option<(Uuid, Uuid)>,
    ) -> Result<Self, EpisodeError> {
        check_ref_list(&evidence, "evidence")?;
        check_ref_list(&parents, "parents")?;
        if payload.len() > MAX_PAYLOAD_BYTES {
            eprintln!(
                "logical-db-episode: rejected payload length {}",
                payload.len()
            );
            return Err(EpisodeError::PayloadTooLarge);
        }
        let mut episode = Self {
            id: EpisodeId([1; 32]),
            seq,
            kind,
            scope,
            time,
            evidence,
            parents,
            payload,
        };
        episode.id = EpisodeId::new(content_id(&episode))?;
        Ok(episode)
    }

    /// Rechecks bounds and that `id` matches recomputed content.
    ///
    /// # Errors
    ///
    /// Returns [`EpisodeError::TooManyReferences`], [`EpisodeError::DuplicateReference`],
    /// [`EpisodeError::PayloadTooLarge`], or [`EpisodeError::IdMismatch`] on violation.
    pub fn validate(&self) -> Result<(), EpisodeError> {
        check_ref_list(&self.evidence, "evidence")?;
        check_ref_list(&self.parents, "parents")?;
        if self.payload.len() > MAX_PAYLOAD_BYTES {
            eprintln!(
                "logical-db-episode: rejected stored payload length {}",
                self.payload.len()
            );
            return Err(EpisodeError::PayloadTooLarge);
        }
        let recomputed = content_id(self);
        if self.id.into_bytes() != recomputed {
            eprintln!("logical-db-episode: rejected mismatched content id");
            return Err(EpisodeError::IdMismatch);
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[serde(deny_unknown_fields)]
struct EpisodeWire {
    version: u16,
    id: EpisodeId,
    seq: u64,
    kind: TokenKind,
    scope: Option<(Uuid, Uuid)>,
    time: EpisodeTime,
    #[serde(deserialize_with = "deserialize_evidence")]
    evidence: Vec<EpisodeId>,
    #[serde(deserialize_with = "deserialize_parents")]
    parents: Vec<EpisodeId>,
    #[serde(deserialize_with = "deserialize_payload")]
    payload: Vec<u8>,
}

impl From<&Episode> for EpisodeWire {
    fn from(episode: &Episode) -> Self {
        Self {
            version: EPISODE_SCHEMA_VERSION,
            id: episode.id,
            seq: episode.seq,
            kind: episode.kind,
            scope: episode.scope,
            time: episode.time,
            evidence: episode.evidence.clone(),
            parents: episode.parents.clone(),
            payload: episode.payload.clone(),
        }
    }
}

impl Serialize for Episode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        EpisodeWire::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Episode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = EpisodeWire::deserialize(deserializer)?;
        if wire.version != EPISODE_SCHEMA_VERSION {
            eprintln!(
                "logical-db-episode: rejected episode schema version {}",
                wire.version
            );
            return Err(D::Error::custom("unsupported episode schema version"));
        }
        let episode = Self {
            id: wire.id,
            seq: wire.seq,
            kind: wire.kind,
            scope: wire.scope,
            time: wire.time,
            evidence: wire.evidence,
            parents: wire.parents,
            payload: wire.payload,
        };
        episode.validate().map_err(D::Error::custom)?;
        Ok(episode)
    }
}

/// One bounded local-world projection derived from canonical episodes.
///
/// `trajectories` and `contradictions` must be covered by `evidence`, so
/// rendered state cannot invent lineage. Guards mirror LEM `lib.rs:115-200`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[non_exhaustive]
pub struct LocalWorldRecord {
    /// Local replay checkpoint, not cross-instance identity.
    pub source_sequence: u64,
    /// Account/space scope as an `(account, space)` [`Uuid`] pair mirroring the
    /// bytes of model `AccountId`/`SpaceId`; kept as [`Uuid`]s here so this
    /// layer does not depend on model types.
    pub scope: Option<(Uuid, Uuid)>,
    /// Causal DAG frontier.
    pub source_heads: Vec<EpisodeId>,
    /// Upstream episodes the rendering rests on.
    pub evidence: Vec<EpisodeId>,
    /// Evidence-covered trajectories surfaced in the rendering.
    pub trajectories: Vec<EpisodeId>,
    /// Evidence-covered contradictions surfaced in the rendering.
    pub contradictions: Vec<EpisodeId>,
    /// Rendered bytes for the projection.
    pub rendered: Vec<u8>,
}

impl LocalWorldRecord {
    /// Builds a record, enforcing the LEM provenance and bound guards.
    ///
    /// # Errors
    ///
    /// Returns [`EpisodeError::BadSequence`], [`EpisodeError::TooManyReferences`],
    /// [`EpisodeError::DuplicateReference`], [`EpisodeError::UnprovenReference`],
    /// [`EpisodeError::EmptyRepresentation`], or [`EpisodeError::RepresentationTooLarge`]
    /// when a guard is violated.
    #[must_use = "construction enforces guards; handle the error"]
    pub fn new(
        source_sequence: u64,
        source_heads: Vec<EpisodeId>,
        evidence: Vec<EpisodeId>,
        trajectories: Vec<EpisodeId>,
        contradictions: Vec<EpisodeId>,
        rendered: Vec<u8>,
        scope: Option<(Uuid, Uuid)>,
    ) -> Result<Self, EpisodeError> {
        if source_sequence > i64::MAX as u64 {
            eprintln!("logical-db-episode: rejected source sequence {source_sequence}");
            return Err(EpisodeError::BadSequence);
        }
        if source_heads.is_empty() {
            eprintln!("logical-db-episode: rejected empty source heads");
            return Err(EpisodeError::UnprovenReference);
        }
        check_ref_list(&source_heads, "source_heads")?;
        if evidence.is_empty() {
            eprintln!("logical-db-episode: rejected empty projection evidence");
            return Err(EpisodeError::UnprovenReference);
        }
        check_ref_list(&evidence, "evidence")?;
        check_ref_list(&trajectories, "trajectories")?;
        check_ref_list(&contradictions, "contradictions")?;
        let cover = evidence.iter().copied().collect::<BTreeSet<_>>();
        let covered = trajectories
            .iter()
            .chain(contradictions.iter())
            .all(|id| cover.contains(id));
        if !covered {
            eprintln!("logical-db-episode: rejected uncovered projection reference");
            return Err(EpisodeError::UnprovenReference);
        }
        if rendered.is_empty() {
            eprintln!("logical-db-episode: rejected empty rendered projection");
            return Err(EpisodeError::EmptyRepresentation);
        }
        if rendered.len() > MAX_RENDERED_BYTES {
            eprintln!(
                "logical-db-episode: rejected rendered length {}",
                rendered.len()
            );
            return Err(EpisodeError::RepresentationTooLarge);
        }
        Ok(Self {
            source_sequence,
            scope,
            source_heads,
            evidence,
            trajectories,
            contradictions,
            rendered,
        })
    }
}

/// In-memory append-only episode-log model with the eight-operator surface.
/// This is not the canonical ORIGIN database.
#[derive(Clone, Debug, Default)]
pub struct OriginLog {
    episodes: Vec<Episode>,
    index: HashMap<[u8; 32], usize>,
}

#[derive(Deserialize, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[serde(deny_unknown_fields)]
struct OriginLogWire {
    version: u16,
    #[serde(deserialize_with = "deserialize_episodes")]
    episodes: Vec<Episode>,
}

impl Serialize for OriginLog {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        OriginLogWire {
            version: EPISODE_SCHEMA_VERSION,
            episodes: self.episodes.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for OriginLog {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = OriginLogWire::deserialize(deserializer)?;
        if wire.version != EPISODE_SCHEMA_VERSION {
            eprintln!(
                "logical-db-episode: rejected origin-log schema version {}",
                wire.version
            );
            return Err(D::Error::custom("unsupported origin-log schema version"));
        }
        let mut log = Self::default();
        for episode in wire.episodes {
            log.append(episode).map_err(D::Error::custom)?;
        }
        Ok(log)
    }
}

impl OriginLog {
    /// Validates, dedupes on identity, enforces head sequence, and appends.
    ///
    /// # Errors
    ///
    /// Returns the [`EpisodeError`] from validation, [`EpisodeError::Duplicate`] for a
    /// known identity, or [`EpisodeError::BadSequence`] off the log head.
    pub fn append(&mut self, episode: Episode) -> Result<EpisodeId, EpisodeError> {
        episode.validate()?;
        if self.index.contains_key(&episode.id.into_bytes()) {
            eprintln!(
                "logical-db-episode: rejected duplicate append {}",
                episode.id
            );
            return Err(EpisodeError::Duplicate);
        }
        if self.episodes.len() == MAX_EPISODES_PER_SNAPSHOT {
            eprintln!("logical-db-episode: rejected append above log capacity");
            return Err(EpisodeError::LogTooLarge);
        }
        if episode.seq != self.episodes.len() as u64 {
            eprintln!(
                "logical-db-episode: rejected append sequence {} at head {}",
                episode.seq,
                self.episodes.len()
            );
            return Err(EpisodeError::BadSequence);
        }
        let id = episode.id;
        self.index.insert(id.into_bytes(), self.episodes.len());
        self.episodes.push(episode);
        Ok(id)
    }

    /// Returns the exact episode as appended, without projection.
    ///
    /// # Errors
    ///
    /// Returns [`EpisodeError::NotFound`] for an unknown identity.
    pub fn get(&self, id: EpisodeId) -> Result<&Episode, EpisodeError> {
        let position = *self
            .index
            .get(&id.into_bytes())
            .ok_or_else(|| EpisodeError::NotFound {
                context: "OriginLog::get.index",
                id: id.to_string(),
            })?;
        self.episodes
            .get(position)
            .ok_or_else(|| EpisodeError::NotFound {
                context: "OriginLog::get.episodes",
                id: id.to_string(),
            })
    }

    /// Walks `evidence` and `parents` backwards in BFS order, capped by budget.
    ///
    /// # Errors
    ///
    /// Returns [`EpisodeError::NotFound`] for an unknown anchor identity or a
    /// dangling parent/evidence link (fail closed, with the missing id hex).
    pub fn ancestors(&self, id: EpisodeId, budget: usize) -> Result<Vec<EpisodeId>, EpisodeError> {
        let root = self.get(id)?;
        let mut seen = BTreeSet::from([id.into_bytes()]);
        let mut queue: Vec<EpisodeId> = Vec::new();
        queue.extend(root.evidence.iter().chain(root.parents.iter()).copied());
        let mut out = Vec::new();
        let mut head = 0;
        while head < queue.len() {
            let current = queue[head];
            head += 1;
            if !seen.insert(current.into_bytes()) {
                continue;
            }
            if out.len() >= budget {
                break;
            }
            let episode = self.get(current)?;
            out.push(current);
            for link in episode.evidence.iter().chain(episode.parents.iter()) {
                queue.push(*link);
            }
        }
        Ok(out)
    }

    /// Shared forward BFS over inverse edges; `parents_only` selects worldlines.
    /// Builds the inverse adjacency index in one pass, then walks it with a
    /// visited set, so traversal is O(episodes + edges), not O(episodes²).
    fn forward(
        &self,
        id: EpisodeId,
        parents_only: bool,
        budget: usize,
    ) -> Result<Vec<EpisodeId>, EpisodeError> {
        self.get(id)?;
        let mut children: HashMap<[u8; 32], Vec<EpisodeId>> = HashMap::new();
        for episode in &self.episodes {
            let child = episode.id;
            if parents_only {
                for link in &episode.parents {
                    children.entry(link.into_bytes()).or_default().push(child);
                }
            } else {
                // `descendants` covers both edge kinds in one entry per edge.
                // Deduping dual-cited edges here is unnecessary: `seen` below
                // emits each child once regardless of how many edges match.
                for link in episode.evidence.iter().chain(episode.parents.iter()) {
                    children.entry(link.into_bytes()).or_default().push(child);
                }
            }
        }
        let mut seen = HashSet::from([id.into_bytes()]);
        let mut frontier = vec![id];
        let mut out = Vec::new();
        let mut head = 0;
        while head < frontier.len() {
            let current = frontier[head];
            head += 1;
            let Some(next) = children.get(&current.into_bytes()) else {
                continue;
            };
            for child in next {
                if seen.insert(child.into_bytes()) {
                    if out.len() >= budget {
                        return Ok(out);
                    }
                    out.push(*child);
                    frontier.push(*child);
                }
            }
        }
        Ok(out)
    }

    /// Walks inverse `evidence` and `parents` edges forwards, capped by budget.
    ///
    /// # Errors
    ///
    /// Returns [`EpisodeError::NotFound`] for an unknown anchor identity.
    pub fn descendants(
        &self,
        id: EpisodeId,
        budget: usize,
    ) -> Result<Vec<EpisodeId>, EpisodeError> {
        self.forward(id, false, budget)
    }

    /// Reconstructs the forward `parents` chain starting after the anchor,
    /// capped by budget.
    ///
    /// # Errors
    ///
    /// Returns [`EpisodeError::NotFound`] for an unknown anchor identity.
    pub fn worldline(&self, id: EpisodeId, budget: usize) -> Result<Vec<EpisodeId>, EpisodeError> {
        self.forward(id, true, budget)
    }

    /// Returns episodes with `seq` at or below the slice point, capped by budget.
    ///
    /// # Errors
    ///
    /// Returns [`EpisodeError::InvalidBounds`] for a zero budget.
    pub fn at(&self, seq: u64, limit: usize) -> Result<Vec<&Episode>, EpisodeError> {
        if limit == 0 {
            eprintln!("logical-db-episode: rejected zero at-slice limit");
            return Err(EpisodeError::InvalidBounds);
        }
        Ok(self
            .episodes
            .iter()
            .filter(|episode| episode.seq <= seq)
            .take(limit)
            .collect())
    }

    /// Returns episodes of one kind, capped by budget; the simplified
    /// neighborhood operator.
    ///
    /// # Errors
    ///
    /// Returns [`EpisodeError::InvalidBounds`] for a zero budget.
    pub fn near(&self, kind: &TokenKind, limit: usize) -> Result<Vec<&Episode>, EpisodeError> {
        if limit == 0 {
            eprintln!("logical-db-episode: rejected zero near-query limit");
            return Err(EpisodeError::InvalidBounds);
        }
        Ok(self
            .episodes
            .iter()
            .filter(|episode| &episode.kind == kind)
            .take(limit)
            .collect())
    }

    /// Recomputes every identity and rechecks the sequence chain.
    ///
    /// # Errors
    ///
    /// Returns [`EpisodeError::IdMismatch`] for a tampered episode or
    /// [`EpisodeError::BadSequence`] when positions drift from `seq`.
    pub fn verify_log(&self) -> Result<(), EpisodeError> {
        for (position, episode) in self.episodes.iter().enumerate() {
            episode.validate()?;
            if episode.seq != position as u64 {
                eprintln!(
                    "logical-db-episode: rejected log sequence {} at position {position}",
                    episode.seq
                );
                return Err(EpisodeError::BadSequence);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn episode_id(value: u8) -> EpisodeId {
        EpisodeId::new([value; 32]).expect("non-zero fixture id")
    }

    fn scope() -> (Uuid, Uuid) {
        (Uuid::from_bytes([1u8; 16]), Uuid::from_bytes([2u8; 16]))
    }

    fn episode(seq: u64, payload: &[u8]) -> Episode {
        Episode::new(
            seq,
            TokenKind::Event,
            EpisodeTime::default(),
            vec![episode_id(1)],
            vec![],
            payload.to_vec(),
            Some(scope()),
        )
        .expect("valid fixture episode")
    }

    fn linked(seq: u64, payload: &[u8], parents: Vec<EpisodeId>) -> Episode {
        Episode::new(
            seq,
            TokenKind::Event,
            EpisodeTime::default(),
            vec![],
            parents,
            payload.to_vec(),
            Some(scope()),
        )
        .expect("valid fixture episode")
    }
    #[test]
    fn zero_id_is_rejected() {
        assert_eq!(EpisodeId::new([0; 32]), Err(EpisodeError::ZeroId));
        assert_eq!(episode_id(9).to_string().len(), 64);
    }

    #[test]
    fn content_id_is_deterministic() {
        let first = episode(0, b"same");
        assert_eq!(first.id, episode(0, b"same").id);
        assert_ne!(first.id, episode(0, b"different").id);
        let time = EpisodeTime {
            observed: 1,
            ..EpisodeTime::default()
        };
        let retimed = Episode::new(
            0,
            TokenKind::Event,
            time,
            vec![episode_id(1)],
            vec![],
            b"same".to_vec(),
            Some(scope()),
        )
        .expect("retimed episode");
        assert_ne!(first.id, retimed.id);
        let rescoped = Episode::new(
            0,
            TokenKind::Event,
            EpisodeTime::default(),
            vec![episode_id(1)],
            vec![],
            b"same".to_vec(),
            None,
        )
        .expect("rescoped episode");
        assert_ne!(first.id, rescoped.id);
        let reparented = Episode::new(
            0,
            TokenKind::Event,
            EpisodeTime::default(),
            vec![episode_id(1)],
            vec![episode_id(2)],
            b"same".to_vec(),
            Some(scope()),
        )
        .expect("reparented episode");
        assert_ne!(first.id, reparented.id);
        let reattested = Episode::new(
            0,
            TokenKind::Event,
            EpisodeTime::default(),
            vec![episode_id(2)],
            vec![],
            b"same".to_vec(),
            Some(scope()),
        )
        .expect("reattested episode");
        assert_ne!(first.id, reattested.id);
    }

    #[test]
    fn append_get_roundtrip() {
        let mut log = OriginLog::default();
        let expected = episode(0, b"hello");
        let id = log.append(expected.clone()).expect("append");
        assert_eq!(log.get(id), Ok(&expected));
        assert!(log.verify_log().is_ok());
    }

    #[test]
    fn bad_sequence_is_rejected() {
        let mut log = OriginLog::default();
        let skipped = log.append(episode(3, b"skipped"));
        assert_eq!(skipped, Err(EpisodeError::BadSequence));
    }

    #[test]
    fn duplicate_append_is_rejected() {
        let mut log = OriginLog::default();
        let first = episode(0, b"once");
        log.append(first.clone()).expect("first append");
        assert_eq!(log.append(first), Err(EpisodeError::Duplicate));
    }

    #[test]
    fn local_world_subset_guard_holds() {
        let heads = vec![episode_id(1)];
        let evidence = vec![episode_id(2)];
        let bad = vec![episode_id(3)];
        let uncovered =
            LocalWorldRecord::new(0, heads, evidence, bad, vec![], vec![1], Some(scope()));
        assert_eq!(uncovered, Err(EpisodeError::UnprovenReference));
    }

    #[test]
    fn rendered_bounds_are_rejected() {
        let build = |rendered: Vec<u8>| {
            LocalWorldRecord::new(
                0,
                vec![episode_id(1)],
                vec![episode_id(2)],
                vec![],
                vec![],
                rendered,
                Some(scope()),
            )
        };
        assert_eq!(build(vec![]), Err(EpisodeError::EmptyRepresentation));
        assert_eq!(
            build(vec![0; MAX_RENDERED_BYTES + 1]),
            Err(EpisodeError::RepresentationTooLarge)
        );
        assert!(build(b"state".to_vec()).is_ok());
    }

    #[test]
    fn verify_log_catches_tamper() {
        let mut log = OriginLog::default();
        log.append(episode(0, b"pristine")).expect("append");
        log.episodes[0].payload.push(0xFF);
        assert_eq!(log.verify_log(), Err(EpisodeError::IdMismatch));
        let missing = episode_id(0xAA);
        assert_eq!(
            log.get(missing),
            Err(EpisodeError::NotFound {
                context: "OriginLog::get.index",
                id: missing.to_string(),
            })
        );
    }

    #[test]
    fn dangling_ancestry_fails_closed() {
        let mut log = OriginLog::default();
        let id = log.append(episode(0, b"root")).expect("append");
        let missing = episode_id(1);
        assert_eq!(
            log.ancestors(id, 16),
            Err(EpisodeError::NotFound {
                context: "OriginLog::get.index",
                id: missing.to_string(),
            })
        );
    }

    #[test]
    fn at_and_near_enforce_budget() {
        let mut log = OriginLog::default();
        for (seq, payload) in [b"a".as_slice(), b"b", b"c"].iter().enumerate() {
            log.append(linked(seq as u64, payload, vec![]))
                .expect("append");
        }
        assert_eq!(log.at(10, 0), Err(EpisodeError::InvalidBounds));
        assert_eq!(
            log.near(&TokenKind::Event, 0),
            Err(EpisodeError::InvalidBounds)
        );
        assert_eq!(log.at(10, 2).expect("at").len(), 2);
        assert_eq!(log.at(0, 8).expect("at").len(), 1);
        assert_eq!(log.near(&TokenKind::Event, 8).expect("near").len(), 3);
        assert!(log.near(&TokenKind::Entity, 8).expect("near").is_empty());
    }

    #[test]
    fn worldline_terminates_on_forks_and_respects_budget() {
        let mut log = OriginLog::default();
        let root_id = log.append(linked(0, b"root", vec![])).expect("append");
        let left_id = log
            .append(linked(1, b"left", vec![root_id]))
            .expect("append");
        let right_id = log
            .append(linked(2, b"right", vec![root_id]))
            .expect("append");
        let joined_id = log
            .append(linked(3, b"joined", vec![left_id, right_id]))
            .expect("append");
        let line = log.worldline(root_id, 16).expect("worldline");
        assert_eq!(line, vec![left_id, right_id, joined_id]);
        assert_eq!(log.worldline(root_id, 2).expect("worldline").len(), 2);
    }

    #[test]
    fn content_id_uses_stable_wire_string() {
        let item = Episode::new(
            7,
            TokenKind::MemoryEpisode,
            EpisodeTime::default(),
            vec![],
            vec![],
            b"payload".to_vec(),
            Some(scope()),
        )
        .expect("episode");
        assert_eq!(content_id(&item), item.id.into_bytes());
        assert_ne!(item.kind.as_str(), format!("{:?}", item.kind));
    }

    #[test]
    fn episode_ref_conversions_roundtrip() {
        let id = episode_id(4);
        let reference = EpisodeRef::try_from(id).expect("valid reference");
        assert_eq!(reference.into_bytes(), id.into_bytes());
        assert_eq!(EpisodeId::try_from(reference), Ok(id));
    }

    #[test]
    fn aggregates_are_serde_snapshots() {
        fn roundtrip<T>(value: &T) -> T
        where
            T: Serialize + for<'de> Deserialize<'de>,
        {
            let mut encoded = Vec::new();
            ciborium::into_writer(value, &mut encoded).expect("CBOR encode");
            ciborium::from_reader(encoded.as_slice()).expect("CBOR decode")
        }

        let item = episode(0, b"snapshot");
        assert_eq!(roundtrip(&item), item);

        let evidence = Evidence {
            episode: episode_id(1),
            role: "source".into(),
        };
        assert_eq!(roundtrip(&evidence), evidence);

        let record = LocalWorldRecord::new(
            0,
            vec![episode_id(1)],
            vec![episode_id(1)],
            vec![episode_id(1)],
            vec![],
            b"state".to_vec(),
            Some(scope()),
        )
        .expect("valid local-world record");
        assert_eq!(roundtrip(&record), record);

        let mut log = OriginLog::default();
        log.append(item).expect("valid log");
        let rebuilt = roundtrip(&log);
        assert_eq!(rebuilt.get(log.episodes[0].id), Ok(&log.episodes[0]));
    }

    #[test]
    fn full_log_replay_remains_duplicate() {
        let mut log = OriginLog::default();
        for seq in 0..MAX_EPISODES_PER_SNAPSHOT {
            log.append(episode(seq as u64, b"capacity"))
                .expect("append");
        }
        let replay = log.episodes[0].clone();
        assert_eq!(log.append(replay), Err(EpisodeError::Duplicate));
    }

    #[test]
    fn request_digest_is_deterministic_and_sensitive() {
        let first = request_digest("schema", b"op", b"record");
        assert_eq!(first, request_digest("schema", b"op", b"record"));
        assert_ne!(first, request_digest("schema", b"op", b"changed"));
        assert_ne!(first, request_digest("schema", b"other", b"record"));
    }
}
