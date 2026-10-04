//! Non-authoritative Cairn semantic observations ported into Logical-DB.
//!
//! Evidence: S11 S12 S13 establish local-first, causal, and authenticated
//! semantic records; S17 S22 bound hostile values and corruption handling;
//! S23 S24 define stable request/error semantics; S25 S26 define canonical
//! bytes and identifiers; S29 S30 support digest construction; S34 S35 S36
//! S40 S41 map capability, admission, capsule, invocation, and proof concepts;
//! S39 covers the bounded wire representation boundary; S14 S18 S42 inform
//! transaction identity and exact retry boundaries at the integration edge.
//! Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! Re-expresses the typed world-token vocabulary from
//! `cairn/quarantine/cairn_python/world_token.py` (12 token kinds,
//! 10 modalities, 9 link relations, 6 actionability levels), the
//! authority-weight and content-hash concepts from
//! `cairn/crates/wm-core`, and deny-first steering with trust-graded
//! memory claims from the Cairn steering/memory broker (smb).
//! Bounds fail closed with rejection diagnostics; no storage, network,
//! ingestion, or canonicity authority is present here.

#![forbid(unsafe_code)]

use lgwks_std::hash::Hasher;
use lgwks_std::json::serde::{self, Deserialize, Deserializer, Serialize};
use thiserror::Error;

/// Maximum links accepted on one observation (per-observation bound; no model peer).
pub const MAX_LINKS: usize = 64;
/// Maximum value bytes accepted on one observation (per-observation bound; no model peer).
pub const MAX_VALUE_BYTES: usize = 65_536;
/// Maximum UTF-8 bytes accepted in one memory claim (per-claim bound; no model peer).
pub const MAX_CLAIM_BYTES: usize = 8_192;

/// Declares one closed string vocabulary with canonical `snake_case` names.
///
/// Generates a `#[non_exhaustive]` enum whose variants carry their own doc, a
/// canonical `as_str`, and a fail-closed `parse` that returns the caller-named
/// `$err` variant for an unknown name.
macro_rules! string_vocab {
    ($doc:literal, $name:ident, $err:ident, $($vdoc:literal $variant:ident => $str:literal),* $(,)?) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
        #[serde(rename_all = "snake_case")]
        #[non_exhaustive]
        pub enum $name {
            $(
                #[doc = $vdoc]
                $variant,
            )*
        }

        impl $name {
            /// Canonical `snake_case` name for hashing and wire use.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $str,)*
                }
            }

            /// Parses a canonical name, rejecting unknown values closed.
            ///
            /// # Errors
            ///
            /// Returns the vocabulary's `CairnError` variant for unknown names.
            pub fn parse(value: &str) -> Result<Self, CairnError> {
                match value {
                    $($str => Ok(Self::$variant),)*
                    _ => Err(CairnError::$err {
                        value: value.to_owned(),
                        context: concat!(stringify!($name), "::parse"),
                    }),
                }
            }
        }
    };
}

string_vocab!("Semantic token kind, ported from `world_token.TOKEN_KINDS`.", TokenKind, InvalidKind,
    "Addressable object or identity." Entity => "entity", "Point-in-time occurrence." Event => "event",
    "Persisted condition of an entity." State => "state", "Typed edge reified as a token." Relation => "relation",
    "Opportunity for action an entity offers." Affordance => "affordance", "Corroborating record for a claim." Evidence => "evidence",
    "Capability grant bounding an action." Permission => "permission", "Recorded outcome of an executed action." ActionResult => "action_result",
    "Liveness or health indicator." ViabilitySignal => "viability_signal", "Recalled experience usable as context." MemoryEpisode => "memory_episode",
    "Pair of irreconcilable claims." Contradiction => "contradiction", "Quantified gap in knowledge." Uncertainty => "uncertainty",
);

string_vocab!("Directed edge type, ported from `world_token.LINK_RELATIONS`.", LinkRelation, InvalidRelation,
    "Source precedes target in time." Before => "before", "Source follows target in time." After => "after",
    "Source was caused by target." CausedBy => "caused_by", "Source corroborates target." Supports => "supports",
    "Source refutes target." Contradicts => "contradicts", "Both ends describe the same entity." SameEntityAs => "same_entity_as",
    "Source requires target to hold first." DependsOn => "depends_on", "Source makes target possible." Enables => "enables",
    "Source prevents target." Blocks => "blocks",
);

string_vocab!("Source channel, ported from `world_token.MODALITIES`.", Modality, InvalidModality,
    "Natural-language text." Text => "text", "Source code or structured config." Code => "code",
    "Rendered interface state." Ui => "ui", "Still image or screenshot." Vision => "vision",
    "Moving image." Video => "video", "Sound or speech." Audio => "audio",
    "Runtime or platform signal." System => "system", "Recalled context." Memory => "memory",
    "Tool output." Tool => "tool", "Workflow or plan state." Workflow => "workflow",
);

string_vocab!("Permitted handling, ported from `world_token.VALID_ACTIONS`.", Actionability, InvalidActionability,
    "No action permitted or needed." None => "none", "Read-only inspection only." Inspect => "inspect",
    "May request clarification." Ask => "ask", "May attempt a bounded fix." Repair => "repair",
    "May change world state; needs confirmation." Execute => "execute", "Must refuse the action; needs confirmation." Block => "block",
);

string_vocab!("Epistemic grade of a memory claim, weakest inference last.", TrustLevel, InvalidTrust,
    "Directly recorded observation." Observed => "observed", "Derived from observations by rule." Inferred => "inferred",
    "Extrapolated beyond current evidence." Predicted => "predicted", "Composed from multiple weaker claims." Synthesized => "synthesized",
);

/// Shared digest framing: `blake3` over each part as `len(u64LE) + bytes`.
#[must_use]
pub fn digest_frame(parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Hasher::new();
    for part in parts {
        let length = u64::try_from(part.len()).unwrap_or(u64::MAX);
        hasher.update(&length.to_le_bytes());
        hasher.update(part);
    }
    *hasher.finalize().as_bytes()
}

/// Content-derived observation identity; the zero digest is rejected.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[serde(transparent)]
pub struct EpisodeRef([u8; 32]);

impl<'de> Deserialize<'de> for EpisodeRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = <[u8; 32]>::deserialize(deserializer)?;
        Self::new(bytes).map_err(serde::de::Error::custom)
    }
}

impl EpisodeRef {
    /// Wraps a non-zero 32-byte digest.
    ///
    /// # Errors
    ///
    /// Returns [`CairnError::ZeroId`] for the all-zero digest.
    #[must_use = "the zero-digest rejection must be handled"]
    pub fn new(bytes: [u8; 32]) -> Result<Self, CairnError> {
        if bytes == [0; 32] {
            return Err(CairnError::ZeroId);
        }
        Ok(Self(bytes))
    }

    /// Exposes the raw digest bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl std::fmt::Display for EpisodeRef {
    /// Renders the digest as lowercase hex.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Directed, confidence-weighted edge between two observations.
///
/// `#[non_exhaustive]` so a new edge field is a compatible addition for
/// downstream crates rather than a breaking change.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[non_exhaustive]
pub struct TokenLink {
    /// Relation this edge asserts.
    pub relation: LinkRelation,
    /// Target observation identifier; must be non-zero.
    pub target: EpisodeRef,
    /// Edge confidence in `[0.0, 1.0]`.
    pub confidence: f32,
}

impl TokenLink {
    /// Rejects zero targets and out-of-range confidence.
    ///
    /// # Errors
    ///
    /// Returns [`CairnError::ZeroId`] or [`CairnError::InvalidConfidence`].
    pub fn validate(&self) -> Result<(), CairnError> {
        if self.target.into_bytes() == [0u8; 32] {
            return Err(CairnError::ZeroId);
        }
        if !(0.0..=1.0).contains(&self.confidence) {
            return Err(CairnError::InvalidConfidence(self.confidence));
        }
        Ok(())
    }
}

/// One validated semantic observation.
///
/// `#[non_exhaustive]` so a new observation field is a compatible addition for
/// downstream crates rather than a breaking change.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[non_exhaustive]
pub struct Observation {
    /// Observation identifier; must be non-zero.
    pub id: EpisodeRef,
    /// Semantic kind of this observation.
    pub kind: TokenKind,
    /// Channel the observation arrived on.
    pub modality: Modality,
    /// Self-reported uncertainty in `[0.0, 1.0]`.
    pub uncertainty: f32,
    /// Permitted handling of this observation.
    pub actionability: Actionability,
    /// Opaque payload bytes; non-empty and bounded.
    value: Vec<u8>,
    /// Outgoing edges, bounded to [`MAX_LINKS`].
    links: Vec<TokenLink>,
}

impl Observation {
    /// Builds one observation from its identity, classification, and payload.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose observations through here.
    #[must_use]
    pub fn new(
        id: EpisodeRef,
        kind: TokenKind,
        modality: Modality,
        uncertainty: f32,
        actionability: Actionability,
        value: Vec<u8>,
        links: Vec<TokenLink>,
    ) -> Self {
        Self {
            id,
            kind,
            modality,
            uncertainty,
            actionability,
            value,
            links,
        }
    }

    /// The bounded opaque payload bytes.
    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// The bounded outgoing edges of this observation.
    #[must_use]
    pub fn links(&self) -> &[TokenLink] {
        &self.links
    }

    /// Replaces the payload, for composing an observation before validation.
    pub fn set_value(&mut self, value: Vec<u8>) {
        self.value = value;
    }

    /// Validates bounds on id, uncertainty, value, and links.
    ///
    /// # Errors
    ///
    /// Returns the [`CairnError`] matching the first bound violated.
    pub fn validate(&self) -> Result<(), CairnError> {
        if self.id.into_bytes() == [0u8; 32] {
            return Err(CairnError::ZeroId);
        }
        if !(0.0..=1.0).contains(&self.uncertainty) {
            return Err(CairnError::InvalidConfidence(self.uncertainty));
        }
        if self.value.is_empty() {
            return Err(CairnError::EmptyValue);
        }
        if self.value.len() > MAX_VALUE_BYTES {
            return Err(CairnError::ValueTooLarge {
                actual: self.value.len(),
                maximum: MAX_VALUE_BYTES,
            });
        }
        if self.links.len() > MAX_LINKS {
            return Err(CairnError::TooManyLinks {
                actual: self.links.len(),
                maximum: MAX_LINKS,
            });
        }
        for link in &self.links {
            link.validate()?;
        }
        Ok(())
    }

    /// SHA-256 over the complete semantic content using the v1 schema.
    #[must_use]
    pub fn content_hash(&self) -> [u8; 32] {
        let uncertainty = self.uncertainty.to_bits().to_le_bytes();
        let links = self.link_digest();
        digest_frame(&[
            b"logical-db-cairn-observation.v1",
            self.kind.as_str().as_bytes(),
            self.modality.as_str().as_bytes(),
            &uncertainty,
            self.actionability.as_str().as_bytes(),
            &self.value,
            &links,
        ])
    }

    /// Frames every outgoing edge into the digest stream.
    ///
    /// Each edge contributes a fixed 32-byte digest, so the reserve is exact.
    fn link_digest(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.links.len().saturating_mul(32));
        for link in &self.links {
            let confidence = link.confidence.to_bits().to_le_bytes();
            let digest = digest_frame(&[
                link.relation.as_str().as_bytes(),
                &link.target.into_bytes(),
                &confidence,
            ]);
            bytes.extend_from_slice(&digest);
        }
        bytes
    }
}

/// Authority weight by tier, adapted from the `wm-core` authority ordering.
#[must_use]
pub const fn authority_weight(tier: u8) -> f64 {
    match tier {
        0 => 1.0,
        1 => 0.8,
        2 => 0.5,
        _ => 0.2,
    }
}

/// Trust-graded claim held in memory.
///
/// `#[non_exhaustive]` so a new claim field is a compatible addition for
/// downstream crates rather than a breaking change.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[non_exhaustive]
pub struct MemoryClaim {
    /// Claim text; non-empty UTF-8 bounded to [`MAX_CLAIM_BYTES`].
    claim: String,
    /// Epistemic grade of the claim.
    pub trust: TrustLevel,
    /// Claim confidence in `[0.0, 1.0]`.
    pub confidence: f32,
}

impl MemoryClaim {
    /// Builds one trust-graded claim for validation.
    #[must_use]
    pub fn new(claim: String, trust: TrustLevel, confidence: f32) -> Self {
        Self {
            claim,
            trust,
            confidence,
        }
    }

    /// The claim text.
    #[must_use]
    pub fn claim(&self) -> &str {
        &self.claim
    }

    /// Replaces the claim text, for composition before validation.
    pub fn set_claim(&mut self, claim: String) {
        self.claim = claim;
    }

    /// Validates claim bounds and confidence range.
    ///
    /// # Errors
    ///
    /// Returns the [`CairnError`] matching the first bound violated.
    pub fn validate(&self) -> Result<(), CairnError> {
        if self.claim.is_empty() {
            return Err(CairnError::EmptyValue);
        }
        if self.claim.len() > MAX_CLAIM_BYTES {
            return Err(CairnError::ValueTooLarge {
                actual: self.claim.len(),
                maximum: MAX_CLAIM_BYTES,
            });
        }
        if !(0.0..=1.0).contains(&self.confidence) {
            return Err(CairnError::InvalidConfidence(self.confidence));
        }
        Ok(())
    }
}

/// Steering verdict for a proposed action.
///
/// `#[non_exhaustive]` so a new verdict field is a compatible addition for
/// downstream crates rather than a breaking change.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[non_exhaustive]
pub struct SteeringDecision {
    /// Whether the action may proceed.
    pub allow: bool,
    /// Machine-readable reason for the verdict.
    pub reason: &'static str,
}

/// Deny-first steering: dangerous actions need explicit confirmation.
#[must_use]
pub const fn decide(action: Actionability, confirmed: bool) -> SteeringDecision {
    match action {
        Actionability::Execute | Actionability::Block => {
            if confirmed {
                SteeringDecision {
                    allow: true,
                    reason: "dangerous action explicitly confirmed",
                }
            } else {
                SteeringDecision {
                    allow: false,
                    reason: "dangerous action requires confirmation",
                }
            }
        }
        _ => SteeringDecision {
            allow: true,
            reason: "non-dangerous action permitted",
        },
    }
}

impl SteeringDecision {
    /// Runs [`decide`] and converts an unconfirmed dangerous action to an error.
    ///
    /// # Errors
    ///
    /// Returns [`CairnError::UnconfirmedDangerous`] when denied.
    pub fn enforce(action: Actionability, confirmed: bool) -> Result<Self, CairnError> {
        let decision = decide(action, confirmed);
        if decision.allow {
            Ok(decision)
        } else {
            Err(CairnError::UnconfirmedDangerous)
        }
    }
}

/// Errors for Cairn semantic validation and steering.
///
/// `#[non_exhaustive]` so a new refusal is a compatible addition rather than a
/// breaking change to downstream exhaustive matches.
#[derive(Debug, Error, PartialEq)]
#[non_exhaustive]
pub enum CairnError {
    /// Unknown token kind name.
    #[error("invalid token kind {value:?} at {context}")]
    InvalidKind {
        /// Rejected wire value.
        value: String,
        /// Vocabulary boundary that rejected it.
        context: &'static str,
    },
    /// Unknown link relation name.
    #[error("invalid link relation {value:?} at {context}")]
    InvalidRelation {
        /// Rejected wire value.
        value: String,
        /// Vocabulary boundary that rejected it.
        context: &'static str,
    },
    /// Unknown modality name.
    #[error("invalid modality {value:?} at {context}")]
    InvalidModality {
        /// Rejected wire value.
        value: String,
        /// Vocabulary boundary that rejected it.
        context: &'static str,
    },
    /// Unknown actionability name.
    #[error("invalid actionability {value:?} at {context}")]
    InvalidActionability {
        /// Rejected wire value.
        value: String,
        /// Vocabulary boundary that rejected it.
        context: &'static str,
    },
    /// Unknown trust level name.
    #[error("invalid trust level {value:?} at {context}")]
    InvalidTrust {
        /// Rejected wire value.
        value: String,
        /// Vocabulary boundary that rejected it.
        context: &'static str,
    },
    /// Confidence or uncertainty outside `[0.0, 1.0]`.
    #[error("confidence {0} outside [0.0, 1.0]")]
    InvalidConfidence(f32),
    /// All-zero 32-byte identifier.
    #[error("identifier must be non-zero")]
    ZeroId,
    /// More than [`MAX_LINKS`] links on one observation.
    #[error("too many links: {actual} exceeds maximum {maximum}")]
    TooManyLinks {
        /// Link count supplied.
        actual: usize,
        /// Maximum links accepted.
        maximum: usize,
    },
    /// Empty value or claim text where content is required.
    #[error("value must be non-empty")]
    EmptyValue,
    /// Value or claim text exceeds its byte bound.
    #[error("value length {actual} exceeds maximum {maximum}")]
    ValueTooLarge {
        /// Byte length supplied.
        actual: usize,
        /// Maximum bytes accepted.
        maximum: usize,
    },
    /// Dangerous action proposed without explicit confirmation.
    #[error("dangerous action requires explicit confirmation")]
    UnconfirmedDangerous,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation() -> Observation {
        Observation {
            // Constructed through the private field: the test module is inside
            // this crate, so no fallible `new` call is needed for a literal.
            id: EpisodeRef([7u8; 32]),
            kind: TokenKind::Evidence,
            modality: Modality::Text,
            uncertainty: 0.1,
            actionability: Actionability::Inspect,
            value: b"sensor reading".to_vec(),
            links: Vec::new(),
        }
    }

    fn link(seed: u8, confidence: f32) -> TokenLink {
        TokenLink {
            relation: LinkRelation::Supports,
            target: EpisodeRef([seed; 32]),
            confidence,
        }
    }

    fn claim(text: String, trust: TrustLevel, confidence: f32) -> MemoryClaim {
        MemoryClaim {
            claim: text,
            trust,
            confidence,
        }
    }

    fn too_large(actual: usize, maximum: usize) -> CairnError {
        CairnError::ValueTooLarge { actual, maximum }
    }

    fn assert_weight(tier: u8, want: f64) {
        assert!((authority_weight(tier) - want).abs() < f64::EPSILON);
    }

    fn roundtrip<T>(
        values: &[T],
        to_str: fn(T) -> &'static str,
        from_str: fn(&str) -> Result<T, CairnError>,
    ) where
        T: Copy + PartialEq + std::fmt::Debug,
    {
        for &value in values {
            assert_eq!(from_str(to_str(value)), Ok(value));
        }
    }

    #[test]
    fn vocab_parse_roundtrip() {
        roundtrip(
            &[TokenKind::Entity, TokenKind::Event, TokenKind::State],
            TokenKind::as_str,
            TokenKind::parse,
        );
        roundtrip(
            &[TokenKind::Relation, TokenKind::Affordance],
            TokenKind::as_str,
            TokenKind::parse,
        );
        roundtrip(
            &[TokenKind::Evidence, TokenKind::Permission],
            TokenKind::as_str,
            TokenKind::parse,
        );
        roundtrip(
            &[TokenKind::ActionResult, TokenKind::ViabilitySignal],
            TokenKind::as_str,
            TokenKind::parse,
        );
        roundtrip(
            &[TokenKind::MemoryEpisode, TokenKind::Contradiction],
            TokenKind::as_str,
            TokenKind::parse,
        );
        roundtrip(
            &[TokenKind::Uncertainty],
            TokenKind::as_str,
            TokenKind::parse,
        );
        roundtrip(
            &[
                LinkRelation::Before,
                LinkRelation::After,
                LinkRelation::CausedBy,
            ],
            LinkRelation::as_str,
            LinkRelation::parse,
        );
        roundtrip(
            &[LinkRelation::Supports, LinkRelation::Contradicts],
            LinkRelation::as_str,
            LinkRelation::parse,
        );
        roundtrip(
            &[LinkRelation::SameEntityAs, LinkRelation::DependsOn],
            LinkRelation::as_str,
            LinkRelation::parse,
        );
        roundtrip(
            &[LinkRelation::Enables, LinkRelation::Blocks],
            LinkRelation::as_str,
            LinkRelation::parse,
        );
        roundtrip(
            &[Modality::Text, Modality::Code, Modality::Ui],
            Modality::as_str,
            Modality::parse,
        );
        roundtrip(
            &[Modality::Vision, Modality::Video, Modality::Audio],
            Modality::as_str,
            Modality::parse,
        );
        roundtrip(
            &[Modality::System, Modality::Memory, Modality::Tool],
            Modality::as_str,
            Modality::parse,
        );
        roundtrip(&[Modality::Workflow], Modality::as_str, Modality::parse);
        roundtrip(
            &[
                Actionability::None,
                Actionability::Inspect,
                Actionability::Ask,
            ],
            Actionability::as_str,
            Actionability::parse,
        );
        roundtrip(
            &[
                Actionability::Repair,
                Actionability::Execute,
                Actionability::Block,
            ],
            Actionability::as_str,
            Actionability::parse,
        );
        roundtrip(
            &[TrustLevel::Observed, TrustLevel::Inferred],
            TrustLevel::as_str,
            TrustLevel::parse,
        );
        roundtrip(
            &[TrustLevel::Predicted, TrustLevel::Synthesized],
            TrustLevel::as_str,
            TrustLevel::parse,
        );
        assert_eq!(
            TokenKind::parse("bogus"),
            Err(CairnError::InvalidKind {
                value: "bogus".into(),
                context: "TokenKind::parse",
            })
        );
        assert!(matches!(
            LinkRelation::parse("frobs"),
            Err(CairnError::InvalidRelation { .. })
        ));
    }

    #[test]
    fn link_confidence_rejected() {
        let bad = link(3, 1.5);
        assert_eq!(bad.validate(), Err(CairnError::InvalidConfidence(1.5)));
        let nan = link(3, f32::NAN);
        let nan_err = nan.validate();
        assert!(matches!(nan_err, Err(CairnError::InvalidConfidence(_))));
        assert!(link(3, 1.0).validate().is_ok());
    }

    #[test]
    fn zero_id_rejected() {
        assert_eq!(EpisodeRef::new([0u8; 32]), Err(CairnError::ZeroId));
        assert!(observation().validate().is_ok());
        assert!(link(1, 1.0).validate().is_ok());
    }

    #[test]
    fn episode_ref_display_and_framing() {
        let reference = EpisodeRef([7u8; 32]);
        assert_eq!(reference.to_string(), "07".repeat(32));
        assert_eq!(reference.into_bytes(), [7u8; 32]);
        // digest_frame spells length-prefix (u64LE) + bytes per part.
        let mut manual = Hasher::new();
        for part in [b"ab".as_slice(), b"c".as_slice()] {
            manual.update(&u64::try_from(part.len()).unwrap_or(u64::MAX).to_le_bytes());
            manual.update(part);
        }
        let want: [u8; 32] = *manual.finalize().as_bytes();
        assert_eq!(digest_frame(&[b"ab", b"c"]), want);
        assert_ne!(digest_frame(&[b"ab", b"c"]), digest_frame(&[b"a", b"bc"]));
    }

    #[test]
    fn deny_first_steering() {
        assert!(!decide(Actionability::Execute, false).allow);
        assert!(!decide(Actionability::Block, false).allow);
        assert!(decide(Actionability::Execute, true).allow);
        assert!(decide(Actionability::Block, true).allow);
        let safe = [
            Actionability::None,
            Actionability::Inspect,
            Actionability::Ask,
            Actionability::Repair,
        ];
        for action in safe {
            assert!(decide(action, false).allow);
        }
        let denied = SteeringDecision::enforce(Actionability::Execute, false);
        assert_eq!(denied, Err(CairnError::UnconfirmedDangerous));
        assert!(SteeringDecision::enforce(Actionability::Execute, true).is_ok());
    }

    #[test]
    fn authority_weights_match_tiers() {
        assert_weight(0, 1.0);
        assert_weight(1, 0.8);
        assert_weight(2, 0.5);
        assert_weight(3, 0.2);
        assert_weight(u8::MAX, 0.2);
    }

    #[test]
    fn content_hash_deterministic_and_sensitive() {
        let base = observation();
        assert_eq!(base.content_hash(), observation().content_hash());
        let mut changed = observation();
        changed.value = b"other reading".to_vec();
        assert_ne!(base.content_hash(), changed.content_hash());
        let mut rekinded = observation();
        rekinded.kind = TokenKind::Event;
        assert_ne!(base.content_hash(), rekinded.content_hash());
        let mut rescoped = observation();
        rescoped.actionability = Actionability::Repair;
        assert_ne!(base.content_hash(), rescoped.content_hash());
        let mut uncertain = observation();
        uncertain.uncertainty = 0.2;
        assert_ne!(base.content_hash(), uncertain.content_hash());
        let mut linked = observation();
        linked.links.push(link(2, 0.9));
        assert_ne!(base.content_hash(), linked.content_hash());
    }

    #[test]
    fn claim_bounds_enforced() {
        let good = claim("rotor nominal".into(), TrustLevel::Observed, 0.9);
        assert!(good.validate().is_ok());
        let empty = claim(String::new(), TrustLevel::Inferred, 0.5);
        assert_eq!(empty.validate(), Err(CairnError::EmptyValue));
        let big = claim("x".repeat(MAX_CLAIM_BYTES + 1), TrustLevel::Predicted, 0.5);
        let big_err = big.validate();
        let want = too_large(MAX_CLAIM_BYTES + 1, MAX_CLAIM_BYTES);
        assert_eq!(big_err, Err(want));
        let edge = claim("x".repeat(MAX_CLAIM_BYTES), TrustLevel::Synthesized, 0.5);
        assert!(edge.validate().is_ok());
        let wild = claim("rotor nominal".into(), TrustLevel::Observed, 2.0);
        assert_eq!(wild.validate(), Err(CairnError::InvalidConfidence(2.0)));
    }

    #[test]
    fn observation_bounds_enforced() {
        let mut obs = observation();
        for seed in 1..=64u8 {
            obs.links.push(link(seed, 1.0));
        }
        assert!(obs.validate().is_ok());
        obs.links.push(link(65, 1.0));
        let many = CairnError::TooManyLinks {
            actual: 65,
            maximum: MAX_LINKS,
        };
        assert_eq!(obs.validate(), Err(many));
        let mut obs = observation();
        obs.uncertainty = 2.0;
        assert_eq!(obs.validate(), Err(CairnError::InvalidConfidence(2.0)));
        obs.uncertainty = 0.1;
        obs.value.clear();
        assert_eq!(obs.validate(), Err(CairnError::EmptyValue));
        obs.value = vec![0u8; MAX_VALUE_BYTES + 1];
        let huge_err = obs.validate();
        let want = too_large(MAX_VALUE_BYTES + 1, MAX_VALUE_BYTES);
        assert_eq!(huge_err, Err(want));
        let bytes = [0x5au8; 16];
        let mut id = [0u8; 32];
        id[..16].copy_from_slice(&bytes);
        id[16..].copy_from_slice(&bytes);
        obs.value = b"sensor reading".to_vec();
        obs.id = EpisodeRef(id);
        assert!(obs.validate().is_ok());
    }
}
