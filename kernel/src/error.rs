//! Owns the kernel error type: every variant names the violated
//! invariant or the boundary that failed. No strings-only errors; the
//! io boundary keeps its source error intact.

use std::fmt;

// ── KernelError ──────────────────────────────────────────────

#[derive(Debug)]
pub enum KernelError {
    /// An io boundary failed; the source error is preserved.
    Io(std::io::Error),
    /// INV-TS-MONOTONIC violated: a row's ts precedes committed time.
    TsRegression { last: u64, got: u64 },
    /// INV-ONE-CORE-FIVE-CLASSES violated at the wire boundary.
    UnknownClass(u8),
    /// A row codec hit end-of-buffer mid-field.
    TruncatedRow,
    /// A length field exceeds the wire limit (u32).
    RowTooLarge,
    /// A composite-key component exceeds the maximum length (u16).
    ComponentTooLong,
    /// An encoded composite key is truncated or malformed.
    MalformedCompositeKey,
    /// INV-ID-PREFIX-DISJOINT violated at decode: a stored key's id
    /// region breaks the escape framing (bare 0x00, missing
    /// terminator) and cannot be trusted.
    MalformedKey,
    /// Attempted to store NaN or infinite confidence.
    InvalidConfidence,
    /// Attempted to store a NaN or infinite vector norm.
    InvalidNorm,
    /// A Hamming distance was requested between codes of different
    /// lengths; the distance is undefined there.
    CodeLengthMismatch { expected: usize, got: usize },
    /// The durable backend (issue #6 §5) refused an operation; the
    /// message names the backend-level cause.
    Backend(String),
    /// FDB transaction exceeded the explicit size limit (issue #10 F1).
    /// Returned BEFORE the binding sees the op (INV-FDB-PIN).
    TransactionTooLarge { size: usize, limit: usize },
    /// FDB transaction exceeded the explicit time limit (issue #10 F1).
    TransactionTimedOut { elapsed_ms: u64, limit_ms: u64 },
}

impl fmt::Display for KernelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KernelError::Io(e) => write!(f, "io: {e}"),
            KernelError::TsRegression { last, got } => {
                write!(f, "ts regression: got {got}, last committed {last}")
            }
            KernelError::UnknownClass(tag) => {
                write!(f, "unknown class tag {tag:#04x}")
            }
            KernelError::TruncatedRow => write!(f, "row ends mid-field"),
            KernelError::RowTooLarge => write!(f, "row field exceeds u32 limit"),
            KernelError::ComponentTooLong => {
                write!(f, "composite-key component exceeds u16 limit")
            }
            KernelError::MalformedCompositeKey => {
                write!(f, "composite key is truncated or malformed")
            }
            KernelError::MalformedKey => {
                write!(f, "stored key breaks the id escape framing")
            }
            KernelError::InvalidConfidence => {
                write!(f, "confidence must be finite (no NaN or infinity)")
            }
            KernelError::InvalidNorm => {
                write!(f, "norm must be finite (no NaN or infinity)")
            }
            KernelError::CodeLengthMismatch { expected, got } => {
                write!(
                    f,
                    "code lengths differ: expected {expected} bytes, got {got}"
                )
            }
            KernelError::Backend(msg) => write!(f, "durable backend: {msg}"),
            KernelError::TransactionTooLarge { size, limit } => write!(
                f,
                "fdb transaction too large: {size} bytes exceeds limit {limit}"
            ),
            KernelError::TransactionTimedOut {
                elapsed_ms,
                limit_ms,
            } => write!(
                f,
                "fdb transaction timed out: {elapsed_ms} ms exceeds limit {limit_ms} ms"
            ),
        }
    }
}

impl std::error::Error for KernelError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            KernelError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for KernelError {
    fn from(e: std::io::Error) -> Self {
        KernelError::Io(e)
    }
}

// ── Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_strings_are_stable_per_variant() {
        let io = KernelError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "boom"));
        assert_eq!(io.to_string(), "io: boom");
        assert_eq!(
            KernelError::TsRegression { last: 11, got: 3 }.to_string(),
            "ts regression: got 3, last committed 11"
        );
        assert_eq!(
            KernelError::UnknownClass(0).to_string(),
            "unknown class tag 0x00"
        );
        assert_eq!(KernelError::TruncatedRow.to_string(), "row ends mid-field");
        assert_eq!(
            KernelError::RowTooLarge.to_string(),
            "row field exceeds u32 limit"
        );
        assert_eq!(
            KernelError::ComponentTooLong.to_string(),
            "composite-key component exceeds u16 limit"
        );
        assert_eq!(
            KernelError::MalformedCompositeKey.to_string(),
            "composite key is truncated or malformed"
        );
        assert_eq!(
            KernelError::MalformedKey.to_string(),
            "stored key breaks the id escape framing"
        );
        assert_eq!(
            KernelError::InvalidConfidence.to_string(),
            "confidence must be finite (no NaN or infinity)"
        );
        assert_eq!(
            KernelError::InvalidNorm.to_string(),
            "norm must be finite (no NaN or infinity)"
        );
        assert_eq!(
            KernelError::CodeLengthMismatch {
                expected: 32,
                got: 30
            }
            .to_string(),
            "code lengths differ: expected 32 bytes, got 30"
        );
        assert_eq!(
            KernelError::Backend("lock held".into()).to_string(),
            "durable backend: lock held"
        );
        assert_eq!(
            KernelError::TransactionTooLarge {
                size: 5_000_000,
                limit: 1_000_000
            }
            .to_string(),
            "fdb transaction too large: 5000000 bytes exceeds limit 1000000"
        );
        assert_eq!(
            KernelError::TransactionTimedOut {
                elapsed_ms: 5000,
                limit_ms: 1000
            }
            .to_string(),
            "fdb transaction timed out: 5000 ms exceeds limit 1000 ms"
        );
    }

    #[test]
    fn io_errors_keep_their_source_and_others_have_none() {
        let source_err = std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
        let wrapped = KernelError::Io(source_err);
        let src = std::error::Error::source(&wrapped)
            .expect("io variant must preserve its source")
            .downcast_ref::<std::io::Error>()
            .expect("the preserved source is the original io error");
        assert_eq!(src.kind(), std::io::ErrorKind::NotFound);
        assert!(std::error::Error::source(&KernelError::TruncatedRow).is_none());
    }
}
