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
    /// INV-ONE-CORE-FOUR-CLASSES violated at the wire boundary.
    UnknownClass(u8),
    /// A row codec hit end-of-buffer mid-field.
    TruncatedRow,
    /// A length field exceeds the wire limit (u32).
    RowTooLarge,
    /// A composite-key component exceeds the maximum length (u16).
    ComponentTooLong,
    /// An encoded composite key is truncated or malformed.
    MalformedCompositeKey,
    /// Attempted to store NaN or infinite confidence.
    InvalidConfidence,
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
            KernelError::InvalidConfidence => {
                write!(f, "confidence must be finite (no NaN or infinity)")
            }
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
