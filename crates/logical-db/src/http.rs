//! Opt-in production thin-client transport exposed by the same app facade.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

pub use logical_db_http::{
    CredentialError, CredentialProvider, HttpError, HttpTransport, StaticBearer, Url,
};
