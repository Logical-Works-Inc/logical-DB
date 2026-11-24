//! Hostile simulation of the journey's push retry rule (`journey_retries`).
//!
//! Requirement (regression for the #135 fold): under client buggify the
//! server answers FDB failures the store classifies as permanent with 500,
//! so the journey must retry every 5xx (RFC 9110 §15.6, server error) and the
//! transport's own retryable set (408, 425, 429), and must stop on every other
//! code so a real refusal (401, 403, 409, 413) still ends the journey.
//!
//! Oracle: a reference table written from RFC 9110 and RFC 6585, independent
//! of the code. Fools: fat-finger (every `u16` status, including 0, 1xx, 3xx
//! and codes past 599), liar-upstream (a seeded `Retry-After` from none to
//! `u64::MAX` seconds that must never change the decision), saboteur (local
//! failures that never reached a server).

use std::time::Duration;

use logical_db::http::HttpError;

use super::binding::journey_retries;

/// Seeded `Retry-After` values per status code.
const DELAYS_PER_CODE: u64 = 4;

/// The reference rule: retry a server error or a status RFC 9110 / RFC 6585
/// mark as safe to repeat for an idempotent request; stop on everything else.
const fn reference_retries(code: u16) -> bool {
    matches!(code, 500..=599 | 408 | 425 | 429)
}

/// A seeded delay (splitmix64 of `seed`): none, zero, huge or arbitrary.
fn liar_delay(seed: u64) -> Option<Duration> {
    let mut mixed = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    mixed ^= mixed >> 31;
    match mixed % 4 {
        0 => None,
        1 => Some(Duration::ZERO),
        2 => Some(Duration::from_secs(u64::MAX)),
        _ => Some(Duration::from_millis(mixed)),
    }
}

#[test]
/// fat-finger and liar-upstream: every status code, each with seeded delays,
/// decides exactly as the reference table does.
fn sim_push_retry_every_status_and_lying_delay_matches_the_reference() {
    let mut retried = 0_u32;
    for code in 0..=u16::MAX {
        for draw in 0..DELAYS_PER_CODE {
            let seed = u64::from(code)
                .wrapping_mul(DELAYS_PER_CODE)
                .wrapping_add(draw);
            let error = HttpError::Status {
                code,
                retry_after: liar_delay(seed),
            };
            assert_eq!(
                journey_retries(&error),
                reference_retries(code),
                "status {code} with Retry-After {:?} (seed {seed})",
                liar_delay(seed)
            );
        }
        retried = retried.saturating_add(u32::from(reference_retries(code)));
    }
    assert_eq!(retried, 103, "100 server errors plus 408, 425 and 429");
}

#[test]
/// saboteur: failures that never reached a server keep the transport's own
/// decision; a deadline is retried, a refused endpoint or body is final.
fn sim_push_retry_saboteur_local_failures_keep_the_transport_decision() {
    let retried = [HttpError::CredentialTimeout, HttpError::OperationTimeout];
    let final_ = [
        HttpError::InvalidEndpoint,
        HttpError::InsecureEndpoint,
        HttpError::BodyTooLarge,
        HttpError::MediaType,
        HttpError::TrailingData,
    ];
    for error in &retried {
        assert!(journey_retries(error), "{error} is retried");
    }
    for error in &final_ {
        assert!(!journey_retries(error), "{error} is final");
    }
}
