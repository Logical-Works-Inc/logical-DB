//! The seeded wire of the sync simulation: the trace, the choice stream, the
//! fault model and the simulated transport between a replica and the service.

use std::{
    fmt,
    hash::{DefaultHasher, Hasher},
};

use logical_db::transport::{
    BaselineRequest, BaselineResult, PullRequest, PullResult, PushRequest, PushResponse, Transport,
};
use logical_db_service::{ServiceBaseline, ServicePull};

use super::{Failure, Faults, Service};

/// Order-sensitive digest of everything one run observed.
#[derive(Debug, Default)]
pub(super) struct Trace(DefaultHasher);

impl Trace {
    /// Folds `bytes` into the digest.
    pub(super) fn record(&mut self, bytes: &[u8]) -> &mut Self {
        self.0.write(bytes);
        self
    }

    /// The digest of everything recorded so far.
    pub(super) fn digest(&self) -> u64 {
        self.0.finish()
    }
}

/// Seeded choice stream (SplitMix64): std has no seeded generator and the
/// simulation must not draw from the OS.
#[derive(Debug)]
pub(super) struct Draws {
    /// The generator state.
    state: u64,
}

impl Draws {
    /// A stream fully determined by `seed`.
    pub(super) const fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x6c62_272e_07bb_0142,
        }
    }

    /// The next 64 seeded bits.
    pub(super) const fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut mixed = self.state;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^ (mixed >> 31)
    }

    /// Thirty-two seeded bytes.
    pub(super) fn bytes(&mut self) -> [u8; 32] {
        let mut bytes = [0; 32];
        for chunk in bytes.as_chunks_mut::<8>().0 {
            *chunk = self.next().to_le_bytes();
        }
        bytes
    }

    /// A choice in `0..bound`, `bound` being one of the simulation's small constants.
    pub(super) fn below(&mut self, bound: usize) -> Result<usize, Failure> {
        let bound = u64::try_from(bound)?;
        let choice = self
            .next()
            .checked_rem(bound)
            .ok_or("a simulated choice needs a nonzero range")?;
        Ok(usize::try_from(choice)?)
    }
}

/// What the network does to one exchange.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Fault {
    /// Delivered once.
    Clean,
    /// Lost before the service saw it.
    LoseRequest,
    /// Committed by the service, answer lost on the way back.
    LoseResponse,
    /// Delivered twice; the client sees the second answer.
    Duplicate,
}

impl Fault {
    /// The fault's slot in a [`Faults`] tally and its trace byte.
    const fn tag(self) -> u8 {
        match self {
            Self::Clean => 0,
            Self::LoseRequest => 1,
            Self::LoseResponse => 2,
            Self::Duplicate => 3,
        }
    }
}

/// A transport failure the client must survive without acknowledging anything.
#[derive(Debug)]
pub(super) enum NetError {
    /// The request never reached the service.
    LostRequest,
    /// The service answered; the answer was lost.
    LostResponse,
    /// The service refused the exchange.
    Service(String),
    /// The service returned a variant this simulation does not model.
    Unmodelled,
}

impl fmt::Display for NetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::LostRequest => formatter.write_str("simulated request loss"),
            Self::LostResponse => formatter.write_str("simulated response loss"),
            Self::Service(ref error) => write!(formatter, "service refused: {error}"),
            Self::Unmodelled => formatter.write_str("unmodelled service answer"),
        }
    }
}

impl std::error::Error for NetError {}

/// The simulated wire between one replica and the shared service.
#[derive(Debug)]
pub(super) struct Net<'world> {
    /// The service every exchange reaches.
    pub(super) service: &'world Service,
    /// The seeded stream that picks each fault.
    pub(super) draws: &'world mut Draws,
    /// The run's trace.
    pub(super) trace: &'world mut Trace,
    /// The run's fault tally.
    pub(super) faults: &'world mut Faults,
    /// Simulated wall time handed to the service.
    pub(super) now_unix_ms: &'world mut u64,
    /// Whether this cycle runs under faults.
    pub(super) faulty: bool,
    /// Whether the last exchange was lost by the simulated network.
    pub(super) lost: bool,
}

impl Net<'_> {
    /// Picks and records the fault for one exchange of `kind`.
    fn exchange(&mut self, kind: &[u8]) -> Fault {
        let fault = if self.faulty {
            match self.draws.next().checked_rem(10) {
                Some(0) => Fault::LoseRequest,
                Some(1) => Fault::LoseResponse,
                Some(2) => Fault::Duplicate,
                _ => Fault::Clean,
            }
        } else {
            Fault::Clean
        };
        if let Some(count) = self.faults.get_mut(usize::from(fault.tag())) {
            *count = count.saturating_add(1);
        }
        self.trace.record(kind).record(&[fault.tag()]);
        fault
    }

    /// Advances simulated time well inside the checkpoint lifetime.
    fn tick(&mut self) -> u64 {
        *self.now_unix_ms = self.now_unix_ms.saturating_add(7);
        *self.now_unix_ms
    }
}

/// Runs `call` against the service as `fault` dictates.
macro_rules! deliver {
    ($net:ident, $fault:ident, $call:expr) => {{
        $net.lost = $fault == Fault::LoseRequest || $fault == Fault::LoseResponse;
        if $fault == Fault::LoseRequest {
            return Err(NetError::LostRequest);
        }
        let mut answer = $call;
        if $fault == Fault::Duplicate {
            answer = $call;
        }
        if $fault == Fault::LoseResponse {
            return Err(NetError::LostResponse);
        }
        answer
    }};
}

impl Transport for Net<'_> {
    type Error = NetError;

    async fn push(&mut self, request: PushRequest) -> Result<PushResponse, NetError> {
        let fault = self.exchange(b"push");
        let response = deliver!(self, fault, {
            let now = self.tick();
            self.service
                .push(&request, now)
                .await
                .map_err(|error| NetError::Service(error.to_string()))?
        });
        Ok(response)
    }

    async fn pull(&mut self, request: PullRequest) -> Result<PullResult, NetError> {
        let fault = self.exchange(b"pull");
        let answer = deliver!(self, fault, {
            let now = self.tick();
            self.service
                .pull(&request, now)
                .await
                .map_err(|error| NetError::Service(error.to_string()))?
        });
        match answer {
            ServicePull::Page(page) => Ok(PullResult::Page(page)),
            ServicePull::CheckpointExpired => Ok(PullResult::CheckpointExpired),
            ServicePull::BaselineRequired { .. } => Ok(PullResult::BaselineRequired),
            _ => Err(NetError::Unmodelled),
        }
    }

    async fn baseline(&mut self, request: BaselineRequest) -> Result<BaselineResult, NetError> {
        let fault = self.exchange(b"baseline");
        let answer = deliver!(self, fault, {
            let now = self.tick();
            self.service
                .recover(&request, now)
                .await
                .map_err(|error| NetError::Service(error.to_string()))?
        });
        match answer {
            ServiceBaseline::Artifact(page) => Ok(BaselineResult::Artifact(*page)),
            ServiceBaseline::Unavailable => Ok(BaselineResult::Unavailable),
            _ => Err(NetError::Unmodelled),
        }
    }
}
