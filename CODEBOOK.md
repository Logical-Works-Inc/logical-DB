# Logical Works Codebook

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S42 S43. Source URLs are in `evidence/SOURCES.md`.

The code rules for this repository. Process is in `WORKFLOW.md`. Authority,
decisions, and the completion ledger are in `GOVERNANCE.md`. The agent entry
point is `AGENTS.md`.

This file is binding on every line of source in the repository, including test
code. There is no separate, weaker standard for tests.

---

## 1. Precedence

| Rank | Source | Wins over |
|---|---|---|
| 1 | Director correction, recorded in `GOVERNANCE.md` | everything below |
| 2 | `GOVERNANCE.md` recorded decisions and ADRs | this codebook |
| 3 | This codebook | convention, habit, prior art in the tree |
| 4 | The file you are in (local idiom) | generic style guides |

A Director correction is a defect verdict. Preserve it as a regression, repair
the lowest invariant that admits the failure, and do not argue the scope down.

Conflicting historical contracts are not silently resolved. Record the conflict
in `GOVERNANCE.md` and pause the work that depends on the choice.

---

## 2. The lint contract

### 2.1 `forbid`, never `deny`

Every lint in `[workspace.lints]` is `forbid` unless this codebook records an
exception. A `deny` is defeated by any single `#[allow]` anywhere in the crate,
including one smuggled in by a macro this workspace does not own. `forbid`
cannot be lowered from source; a conflicting allow is a hard `E0453`. That
property is the only reason the contract binds without a human in the loop.

**Documented exceptions** (each is `deny`, and each is `deny` for a reason that
`forbid` cannot express):

| Lint | Why `deny` |
|---|---|
| `print_stderr` / `print_stdout` | `main.rs`, `src/bin/`, `examples/`, `benches/` are exempt surfaces under the PRINTS rule and must be able to lower the lint with a reasoned `#[expect]` |
| `exit` | crash and process-kill fixtures must call `process::exit` to skip `Drop`; `forbid` would make those fixtures unexpressible |
| `unsafe_code` | one reviewed FFI leaf must carry `#[expect(unsafe_code, reason = "...")]`; `E0453` forbids lowering `forbid`. Every safe crate still states `#![forbid(unsafe_code)]` at its root and is therefore stricter than the table |

### 2.2 The four silent traps

An unknown lint name in `[lints]` produces **no diagnostic at all**. A typo is a
rule that does not exist. Four names in the anti-pattern corpus do not work as
written:

| Corpus said | Truth | Consequence as written |
|---|---|---|
| `clippy::unwrap_or_else_default` | renamed to `clippy::unwrap_or_default` | renamed-lint warning, rule does nothing |
| `clippy::improper_ctypes` | **rustc** lint | `E0602`; belongs in `[lints.rust]` |
| `clippy::improper_ctypes_definitions` | **rustc** lint | `E0602`; belongs in `[lints.rust]` |
| any unknown name | silently ignored | no diagnostic at all |

Validate every lint addition against a deliberately fake control lint in the
same run and confirm the control actually warned. A TOML parse error (duplicate
key, missing newline) silently voids the entire table, control included.

Two further shape-level no-ops were verified against rustc 1.98.1 on 2026-09-22
with a control crate (`exit`, `print_stderr`, `print_stdout` all at `deny`):

| Shape | Truth | Consequence |
|---|---|---|
| `std::process::exit` inside `fn main` | `clippy::exit` does not fire there under any call form (`std::process::exit`, `use std::process::exit`, `use std::process`) | a `#[expect(clippy::exit)]` on a bin that only exits from `main` is unfulfilled, and the `deny` never bites. A crash fixture that must exit calls it from a helper (`fn fail(...) -> !`), never from `main` |
| `writeln!(std::io::stderr(), ..)` | `clippy::print_stderr` does not fire | the PRINTS rule bans it and the lint does not. It is an API-ban and review rule, not a lint rule |

`clippy::exit` does fire on the same call from any other function. That is the
whole reason the crash fixtures keep their exit site in a helper.

### 2.3 The coverage ceiling

**25 of 75 catalogued anti-patterns name an enforceable lint.** The other 50
need Miri, hardware performance counters, allocation profiling, or human review.
They are not lint-expressible and no configuration will catch them.

Claiming "all anti-patterns are enforced" is false. State the ceiling wherever
coverage is claimed. The non-expressible remainder clusters as UB and aliasing
(Miri), false sharing and cache layout (counters), allocation and
monomorphization (profilers), and API and domain-modelling shape (review).

### 2.4 API bans are half the gate

`clippy.toml` `disallowed-methods` and `disallowed-types` are warn-by-default.
Config states the *what*; `[lints]` states the *fatal*. Both must be present.
Every banned entry names the replacement, not just the prohibition.

---

## 3. Anti-slop invariants

Zero tolerance. Each line is a rule the gate enforces where it can and a review
rule where it cannot.

| Invariant | Required form |
|---|---|
| No production `.unwrap()` / `.expect()` | typed domain errors and `?`. Exception: compile-time constants, or an immediately preceding length check with explicit `// SAFETY:` rationale |
| No invariant-breaking `pub` | fields private by default; `pub(crate)` or accessors (`as_slice()`) |
| No defensive `.clone()` | borrows (`&str`, `&[T]`), `Cow<'a, T>`, or in-place transformation. Passing ownership is for permanent retention only |
| No mutex across `.await` | scope the lock to the critical section, or use a bounded actor channel |
| No unbounded memory or channels | `unbounded_channel()` forbidden; explicit buffer bounds; every cache declares TTL or LRU eviction |
| No fire-and-forget spawns | every task tracked in a `JoinSet` or supervisor with a `CancellationToken` |
| No strong reference cycles | back-pointers and listeners use `Weak` |
| No infinite loops without cancellation | every loop has a timeout, iteration or retry ceiling, or cancellation branch. No spin-waiting |

### 3.1 Type system as contract

- Make invalid states unrepresentable: sum types for state machines, newtypes
  for domain primitives (`UserId(u64)` not `u64`), phantom types for
  compile-time capability tracking, builders for validated construction.
- Prefer static dispatch (`impl Trait`, generics) over `Box<dyn Trait>` unless
  runtime polymorphism is the actual requirement.
- State machine as type parameter: `Connection<Disconnected>` →
  `Connection<Handshaking>` → `Connection<Ready>`. A runtime `is_ready` boolean
  is a design smell.
- `#[non_exhaustive]` on public enums and structs that will grow, with a
  `pub fn new(...)` constructor. Struct-expression construction of a
  non-exhaustive type outside its crate is `E0639`; build through the
  constructor.

### 3.2 Errors

- Typed, exhaustive error enums with structured context and `source()` chains.
  No `String` errors in library code. No `anyhow` in a public API.
- `?` propagation with explicit conversion at module boundaries.
- `type Error = Infallible` is a claim that the operation cannot fail. Do not
  use it on a port whose implementation can refuse; introduce a real error type
  so the refusal is expressible.
- `unreachable!()` is a panic. Where a value can be refused, return the typed
  error instead.

### 3.3 Arithmetic and casts

- `checked_*` / `saturating_*` for arithmetic that can overflow.
- `div_ceil` or `checked_div` for integer division.
- `try_from(...).unwrap_or(...)` instead of `as` casts. `as_conversions` is
  forbidden.

### 3.4 Reference patterns

`pattern_type_mismatch` is forbidden. Matching on `&T` or `&mut T` needs `&_`
or `&mut _` patterns, or `match *t`. Non-Copy payloads use `ref` / `ref mut`.
Slice destructuring of owned data:

```rust
let &[ref path, ref account, ref space] = arguments.as_slice() else {
    return Err(Error::WrongArity(arguments.len()));
};
```

### 3.5 Non-async `fn -> impl Future`

A plain `fn` returning `impl Future` cannot use `?` in its body. Build the
result first and wrap with `ready(result)`.

### 3.6 Locks

`Mutex::lock()` recovers from poisoning with
`.unwrap_or_else(std::sync::PoisonError::into_inner)`. Do not `unwrap()` a
poisoned lock.

---

## 4. API shape

- Private by default. A `pub` field is a promise that any value of that type is
  valid; only make it when that promise is true.
- `#[must_use]` on anything whose return value is the point.
- Every public function with branching logic has unit tests covering each branch.
- New public API ships with a `/// ```rust` doc example so `cargo test --doc`
  compiles it, or with an example under `examples/`.
- Every `pub mod` listing carries a `///` on the declaration line.
- Semver is strict. Additive changes with defaults and new `#[non_exhaustive]`
  variants are minor. Removals and signature changes are major.
- Idempotent writes: an operation that may be retried is safe to retry. Document
  which operations are idempotent.
- Cursor-based pagination for lists. No offset pagination, no unbounded result
  sets.
- Deprecated APIs warn for two minor versions before removal, and the
  deprecation message carries the migration guide.

---

## 5. Concurrency and resources

- Structured concurrency: every spawned task has an owning scope, a cancellation
  token, and a bounded lifetime. `JoinHandle` is stored and awaited.
- RAII for all resources: files, connections, locks, handles, temp dirs are owned
  by a struct with deterministic cleanup in `Drop`. No `close()` a caller might
  forget.
- Document the global lock acquisition order. Prefer bounded `mpsc` over `Mutex`
  where a channel expresses the ownership. When a `Mutex` is necessary, hold it
  for the minimum critical section.
- Backpressure by design. Every queue, channel, and buffer declares capacity and
  overflow strategy at construction (bounded channel blocks, ring buffer
  overwrites oldest, error returned). Never discover the policy at 3am.
- Connection pools are bounded, with health checks, idle timeouts, and circuit
  breakers. No per-request connection establishment. No unbounded pool growth.

---

## 6. Observability

- `tracing` spans with correlation IDs, module owner, operation name, duration.
  No bare `println!` in library code. That is the PRINTS rule.
- Levels: `ERROR` broken contract, `WARN` degraded but serving, `INFO` lifecycle
  events, `DEBUG` developer diagnostics, `TRACE` wire-level.
- Service boundaries emit request latency histograms (p50/p95/p99), throughput
  counters, error rate by type, and queue depth or saturation gauge. Histograms,
  never averages.
- Health endpoints check real dependencies: liveness (process alive), readiness
  (dependencies reachable, can serve), startup (initialization complete). A 200
  that checks nothing is not a health endpoint.

---

## 7. Tests

- **Unit**: pure functions, isolated logic, fast, deterministic, no I/O.
- **Integration**: real module boundaries with real or containerized
  dependencies. Test the contract: these inputs produce these outputs and this
  persisted state.
- **End-to-end journeys**: critical user paths through public APIs from clean
  state. Happy path, error paths, concurrent access, crash-and-recover. At least
  10% of e2e tests induce real process kills or panic injection and verify
  recovery and data integrity.
- **Property-based**: for algorithms and parsers, explore the input space and
  encode round-trip and permutation invariants.

Every assertion checks a real value.

```rust
// hollow
assert!(result.is_ok());
assert!(db.count() > 0);

// real
assert_eq!(response.status, StatusCode::OK, "unauthenticated read must be refused");
assert_eq!(db.count(), 5, "five committed documents must survive restart");
```

Test style for this estate:

- `fn test() -> Result<(), Box<dyn Error>>` and `?` inside. Fixture helpers
  bubble `Result`.
- Every assert carries a trailing message.
- Every private item carries a `///` doc.
- No `unwrap` / `expect` / `panic` / `unreachable!` in test code either. The
  `clippy.toml` has no `allow-*-in-tests` relaxation on purpose.

A happy-path-only suite is a failing implementation. Mocks may assist but cannot
be the only proof.

---

## 8. Comments and naming

Comments state constraints the code cannot show. No narration, no history, no
reassurance. If a comment says what the next line does, delete the comment.

Name for systems design and architectural fidelity. A name states the entity's
role in the architecture using global, industry-standard terms, not internal
shorthand or implementation incidentals. An outside architect should recognize
it: "content-addressed fact log", "materialized read-model by replay",
"bounded capability vocabulary". A name that misleads is bad context; kill it at
the source.

Match the file you are in: naming, comment density, idiom.

---

## 9. Suppressions

Every `#[allow(...)]` and `#[expect(...)]` carries `reason = "..."`.

```rust
#[expect(unsafe_code, reason = "FoundationDB network-boot FFI leaf; no safe binding exists")]
```

`expect` is a suppression exactly as `allow` is. A reasonless suppression is
invisible to the detector and is a rule that does not exist. `allow_attributes`
and `allow_attributes_without_reason` are `forbid` so the compiler enforces this.

Never widen a lint level, edit a pinned version, or add a consumer to a register
to clear a refusal. That is a suppressed gate, not a fix.

---

## 10. Dependencies

Estate first, in this order:

```
std / core / alloc
  → lgwks_std     (sync primitives, JSON via lgwks_std::json, hash)
    → lgwks_bot   (async runtime, runners, actors; owns the single tokio edge)
      → lgwks_ast (parsing, multi-language, typed diagnostics)
        → lgwks_deps (third-party storefront, admission gate)
```

- JSON is `lgwks_std::json` with `#[serde(crate = "lgwks_std::json::serde")]`.
  Do not declare `serde` directly.
- Async is `lgwks_bot::rt`. `lgwks_std::task` is sync-only. Do not author
  `tokio` or `futures` edges.
- A new third-party dependency is an optional, feature-gated edge of
  `lgwks_deps`, registered in `contract/APPROVED.toml`, never a direct edge and
  never a new top-level crate.
- Estate edges come from the registry (crates.io), not a git rev. A git pin
  cannot witness that it still matches a published release and silently drifts
  behind the estate's own published surface.
- `allow-git = []` in `deny.toml`. `required-git-spec = "rev"` if a git edge is
  ever authorized. Exact resolved versions are pinned by `Cargo.lock`.
- If a capability exists in `std`, `lgwks_std`, `lgwks_bot`, or `lgwks_ast`, use
  it. Duplicating a parser, runtime, codec, HTTP client, ID type, time helper, or
  fs wrapper the estate already provides is a defect.

---

## 11. Banned constructs and their replacement

| Banned | Replacement |
|---|---|
| `tokio::sync::mpsc::unbounded_channel` | `lgwks_bot::rt::sync::mpsc::channel` (bounded) |
| `std::sync::mpsc::channel` | the bounded channel |
| `tokio::spawn` | `lgwks_bot::rt::task::spawn` inside a tracked scope |
| `std::thread::spawn` | `std::thread::Builder::spawn` with a joined handle, or `lgwks_bot` |
| `std::thread::sleep` | `std::thread::park_timeout` (sync) or `rt::time::sleep` (async) |
| `std::mem::uninitialized` | `std::mem::MaybeUninit` |
| `std::mem::zeroed` | `MaybeUninit`, or a typed constructor |
| `std::mem::forget` | break the cycle; use `Weak` |
| `std::rc::Rc::new_cyclic` | `Weak` back-pointers |
| `std::vec::Vec::remove(0)` | `VecDeque` for queue semantics |
| `String` errors in library code | typed error enums with `source()` |
| `unreachable!()` | a typed `Err` the caller can handle |
| struct expression on a `#[non_exhaustive]` type | the type's `pub fn new(...)` |
| `println!` / `eprintln!` in library code | `tracing` |
| `process::exit` outside a crash fixture | return the typed error |

---

## 12. What a green gate does not prove

A green compile, a green test suite, and green CI are first-party evidence of
exactly what they ran and nothing more. They do not prove:

- that the 50 non-lint-expressible anti-patterns are absent;
- that a measurement environment exists where none is committed;
- that a migration is complete because its documentation says "canonical";
- that a scale, cost, availability, or superiority claim is established.

Where evidence is missing, say **unknown**, **not exercised**, or **not
independently verified**. Never substitute a plausible story. Self-authored
tests, fixtures, PR descriptions, docs, and model summaries are first-party
evidence only. Mergeable and green is not merge-safe.

This contract outranks velocity and model confidence.
