# Invariants

One line per rule: rule | why | enforcing test or gate. Test paths are under
`crates/logical-db-e2e/` unless named.

## Data

- A committed local edit survives a process exit that skips destructors | a crash must not lose an acknowledged write | `tests/crash_recovery.rs::committed_edit_survives_process_exit_without_destructors`
- A forced kill never exposes a half-applied local transition | redb commits are atomic | `tests/kill_recovery.rs::forced_termination_never_exposes_a_partial_embedded_transition`
- A kill during baseline restore commits nothing; a rerun installs all of it | an in-memory accumulator is not durable state | `tests/fdb_process_kill_restore.rs::killed_restore_commits_nothing_and_a_clean_rerun_recovers`
- A replica behind the retention floor recovers state and checkpoint in one install | pruning must never strand a replica | `src/journeys_recovery.rs::fresh_replica_recovers_a_pruned_document_through_real_http`, `tests/fdb_recovery_composition.rs::fdb_recovery_composition`
- Two replicas converge through real HTTP and keep indexes current | sync is the product | `src/journeys_converge.rs::two_embedded_replicas_converge_through_real_http_and_keep_indexes_current`
- Lost, duplicated and post-commit-lost exchanges converge to the model on every seed | networks fail in every way | `tests/sim_sync.rs::sim_sync_converges_under_seeded_network_faults`
- One seed replays one trace | a failure must reproduce | `tests/sim_sync.rs::sim_sync_same_seed_replays_the_same_trace`
- Tenants sharing one service never see each other's documents | scope is the security boundary | `tests/sim_sync.rs::sim_sync_tenants_sharing_a_service_stay_isolated`
- An FDB may-have-committed retry returns the first receipt and appends once | exactly-once under ambiguity | `tests/fdb_ambiguity.rs::live_fdb_recovers_unknown_commit_and_preserves_all_axes`
- The KV cache adapter holds its contract end to end | the cache is a shipped surface | `tests/kv_journey.rs::kv_cache_journey_holds_end_to_end`

## Platform

- Windows default path handles locks, Unicode and read-only roots | Windows is a release target | `src/journeys_converge.rs::windows_default_path_lock_unicode_restart_and_read_only_failure_are_real`
- The documented embedded install is explicit and immutable | a first-timer must succeed from the README | `tests/installation_contract.rs::documented_embedded_installation_is_immutable_and_explicit`

## Structure

- Identity bytes decode through RFC 9562 hyphenated text and only at 16 bytes | lgwks_std 5 `Uuid` refuses every other form, which broke every derived identity | `crates/logical-db/src/identity.rs::sixteen_bytes_round_trip_and_other_lengths_are_refused`
- Every workspace crate is declared and depends only on the crates below it | a lower crate importing a higher one breaks the one-way core | `tests/crate_graph.rs::workspace_crate_graph_is_one_way`
- Every external dependency edge is registered with its consumers | no silent dependency growth | `tests/dependency_contract.rs::every_authored_external_edge_is_owned`
- CI runs on pinned GitHub-hosted images only | a public repo must never run on the owner's machine | `tests/ci_routing.rs::committed_workflows_route_to_hosted_images`
- Every rust-cache step saves from main only | pull requests must not evict main's cache | `tests/ci_routing.rs::every_rust_cache_saves_from_main_only`
- No advisory, unknown licence or git source | supply chain | `cargo deny check` (`deny.toml`)
- `unsafe` exists only in `crates/logical-db-daemon/src/fdb_boot.rs` | one reviewed FFI leaf | workspace `unsafe_code = "deny"`, `#![forbid(unsafe_code)]` per crate root, estate guard lint register
- No lint suppression in source | a suppression is a rule that does not exist | clippy `allow_attributes = "forbid"`
- No code file over 750 code lines or 1000 total; no `.md` over 120 lines | small files stay reviewable | estate pre-commit size law
