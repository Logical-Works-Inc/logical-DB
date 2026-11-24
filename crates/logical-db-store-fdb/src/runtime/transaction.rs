//! Log transactions: accept, prune, page reads and native failure classification.

use foundationdb::{FdbBindingError, KeySelector, RangeOption, options::TransactionOption};
use foundationdb_tuple::Subspace;
use logical_db_model::SnapshotRecord;
use logical_db_protocol::ChangeEnvelope;
use logical_db_service::{
    AcceptResult, PageResult, PruneReceipt, PruneRefusal, PruneResult, StoredPage, prune_decision,
};

use super::baseline::{load_artifact, read_snapshot_record};
use super::{
    AcceptOperation, FdbStoreError, KIND_INVOCATION, KIND_OPERATION, PageQuery, PageSpaces,
    PruneOperation, TRANSACTION_RETRY_LIMIT, TRANSACTION_TIMEOUT_MS,
};
use crate::{
    CHUNK_BYTES, DataError, MANIFEST_BYTES, Record, decode_manifest, decode_operation,
    decode_retention, decode_u64, encode_manifest, encode_retention, invocation_binding,
    invocation_binding_record, logical_page_more, page_read_plan, tuple_id,
};

/// Runs `accept_transaction`.
pub(super) async fn accept_transaction(
    transaction: &foundationdb::RetryableTransaction,
    op: AcceptOperation,
) -> Result<AcceptResult, FdbBindingError> {
    configure_transaction(transaction)?;
    let existing = existing_accept_result(
        transaction,
        &op.scope_space,
        &op.chunk_space,
        &op.operation_key,
        &op.retention_key,
        &op.change,
        op.maybe_committed,
    );
    if let Some(result) = existing.await? {
        return Ok(result);
    }
    if transaction.get(&op.invocation_key, false).await?.is_some() {
        return Ok(AcceptResult::InvocationConsumed);
    }
    let previous = read_frontier(transaction, &op.sequence_key).await?;
    if previous != op.expected_frontier {
        return Ok(AcceptResult::StaleAuthority);
    }
    let sequence = previous
        .checked_add(1)
        .filter(|value| i64::try_from(*value).is_ok())
        .ok_or_else(|| binding(DataError::SequenceExhausted))?;
    let manifest = encode_manifest(&op.change, sequence, op.invocation_id).map_err(binding)?;
    for (index, bytes) in op.change.bytes().chunks(CHUNK_BYTES).enumerate() {
        let index = u16::try_from(index).map_err(binding_invalid)?;
        transaction.set(&op.chunk_space.pack(&(sequence, index)), bytes);
    }
    transaction.set(
        &op.log_space.pack(&(sequence,)),
        &manifest[..MANIFEST_BYTES],
    );
    transaction.set(&op.operation_key, &manifest);
    transaction.set(&op.sequence_key, &sequence.to_be_bytes());
    transaction.set(&op.invocation_key, &invocation_binding(&op.change));
    Ok(AcceptResult::Accepted)
}

/// Runs `prune_transaction`.
pub(super) async fn prune_transaction(
    transaction: &foundationdb::RetryableTransaction,
    operation: PruneOperation,
) -> Result<PruneResult, FdbBindingError> {
    let PruneOperation {
        scope,
        expected_frontier,
        up_to,
        min_live,
        sequence_key,
        retention_key,
        log_space,
        chunk_space,
        baseline,
    } = operation;
    configure_transaction(transaction)?;
    let frontier = read_frontier(transaction, &sequence_key).await?;
    if frontier != expected_frontier {
        return Ok(PruneResult::StaleAuthority);
    }
    let (floor, consumed) = read_retention(transaction, &retention_key).await?;
    let through = match prune_decision(frontier, floor, up_to, min_live, consumed) {
        Ok(value) => value,
        Err(refusal) => return Ok(PruneResult::Refused(refusal)),
    };
    if through <= floor {
        return Ok(PruneResult::Pruned(PruneReceipt::new(floor, floor)));
    }
    let baseline_record = read_snapshot_record(transaction, &baseline.pointer_key).await?;
    let state = match baseline_record {
        Some(SnapshotRecord::State(state)) if state.position() >= through => state,
        Some(SnapshotRecord::State(state)) => {
            return Ok(PruneResult::Refused(PruneRefusal::BaselineBehind {
                position: state.position(),
            }));
        }
        Some(SnapshotRecord::LegacyPosition(_)) | None => {
            return Ok(PruneResult::Refused(PruneRefusal::MissingBaseline));
        }
        // A future descriptor class cannot prove coverage of the prunable
        // prefix; refuse collection as if no state-bearing baseline exists.
        Some(_) => {
            return Ok(PruneResult::Refused(PruneRefusal::MissingBaseline));
        }
    };
    load_artifact(transaction, scope, &state, &baseline).await?;
    if through > floor {
        let log_begin = log_space.pack(&(floor.saturating_add(1),));
        transaction.clear_range(&log_begin, &end_after(&log_space, through));
        let chunk_begin = chunk_space.pack(&(floor.saturating_add(1),));
        transaction.clear_range(&chunk_begin, &end_after(&chunk_space, through));
        transaction.set(&retention_key, &encode_retention(through, consumed));
    }
    Ok(PruneResult::Pruned(PruneReceipt::new(
        through,
        through.max(floor),
    )))
}

/// Runs `existing_accept_result`.
pub(super) async fn existing_accept_result(
    transaction: &foundationdb::RetryableTransaction,
    scope_space: &Subspace,
    chunk_space: &Subspace,
    operation_key: &[u8],
    retention_key: &[u8],
    change: &ChangeEnvelope,
    maybe_committed: bool,
) -> Result<Option<AcceptResult>, FdbBindingError> {
    let Some(existing) = transaction.get(operation_key, false).await? else {
        return Ok(None);
    };
    let operation = decode_operation(existing.as_ref()).map_err(binding)?;
    let stored_invocation_key =
        scope_space.pack(&(KIND_INVOCATION, operation.invocation_id.as_slice()));
    let stored_binding = transaction
        .get(&stored_invocation_key, false)
        .await?
        .ok_or_else(|| binding(DataError::MissingInvocation))?;
    if stored_binding.as_ref() != invocation_binding_record(&operation.record) {
        return Err(binding(DataError::InvalidInvocation));
    }
    let record = operation.record;
    let (floor, _) = read_retention(transaction, retention_key).await?;
    let exact_bytes = if record.sequence <= floor {
        // Chunks below the floor were collected by design; the manifest
        // digest remains the content identity for the retry decision.
        None
    } else {
        Some(load_change_bytes(transaction, chunk_space, record.sequence, &record).await?)
    };
    let exact_retry = record.matches(change)
        && exact_bytes
            .as_ref()
            .is_none_or(|bytes| *bytes == change.bytes());
    Ok(Some(if exact_retry {
        if maybe_committed {
            AcceptResult::RecoveredUnknownCommit
        } else {
            AcceptResult::Duplicate
        }
    } else {
        AcceptResult::Conflict
    }))
}

/// Runs `load_page`.
pub(super) async fn load_page(
    transaction: &foundationdb::RetryableTransaction,
    spaces: &PageSpaces,
    query: PageQuery,
) -> Result<PageResult, FdbBindingError> {
    let frontier = read_frontier(transaction, &spaces.sequence_key).await?;
    if frontier != query.frontier {
        return Ok(PageResult::StaleAuthority);
    }
    let (floor, _) = read_retention(transaction, &spaces.retention_key).await?;
    if query.after < floor {
        return Ok(PageResult::BehindFloor);
    }
    if query.after > frontier {
        return Err(binding(DataError::InvalidManifest { at: "manifest" }));
    }
    let begin = spaces.log.pack(&(query.after.saturating_add(1),));
    let (_, end) = spaces.log.range();
    let plan = page_read_plan(query.max_changes);
    let mut options = RangeOption::from((begin.as_slice(), end.as_slice()));
    options.limit = Some(plan.limit);
    options.target_bytes = plan.target_bytes;
    let values = transaction.get_range(&options, 1, false).await?;
    let range_more = values.more();
    let manifests = values
        .iter()
        .map(|value| (value.key().to_vec(), value.value().to_vec()))
        .collect::<Vec<_>>();
    drop(values);
    materialize_page(transaction, spaces, query, frontier, range_more, manifests).await
}

/// Configure transaction
pub(super) fn configure_transaction(
    transaction: &foundationdb::RetryableTransaction,
) -> Result<(), FdbBindingError> {
    transaction.set_option(TransactionOption::Timeout(TRANSACTION_TIMEOUT_MS))?;
    transaction.set_option(TransactionOption::RetryLimit(TRANSACTION_RETRY_LIMIT))?;
    Ok(())
}

/// Reads a scope's durable frontier. An absent sequence key is a scope that
/// never committed, so its frontier is the log origin, zero.
pub(super) async fn read_frontier(
    transaction: &foundationdb::RetryableTransaction,
    key: &[u8],
) -> Result<u64, FdbBindingError> {
    match transaction.get(key, false).await? {
        Some(bytes) => decode_u64(bytes.as_ref()).map_err(binding),
        None => Ok(0),
    }
}

/// Reads a scope's retention floor and consumed mark. An absent key is a
/// scope that never pruned or consumed, so both stand at the log origin.
pub(super) async fn read_retention(
    transaction: &foundationdb::RetryableTransaction,
    key: &[u8],
) -> Result<(u64, u64), FdbBindingError> {
    match transaction.get(key, false).await? {
        Some(bytes) => decode_retention(bytes.as_ref()).map_err(binding),
        None => Ok((0, 0)),
    }
}

/// Exclusive range end just past `through` in `space`; at `u64::MAX` the end
/// is the end of the whole space, since no later key can exist.
pub(super) fn end_after(space: &Subspace, through: u64) -> Vec<u8> {
    match through.checked_add(1) {
        Some(next) => space.pack(&(next,)),
        None => space.range().1,
    }
}

/// Runs `materialize_page`.
pub(super) async fn materialize_page(
    transaction: &foundationdb::RetryableTransaction,
    spaces: &PageSpaces,
    query: PageQuery,
    frontier: u64,
    range_more: bool,
    manifests: Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<PageResult, FdbBindingError> {
    let mut changes = Vec::with_capacity(usize::from(query.max_changes));
    let mut aggregate = 0usize;
    let mut position = query.after;
    let mut stopped_at_page_boundary = false;
    for (key, value) in manifests {
        if changes.len() == usize::from(query.max_changes) {
            stopped_at_page_boundary = true;
            break;
        }
        let (sequence,) = spaces.log.unpack::<(u64,)>(&key).map_err(binding_invalid)?;
        if position.checked_add(1) != Some(sequence) {
            return Err(binding(DataError::InvalidManifest { at: "manifest" }));
        }
        let record = match decode_manifest(&value, sequence).map_err(binding) {
            Ok(record) => record,
            Err(error) => {
                return Err(error);
            }
        };
        verify_operation(transaction, &spaces.scope, &record).await?;
        let Some(next_bytes) = aggregate.checked_add(record.byte_len) else {
            return Err(binding(DataError::InvalidManifest { at: "manifest" }));
        };
        let max_bytes = usize::try_from(query.max_bytes).map_err(binding_invalid)?;
        if next_bytes > max_bytes {
            if changes.is_empty() {
                return Err(binding(DataError::ChangeExceedsPage));
            }
            stopped_at_page_boundary = true;
            break;
        }
        let bytes = match load_change_bytes(transaction, &spaces.chunk, sequence, &record).await {
            Ok(bytes) => bytes,
            Err(error) => {
                return Err(error);
            }
        };
        changes.push(record.envelope(query.scope, bytes));
        aggregate = next_bytes;
        position = sequence;
    }
    let has_entry = match log_has_entry_after(transaction, &spaces.log, frontier).await {
        Ok(has_entry) => has_entry,
        Err(error) => {
            return Err(error);
        }
    };
    if position == frontier && has_entry {
        return Err(binding(DataError::InvalidManifest { at: "manifest" }));
    }
    let more = match logical_page_more(frontier, position, range_more, stopped_at_page_boundary)
        .map_err(binding)
    {
        Ok(more) => more,
        Err(error) => {
            return Err(error);
        }
    };
    Ok(PageResult::Page(StoredPage::new(changes, position, more)))
}

/// Runs `log_has_entry_after`.
pub(super) async fn log_has_entry_after(
    transaction: &foundationdb::RetryableTransaction,
    log: &Subspace,
    frontier: u64,
) -> Result<bool, FdbBindingError> {
    let terminal = log.pack(&(frontier,));
    let (_, end) = log.range();
    let options = RangeOption {
        begin: KeySelector::first_greater_than(terminal),
        end: KeySelector::first_greater_or_equal(end),
        limit: Some(1),
        target_bytes: MANIFEST_BYTES,
        ..RangeOption::default()
    };
    Ok(!transaction.get_range(&options, 1, false).await?.is_empty())
}

/// Runs `verify_operation`.
pub(super) async fn verify_operation(
    transaction: &foundationdb::RetryableTransaction,
    scope_space: &Subspace,
    record: &Record,
) -> Result<(), FdbBindingError> {
    let operation_key = scope_space.pack(&(KIND_OPERATION, tuple_id(record.operation.as_uuid())));
    let operation_bytes = transaction
        .get(&operation_key, false)
        .await?
        .ok_or_else(|| binding(DataError::InvalidManifest { at: "manifest" }))?;
    let operation = decode_operation(operation_bytes.as_ref()).map_err(binding)?;
    if operation.record != *record {
        return Err(binding(DataError::InvalidManifest { at: "manifest" }));
    }
    let invocation_key = scope_space.pack(&(KIND_INVOCATION, operation.invocation_id.as_slice()));
    let invocation = transaction
        .get(&invocation_key, false)
        .await?
        .ok_or_else(|| binding(DataError::MissingInvocation))?;
    if invocation.as_ref() != invocation_binding_record(record) {
        return Err(binding(DataError::InvalidInvocation));
    }
    Ok(())
}

/// Runs `load_change_bytes`.
pub(super) async fn load_change_bytes(
    transaction: &foundationdb::RetryableTransaction,
    chunk_space: &Subspace,
    sequence: u64,
    record: &Record,
) -> Result<Vec<u8>, FdbBindingError> {
    let mut bytes = Vec::with_capacity(record.byte_len);
    for index in 0..record.chunks {
        let chunk = transaction
            .get(&chunk_space.pack(&(sequence, index)), false)
            .await?
            .ok_or_else(|| binding(DataError::MissingChunk))?;
        if chunk.is_empty() || chunk.len() > CHUNK_BYTES {
            return Err(binding(DataError::InvalidChunk));
        }
        bytes.extend_from_slice(chunk.as_ref());
    }
    if bytes.len() != record.byte_len {
        return Err(binding(DataError::InvalidChunk));
    }
    Ok(bytes)
}

/// Binding
pub(super) fn binding(error: DataError) -> FdbBindingError {
    FdbBindingError::new_custom_error(Box::new(error))
}

/// Binding invalid
pub(super) fn binding_invalid<E: std::fmt::Debug>(_cause: E) -> FdbBindingError {
    binding(DataError::InvalidManifest { at: "manifest" })
}

/// Binding baseline
pub(super) fn binding_baseline<E: std::fmt::Debug>(_cause: E) -> FdbBindingError {
    binding(DataError::BaselineCorrupt)
}

/// Classify transaction
pub(super) fn classify_transaction(error: FdbBindingError) -> FdbStoreError {
    if let FdbBindingError::CustomError(ref custom) = error
        && let Some(data) = custom.downcast_ref::<DataError>()
    {
        return FdbStoreError::Corruption(*data);
    }
    if let Some(native) = error.get_fdb_error() {
        let code = native.code();
        if code == 1_025 {
            return FdbStoreError::Cancelled { code };
        }
        if code == 1_039 {
            return FdbStoreError::VersionSkew {
                code,
                may_have_committed: native.is_maybe_committed(),
            };
        }
        if native.is_maybe_committed() {
            return FdbStoreError::CommitMayHaveSucceeded { code };
        }
        if native.is_retryable() || native.is_retryable_not_committed() {
            return FdbStoreError::RetryExhausted { code };
        }
        return FdbStoreError::Native { code };
    }
    FdbStoreError::BindingInvariant(error)
}
