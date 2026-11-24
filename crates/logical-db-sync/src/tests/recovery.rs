//! Baseline recovery paging, budgets and error display.

use super::*;

struct RecoveryWire {
    pulls: std::collections::VecDeque<PullResult>,
    baseline: Option<BaselinePage>,
}

impl Transport for RecoveryWire {
    type Error = MissingScriptedResponse;
    fn push(
        &mut self,
        _request: PushRequest,
    ) -> impl Future<Output = Result<PushResponse, Self::Error>> {
        std::future::ready(Ok(PushResponse::new(PROTOCOL_VERSION, Vec::new())))
    }
    fn pull(&mut self, _: PullRequest) -> impl Future<Output = Result<PullResult, Self::Error>> {
        std::future::ready(self.pulls.pop_front().ok_or(MissingScriptedResponse))
    }
    fn baseline(
        &mut self,
        _request: BaselineRequest,
    ) -> impl Future<Output = Result<BaselineResult, Self::Error>> {
        let result = match self.baseline.take() {
            Some(page) => BaselineResult::Artifact(page),
            None => BaselineResult::Unavailable,
        };
        std::future::ready(Ok(result))
    }
}

struct BudgetWire<'a> {
    pages: std::collections::VecDeque<BaselinePage>,
    requests: &'a std::cell::Cell<usize>,
    cancel: std::rc::Rc<std::cell::Cell<bool>>,
}

impl Transport for BudgetWire<'_> {
    type Error = Infallible;

    fn push(
        &mut self,
        _request: PushRequest,
    ) -> impl Future<Output = Result<PushResponse, Self::Error>> {
        std::future::ready(Ok(PushResponse::new(PROTOCOL_VERSION, Vec::new())))
    }

    fn pull(
        &mut self,
        _request: PullRequest,
    ) -> impl Future<Output = Result<PullResult, Self::Error>> {
        std::future::ready(Ok(PullResult::BaselineRequired))
    }

    fn baseline(
        &mut self,
        _request: BaselineRequest,
    ) -> impl Future<Output = Result<BaselineResult, Self::Error>> {
        self.requests.set(self.requests.get().saturating_add(1));
        let result = match self.pages.pop_front() {
            Some(page) => BaselineResult::Artifact(page),
            None => BaselineResult::Unavailable,
        };
        if self.requests.get() == 1 {
            self.cancel.set(true);
        }
        std::future::ready(Ok(result))
    }
}

fn paged_recovery_pages(
    scope: Scope,
) -> Result<std::collections::VecDeque<BaselinePage>, Box<dyn Error>> {
    let total = MAX_BASELINE_PAGE_DOCUMENTS.saturating_add(44);
    let mut documents = (0..total)
        .map(|_| logical_db_model::SnapshotDocument::new(DocumentId::new(), vec![1], false))
        .collect::<Vec<_>>();
    documents.sort_by_key(|document| *document.document().as_uuid().as_bytes());
    let artifact = SnapshotArtifact::new(scope, 7, documents.clone())?;
    let (first, rest) = documents.split_at(MAX_BASELINE_PAGE_DOCUMENTS);
    let mut pages = std::collections::VecDeque::new();
    pages.push_back(page_for(&artifact, first, true)?);
    pages.push_back(page_for(&artifact, rest, false)?);
    Ok(pages)
}

#[test]
fn recovery_stops_between_pages_when_cancelled() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = empty_replica(scope);
    *replica.checkpoint.borrow_mut() = Some(vec![9]);
    let pages = paged_recovery_pages(scope)?;
    let requests = std::cell::Cell::new(0usize);
    let cancel = std::rc::Rc::new(std::cell::Cell::new(false));
    let mut wire = BudgetWire {
        pages,
        requests: &requests,
        cancel: std::rc::Rc::clone(&cancel),
    };
    let result = lgwks_std::task::block_on(synchronize(
        &replica,
        PeerId::new(),
        &mut wire,
        SyncLimits::default(),
        || cancel.get(),
    ));
    assert!(
        matches!(result, Err(SyncError::Cancelled)),
        "cancel must stop recovery"
    );
    assert_eq!(requests.get(), 1, "cancellation must stop the next fetch");
    assert!(
        replica.baselines.borrow().is_empty(),
        "a cancelled recovery must install nothing"
    );
    assert_eq!(
        *replica.checkpoint.borrow(),
        Some(vec![9]),
        "cancellation must leave the durable checkpoint untouched"
    );
    Ok(())
}

#[test]
fn recovery_refuses_to_exceed_its_page_budget_without_mutating_state() -> Result<(), Box<dyn Error>>
{
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = empty_replica(scope);
    *replica.checkpoint.borrow_mut() = Some(vec![9]);
    let pages = paged_recovery_pages(scope)?;
    let requests = std::cell::Cell::new(0usize);
    let mut wire = BudgetWire {
        pages,
        requests: &requests,
        cancel: std::rc::Rc::new(std::cell::Cell::new(false)),
    };
    let limits = SyncLimits {
        recovery_pages: 1,
        ..SyncLimits::default()
    };
    let result = lgwks_std::task::block_on(synchronize(
        &replica,
        PeerId::new(),
        &mut wire,
        limits,
        || false,
    ));
    assert!(
        matches!(result, Err(SyncError::RecoveryBudgetExhausted)),
        "exceeding the declared page budget must fail closed"
    );
    assert_eq!(requests.get(), 1, "one page is the whole declared budget");
    assert!(
        replica.baselines.borrow().is_empty(),
        "an exhausted recovery must install nothing"
    );
    assert_eq!(
        *replica.checkpoint.borrow(),
        Some(vec![9]),
        "an exhausted recovery must leave the durable checkpoint untouched"
    );
    Ok(())
}

fn non_progressing_page(scope: Scope) -> Result<BaselinePage, Box<dyn Error>> {
    let mut documents = (0..2)
        .map(|_| logical_db_model::SnapshotDocument::new(DocumentId::new(), vec![1], false))
        .collect::<Vec<_>>();
    documents.sort_by_key(|document| *document.document().as_uuid().as_bytes());
    let artifact = SnapshotArtifact::new(scope, 5, documents.clone())?;
    let page = page_for(&artifact, &documents, true)?;
    // A continued page whose cursor points back inside the page body rather
    // than strictly past it must be refused before the next fetch.
    let cursor = documents
        .first()
        .map(logical_db_model::SnapshotDocument::document);
    Ok(BaselinePage::new(
        PROTOCOL_VERSION,
        page.descriptor().to_vec(),
        page.documents().to_vec(),
        page.checkpoint().clone(),
        true,
        cursor,
    ))
}

#[test]
fn recovery_fails_closed_when_a_page_cursor_does_not_advance() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = empty_replica(scope);
    *replica.checkpoint.borrow_mut() = Some(vec![9]);
    let mut pages = std::collections::VecDeque::new();
    pages.push_back(non_progressing_page(scope)?);
    let requests = std::cell::Cell::new(0usize);
    let mut wire = BudgetWire {
        pages,
        requests: &requests,
        cancel: std::rc::Rc::new(std::cell::Cell::new(false)),
    };
    let result = lgwks_std::task::block_on(synchronize(
        &replica,
        PeerId::new(),
        &mut wire,
        SyncLimits::default(),
        || false,
    ));
    assert!(
        matches!(
            result,
            Err(SyncError::Protocol(ProtocolError::NonProgressPage))
        ),
        "a non-advancing cursor must fail closed as a protocol error"
    );
    assert_eq!(
        requests.get(),
        1,
        "a non-advancing cursor stops before a second fetch"
    );
    assert!(
        replica.baselines.borrow().is_empty(),
        "a non-advancing recovery must install nothing"
    );
    assert_eq!(
        *replica.checkpoint.borrow(),
        Some(vec![9]),
        "a non-advancing recovery must leave the durable checkpoint untouched"
    );
    Ok(())
}

fn recovery_page(scope: Scope, position: u64) -> Result<BaselinePage, Box<dyn Error>> {
    let artifact = SnapshotArtifact::new(
        scope,
        position,
        vec![logical_db_model::SnapshotDocument::new(
            DocumentId::new(),
            vec![1, 2, 3],
            false,
        )],
    )?;
    page_for(&artifact, artifact.documents(), false)
}

fn page_for(
    artifact: &SnapshotArtifact,
    slice: &[logical_db_model::SnapshotDocument],
    more: bool,
) -> Result<BaselinePage, Box<dyn Error>> {
    Ok(BaselinePage::new(
        PROTOCOL_VERSION,
        artifact.baseline().encode().to_vec(),
        slice
            .iter()
            .map(|document| {
                logical_db_protocol::BaselineDocument::new(
                    document.document(),
                    document.is_tombstone(),
                    document.snapshot().to_vec(),
                )
            })
            .collect(),
        Checkpoint::from_wire(vec![5])?,
        more,
        if more {
            slice
                .last()
                .map(logical_db_model::SnapshotDocument::document)
        } else {
            None
        },
    ))
}

#[test]
fn baseline_required_installs_state_before_the_suffix_commit() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = empty_replica(scope);
    *replica.checkpoint.borrow_mut() = Some(vec![9]);
    let baseline = recovery_page(scope, 2)?;
    let baseline_checkpoint = baseline.checkpoint().as_wire().to_vec();
    let suffix_checkpoint = Checkpoint::from_wire(vec![10])?;
    let mut wire = RecoveryWire {
        pulls: [
            PullResult::BaselineRequired,
            PullResult::Page(PullPage::new(
                PROTOCOL_VERSION,
                Vec::new(),
                suffix_checkpoint.clone(),
                false,
            )),
        ]
        .into(),
        baseline: Some(baseline),
    };
    let report = lgwks_std::task::block_on(synchronize(
        &replica,
        PeerId::new(),
        &mut wire,
        SyncLimits::default(),
        || false,
    ))?;
    assert!(report.recovered, "the baseline install must mark recovery");
    assert!(
        !report.full_resync,
        "a baseline recovery must not claim a full resync"
    );
    assert_eq!(
        report.pages, 2,
        "the recovery page and the suffix page are both accounted"
    );
    let baselines = replica.baselines.borrow();
    assert_eq!(baselines.len(), 1, "exactly one baseline must install");
    let Some((descriptor, checkpoint)) = baselines.first().map(|entry| (&entry.0, &entry.1)) else {
        return Err("the baseline must install".into());
    };
    assert_eq!(
        checkpoint, &baseline_checkpoint,
        "the baseline must bind its durable checkpoint"
    );
    assert!(
        matches!(
            logical_db_model::SnapshotBaseline::decode(descriptor),
            Ok(logical_db_model::SnapshotRecord::State(baseline)) if baseline.position() == 2
        ),
        "the installed baseline must decode as position two"
    );
    assert_eq!(
        *replica.checkpoint.borrow(),
        Some(suffix_checkpoint.as_wire().to_vec()),
        "the suffix page checkpoint must be durable after recovery"
    );
    Ok(())
}

#[test]
fn a_broken_paged_recovery_installs_nothing() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = empty_replica(scope);
    *replica.checkpoint.borrow_mut() = Some(vec![9]);
    let mut documents = (0..=logical_db_model::MAX_BASELINE_PAGE_DOCUMENTS)
        .map(|index| {
            let byte = u8::try_from(index % 251)?;
            Ok::<_, std::num::TryFromIntError>(logical_db_model::SnapshotDocument::new(
                DocumentId::new(),
                vec![byte.saturating_add(1)],
                false,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    documents.sort_by_key(|document| *document.document().as_uuid().as_bytes());
    let artifact = SnapshotArtifact::new(scope, 3, documents.clone())?;
    let (first, _rest) = documents.split_at(logical_db_model::MAX_BASELINE_PAGE_DOCUMENTS);
    let mut wire = RecoveryWire {
        pulls: [PullResult::BaselineRequired].into(),
        baseline: Some(page_for(&artifact, first, true)?),
    };
    let result = lgwks_std::task::block_on(synchronize(
        &replica,
        PeerId::new(),
        &mut wire,
        SyncLimits::default(),
        || false,
    ));
    assert!(
        result.is_err(),
        "a broken recovery page must fail before installing anything"
    );
    assert!(
        replica.baselines.borrow().is_empty(),
        "a failed recovery must install nothing"
    );
    assert_eq!(
        *replica.checkpoint.borrow(),
        Some(vec![9]),
        "a failed recovery must leave the durable checkpoint untouched"
    );
    Ok(())
}

#[test]
fn report_counts_every_installed_change_and_page() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = empty_replica(scope);
    let remote_replica = ReplicaId::new();
    let change = ChangeEnvelope::new(
        scope,
        DocumentId::new(),
        remote_replica,
        OperationId::new(),
        OperationDigest::new([5; 32]),
        ChangeDigest::new([6; 32]),
        vec![1],
    );
    let mut wire = Wire {
        push: None,
        pulls: [PullResult::Page(PullPage::new(
            PROTOCOL_VERSION,
            vec![change.clone()],
            Checkpoint::from_wire(vec![4])?,
            false,
        ))]
        .into(),
    };
    let report = lgwks_std::task::block_on(synchronize(
        &replica,
        PeerId::new(),
        &mut wire,
        SyncLimits::default(),
        || false,
    ))?;
    assert_eq!(report.pulled, 1, "the single installed change is counted");
    assert_eq!(report.pages, 1, "the single pull page is counted");
    assert_eq!(
        replica.installed.borrow().len(),
        1,
        "one change lands on the replica"
    );
    assert_eq!(
        replica
            .installed
            .borrow()
            .first()
            .map(InboundChange::document),
        Some(change.document()),
        "the installed change keeps its document identity"
    );
    Ok(())
}

#[test]
fn exact_pull_page_budget_never_fetches_one_page_too_many() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = empty_replica(scope);
    let change = ChangeEnvelope::new(
        scope,
        DocumentId::new(),
        ReplicaId::new(),
        OperationId::new(),
        OperationDigest::new([7; 32]),
        ChangeDigest::new([8; 32]),
        vec![1],
    );
    let mut wire = Wire {
        push: None,
        pulls: [PullResult::Page(PullPage::new(
            PROTOCOL_VERSION,
            vec![change],
            Checkpoint::from_wire(vec![2])?,
            true,
        ))]
        .into(),
    };
    let report = lgwks_std::task::block_on(synchronize(
        &replica,
        PeerId::new(),
        &mut wire,
        SyncLimits {
            pages_per_cycle: 1,
            ..SyncLimits::default()
        },
        || false,
    ))?;
    assert_eq!(report.pages, 1, "the page budget is never exceeded");
    assert!(report.more, "more data remains past the budget");
    assert!(wire.pulls.is_empty(), "no extra pull is issued");
    Ok(())
}

#[test]
fn sync_error_display_keeps_each_recovery_category_visible() {
    let operation = OperationId::new();
    assert_eq!(
        SyncError::<&str, &str>::Replica("disk").to_string(),
        "replica synchronization failed: disk"
    );
    assert_eq!(
        SyncError::<&str, &str>::Transport("offline").to_string(),
        "transport synchronization failed: offline"
    );
    assert!(
        SyncError::<&str, &str>::Protocol(ProtocolError::PageLimit)
            .to_string()
            .contains("protocol synchronization failed")
    );
    assert!(
        SyncError::<&str, &str>::Rejected {
            operation,
            code: RejectionCode::Forbidden,
        }
        .to_string()
        .contains(&operation.to_string())
    );
    assert_eq!(
        SyncError::<&str, &str>::Cancelled.to_string(),
        "synchronization was cancelled"
    );
}
