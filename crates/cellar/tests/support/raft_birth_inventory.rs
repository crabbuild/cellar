//! Real conditional namespace admission/freeze races. This is the birth half
//! of import quiescence, not membership or complete-removal certification.
use super::*;

fn inventory_key(tail: &str) -> Path {
    Path::from(format!(
        "enrollment/namespaces/{}/births/{tail}",
        hex::encode([2; 16])
    ))
}

fn another(f: &Fixture, id: u8) -> Result<BirthProposal> {
    let mut proposal = f.proposal.clone();
    proposal.scope.cell = format!("Counter:inventory-{id}");
    proposal.scope.group = GroupId::new([id; 16])?;
    proposal.scope.incarnation = IncarnationId::new([id; 16])?;
    Ok(proposal)
}

#[tokio::test]
async fn namespace_birth_inventory_freeze_pending() -> Result<()> {
    let f = Fixture::new().await?;
    f.store.reject_listing.store(true, Ordering::Release);
    f.hold_birth();
    let (namespace, proposal) = (f.namespace.clone(), f.proposal.clone());
    let writer = f
        .domain
        .spawn(async move { namespace.resolve_birth(proposal).await })?;
    f.waiting().await?;
    let op = ProposalId::new([20; 16])?;
    let cut = f
        .domain
        .run(f.namespace.freeze_births(op, TIMEOUT))
        .await??;
    ensure!(cut.len() == 1 && cut.revision() == 3 && !cut.is_empty());
    let page = f
        .domain
        .run(f.namespace.frozen_birth_page(&cut, None, 16, TIMEOUT))
        .await??;
    ensure!(page.entries.len() == 1 && page.next.is_none());
    ensure!(page.entries[0].proposal().scope == f.proposal.scope);
    ensure!(f
        .namespace
        .birth_disposition(&f.proposal.scope.cell)
        .await?
        .is_none());
    let fresh = another(&f, 21)?;
    ensure!(matches!(
        f.namespace.resolve_birth(fresh.clone()).await,
        Err(CatalogError::BirthsFrozen)
    ));
    ensure!(f
        .namespace
        .birth_disposition(&fresh.scope.cell)
        .await?
        .is_none());
    let mut wrong = f.proposal.clone();
    wrong.scope.group = GroupId::new([22; 16])?;
    wrong.scope.incarnation = IncarnationId::new([22; 16])?;
    ensure!(matches!(
        f.domain
            .run(f.namespace.cancel_birth(wrong, op, TIMEOUT))
            .await?,
        Err(CatalogError::Conflict("birth intent changed"))
    ));
    f.release();
    let birth = writer.await??;
    ensure!(birth.digest() == page.entries[0].expected_digest());
    let retried = f
        .domain
        .run(f.namespace.freeze_births(op, TIMEOUT))
        .await??;
    ensure!(retried.revision() == cut.revision() && retried.len() == 1);
    ensure!(matches!(
        f.domain
            .run(
                f.namespace
                    .freeze_births(ProposalId::new([23; 16])?, TIMEOUT)
            )
            .await?,
        Err(CatalogError::Conflict("namespace already frozen"))
    ));
    ensure!(f.store.listing_calls.load(Ordering::Acquire) == 0);
    f.close().await;
    Ok(())
}

#[tokio::test]
async fn namespace_birth_inventory_freeze_wins_admission() -> Result<()> {
    let f = Fixture::new().await?;
    f.store
        .birth_inventory_cas_hang
        .store(true, Ordering::Release);
    f.store
        .hold_next_birth_inventory_cas
        .store(true, Ordering::Release);
    let (namespace, proposal) = (f.namespace.clone(), f.proposal.clone());
    let writer = f
        .domain
        .spawn(async move { namespace.resolve_birth(proposal).await })?;
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while !f.store.birth_inventory_cas_waiting.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        }))
        .await??;
    let op = ProposalId::new([20; 16])?;
    let cut = f
        .domain
        .run(f.namespace.freeze_births(op, TIMEOUT))
        .await??;
    ensure!(cut.is_empty());
    f.store
        .birth_inventory_cas_hang
        .store(false, Ordering::Release);
    f.store.birth_inventory_cas_resume.notify_waiters();
    ensure!(matches!(writer.await?, Err(CatalogError::BirthsFrozen)));
    ensure!(!f.store.birth_inventory_cas_waiting.load(Ordering::Acquire));
    ensure!(f
        .namespace
        .birth_disposition(&f.proposal.scope.cell)
        .await?
        .is_none());
    let page = f
        .domain
        .run(f.namespace.frozen_birth_page(&cut, None, 16, TIMEOUT))
        .await??;
    ensure!(page.entries.is_empty() && page.next.is_none());
    // Orphan identity/pages grant no birth authority after the losing CAS.
    ensure!(f
        .store
        .inner
        .get(&Path::from(format!(
            "enrollment/groups/{}",
            hex::encode(f.proposal.scope.group.bytes())
        )))
        .await
        .is_ok());
    f.close().await;
    Ok(())
}

#[tokio::test]
async fn namespace_birth_inventory_pending_cancel_fence() -> Result<()> {
    let f = Fixture::new().await?;
    f.hold_birth();
    let (namespace, proposal) = (f.namespace.clone(), f.proposal.clone());
    let writer = f
        .domain
        .spawn(async move { namespace.resolve_birth(proposal).await })?;
    f.waiting().await?;
    let op = ProposalId::new([20; 16])?;
    let cut = f
        .domain
        .run(f.namespace.freeze_births(op, TIMEOUT))
        .await??;
    let page = f
        .domain
        .run(f.namespace.frozen_birth_page(&cut, None, 16, TIMEOUT))
        .await??;
    let cancelled = f
        .domain
        .run(
            f.namespace
                .cancel_birth(page.entries[0].proposal().clone(), op, TIMEOUT),
        )
        .await??;
    ensure!(
        matches!(cancelled, BirthDisposition::Cancelled { scope, operation }
        if scope == f.proposal.scope && operation == op)
    );
    let before = f.store.inner.get(&f.key()).await?.bytes().await?;
    f.release();
    ensure!(matches!(writer.await?, Err(CatalogError::BirthCancelled)));
    ensure!(f.store.inner.get(&f.key()).await?.bytes().await? == before);
    ensure!(matches!(
        f.namespace.birth(&f.proposal.scope.cell).await,
        Err(CatalogError::BirthCancelled)
    ));
    let retried = f
        .domain
        .run(f.namespace.freeze_births(op, TIMEOUT))
        .await??;
    ensure!(retried.len() == 1 && retried.revision() == cut.revision());
    f.close().await;
    Ok(())
}

#[tokio::test]
async fn namespace_birth_inventory_lost_freeze_reply() -> Result<()> {
    let f = Fixture::new().await?;
    f.namespace.resolve_birth(f.proposal.clone()).await?;
    let op = ProposalId::new([20; 16])?;
    f.store
        .fail_birth_inventory_confirmation
        .store(true, Ordering::Release);
    ensure!(f
        .domain
        .run(f.namespace.freeze_births(op, TIMEOUT))
        .await?
        .is_err());
    let recreated = Fixture::catalog(f.store.clone())?
        .existing_namespace(f.namespace.config().clone())
        .await?;
    let cut = f.domain.run(recreated.freeze_births(op, TIMEOUT)).await??;
    ensure!(cut.len() == 1 && cut.revision() == 3);
    let updates = f.store.birth_inventory_cas.load(Ordering::Acquire);
    let retry = f.domain.run(recreated.freeze_births(op, TIMEOUT)).await??;
    ensure!(retry.revision() == cut.revision());
    ensure!(f.store.birth_inventory_cas.load(Ordering::Acquire) == updates);
    let page = f
        .domain
        .run(recreated.frozen_birth_page(&cut, None, 16, TIMEOUT))
        .await??;
    ensure!(page.entries[0].proposal().scope == f.proposal.scope);
    f.close().await;
    Ok(())
}

#[tokio::test]
async fn namespace_birth_inventory_missing_state() -> Result<()> {
    for missing in ["head", "pin", "both", "page", "intent"] {
        let f = Fixture::new().await?;
        let birth = f.namespace.resolve_birth(f.proposal.clone()).await?;
        let head_path = inventory_key("head");
        let head = f.store.inner.get(&head_path).await?.bytes().await?;
        let value: serde_json::Value = serde_json::from_slice(&head[13..head.len() - 32])?;
        let index: Vec<u8> = serde_json::from_value(value["index"].clone())?;
        let paths = match missing {
            "both" => vec![head_path.clone(), inventory_key("pin")],
            "page" => vec![inventory_key(&format!("objects/{}", hex::encode(index)))],
            "intent" => vec![inventory_key(&format!(
                "intents/{}",
                hex::encode(birth.digest())
            ))],
            tail => vec![inventory_key(tail)],
        };
        for path in &paths {
            f.store.inner.delete(path).await?;
        }
        ensure!(
            matches!(
                f.namespace.birth(&f.proposal.scope.cell).await,
                Err(CatalogError::Corrupt(_))
            ),
            "missing {missing} did not refuse birth authority"
        );
        if matches!(missing, "head" | "pin" | "both") {
            ensure!(Fixture::catalog(f.store.clone())?
                .open_namespace(f.namespace.config().clone())
                .await
                .is_err());
            for path in &paths {
                ensure!(f.store.inner.get(path).await.is_err());
            }
        } else {
            let cut = f
                .domain
                .run(
                    f.namespace
                        .freeze_births(ProposalId::new([20; 16])?, TIMEOUT),
                )
                .await??;
            ensure!(f
                .domain
                .run(f.namespace.frozen_birth_page(&cut, None, 16, TIMEOUT))
                .await?
                .is_err());
        }
        f.close().await;
    }
    let f = Fixture::new().await?;
    f.namespace.resolve_birth(f.proposal.clone()).await?;
    let path = Path::from(format!(
        "enrollment/namespaces/{}/root",
        hex::encode([2; 16])
    ));
    f.store.inner.delete(&path).await?;
    ensure!(Fixture::catalog(f.store.clone())?
        .open_namespace(f.namespace.config().clone())
        .await
        .is_err());
    ensure!(f.store.inner.get(&path).await.is_err());
    f.close().await;
    Ok(())
}

#[tokio::test]
async fn namespace_birth_inventory_paging_and_bounds() -> Result<()> {
    let f = Fixture::new().await?;
    f.store.reject_listing.store(true, Ordering::Release);
    let mut config = f.namespace.config().clone();
    config.namespace = NamespaceId::new([8; 16])?;
    config.voters = VoterPolicy::SingleVoterDevelopment;
    let small = Catalog::new(
        f.store.clone(),
        CatalogOptions {
            prefix: "inventory-small/".into(),
            max_record_bytes: 1024,
            ..Default::default()
        },
    )?
    .open_namespace(config)
    .await?;
    let mut expected = std::collections::BTreeMap::new();
    for n in 10..150u8 {
        let mut proposal = another(&f, n)?;
        proposal.scope.namespace = small.config().namespace;
        proposal.voters.truncate(1);
        let birth = small.resolve_birth(proposal).await?;
        expected.insert(birth.scope().cell.clone(), birth.digest());
    }
    let op = ProposalId::new([20; 16])?;
    let cut = f.domain.run(small.freeze_births(op, TIMEOUT)).await??;
    ensure!(cut.len() == 140);
    for limit in [0, 17] {
        ensure!(matches!(
            f.domain
                .run(small.frozen_birth_page(&cut, None, limit, TIMEOUT))
                .await?,
            Err(CatalogError::Limit)
        ));
    }
    let mut seen = std::collections::BTreeMap::new();
    let mut cursor = None;
    loop {
        let page = f
            .domain
            .run(small.frozen_birth_page(&cut, cursor.as_ref(), 7, TIMEOUT))
            .await??;
        ensure!(page.entries.len() <= 7);
        for entry in page.entries {
            ensure!(seen
                .insert(entry.proposal().scope.cell.clone(), entry.expected_digest())
                .is_none());
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    ensure!(seen == expected);
    let foreign = f
        .domain
        .run(f.namespace.freeze_births(op, TIMEOUT))
        .await??;
    ensure!(matches!(
        f.domain
            .run(f.namespace.frozen_birth_page(&cut, None, 16, TIMEOUT))
            .await?,
        Err(CatalogError::Conflict(_))
    ));
    let page = f
        .domain
        .run(small.frozen_birth_page(&cut, None, 1, TIMEOUT))
        .await??;
    ensure!(matches!(
        f.domain
            .run(
                f.namespace
                    .frozen_birth_page(&foreign, page.next.as_ref(), 16, TIMEOUT)
            )
            .await?,
        Err(CatalogError::Conflict(_))
    ));
    ensure!(f.store.listing_calls.load(Ordering::Acquire) == 0);
    f.close().await;
    Ok(())
}

fn replace_scope(bytes: &[u8], old: &GroupScope, new: &GroupScope) -> Result<Vec<u8>> {
    let mut body = std::str::from_utf8(&bytes[13..bytes.len() - 32])?.to_owned();
    for (field, before, after) in [
        ("cell", old.cell.clone(), new.cell.clone()),
        (
            "group",
            hex::encode(old.group.bytes()),
            hex::encode(new.group.bytes()),
        ),
        (
            "incarnation",
            hex::encode(old.incarnation.bytes()),
            hex::encode(new.incarnation.bytes()),
        ),
    ] {
        body = body.replacen(
            &format!("\"{field}\":{}", serde_json::to_string(&before)?),
            &format!("\"{field}\":{}", serde_json::to_string(&after)?),
            1,
        );
    }
    let mut result = bytes[..9].to_vec();
    result.extend_from_slice(&(body.len() as u32).to_be_bytes());
    result.extend_from_slice(body.as_bytes());
    result.extend_from_slice(&Sha256::digest(&result));
    Ok(result)
}

#[tokio::test]
async fn namespace_birth_inventory_untracked_decision() -> Result<()> {
    let f = Fixture::new().await?;
    f.namespace.resolve_birth(f.proposal.clone()).await?;
    let cut = f
        .domain
        .run(
            f.namespace
                .freeze_births(ProposalId::new([20; 16])?, TIMEOUT),
        )
        .await??;
    let fake = another(&f, 13)?;
    let birth = f.store.inner.get(&f.key()).await?.bytes().await?;
    let claim_key = Path::from(format!(
        "enrollment/groups/{}",
        hex::encode(f.proposal.scope.group.bytes())
    ));
    let claim = f.store.inner.get(&claim_key).await?.bytes().await?;
    f.store
        .inner
        .put(
            &Path::from(format!(
                "enrollment/groups/{}",
                hex::encode(fake.scope.group.bytes())
            )),
            replace_scope(&claim, &f.proposal.scope, &fake.scope)?.into(),
        )
        .await?;
    let fake_key = Path::from(format!(
        "enrollment/namespaces/{}/cells/{}/birth",
        hex::encode(fake.scope.namespace.bytes()),
        hex::encode(Sha256::digest(fake.scope.cell.as_bytes()))
    ));
    f.store
        .inner
        .put(
            &fake_key,
            replace_scope(&birth, &f.proposal.scope, &fake.scope)?.into(),
        )
        .await?;
    ensure!(matches!(
        f.namespace.birth(&fake.scope.cell).await,
        Err(CatalogError::Corrupt("untracked birth decision"))
    ));
    ensure!(matches!(
        f.namespace.resolve_birth(fake).await,
        Err(CatalogError::BirthsFrozen)
    ));
    let page = f
        .domain
        .run(f.namespace.frozen_birth_page(&cut, None, 16, TIMEOUT))
        .await??;
    ensure!(page.entries.len() == 1 && page.entries[0].proposal().scope == f.proposal.scope);
    f.close().await;
    Ok(())
}
