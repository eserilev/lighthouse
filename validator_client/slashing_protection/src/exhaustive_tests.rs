//! Bounded exhaustive equivalence check for the attester slashing conditions.
//!
//! `check_attestation` spreads the slashing rules across four SQL queries. This module states
//! the same rules once, in ~20 lines of straight-line Rust (`reference_check`), and then
//! compares the two on *every* history of up to `MAX_HISTORY` attestations drawn from epochs
//! `0..=MAX_EPOCH`.
//!
//! Exhaustive enumeration is unusually strong here because the rules depend only on the
//! *ordering* between epochs, never on their magnitudes: there is no arithmetic, only `<`,
//! `>` and `=`. Any larger history is order-isomorphic, for the purposes of the pairwise
//! conditions, to one enumerated below.
//!
//! What this catches that a test suite of hand-written cases does not: a guard whose SQL
//! quietly means something other than the rule it is supposed to implement — `MIN` where
//! `MAX` was meant, a `<` that should be `<=`, a `WHERE` clause that drops a case. Those are
//! transcription errors rather than logic errors, and they survive both code review and a
//! proof of the rules themselves.

#![cfg(test)]

use crate::test_utils::*;
use crate::*;
use tempfile::tempdir;
use types::{AttestationData, Checkpoint, Epoch, Slot};

/// Epochs are drawn from `0..=MAX_EPOCH`.
const MAX_EPOCH: u64 = 3;

/// Histories of up to this many attestations are enumerated.
const MAX_HISTORY: usize = 3;

/// An attestation reduced to what slashing depends on. The signing root is a function of
/// `(source, target)` here, because `attestation_data` fixes every other field.
type Att = (u64, u64);

fn attestation_data(source: u64, target: u64) -> AttestationData {
    let checkpoint = |epoch| Checkpoint {
        epoch: Epoch::from(epoch),
        root: Hash256::ZERO,
    };
    AttestationData {
        slot: Slot::from(0u64),
        index: 0,
        beacon_block_root: Hash256::ZERO,
        source: checkpoint(source),
        target: checkpoint(target),
    }
}

/// Every well-formed attestation over `0..=MAX_EPOCH`.
fn all_attestations() -> Vec<Att> {
    (0..=MAX_EPOCH)
        .flat_map(|source| (source..=MAX_EPOCH).map(move |target| (source, target)))
        .collect()
}

/// Every history of up to `MAX_HISTORY` attestations, as ordered insertion sequences.
///
/// Ordered rather than unordered: the database is stateful, and an insertion rejected early
/// changes what later insertions see. Sequences cover that; sets would not.
fn all_histories() -> Vec<Vec<Att>> {
    let atts = all_attestations();
    let mut histories = vec![vec![]];
    let mut frontier = vec![vec![]];

    for _ in 0..MAX_HISTORY {
        let mut next = Vec::new();
        for history in &frontier {
            for att in &atts {
                let mut extended = history.clone();
                extended.push(*att);
                next.push(extended);
            }
        }
        histories.extend(next.iter().cloned());
        frontier = next;
    }
    histories
}

/// The slashing conditions, stated once. Returns `true` if the database should accept
/// `candidate` given that it currently holds `history`.
///
/// Mirrors `SlashingDatabase::check_attestation`, guard for guard, in the same order —
/// including the early return for identical data, which is reached *before* the lower bounds
/// and so admits a resubmission the bounds would otherwise reject.
fn reference_check(history: &[Att], candidate: Att) -> bool {
    let (source, target) = candidate;

    // Invalid: source after target.
    if source > target {
        return false;
    }

    // Double vote: an existing attestation with the same target. The schema's
    // `UNIQUE (validator_id, target_epoch)` means there is at most one, and an exact match is
    // `Safe::SameData` rather than an error.
    if let Some(&(existing_source, _)) = history.iter().find(|&&(_, t)| t == target) {
        return existing_source == source;
    }

    // A stored attestation surrounds the candidate.
    if history.iter().any(|&(s, t)| s < source && t > target) {
        return false;
    }

    // The candidate surrounds a stored attestation.
    if history.iter().any(|&(s, t)| s > source && t < target) {
        return false;
    }

    // Lower bounds. Note MIN, not MAX: the candidate must sit at or above the *oldest*
    // retained source, and strictly above the *oldest* retained target.
    if let Some(min_source) = history.iter().map(|&(s, _)| s).min()
        && source < min_source
    {
        return false;
    }

    if let Some(min_target) = history.iter().map(|&(_, t)| t).min()
        && target <= min_target
    {
        return false;
    }

    true
}

/// Compare `reference_check` against the database on every enumerated history.
///
/// Two comparisons are made per history:
///
/// 1. **Insertion.** Each attestation of the history is inserted in turn and the verdict
///    compared, so the reference and the database stay in agreement about what is stored.
/// 2. **Candidates.** Every attestation is then offered read-only, via
///    `preliminary_check_attestation`, and the verdict compared.
///
/// `preliminary_check_attestation` is clippy-disallowed because it must never decide whether
/// to sign. Here nothing is signed and nothing is stored: it is used precisely for its
/// read-only property, so that one database can serve every candidate for a given history.
#[test]
#[allow(clippy::disallowed_methods)]
fn reference_agrees_with_database() {
    let dir = tempdir().unwrap();
    let validator = pubkey(DEFAULT_VALIDATOR_INDEX);
    let candidates = all_attestations();
    let histories = all_histories();

    let mut comparisons = 0usize;

    for (i, history) in histories.iter().enumerate() {
        let db_file = dir.path().join(format!("slashing_protection_{i}.sqlite"));
        let db = SlashingDatabase::create(&db_file).unwrap();
        db.register_validator(validator).unwrap();

        // (1) Build the history, checking agreement on every insertion.
        let mut stored: Vec<Att> = Vec::new();
        for &att in history {
            let data = attestation_data(att.0, att.1);
            let db_verdict = db
                .with_transaction(|txn| {
                    db.check_and_insert_attestation(&validator, &data, DEFAULT_DOMAIN, txn)
                })
                .is_ok();
            let reference_verdict = reference_check(&stored, att);

            assert_eq!(
                db_verdict, reference_verdict,
                "insertion disagreement: history {stored:?}, inserting {att:?} \
                 (database accepted: {db_verdict}, reference accepted: {reference_verdict})"
            );
            comparisons += 1;

            // `Safe::SameData` is accepted but not stored, so only record genuinely new rows.
            if db_verdict && !stored.iter().any(|&(_, t)| t == att.1) {
                stored.push(att);
            }
        }

        // (2) Offer every candidate against the resulting history, without mutating it.
        for &candidate in &candidates {
            let data = attestation_data(candidate.0, candidate.1);
            let db_verdict = db
                .preliminary_check_attestation(&validator, &data, DEFAULT_DOMAIN)
                .is_ok();
            let reference_verdict = reference_check(&stored, candidate);

            assert_eq!(
                db_verdict, reference_verdict,
                "check disagreement: history {stored:?}, candidate {candidate:?} \
                 (database accepted: {db_verdict}, reference accepted: {reference_verdict})"
            );
            comparisons += 1;
        }
    }

    // Guard against the enumeration silently collapsing to nothing.
    assert!(
        comparisons > 10_000,
        "expected a large number of comparisons, got {comparisons}"
    );
}

/// The enumeration must actually reach the cases that distinguish `MIN` from `MAX` bounds:
/// a history holding two attestations with different sources, and a candidate whose source
/// falls between them.
///
/// Without such a history the two encodings agree and the check above would pass vacuously.
#[test]
fn enumeration_reaches_distinguishing_cases() {
    let history = [(0, 1), (2, 3)];
    let candidate = (1, 2);

    // `>= MIN(source)` accepts: 1 >= 0.
    assert!(reference_check(&history, candidate));

    // `>= MAX(source)` would reject: 1 < 2. The two encodings differ here, so any test that
    // exercises this shape distinguishes them.
    let max_source = history.iter().map(|&(s, _)| s).max().unwrap();
    assert!(candidate.0 < max_source);

    // And the shape is inside the enumerated space.
    assert!(history.iter().all(|&(s, t)| t <= MAX_EPOCH && s <= t));
    assert!(history.len() <= MAX_HISTORY);
}
