//! Bounded exhaustive equivalence check for the attester slashing conditions.
//!
//! `check_attestation` spreads the slashing rules across four SQL queries. This module states
//! the same rules once, in ~20 lines of straight-line Rust (`reference_check`), and then
//! compares the two on *every* history of up to `MAX_HISTORY` attestations drawn from epochs
//! `0..=MAX_EPOCH`.
//!
//! A small bound goes a long way here because the rules use no arithmetic, only `<`, `>` and
//! `=` between epochs, and each guard is decided by a bounded number of witness rows: one
//! sharing the target, one surrounding, one surrounded, and the two holding the minima. A
//! history of three suffices to place those witnesses independently. (This is not the same
//! as saying every larger history is order-isomorphic to an enumerated one — four rows admit
//! order types three cannot.)
//!
//! What this catches: a guard whose SQL quietly means something other than the rule it is
//! supposed to implement — `MIN` where `MAX` was meant, a `<` that should be `<=`, a `WHERE`
//! clause that drops a case. Those are transcription errors rather than logic errors, and
//! they survive both code review and a proof of the rules themselves. Hand-written cases do
//! catch some of them; the value here is not needing to have thought of the case first.

#![cfg(test)]

use crate::pure_check::{AttRow, Verdict, check_attestation_pure};
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

/// The slashing conditions, stated once. Returns the verdict the database should reach for
/// `candidate` given that it currently holds `history`.
///
/// Mirrors `SlashingDatabase::check_attestation`, guard for guard, in the same order —
/// including the early return for identical data, which is reached *before* the lower bounds
/// and so admits a resubmission the bounds would otherwise reject.
///
/// Returns a `Verdict` rather than a bool so that a guard reporting the *wrong reason* is
/// caught as well as one reaching the wrong accept/reject. Swapping the two surround
/// branches, for instance, changes no acceptance decision anywhere.
fn reference_check(history: &[Att], candidate: Att) -> Verdict {
    let (source, target) = candidate;

    // Invalid: source after target.
    if source > target {
        return Verdict::SourceExceedsTarget;
    }

    // Double vote: an existing attestation with the same target. The schema's
    // `UNIQUE (validator_id, target_epoch)` means there is at most one, and an exact match is
    // `Safe::SameData` rather than an error. The signing root is a function of
    // `(source, target)` here, so an equal source means an identical attestation.
    if let Some(&(existing_source, _)) = history.iter().find(|&&(_, t)| t == target) {
        if existing_source == source {
            return Verdict::SameData;
        }
        return Verdict::DoubleVote;
    }

    // A stored attestation surrounds the candidate.
    if history.iter().any(|&(s, t)| s < source && t > target) {
        return Verdict::PrevSurroundsNew;
    }

    // The candidate surrounds a stored attestation.
    if history.iter().any(|&(s, t)| s > source && t < target) {
        return Verdict::NewSurroundsPrev;
    }

    // Lower bounds. Note MIN, not MAX: the candidate must sit at or above the *oldest*
    // retained source, and strictly above the *oldest* retained target.
    if let Some(min_source) = history.iter().map(|&(s, _)| s).min()
        && source < min_source
    {
        return Verdict::SourceLessThanLowerBound;
    }

    if let Some(min_target) = history.iter().map(|&(_, t)| t).min()
        && target <= min_target
    {
        return Verdict::TargetLessThanOrEqLowerBound;
    }

    Verdict::Valid
}

/// `true` if the verdict means the database accepts the attestation.
fn accepts(verdict: &Verdict) -> bool {
    matches!(verdict, Verdict::Valid | Verdict::SameData)
}

/// The verdict corresponding to a `check_attestation` result, so the database can be compared
/// against the reference by reason and not merely by accept/reject.
fn verdict_of(result: &Result<Safe, NotSafe>) -> Verdict {
    match result {
        Ok(Safe::Valid) => Verdict::Valid,
        Ok(Safe::SameData) => Verdict::SameData,
        Err(NotSafe::InvalidAttestation(invalid)) => match invalid {
            InvalidAttestation::SourceExceedsTarget => Verdict::SourceExceedsTarget,
            InvalidAttestation::DoubleVote(_) => Verdict::DoubleVote,
            InvalidAttestation::PrevSurroundsNew { .. } => Verdict::PrevSurroundsNew,
            InvalidAttestation::NewSurroundsPrev { .. } => Verdict::NewSurroundsPrev,
            InvalidAttestation::SourceLessThanLowerBound { .. } => {
                Verdict::SourceLessThanLowerBound
            }
            InvalidAttestation::TargetLessThanOrEqLowerBound { .. } => {
                Verdict::TargetLessThanOrEqLowerBound
            }
        },
        other => panic!("unexpected result outside the slashing conditions: {other:?}"),
    }
}

/// The signing root `attestation_data` produces for a given `(source, target)`.
fn signing_root_bytes(att: Att) -> [u8; 32] {
    let data = attestation_data(att.0, att.1);
    SignedAttestation::from_attestation(&data, DEFAULT_DOMAIN)
        .signing_root
        .to_hash256_raw()
        .0
}

fn to_row(att: Att) -> AttRow {
    AttRow {
        source: att.0,
        target: att.1,
        root: signing_root_bytes(att),
    }
}

fn to_rows(atts: &[Att]) -> Vec<AttRow> {
    atts.iter().map(|att| to_row(*att)).collect()
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
            let db_result = db.with_transaction(|txn| {
                db.check_and_insert_attestation(&validator, &data, DEFAULT_DOMAIN, txn)
            });
            let db_verdict = verdict_of(&db_result);
            let reference_verdict = reference_check(&stored, att);

            assert_eq!(
                db_verdict, reference_verdict,
                "insertion disagreement: history {stored:?}, inserting {att:?}"
            );
            comparisons += 1;

            // `Safe::SameData` is accepted but not stored, so only record genuinely new rows.
            if accepts(&db_verdict) && !stored.iter().any(|&(_, t)| t == att.1) {
                stored.push(att);
            }
        }

        // (2) Offer every candidate against the resulting history, without mutating it.
        for &candidate in &candidates {
            let data = attestation_data(candidate.0, candidate.1);
            let db_result = db.preliminary_check_attestation(&validator, &data, DEFAULT_DOMAIN);
            let db_verdict = verdict_of(&db_result);
            let reference_verdict = reference_check(&stored, candidate);

            assert_eq!(
                db_verdict, reference_verdict,
                "check disagreement: history {stored:?}, candidate {candidate:?}"
            );
            comparisons += 1;

            // (3) The same candidate, offered straight to the pure function with no database
            // in the way. Agreement here separates a logic bug from a plumbing bug in the
            // `Epoch`/`u64` and `SigningRoot`/`[u8; 32]` conversions.
            let pure_verdict = check_attestation_pure(&to_rows(&stored), &to_row(candidate));
            assert_eq!(
                pure_verdict, reference_verdict,
                "pure disagreement: history {stored:?}, candidate {candidate:?}"
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
    assert_eq!(reference_check(&history, candidate), Verdict::Valid);

    // `>= MAX(source)` would reject: 1 < 2. The two encodings differ here, so any test that
    // exercises this shape distinguishes them.
    let max_source = history.iter().map(|&(s, _)| s).max().unwrap();
    assert!(candidate.0 < max_source);

    // And the enumeration really produces this history, rather than merely permitting its
    // shape.
    assert!(all_histories().contains(&history.to_vec()));
    assert!(all_attestations().contains(&candidate));
}

/// A null signing root never compares equal, not even to another null root.
///
/// `impl PartialEq for SigningRoot` encodes this: rows written by an interchange import carry
/// a null root, and treating one as "same data" would let a validator re-sign a target epoch
/// it had already voted on. `roots_eq` in `pure_check` has to reproduce it exactly, and plain
/// byte equality would not.
#[test]
fn null_root_is_never_same_data() {
    let null = [0u8; 32];
    let candidate = AttRow {
        source: 1,
        target: 2,
        root: null,
    };

    // Stored row with a null root, identical epochs: a double vote, NOT same data.
    let history = vec![AttRow {
        source: 1,
        target: 2,
        root: null,
    }];
    assert_eq!(
        check_attestation_pure(&history, &candidate),
        Verdict::DoubleVote
    );

    // A non-null stored root matching the candidate's is same data.
    let mut root = [0u8; 32];
    root[0] = 7;
    let history = vec![AttRow {
        source: 1,
        target: 2,
        root,
    }];
    let candidate = AttRow {
        source: 1,
        target: 2,
        root,
    };
    assert_eq!(
        check_attestation_pure(&history, &candidate),
        Verdict::SameData
    );
}
