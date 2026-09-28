#![cfg(feature = "lean_spec_tests")]

use lean_spec_tests::*;

fn assert_category<C: Case>() {
    let summary = run_category::<C>();
    for (reason, count) in &summary.skipped {
        println!("{}: skipped {count} ({reason})", C::CATEGORY);
    }
    assert!(
        summary.failures.is_empty(),
        "{} of {} {} cases failed:\n{}",
        summary.failures.len(),
        summary.failures.len() + summary.passed,
        C::CATEGORY,
        summary.failures.join("\n")
    );
    assert!(summary.passed > 0, "no {} cases ran", C::CATEGORY);
}

#[test]
fn ssz() {
    assert_category::<SszCase>();
}

#[test]
fn justifiability() {
    assert_category::<JustifiabilityCase>();
}

#[test]
fn state_transition() {
    assert_category::<StateTransitionCase>();
}
