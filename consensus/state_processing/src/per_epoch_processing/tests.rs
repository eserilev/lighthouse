#![cfg(test)]
use crate::per_epoch_processing::process_epoch;
use beacon_chain::test_utils::BeaconChainHarness;
use beacon_chain::types::{EthSpec, MinimalEthSpec};
use types::Slot;

#[tokio::test]
async fn runs_without_error() {
    let harness = BeaconChainHarness::builder(MinimalEthSpec)
        .default_spec()
        .deterministic_keypairs(8)
        .fresh_ephemeral_store()
        .mock_execution_layer()
        .build();
    harness.advance_slot();

    let target_slot =
        (MinimalEthSpec::genesis_epoch() + 4).end_slot(MinimalEthSpec::slots_per_epoch());

    let state = harness.get_current_state();
    harness
        .add_attested_blocks_at_slots(
            state,
            (1..target_slot.as_u64())
                .map(Slot::new)
                .collect::<Vec<_>>()
                .as_slice(),
            (0..8).collect::<Vec<_>>().as_slice(),
        )
        .await;
    let mut new_head_state = harness.get_current_state();

    process_epoch(&mut new_head_state, &harness.spec).unwrap();
}

#[cfg(not(feature = "fake_crypto"))]
mod pending_deposits {
    use super::*;
    use crate::state_advance::complete_state_advance;
    use bls::SignatureBytes;
    use std::sync::Arc;
    use types::test_utils::generate_deterministic_keypair;
    use types::{BeaconState, ChainSpec, DepositData, ForkName, Hash256, PendingDeposit};

    type E = MinimalEthSpec;

    const VALIDATOR_COUNT: usize = 8;

    /// A pending deposit with an empty signature.
    fn unsigned_pending_deposit(
        validator_index: usize,
        withdrawal_prefix: u8,
        amount: u64,
        spec: &ChainSpec,
    ) -> PendingDeposit {
        let keypair = generate_deterministic_keypair(validator_index);
        let mut withdrawal_credentials = [0u8; 32];
        withdrawal_credentials[0] = withdrawal_prefix;
        PendingDeposit {
            pubkey: keypair.pk.compress(),
            withdrawal_credentials: Hash256::from_slice(&withdrawal_credentials),
            amount,
            signature: SignatureBytes::empty(),
            slot: spec.genesis_slot,
        }
    }

    fn signed_pending_deposit(
        validator_index: usize,
        withdrawal_prefix: u8,
        amount: u64,
        spec: &ChainSpec,
    ) -> PendingDeposit {
        let mut deposit =
            unsigned_pending_deposit(validator_index, withdrawal_prefix, amount, spec);
        let deposit_data = DepositData {
            pubkey: deposit.pubkey,
            withdrawal_credentials: deposit.withdrawal_credentials,
            amount: deposit.amount,
            signature: SignatureBytes::empty(),
        };
        let keypair = generate_deterministic_keypair(validator_index);
        deposit.signature = deposit_data.create_signature(&keypair.sk, spec);
        deposit
    }

    /// Apply the deposits from `make_deposits` to a genesis state at the first epoch boundary.
    ///
    /// Returns the genesis state, the post state and the spec.
    fn state_after_deposits(
        fork_name: ForkName,
        make_deposits: impl Fn(&ChainSpec) -> Vec<PendingDeposit>,
    ) -> (BeaconState<E>, BeaconState<E>, Arc<ChainSpec>) {
        let spec = Arc::new(fork_name.make_genesis_spec(E::default_spec()));
        let harness = BeaconChainHarness::builder(E::default())
            .spec(spec.clone())
            .deterministic_keypairs(VALIDATOR_COUNT)
            .fresh_ephemeral_store()
            .mock_execution_layer()
            .build();
        let genesis_state = harness.get_current_state();
        let mut state = genesis_state.clone();
        state
            .set_pending_deposits_from_iter(make_deposits(&spec))
            .unwrap();

        complete_state_advance(
            &mut state,
            None,
            Slot::new(E::slots_per_epoch()),
            None,
            &spec,
        )
        .unwrap();

        assert_eq!(state.validators().len(), state.balances().len());
        (genesis_state, state, spec)
    }

    #[tokio::test]
    async fn new_bls_validator_skipped_in_heze() {
        let (_, state, _) = state_after_deposits(ForkName::Heze, |spec| {
            vec![signed_pending_deposit(
                VALIDATOR_COUNT,
                spec.bls_withdrawal_prefix_byte,
                spec.min_activation_balance,
                spec,
            )]
        });
        assert!(state.pending_deposits().unwrap().is_empty());
        assert_eq!(state.validators().len(), VALIDATOR_COUNT);
    }

    #[tokio::test]
    async fn new_bls_validator_added_before_heze() {
        let (_, state, _) = state_after_deposits(ForkName::Gloas, |spec| {
            vec![signed_pending_deposit(
                VALIDATOR_COUNT,
                spec.bls_withdrawal_prefix_byte,
                spec.min_activation_balance,
                spec,
            )]
        });
        assert!(state.pending_deposits().unwrap().is_empty());
        assert_eq!(state.validators().len(), VALIDATOR_COUNT + 1);
    }

    #[tokio::test]
    async fn bls_top_up_applied_in_heze() {
        let validator_index = 0;
        let (genesis_state, state, spec) = state_after_deposits(ForkName::Heze, |spec| {
            vec![signed_pending_deposit(
                validator_index,
                spec.bls_withdrawal_prefix_byte,
                spec.min_activation_balance,
                spec,
            )]
        });
        assert!(state.pending_deposits().unwrap().is_empty());
        assert_eq!(state.validators().len(), VALIDATOR_COUNT);
        assert_eq!(
            *state.balances().get(validator_index).unwrap(),
            genesis_state.balances().get(validator_index).unwrap() + spec.min_activation_balance
        );
    }

    #[tokio::test]
    async fn new_eth1_validator_added_after_skipped_bls_deposit_in_heze() {
        let (_, state, spec) = state_after_deposits(ForkName::Heze, |spec| {
            vec![
                signed_pending_deposit(
                    VALIDATOR_COUNT,
                    spec.bls_withdrawal_prefix_byte,
                    spec.min_activation_balance,
                    spec,
                ),
                signed_pending_deposit(
                    VALIDATOR_COUNT,
                    spec.eth1_address_withdrawal_prefix_byte,
                    spec.min_activation_balance,
                    spec,
                ),
            ]
        });
        assert!(state.pending_deposits().unwrap().is_empty());
        assert_eq!(state.validators().len(), VALIDATOR_COUNT + 1);
        let validator = state.get_validator(VALIDATOR_COUNT).unwrap();
        assert!(validator.has_eth1_withdrawal_credential(&spec));
        assert_eq!(
            *state.balances().get(VALIDATOR_COUNT).unwrap(),
            spec.min_activation_balance
        );
    }

    #[tokio::test]
    async fn bls_top_up_with_invalid_signature_applied_in_heze() {
        let validator_index = 0;
        let (genesis_state, state, spec) = state_after_deposits(ForkName::Heze, |spec| {
            vec![unsigned_pending_deposit(
                validator_index,
                spec.bls_withdrawal_prefix_byte,
                spec.min_activation_balance,
                spec,
            )]
        });
        assert!(state.pending_deposits().unwrap().is_empty());
        assert_eq!(state.validators().len(), VALIDATOR_COUNT);
        assert_eq!(
            *state.balances().get(validator_index).unwrap(),
            genesis_state.balances().get(validator_index).unwrap() + spec.min_activation_balance
        );
    }

    /// A skipped deposit still uses churn and leaves the queue.
    #[tokio::test]
    async fn skipped_bls_deposit_consumes_churn_in_heze() {
        let (genesis_state, state, spec) = state_after_deposits(ForkName::Heze, |spec| {
            vec![
                signed_pending_deposit(
                    VALIDATOR_COUNT,
                    spec.bls_withdrawal_prefix_byte,
                    spec.min_activation_balance,
                    spec,
                ),
                signed_pending_deposit(
                    VALIDATOR_COUNT + 1,
                    spec.eth1_address_withdrawal_prefix_byte,
                    spec.min_per_epoch_churn_limit_electra,
                    spec,
                ),
            ]
        });
        let churn_limit = genesis_state
            .get_activation_exit_churn_limit(&spec)
            .unwrap();
        assert_eq!(churn_limit, spec.min_per_epoch_churn_limit_electra);

        // The second deposit only fits in the churn if the skipped deposit uses none.
        assert_eq!(state.validators().len(), VALIDATOR_COUNT);
        assert_eq!(
            state.deposit_balance_to_consume().unwrap(),
            churn_limit - spec.min_activation_balance
        );
        let pending_deposits = state.pending_deposits().unwrap();
        assert_eq!(pending_deposits.len(), 1);
        let remaining_deposit = pending_deposits.get(0).unwrap();
        assert_eq!(
            remaining_deposit.pubkey,
            generate_deterministic_keypair(VALIDATOR_COUNT + 1)
                .pk
                .compress()
        );
        assert_eq!(remaining_deposit.amount, churn_limit);
    }
}

#[cfg(not(debug_assertions))]
mod release_tests {
    use super::*;
    use crate::{
        EpochProcessingError, GloasVerificationContext, SlotProcessingError,
        per_slot_processing::per_slot_processing,
    };
    use beacon_chain::test_utils::{AttestationStrategy, BlockStrategy};
    use std::sync::Arc;
    use types::{Epoch, ForkName, InconsistentFork, MainnetEthSpec};

    #[tokio::test]
    async fn altair_state_on_base_fork() {
        let mut spec = MainnetEthSpec::default_spec();
        let slots_per_epoch = MainnetEthSpec::slots_per_epoch();
        // The Altair fork happens at epoch 1.
        spec.altair_fork_epoch = Some(Epoch::new(1));

        let altair_state = {
            let harness = BeaconChainHarness::builder(MainnetEthSpec)
                .spec(Arc::new(spec.clone()))
                .deterministic_keypairs(8)
                .fresh_ephemeral_store()
                .build();

            harness.advance_slot();

            harness
                .extend_chain(
                    // Build out enough blocks so we get an Altair block at the very end of an epoch.
                    (slots_per_epoch * 2 - 1) as usize,
                    BlockStrategy::OnCanonicalHead,
                    AttestationStrategy::AllValidators,
                )
                .await;

            harness.get_current_state()
        };

        // Pre-conditions for a valid test.
        assert_eq!(altair_state.fork_name(&spec).unwrap(), ForkName::Altair);
        assert_eq!(
            altair_state.slot(),
            altair_state.current_epoch().end_slot(slots_per_epoch)
        );

        // Check the state is valid before starting this test.
        process_epoch(&mut altair_state.clone(), &spec)
            .expect("state passes intial epoch processing");
        per_slot_processing(
            &mut altair_state.clone(),
            None,
            GloasVerificationContext::FullVerification,
            &spec,
        )
        .expect("state passes intial slot processing");

        // Modify the spec so altair never happens.
        spec.altair_fork_epoch = None;

        let expected_err = InconsistentFork {
            fork_at_slot: ForkName::Base,
            object_fork: ForkName::Altair,
        };

        assert_eq!(altair_state.fork_name(&spec), Err(expected_err));
        assert_eq!(
            process_epoch(&mut altair_state.clone(), &spec),
            Err(EpochProcessingError::InconsistentStateFork(expected_err))
        );
        assert_eq!(
            per_slot_processing(
                &mut altair_state.clone(),
                None,
                GloasVerificationContext::FullVerification,
                &spec
            ),
            Err(SlotProcessingError::InconsistentStateFork(expected_err))
        );
    }

    #[tokio::test]
    async fn base_state_on_altair_fork() {
        let mut spec = MainnetEthSpec::default_spec();
        let slots_per_epoch = MainnetEthSpec::slots_per_epoch();
        // The Altair fork never happens.
        spec.altair_fork_epoch = None;

        let base_state = {
            let harness = BeaconChainHarness::builder(MainnetEthSpec)
                .spec(Arc::new(spec.clone()))
                .deterministic_keypairs(8)
                .fresh_ephemeral_store()
                .build();

            harness.advance_slot();

            harness
                .extend_chain(
                    // Build out enough blocks so we get a block at the very end of an epoch.
                    (slots_per_epoch * 2 - 1) as usize,
                    BlockStrategy::OnCanonicalHead,
                    AttestationStrategy::AllValidators,
                )
                .await;

            harness.get_current_state()
        };

        // Pre-conditions for a valid test.
        assert_eq!(base_state.fork_name(&spec).unwrap(), ForkName::Base);
        assert_eq!(
            base_state.slot(),
            base_state.current_epoch().end_slot(slots_per_epoch)
        );

        // Check the state is valid before starting this test.
        process_epoch(&mut base_state.clone(), &spec)
            .expect("state passes intial epoch processing");
        per_slot_processing(
            &mut base_state.clone(),
            None,
            GloasVerificationContext::FullVerification,
            &spec,
        )
        .expect("state passes intial slot processing");

        // Modify the spec so Altair happens at the first epoch.
        spec.altair_fork_epoch = Some(Epoch::new(1));

        let expected_err = InconsistentFork {
            fork_at_slot: ForkName::Altair,
            object_fork: ForkName::Base,
        };

        assert_eq!(base_state.fork_name(&spec), Err(expected_err));
        assert_eq!(
            process_epoch(&mut base_state.clone(), &spec),
            Err(EpochProcessingError::InconsistentStateFork(expected_err))
        );
        assert_eq!(
            per_slot_processing(
                &mut base_state.clone(),
                None,
                GloasVerificationContext::FullVerification,
                &spec
            ),
            Err(SlotProcessingError::InconsistentStateFork(expected_err))
        );
    }
}
