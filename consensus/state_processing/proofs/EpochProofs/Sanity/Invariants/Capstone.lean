import EpochProofs.Sanity.Invariants.Reachable
import EpochProofs.Sanity.Invariants.LighthouseEpoch

/-!
# Lighthouse epoch processing on every reachable run

On mainnet, `state_transition` with the Lighthouse epoch `process_epoch_lh` has the same `ok`
results as the spec `state_transition`, from every state that a run of spec state transitions
reaches. The start state has the invariants of a genesis state. The economic facts are two
explicit hypotheses on the run: the total active balance before each block, and
`EpochSupply` at each `process_epoch` input.
-/

namespace EpochProofs.Spec

/-- On mainnet, from every reachable state, `state_transition` with `process_epoch_lh` has the
same `ok` results as the spec `state_transition`.

The start state `init` has `FfgInvariant`, `RowsOk`, `ExitDelay`, `ExitEpochsU64` and
`AfterEB`. `SupplyBound` says that the total active balance before each block is at most 139
million ETH. `EpochEcon … EpochSupply` says that each `process_epoch` input of the run has
balances below `2^62`, effective balances that sum below `2^60`, and a slashed sum whose triple
fits in a `u64`. Each block of the run has a `u64` slot and at most 512 sync bits. -/
theorem lighthouse_state_transition_sameOk (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (init : BeaconState)
    (hffg : FfgInvariant Preset.mainnet init) (hrows : RowsOk init)
    (hd : ExitDelay Preset.mainnet init) (hx : ExitEpochsU64 init)
    (h0 : AfterEB Preset.mainnet init)
    (hsupply : SupplyBound o max_blobs_per_block GLOAS_FORK_EPOCH init)
    (hecon : EpochEcon o max_blobs_per_block GLOAS_FORK_EPOCH init EpochSupply)
    (s : BeaconState) (hr : Reachable o max_blobs_per_block GLOAS_FORK_EPOCH init s)
    (signed_block : SignedBeaconBlock) (validate_result : Bool)
    (hslot64 : signed_block.message.slot < 2 ^ 64) :
    SameOk (state_transition_with o max_blobs_per_block GLOAS_FORK_EPOCH (process_epoch_lh o) s
        signed_block validate_result)
      (state_transition Preset.mainnet o max_blobs_per_block GLOAS_FORK_EPOCH s signed_block
        validate_result) :=
  reachable_state_transition_sameOk' o max_blobs_per_block GLOAS_FORK_EPOCH init hffg hrows hd
    hx h0 hsupply EpochSupply hecon (process_epoch_lh o) (process_epoch_lh_hf o) s hr signed_block
    validate_result hslot64

end EpochProofs.Spec
