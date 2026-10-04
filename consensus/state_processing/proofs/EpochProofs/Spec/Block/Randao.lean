import EpochProofs.Spec.Helpers

/-!
# Reference: `process_randao`

Transcribed from `specs/phase0/beacon-chain.md` (v1.7.0-beta.2). Later forks do not change it.
`sha256` and BLS come from the `Oracle`.
-/

namespace EpochProofs.Spec

/-- ```python
def process_randao(state: BeaconState, body: BeaconBlockBody) -> None:
    epoch = get_current_epoch(state)
    # Verify RANDAO reveal
    proposer = state.validators[get_beacon_proposer_index(state)]
    signing_root = compute_signing_root(epoch, get_domain(state, DOMAIN_RANDAO))
    assert bls.Verify(proposer.pubkey, signing_root, body.randao_reveal)
    # Mix in RANDAO reveal
    mix = xor(get_randao_mix(state, epoch), sha256(body.randao_reveal))
    state.randao_mixes[epoch % EPOCHS_PER_HISTORICAL_VECTOR] = mix
``` -/
def process_randao (p : Preset) (o : Oracle) (state : BeaconState) (body : BeaconBlockBody) :
    SpecM BeaconState := do
  let epoch ← get_current_epoch p state
  let proposer ← listGet state.validators (← get_beacon_proposer_index p state)
  let signing_root :=
    compute_signing_root o (o.hash_tree_root_Epoch epoch) (← get_domain p o state DOMAIN_RANDAO)
  if ¬ o.bls_Verify proposer.pubkey signing_root body.randao_reveal then throw .assertionFailed
  let mix := xor (← get_randao_mix p state epoch) (o.hash body.randao_reveal.toList)
  pure { state with
    randao_mixes :=
      ← listSet state.randao_mixes (← uint64Mod epoch p.EPOCHS_PER_HISTORICAL_VECTOR) mix }

end EpochProofs.Spec
