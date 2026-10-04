import EpochProofs.Spec.InactivityUpdates
import EpochProofs.Spec.PendingDeposits
import EpochProofs.Spec.Oracle

/-!
# Reference: accessors, signing domains and `xor`

Transcribed from `specs/phase0/beacon-chain.md` and `specs/fulu/beacon-chain.md`
(v1.7.0-beta.2). Each definition quotes its pyspec. Hashes come from the `Oracle`.
-/

namespace EpochProofs.Spec

/-- ```python
def xor(bytes_1: Bytes32, bytes_2: Bytes32) -> Bytes32:
    return Bytes32(a ^ b for a, b in zip(bytes_1, bytes_2, strict=True))
``` -/
def xor (bytes_1 bytes_2 : Bytes32) : Bytes32 :=
  Vector.zipWith (· ^^^ ·) bytes_1 bytes_2

/-- ```python
def get_randao_mix(state: BeaconState, epoch: Epoch) -> Bytes32:
    return state.randao_mixes[epoch % EPOCHS_PER_HISTORICAL_VECTOR]
``` -/
def get_randao_mix (p : Preset) (state : BeaconState) (epoch : Epoch) : SpecM Bytes32 := do
  listGet state.randao_mixes (← uint64Mod epoch p.EPOCHS_PER_HISTORICAL_VECTOR)

/-- ```python
def get_beacon_proposer_index(state: BeaconState) -> ValidatorIndex:
    return state.proposer_lookahead[state.slot % SLOTS_PER_EPOCH]
``` -/
def get_beacon_proposer_index (p : Preset) (state : BeaconState) : SpecM ValidatorIndex := do
  listGet state.proposer_lookahead (← uint64Mod state.slot p.SLOTS_PER_EPOCH)

/-- ```python
def compute_fork_data_root(current_version: Version, genesis_validators_root: Root) -> Root:
    return hash_tree_root(
        ForkData(
            current_version=current_version,
            genesis_validators_root=genesis_validators_root,
        )
    )
``` -/
def compute_fork_data_root (o : Oracle) (current_version : Version)
    (genesis_validators_root : Root) : Root :=
  o.hash_tree_root_ForkData { current_version, genesis_validators_root }

/-- ```python
def compute_domain(
    domain_type: DomainType,
    fork_version: Optional[Version] = None,
    genesis_validators_root: Optional[Root] = None,
) -> Domain:
    if fork_version is None:
        fork_version = GENESIS_FORK_VERSION
    if genesis_validators_root is None:
        genesis_validators_root = Root()  # all bytes zero by default
    fork_data_root = compute_fork_data_root(fork_version, genesis_validators_root)
    return Domain(domain_type + fork_data_root[:28])
``` -/
def compute_domain (p : Preset) (o : Oracle) (domain_type : DomainType)
    (fork_version : Option Version := none) (genesis_validators_root : Option Root := none) :
    Domain :=
  let fork_version := match fork_version with
    | none => p.GENESIS_FORK_VERSION
    | some v => v
  let genesis_validators_root := match genesis_validators_root with
    | none => Bytes32.zero
    | some r => r
  let fork_data_root := compute_fork_data_root o fork_version genesis_validators_root
  (domain_type ++ fork_data_root.extract 0 28).cast (by decide)

/-- ```python
def get_domain(
    state: BeaconState, domain_type: DomainType, epoch: Optional[Epoch] = None
) -> Domain:
    epoch = get_current_epoch(state) if epoch is None else epoch
    fork_version = (
        state.fork.previous_version if epoch < state.fork.epoch else state.fork.current_version
    )
    return compute_domain(domain_type, fork_version, state.genesis_validators_root)
``` -/
def get_domain (p : Preset) (o : Oracle) (state : BeaconState) (domain_type : DomainType)
    (epoch : Option Epoch := none) : SpecM Domain := do
  let epoch ← match epoch with
    | none => get_current_epoch p state
    | some e => pure e
  let fork_version :=
    if epoch < state.fork.epoch then state.fork.previous_version else state.fork.current_version
  pure (compute_domain p o domain_type fork_version state.genesis_validators_root)

/-- ```python
def compute_signing_root(ssz_object: SSZObject, domain: Domain) -> Root:
    return hash_tree_root(
        SigningData(
            object_root=hash_tree_root(ssz_object),
            domain=domain,
        )
    )
```

`hash_tree_root` is typed, so the caller passes `hash_tree_root(ssz_object)` as `object_root`. -/
def compute_signing_root (o : Oracle) (object_root : Root) (domain : Domain) : Root :=
  o.hash_tree_root_SigningData { object_root, domain }

/-- ```python
def get_block_root_at_slot(state: BeaconState, slot: Slot) -> Root:
    """
    Return the block root at a recent ``slot``.
    """
    assert slot < state.slot <= slot + SLOTS_PER_HISTORICAL_ROOT
    return state.block_roots[slot % SLOTS_PER_HISTORICAL_ROOT]
``` -/
def get_block_root_at_slot (p : Preset) (state : BeaconState) (slot : Slot) : SpecM Root := do
  -- Python evaluates `slot + SLOTS_PER_HISTORICAL_ROOT` only if `slot < state.slot`.
  if !(slot < state.slot) then
    throw .assertionFailed
  if !(state.slot ≤ (← uint64Add slot p.SLOTS_PER_HISTORICAL_ROOT)) then
    throw .assertionFailed
  listGet state.block_roots (slot % p.SLOTS_PER_HISTORICAL_ROOT)

/-- ```python
def get_block_root(state: BeaconState, epoch: Epoch) -> Root:
    """
    Return the block root at the start of a recent ``epoch``.
    """
    return get_block_root_at_slot(state, compute_start_slot_at_epoch(epoch))
``` -/
def get_block_root (p : Preset) (state : BeaconState) (epoch : Epoch) : SpecM Root := do
  get_block_root_at_slot p state (← compute_start_slot_at_epoch p epoch)

end EpochProofs.Spec
