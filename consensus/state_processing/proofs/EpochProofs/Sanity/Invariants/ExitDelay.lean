import EpochProofs.Sanity.Block.Slashings
import EpochProofs.Sanity.Block.Withdrawals
import EpochProofs.Sanity.Block.Attestations
import EpochProofs.Sanity.Block.ParentPayload
import EpochProofs.Sanity.Block.SyncAggregate
import EpochProofs.Sanity.Frame
import EpochProofs.Sanity.RowsRewards
import EpochProofs.Sanity.Driver
import EpochProofs.Sanity.EffectiveBalanceInvariant

/-!
# The exit delay invariant and the validator step of block operations

`ExitDelay` says that each validator has no withdrawable epoch yet, or becomes withdrawable at
least `MIN_VALIDATOR_WITHDRAWABILITY_DELAY` epochs after its exit. Every block operation,
`process_epoch` and `process_slots` keep it. A validator that is withdrawable at the current
epoch is not eligible for rewards at the next epoch boundary.

`OpStep` is the effect of the block operations that neither slash nor withdraw nor apply sync
rewards: effective balances and slashed flags stay, an exit starts only with the delay, and a
balance does not drop below `min old MIN_ACTIVATION_BALANCE`.
-/

namespace EpochProofs.Spec

/-! ## The invariant -/

/-- The validator has no withdrawable epoch yet, or its withdrawable epoch is at least
`MIN_VALIDATOR_WITHDRAWABILITY_DELAY` after its exit epoch. -/
def ExitDelayV (p : Preset) (v : Validator) : Prop :=
  FAR_FUTURE_EPOCH ≤ v.withdrawable_epoch ∨
    v.exit_epoch + p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY ≤ v.withdrawable_epoch

/-- Every validator has `ExitDelayV`. -/
def ExitDelay (p : Preset) (s : BeaconState) : Prop :=
  ∀ v ∈ s.validators, ExitDelayV p v

/-- `ExitDelayV` of a validator with a later withdrawable epoch and the same exit epoch. -/
theorem ExitDelayV.mono {p : Preset} {v v' : Validator} (h : ExitDelayV p v)
    (he : v'.exit_epoch = v.exit_epoch) (hw : v.withdrawable_epoch ≤ v'.withdrawable_epoch) :
    ExitDelayV p v' := by
  rcases h with h | h
  · exact .inl (Nat.le_trans h hw)
  · exact .inr (by rw [he]; exact Nat.le_trans h hw)

/-- `ExitDelay` from a pointwise fact on the new validators. -/
theorem ExitDelay.of_pointwise {p : Preset} {s s' : BeaconState}
    (hpt : ∀ (j : Nat) (v' : Validator), s'.validators[j]? = some v' →
      ∃ v : Validator, s.validators[j]? = some v ∧ (ExitDelayV p v → ExitDelayV p v'))
    (hs : ExitDelay p s) : ExitDelay p s' := by
  intro v' hv'
  obtain ⟨j, hj, rfl⟩ := List.getElem_of_mem hv'
  obtain ⟨v, hv, himp⟩ := hpt j _ (List.getElem?_eq_getElem hj)
  exact himp (hs v (List.mem_of_getElem? hv))

/-- Same validators keep `ExitDelay`. -/
theorem ExitDelay.of_validators_eq {p : Preset} {s s' : BeaconState}
    (hv : s'.validators = s.validators) (hs : ExitDelay p s) : ExitDelay p s' := by
  intro v h
  rw [hv] at h
  exact hs v h

/-- `ExitChange` keeps `ExitDelayV`. A new exit sets the withdrawable epoch to the exit epoch
plus the delay. -/
theorem ExitChange.exitDelay {p : Preset} {s : BeaconState} {v v' : Validator}
    (h : ExitChange p s v v') (hv : ExitDelayV p v) : ExitDelayV p v' := by
  rcases h with rfl | ⟨-, cur, e, -, -, rfl⟩
  · exact hv
  · exact .inr (Nat.le_refl _)

/-- `SlashChange` keeps `ExitDelayV`. The withdrawable epoch only grows. -/
theorem SlashChange.exitDelay {p : Preset} {s : BeaconState} {v v' : Validator}
    (h : SlashChange p s v v') (hv : ExitDelayV p v) : ExitDelayV p v' := by
  obtain ⟨v1, cur, hch, -, rfl⟩ := h
  exact (hch.exitDelay hv).mono rfl (Nat.le_max_left _ _)

/-- A validator with `ExitDelayV` that is withdrawable at epoch `E` is not eligible for rewards
at a previous epoch `previous_epoch ≥ E - 1`. It exited at least one epoch before
`previous_epoch + 1`, and it is withdrawable by `previous_epoch + 1`. -/
theorem rewardsEligible_of_withdrawable {p : Preset} {v : Validator} {E previous_epoch : Nat}
    (hd : ExitDelayV p v) (hw : v.withdrawable_epoch ≤ E) (hfar : E < FAR_FUTURE_EPOCH)
    (hdelay : 1 ≤ p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY) (hE : E ≤ previous_epoch + 1) :
    rewardsEligible previous_epoch v ≠ .ok true := by
  have hx : v.exit_epoch + p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY ≤ v.withdrawable_epoch := by
    rcases hd with hd | hd
    · simp only [Epoch, Uint64] at *; omega
    · exact hd
  have hna : is_active_validator v previous_epoch = false := by
    simp only [is_active_validator, Bool.and_eq_false_iff, decide_eq_false_iff_not]
    right
    simp only [Epoch, Uint64] at *
    omega
  unfold rewardsEligible
  rw [hna]
  intro h
  simp only [Bool.false_eq_true, if_false] at h
  split at h
  · simp only [bind, Except.bind] at h
    split at h
    · cases h
    · rename_i c hc
      simp only [pure, Except.pure, Except.ok.injEq, decide_eq_true_eq] at h
      unfold uint64Add at hc
      split at hc
      · cases hc
        simp only [Epoch, Uint64] at *
        omega
      · cases hc
  · cases h

/-! ## The validator step of block operations -/

/-- A validator step at epoch `E`. The effective balance, the slashed flag and the activation
epoch stay. The exit and withdrawable epochs stay, or an exit starts after `E` and the
withdrawable epoch is the exit epoch plus the delay. -/
def VStep (p : Preset) (E : Epoch) (v v' : Validator) : Prop :=
  v'.effective_balance = v.effective_balance ∧ v'.slashed = v.slashed ∧
    v'.activation_epoch = v.activation_epoch ∧
    ((v'.exit_epoch = v.exit_epoch ∧ v'.withdrawable_epoch = v.withdrawable_epoch) ∨
      (v.exit_epoch = FAR_FUTURE_EPOCH ∧ E < v'.exit_epoch ∧
        v'.withdrawable_epoch = v'.exit_epoch + p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY))

/-- A validator that does not change takes a validator step. -/
theorem VStep.refl (p : Preset) (E : Epoch) (v : Validator) : VStep p E v v :=
  ⟨rfl, rfl, rfl, .inl ⟨rfl, rfl⟩⟩

/-- Two validator steps at the same epoch compose. -/
theorem VStep.trans {p : Preset} {E : Epoch} {v1 v2 v3 : Validator} (h12 : VStep p E v1 v2)
    (h23 : VStep p E v2 v3) : VStep p E v1 v3 := by
  refine ⟨h23.1.trans h12.1, h23.2.1.trans h12.2.1, h23.2.2.1.trans h12.2.2.1, ?_⟩
  rcases h12.2.2.2 with ⟨he, hw⟩ | ⟨hf, hlt, hw⟩
  · rcases h23.2.2.2 with ⟨he', hw'⟩ | ⟨hf', hlt', hw'⟩
    · exact .inl ⟨he'.trans he, hw'.trans hw⟩
    · exact .inr ⟨he ▸ hf', hlt', hw'⟩
  · rcases h23.2.2.2 with ⟨he', hw'⟩ | ⟨-, hlt', hw'⟩
    · exact .inr ⟨hf, he' ▸ hlt, by rw [hw', hw, he']⟩
    · exact .inr ⟨hf, hlt', hw'⟩

/-- A validator step keeps `ExitDelayV`. -/
theorem VStep.exitDelay {p : Preset} {E : Epoch} {v v' : Validator} (h : VStep p E v v')
    (hv : ExitDelayV p v) : ExitDelayV p v' := by
  rcases h.2.2.2 with ⟨he, hw⟩ | ⟨-, -, hw⟩
  · exact hv.mono he (Nat.le_of_eq hw.symm)
  · exact .inr (Nat.le_of_eq hw.symm)

/-- A validator step keeps a validator that is withdrawable at epoch `E'`, if it has
`ExitDelayV` and `E'` is a real epoch. Its exit already started, so no new exit starts. -/
theorem VStep.frozen {p : Preset} {E : Epoch} {v v' : Validator} {E' : Nat}
    (h : VStep p E v v') (hw : v.withdrawable_epoch ≤ E')
    (hx : v.exit_epoch + p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY ≤ v.withdrawable_epoch)
    (hfar : E' < FAR_FUTURE_EPOCH) :
    v'.exit_epoch = v.exit_epoch ∧ v'.withdrawable_epoch = v.withdrawable_epoch := by
  rcases h.2.2.2 with h | ⟨hf, -⟩
  · exact h
  · simp only [Epoch, Uint64] at *; omega

/-- A validator step at `E` keeps the activity at `E`, if `E` is a real epoch. -/
theorem VStep.active_eq {p : Preset} {E : Epoch} {v v' : Validator} (h : VStep p E v v')
    (hfar : E < FAR_FUTURE_EPOCH) : is_active_validator v' E = is_active_validator v E := by
  unfold is_active_validator
  rw [h.2.2.1]
  rcases h.2.2.2 with ⟨he, -⟩ | ⟨hf, hlt, -⟩
  · rw [he]
  · rw [hf]
    simp [hlt, hfar]

/-- The effect of a block operation that neither slashes, withdraws nor applies sync rewards.
The slot and the number of validators stay, and each validator takes a validator step at the
current epoch. No balance list shrinks, and
each balance ends at least at `min old MIN_ACTIVATION_BALANCE`. -/
def OpStep (p : Preset) (s s' : BeaconState) : Prop :=
  s'.slot = s.slot ∧ s'.validators.length = s.validators.length ∧
    (∀ (i : Nat) (v v' : Validator), s.validators[i]? = some v → s'.validators[i]? = some v' →
      VStep p (s.slot / p.SLOTS_PER_EPOCH) v v') ∧
    s.balances.length ≤ s'.balances.length ∧
    ∀ (i : Nat) (b : Gwei), s.balances[i]? = some b →
      ∃ b', s'.balances[i]? = some b' ∧ min b p.MIN_ACTIVATION_BALANCE ≤ b'

/-- A state takes an operation step to itself. -/
theorem OpStep.refl (p : Preset) (s : BeaconState) : OpStep p s s := by
  refine ⟨rfl, rfl, fun i v v' h h' => ?_, Nat.le_refl _, fun i b h => ⟨b, h, Nat.min_le_left _ _⟩⟩
  rw [h] at h'
  cases h'
  exact VStep.refl p _ v

/-- Two operation steps compose. -/
theorem OpStep.trans {p : Preset} {s1 s2 s3 : BeaconState} (h12 : OpStep p s1 s2)
    (h23 : OpStep p s2 s3) : OpStep p s1 s3 := by
  obtain ⟨hs12, hl12, hv12, hbl12, hb12⟩ := h12
  obtain ⟨hs23, hl23, hv23, hbl23, hb23⟩ := h23
  refine ⟨hs23.trans hs12, hl23.trans hl12, fun i v1 v3 h1 h3 => ?_,
    Nat.le_trans hbl12 hbl23, fun i b1 h1 => ?_⟩
  · have hi : i < s2.validators.length := by
      rw [hl12]; exact (List.getElem?_eq_some_iff.mp h1).1
    have h2 := List.getElem?_eq_getElem hi
    have h23' := hv23 i _ _ h2 h3
    rw [hs12] at h23'
    exact (hv12 i _ _ h1 h2).trans h23'
  · obtain ⟨b2, h2, hle2⟩ := hb12 i b1 h1
    obtain ⟨b3, h3, hle3⟩ := hb23 i b2 h2
    refine ⟨b3, h3, ?_⟩
    simp only [Nat.min_def, Gwei] at *
    split at hle2 <;> split at hle3 <;> split <;> omega

/-- An operation step keeps `ExitDelay`. -/
theorem OpStep.exitDelay {p : Preset} {s s' : BeaconState} (h : OpStep p s s')
    (hs : ExitDelay p s) : ExitDelay p s' := by
  refine ExitDelay.of_pointwise (fun j v' hv' => ?_) hs
  have hj : j < s.validators.length := by
    rw [← h.2.1]; exact (List.getElem?_eq_some_iff.mp hv').1
  exact ⟨_, List.getElem?_eq_getElem hj,
    (h.2.2.1 j _ _ (List.getElem?_eq_getElem hj) hv').exitDelay⟩

/-- Same validators and balances, and the same slot, give an operation step. -/
theorem OpStep.of_eq {p : Preset} {s s' : BeaconState} (hslot : s'.slot = s.slot)
    (hv : s'.validators = s.validators) (hb : s'.balances = s.balances) : OpStep p s s' := by
  refine ⟨hslot, by rw [hv], fun i v v' h h' => ?_, by rw [hb]; exact Nat.le_refl _,
    fun i b h => ?_⟩
  · rw [hv, h] at h'
    cases h'
    exact VStep.refl p _ v
  · exact ⟨b, by rw [hb]; exact h, Nat.min_le_left _ _⟩

/-- A step that keeps the slot and the validators and lowers no balance is an operation step. -/
theorem OpStep.of_balancesUp {p : Preset} {s s' : BeaconState} (hslot : s'.slot = s.slot)
    (hv : s'.validators = s.validators) (hb : BalancesUp s s') : OpStep p s s' := by
  refine ⟨hslot, by rw [hv], fun i v v' h h' => ?_, hb.1, fun i b h => ?_⟩
  · rw [hv, h] at h'
    cases h'
    exact VStep.refl p _ v
  · obtain ⟨b', h', hle⟩ := hb.2 i b h
    exact ⟨b', h', Nat.le_trans (Nat.min_le_left _ _) hle⟩

/-- A registry frame that keeps the slot is an operation step. -/
theorem OpStep.of_registryFrame {p : Preset} {s s' : BeaconState} (h : RegistryFrame s s')
    (hslot : s'.slot = s.slot) : OpStep p s s' :=
  .of_eq hslot h.1 h.2.1

/-- `process_slot` is an operation step. -/
theorem process_slot_opStep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (h : process_slot p o s = .ok s') : OpStep p s s' :=
  have hf := process_slot_frame p o s s' h
  .of_registryFrame hf.1 hf.2

/-- `process_block_header` is an operation step. -/
theorem process_block_header_opStep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (block : BeaconBlock) (h : process_block_header p o s block = .ok s') : OpStep p s s' :=
  have hf := process_block_header_frame p o s s' block h
  .of_registryFrame hf.1 hf.2

/-- `process_randao` is an operation step. -/
theorem process_randao_opStep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (body : BeaconBlockBody) (h : process_randao p o s body = .ok s') : OpStep p s s' :=
  have hf := process_randao_frame p o s s' body h
  .of_registryFrame hf.1 hf.2

/-- `process_eth1_data` is an operation step. -/
theorem process_eth1_data_opStep (p : Preset) (s s' : BeaconState) (body : BeaconBlockBody)
    (h : process_eth1_data p s body = .ok s') : OpStep p s s' :=
  have hf := process_eth1_data_frame p s s' body h
  .of_registryFrame hf.1 hf.2

/-- `process_execution_payload_bid` is an operation step. -/
theorem process_execution_payload_bid_opStep (p : Preset) (o : Oracle)
    (max_blobs_per_block : Epoch → Uint64) (s s' : BeaconState)
    (signed_bid : SignedExecutionPayloadBid)
    (h : process_execution_payload_bid p o max_blobs_per_block s signed_bid = .ok s') :
    OpStep p s s' :=
  have hf := process_execution_payload_bid_frame p o max_blobs_per_block s s' signed_bid h
  .of_registryFrame hf.1 hf.2.1

/-- `process_attestation` is an operation step. -/
theorem process_attestation_opStep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (attestation : Attestation) (parent_slot : Slot)
    (h : process_attestation p o s attestation parent_slot = .ok s') : OpStep p s s' :=
  .of_balancesUp (process_attestation_checkpointsStable p o s s' attestation parent_slot h).1
    (process_attestation_validators p o s s' attestation parent_slot h)
    (process_attestation_balancesUp p o s s' attestation parent_slot h)

/-- `process_payload_attestation` is an operation step. -/
theorem process_payload_attestation_opStep (p : Preset) (o : Oracle) (GLOAS_FORK_EPOCH : Epoch)
    (s s' : BeaconState) (payload_attestation : PayloadAttestation)
    (h : process_payload_attestation p o GLOAS_FORK_EPOCH s payload_attestation = .ok s') :
    OpStep p s s' := by
  rw [process_payload_attestation_eq p o GLOAS_FORK_EPOCH s s' payload_attestation h]
  exact OpStep.refl p s

/-- `get_current_epoch` is the slot divided by `SLOTS_PER_EPOCH`. -/
theorem get_current_epoch_ok_eq {p : Preset} {s : BeaconState} {e : Epoch}
    (h : get_current_epoch p s = .ok e) : e = s.slot / p.SLOTS_PER_EPOCH := by
  simp only [get_current_epoch, compute_epoch_at_slot, uint64Div] at h
  split at h
  · cases h
  · cases h; rfl

/-- An `ExitChange` is a validator step at the current epoch. -/
theorem ExitChange.vStep {p : Preset} {s : BeaconState} {v v' : Validator}
    (h : ExitChange p s v v') : VStep p (s.slot / p.SLOTS_PER_EPOCH) v v' := by
  rcases h with rfl | ⟨hf, cur, e, hc, hlt, rfl⟩
  · exact VStep.refl p _ _
  · exact ⟨rfl, rfl, rfl, .inr ⟨hf, get_current_epoch_ok_eq hc ▸ hlt, rfl⟩⟩

/-- `process_voluntary_exit` is an operation step. -/
theorem process_voluntary_exit_opStep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (e : SignedVoluntaryExit) (h : process_voluntary_exit p o s e = .ok s') : OpStep p s s' := by
  obtain ⟨-, hcp, -, hbu, hother, v, v', hv, hv', hch⟩ :=
    process_voluntary_exit_effects p o s s' e h
  obtain ⟨tab, -, -, -, -, -, -, hi⟩ := process_voluntary_exit_initiate p o s s' e h
  obtain ⟨-, hlen, -, -⟩ := initiate_validator_exit_ok p tab s s' _ hi
  refine ⟨hcp.1, hlen, fun j u u' hu hu' => ?_, hbu.1, fun j b hb => ?_⟩
  · by_cases hj : j = e.message.validator_index
    · subst hj
      rw [hv] at hu
      rw [hv'] at hu'
      cases hu
      cases hu'
      exact hch.vStep
    · rw [hother j hj, hu] at hu'
      cases hu'
      exact VStep.refl p _ u
  · obtain ⟨b', h', hle⟩ := hbu.2 j b hb
    exact ⟨b', h', Nat.le_trans (Nat.min_le_left _ _) hle⟩

/-- `process_bls_to_execution_change` is an operation step. -/
theorem process_bls_to_execution_change_opStep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (c : SignedBLSToExecutionChange) (h : process_bls_to_execution_change p o s c = .ok s') :
    OpStep p s s' := by
  obtain ⟨-, hcp, -, hbu, hlen, hpt⟩ := process_bls_to_execution_change_effects p o s s' c h
  refine ⟨hcp.1, hlen, fun j u u' hu hu' => ?_, hbu.1, fun j b hb => ?_⟩
  · obtain ⟨wc, h'⟩ := hpt j u hu
    rw [hu'] at h'
    cases h'
    exact ⟨rfl, rfl, rfl, .inl ⟨rfl, rfl⟩⟩
  · obtain ⟨b', h', hle⟩ := hbu.2 j b hb
    exact ⟨b', h', Nat.le_trans (Nat.min_le_left _ _) hle⟩

/-! ## Execution requests -/

/-- Every validator keeps its slashed flag and its activation epoch. -/
def SlashedKeep (s s' : BeaconState) : Prop :=
  ∀ (i : Nat) (v v' : Validator), s.validators[i]? = some v → s'.validators[i]? = some v' →
    v'.slashed = v.slashed ∧ v'.activation_epoch = v.activation_epoch

/-- Same validators keep the slashed flags. -/
theorem SlashedKeep.of_eq {s s' : BeaconState} (hv : s'.validators = s.validators) :
    SlashedKeep s s' := by
  intro i v v' h h'
  rw [hv, h] at h'
  cases h'
  exact ⟨rfl, rfl⟩

/-- One validator write that keeps the slashed flag and the activation epoch. -/
theorem SlashedKeep.of_set {s s' : BeaconState} {i : Nat} {v' : Validator}
    (hset : s'.validators = s.validators.set i v')
    (hsame : ∀ v, s.validators[i]? = some v →
      v'.slashed = v.slashed ∧ v'.activation_epoch = v.activation_epoch) :
    SlashedKeep s s' := by
  intro j w w' h h'
  rw [hset, List.getElem?_set] at h'
  by_cases hij : i = j
  · subst hij
    have hlt : i < s.validators.length := (List.getElem?_eq_some_iff.mp h).1
    simp only [hlt, if_true, Option.some.injEq] at h'
    subst h'
    exact hsame w h
  · simp only [hij, if_false, h, Option.some.injEq] at h'
    subst h'
    exact ⟨rfl, rfl⟩

/-- A request effect with the exit shape and the slashed flags is an operation step. -/
theorem OpStep.of_request {p : Preset} {s s' : BeaconState} (hr : RequestEffect p s s')
    (hx : ExitShape p s s') (hk : SlashedKeep s s') : OpStep p s s' := by
  obtain ⟨hlen, hv, hblen, hb, hcp⟩ := hr
  refine ⟨hcp.1, hlen, fun i v v' h h' => ?_, Nat.le_of_eq hblen.symm, fun i b h => ?_⟩
  · obtain ⟨u, hu, hst⟩ := hv i v h
    rw [h'] at hu
    cases hu
    refine ⟨hst.1, (hk i v v' h h').1, (hk i v v' h h').2, ?_⟩
    rcases hx i v v' h h' with he | ⟨hf, c, hc, hle, hw⟩
    · exact .inl he
    · refine .inr ⟨hf, ?_, hw⟩
      rw [← get_current_epoch_ok_eq hc]
      simp only [Epoch, Uint64] at *
      omega
  · obtain ⟨b', h', hst⟩ := hb i b h
    exact ⟨b', h', hst.bounds.1⟩

/-- `initiate_validator_exit` keeps the slashed flags. -/
theorem initiate_validator_exit_slashedKeep {p : Preset} {tab : Gwei} {s s' : BeaconState}
    {index : Nat} (h : initiate_validator_exit p tab s index = .ok s') : SlashedKeep s s' := by
  obtain ⟨-, -, hv | ⟨v, c, e, hv, -, -, -, hset⟩⟩ :=
    initiate_validator_exit_cases p tab s s' index h
  · exact .of_eq hv
  · exact .of_set hset fun u hu => by rw [hv] at hu; cases hu; exact ⟨rfl, rfl⟩

/-- `process_withdrawal_request` keeps the slashed flags. -/
theorem process_withdrawal_request_slashedKeep (p : Preset) (s s' : BeaconState)
    (withdrawal_request : WithdrawalRequest)
    (h : process_withdrawal_request p s withdrawal_request = .ok s') : SlashedKeep s s' := by
  unfold process_withdrawal_request at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | (cases h; done)
    | (cases h; exact .of_eq rfl)
    | exact initiate_validator_exit_slashedKeep ‹initiate_validator_exit _ _ _ _ = .ok _›
    | (cases h
       exact initiate_validator_exit_slashedKeep ‹initiate_validator_exit _ _ _ _ = .ok _›)
    | (cases h
       obtain ⟨-, -, -, hv, -, -⟩ := compute_exit_epoch_and_update_churn_bound p _ s _ _ _
         ‹compute_exit_epoch_and_update_churn _ _ _ _ = .ok _›
       exact .of_eq hv)

/-- `switch_to_compounding_validator` keeps the slashed flags. -/
theorem switch_to_compounding_validator_slashedKeep {p : Preset} {s s' : BeaconState}
    {index : Nat} (h : switch_to_compounding_validator p s index = .ok s') : SlashedKeep s s' := by
  unfold switch_to_compounding_validator at h
  obtain ⟨v, hv, h⟩ := specM_bind_ok h
  obtain ⟨l, hl, h⟩ := specM_bind_ok h
  obtain ⟨hvs, -, -⟩ := queue_excess_active_balance_cases p _ s' index h
  refine .of_set (hvs.trans (ExitsSanity.listSet_ok hl).2) fun w hw => ?_
  rw [(listGet_ok hv).2] at hw
  cases hw
  exact ⟨rfl, rfl⟩

/-- A successful `if` ran one of its branches. -/
private theorem ite_ok {α : Type} {c : Prop} [Decidable c] {t e : SpecM α} {x : α}
    (h : (if c then t else e) = .ok x) : (c ∧ t = .ok x) ∨ (¬ c ∧ e = .ok x) := by
  by_cases hc : c
  · exact .inl ⟨hc, by rw [if_pos hc] at h; exact h⟩
  · exact .inr ⟨hc, by rw [if_neg hc] at h; exact h⟩

/-- Splits the binds and `if`s of a successful run without `simp`. -/
local macro "peel_ok" h:ident : tactic => `(tactic| repeat' first
  | (guard_hyp $h:ident : ((_ : SpecM _) >>= _) = _; obtain ⟨_, _, $h:ident⟩ := specM_bind_ok $h)
  | (guard_hyp $h:ident : (@ite (SpecM _) _ (_) _ _) = _
     rcases ite_ok $h with ⟨_, $h:ident⟩ | ⟨_, $h:ident⟩))

/-- The last step of a consolidation keeps the slashed flags. -/
private theorem consolidation_exit_slashedKeep {p : Preset} {tab : Gwei} {s : BeaconState}
    {x i : Nat} {v v' : Validator} {r : Epoch × BeaconState} {l : List Validator}
    (hr : compute_consolidation_epoch_and_update_churn p tab s x = .ok r)
    (hl : listSet r.2.validators i v' = .ok l) (hv : listGet s.validators i = .ok v)
    (hv' : v'.slashed = v.slashed ∧ v'.activation_epoch = v.activation_epoch)
    (pc : List PendingConsolidation) :
    SlashedKeep s { r.2 with validators := l, pending_consolidations := pc } := by
  obtain ⟨e, s1⟩ := r
  obtain ⟨-, -, -, hvs, -, -⟩ :=
    compute_consolidation_epoch_and_update_churn_bound p tab s s1 x e hr
  rw [hvs] at hl
  refine .of_set (ExitsSanity.listSet_ok hl).2 fun w hw => ?_
  rw [(listGet_ok hv).2] at hw
  cases hw
  exact hv'

/-- `process_consolidation_request` keeps the slashed flags. -/
theorem process_consolidation_request_slashedKeep (p : Preset) (s s' : BeaconState)
    (consolidation_request : ConsolidationRequest)
    (h : process_consolidation_request p s consolidation_request = .ok s') :
    SlashedKeep s s' := by
  unfold process_consolidation_request at h
  peel_ok h
  all_goals first
    | (cases h; exact .of_eq rfl)
    | exact switch_to_compounding_validator_slashedKeep h
    | (cases h
       refine consolidation_exit_slashedKeep
         ‹compute_consolidation_epoch_and_update_churn _ _ _ _ = .ok _›
         ‹listSet _ _ _ = .ok _› ‹listGet _ _ = .ok _› ?_ _
       exact ⟨rfl, rfl⟩)

/-- `process_deposit_request` is an operation step. -/
theorem process_deposit_request_opStep (p : Preset) (s s' : BeaconState)
    (deposit_request : DepositRequest)
    (h : process_deposit_request s deposit_request = .ok s') : OpStep p s s' := by
  cases h
  exact .of_eq rfl rfl rfl

/-- `process_withdrawal_request` is an operation step. -/
theorem process_withdrawal_request_opStep (p : Preset) (s s' : BeaconState)
    (withdrawal_request : WithdrawalRequest)
    (h : process_withdrawal_request p s withdrawal_request = .ok s') : OpStep p s s' :=
  .of_request (process_withdrawal_request_effect p s s' withdrawal_request h).2
    (process_withdrawal_request_exitShape p s s' withdrawal_request h)
    (process_withdrawal_request_slashedKeep p s s' withdrawal_request h)

/-- `process_consolidation_request` is an operation step. -/
theorem process_consolidation_request_opStep (p : Preset) (s s' : BeaconState)
    (consolidation_request : ConsolidationRequest)
    (h : process_consolidation_request p s consolidation_request = .ok s') : OpStep p s s' :=
  .of_request (process_consolidation_request_effect p s s' consolidation_request h)
    (process_consolidation_request_exitShape p s s' consolidation_request h)
    (process_consolidation_request_slashedKeep p s s' consolidation_request h)

/-- A builder frame is an operation step. -/
theorem OpStep.of_builderFrame {p : Preset} {s s' : BeaconState} (h : BuilderFrame s s') :
    OpStep p s s' :=
  .of_registryFrame h.1 h.2.1

/-- A request loop of `apply_parent_execution_payload` is an operation step. -/
private theorem request_loop_opStep {α : Type} {p : Preset} {l : List α}
    {body : α → BeaconState → SpecM (ForInStep BeaconState)} {s s' : BeaconState}
    (hloop : forIn l s body = .ok s')
    (hb : ∀ a st r, body a st = .ok r → OpStep p st r.value) : OpStep p s s' :=
  forIn_rel (OpStep p) (OpStep.refl p) (fun _ _ _ => OpStep.trans) l body s s' hloop hb

/-- Proves the body step of a request loop from the step of one request. -/
local macro "loop_body" e:term : tactic => `(tactic| (
  intro _ _ _ hbody
  obtain ⟨_, h1, hbody⟩ := specM_bind_ok hbody
  obtain ⟨_, -, hbody⟩ := specM_bind_ok hbody
  cases hbody
  exact $e h1))

/-- Splits the binds and `if`s of a successful run, and closes a `throw` step. -/
local macro "peel_ok'" h:ident : tactic => `(tactic| repeat' first
  | (guard_hyp $h:ident : ((_ : SpecM _) >>= _) = _
     obtain ⟨_, hx, $h:ident⟩ := specM_bind_ok $h
     try (guard_hyp hx : (throw _ : SpecM _) = _; cases hx))
  | (guard_hyp $h:ident : (@ite (SpecM _) _ (_) _ _) = _;
     rcases ite_ok $h with ⟨_, $h:ident⟩ | ⟨_, $h:ident⟩))

/-- `apply_parent_execution_payload` is an operation step. -/
theorem apply_parent_execution_payload_opStep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (requests : ExecutionRequests)
    (h : apply_parent_execution_payload p o s requests = .ok s') : OpStep p s s' := by
  unfold apply_parent_execution_payload at h
  peel_ok' h
  all_goals first
    | (cases h
       refine (request_loop_opStep (by assumption : forIn requests.deposits _ _ = Except.ok _)
         (by loop_body process_deposit_request_opStep _ _ _ _)).trans
         ((request_loop_opStep (by assumption : forIn requests.withdrawals _ _ = Except.ok _)
         (by loop_body process_withdrawal_request_opStep _ _ _ _)).trans
         ((request_loop_opStep (by assumption : forIn requests.consolidations _ _ = Except.ok _)
         (by loop_body process_consolidation_request_opStep _ _ _ _)).trans
         ((request_loop_opStep (by assumption : forIn requests.builder_deposits _ _ = Except.ok _)
         (by loop_body fun h =>
           OpStep.of_builderFrame (process_builder_deposit_request_frame _ _ _ _ _ h))).trans
         ((request_loop_opStep (by assumption : forIn requests.builder_exits _ _ = Except.ok _)
         (by loop_body fun h =>
           OpStep.of_builderFrame (process_builder_exit_request_frame _ _ _ _ h))).trans
         ?_))))
       first
         | (have hs := settle_builder_payment_frame _ _ _
             ‹settle_builder_payment _ _ = Except.ok _›
            exact (OpStep.of_builderFrame hs).trans (.of_eq rfl rfl rfl))
         | exact .of_eq rfl rfl rfl)

/-- `process_parent_execution_payload` is an operation step. -/
theorem process_parent_execution_payload_opStep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (block : BeaconBlock) (h : process_parent_execution_payload p o s block = .ok s') :
    OpStep p s s' := by
  unfold process_parent_execution_payload at h
  peel_ok' h
  all_goals first
    | (cases h; exact OpStep.refl p _)
    | exact apply_parent_execution_payload_opStep p o _ _ _ h

/-! ## Slashings, withdrawals and sync rewards -/

/-- A slashing keeps `ExitDelay`. -/
theorem SlashFields.exitDelay {p : Preset} {s s' : BeaconState} (h : SlashFields p s s')
    (hlen : s'.validators.length = s.validators.length) (hs : ExitDelay p s) :
    ExitDelay p s' := by
  refine ExitDelay.of_pointwise (fun j v' hv' => ?_) hs
  have hj : j < s.validators.length := by
    rw [← hlen]; exact (List.getElem?_eq_some_iff.mp hv').1
  refine ⟨_, List.getElem?_eq_getElem hj, fun hd => ?_⟩
  rcases h j _ v' (List.getElem?_eq_getElem hj) hv' with rfl | ⟨-, hch⟩
  · exact hd
  · exact hch.exitDelay hd

/-- `process_proposer_slashing` keeps `ExitDelay`. -/
theorem process_proposer_slashing_exitDelay (p : Preset) (o : Oracle) (s s' : BeaconState)
    (ps : ProposerSlashing) (h : process_proposer_slashing p o s ps = .ok s')
    (hs : ExitDelay p s) : ExitDelay p s' := by
  obtain ⟨-, -, -, -, hother, ⟨v, v', hv, hv', -, -, hch⟩, -⟩ :=
    process_proposer_slashing_effects p o s s' ps h
  refine ExitDelay.of_pointwise (fun j u' hu' => ?_) hs
  by_cases hj : j = ps.signed_header_1.message.proposer_index
  · subst hj
    rw [hv'] at hu'
    cases hu'
    exact ⟨v, hv, hch.exitDelay⟩
  · exact ⟨u', (hother j hj).symm.trans hu', id⟩

/-- `process_attester_slashing` keeps `ExitDelay`. -/
theorem process_attester_slashing_exitDelay (p : Preset) (o : Oracle) (s s' : BeaconState)
    (as : AttesterSlashing) (h : process_attester_slashing p o s as = .ok s')
    (hs : ExitDelay p s) : ExitDelay p s' := by
  obtain ⟨heff, hf⟩ := process_attester_slashing_ok p o s s' as h
  exact hf.exitDelay heff.2.1 hs

/-- `process_withdrawals` keeps `ExitDelay`. -/
theorem process_withdrawals_exitDelay (p : Preset) (s s' : BeaconState)
    (h : process_withdrawals p s = .ok s') (hs : ExitDelay p s) : ExitDelay p s' :=
  .of_validators_eq (process_withdrawals_core p s s' h).1 hs

/-- `process_sync_aggregate` keeps `ExitDelay`. -/
theorem process_sync_aggregate_exitDelay (p : Preset) (o : Oracle) (s s' : BeaconState)
    (sync_aggregate : SyncAggregate)
    (h : process_sync_aggregate p o s sync_aggregate = .ok s') (hs : ExitDelay p s) :
    ExitDelay p s' := by
  obtain ⟨hv, -⟩ := process_sync_aggregate_frame p o s s' sync_aggregate h
  exact .of_validators_eq (by rw [hv]) hs

/-! ## Epoch steps -/

/-- Splits the binds, `if`s and `match`es of an epoch step that does not write `validators`,
and closes each success branch by `rfl`. -/
local macro "vs_tac" h:ident : tactic => `(tactic| (
  simp only [bind, Except.bind, pure, Except.pure] at $h:ident
  repeat' split at $h:ident
  all_goals first
    | contradiction
    | (cases $h:ident; rfl)))

/-- `process_inactivity_updates` does not write `validators`. -/
private theorem process_inactivity_updates_validators (p : Preset) (s s' : BeaconState)
    (h : process_inactivity_updates p s = .ok s') : s'.validators = s.validators := by
  unfold process_inactivity_updates at h
  vs_tac h

/-- `process_rewards_and_penalties` does not write `validators`. -/
private theorem process_rewards_and_penalties_validators (p : Preset)
    (total_active_balance : Gwei) (s s' : BeaconState)
    (h : process_rewards_and_penalties p total_active_balance s = .ok s') :
    s'.validators = s.validators := by
  unfold process_rewards_and_penalties at h
  vs_tac h

/-- `process_slashings` does not write `validators`. -/
private theorem process_slashings_validators (p : Preset) (total_active_balance : Gwei)
    (s s' : BeaconState) (h : process_slashings p total_active_balance s = .ok s') :
    s'.validators = s.validators := by
  unfold process_slashings at h
  vs_tac h

/-- `process_eth1_data_reset` does not write `validators`. -/
private theorem process_eth1_data_reset_validators (p : Preset) (s s' : BeaconState)
    (h : process_eth1_data_reset p s = .ok s') : s'.validators = s.validators :=
  (process_eth1_data_reset_frame p s s' h).1.1

/-- `process_builder_pending_payments` does not write `validators`. -/
private theorem process_builder_pending_payments_validators (p : Preset)
    (total_active_balance : Gwei) (s s' : BeaconState)
    (h : process_builder_pending_payments p total_active_balance s = .ok s') :
    s'.validators = s.validators := by
  unfold process_builder_pending_payments at h
  vs_tac h

/-- `process_pending_consolidations` does not write `validators`. -/
private theorem process_pending_consolidations_validators (p : Preset) (s s' : BeaconState)
    (h : process_pending_consolidations p s = .ok s') : s'.validators = s.validators := by
  unfold process_pending_consolidations at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨⟨_, st⟩, hr, h⟩ := specM_bind_ok h
  have hP := SlashingsSanity.forIn_invariant
    (fun r : MProd Nat BeaconState => r.snd.validators = s.validators) _ ?_ _ _ _ rfl hr
  · cases h
    exact hP
  · intro _ ⟨_, _⟩ r hst hb
    simp only [bind, Except.bind, pure, Except.pure] at hb
    repeat' split at hb
    all_goals first
      | contradiction
      | (cases hb; exact hst)

/-- One validator write that keeps the exit and withdrawable epochs keeps `ExitDelay`. -/
theorem ExitDelay.of_listSet {p : Preset} {s : BeaconState} {i : Nat} {v v' : Validator}
    {l : List Validator} (hl : listSet s.validators i v' = .ok l)
    (hv : listGet s.validators i = .ok v) (he : v'.exit_epoch = v.exit_epoch)
    (hw : v'.withdrawable_epoch = v.withdrawable_epoch) (hd : ExitDelay p s) :
    ∀ u ∈ l, ExitDelayV p u := by
  rw [(ExitsSanity.listSet_ok hl).2]
  intro u hu
  rcases List.mem_or_eq_of_mem_set hu with hu | rfl
  · exact hd u hu
  · exact (hd v (List.mem_of_getElem? (listGet_ok hv).2)).mono he (Nat.le_of_eq hw.symm)

/-- `initiate_validator_exit` keeps `ExitDelay`. -/
theorem initiate_validator_exit_exitDelay {p : Preset} {tab : Gwei} {s s' : BeaconState}
    {index : Nat} (h : initiate_validator_exit p tab s index = .ok s') (hd : ExitDelay p s) :
    ExitDelay p s' := by
  obtain ⟨-, -, hother, v, v', hv, hv', hch⟩ := initiate_validator_exit_ok p tab s s' index h
  refine ExitDelay.of_pointwise (fun j u' hu' => ?_) hd
  by_cases hj : j = index
  · subst hj
    rw [hv'] at hu'
    cases hu'
    exact ⟨v, hv, hch.exitDelay⟩
  · exact ⟨u', (hother j hj).symm.trans hu', id⟩

/-- `process_registry_updates` keeps `ExitDelay`. It writes activation epochs, or starts an
exit through `initiate_validator_exit`. -/
theorem process_registry_updates_exitDelay (p : Preset) (total_active_balance : Gwei)
    (s s' : BeaconState) (h : process_registry_updates p total_active_balance s = .ok s')
    (hd : ExitDelay p s) : ExitDelay p s' := by
  unfold process_registry_updates at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨r, hr, h⟩ := specM_bind_ok h
  cases h
  refine SlashingsSanity.forIn_invariant (ExitDelay p) _ ?_ _ s _ hd hr
  intro index st r hst hb
  simp only [bind, Except.bind, pure, Except.pure] at hb
  repeat' split at hb
  all_goals first
    | contradiction
    | (cases hb; exact hst)
    | (cases hb
       exact initiate_validator_exit_exitDelay ‹initiate_validator_exit _ _ _ _ = _› hst)
    | (cases hb
       refine ExitDelay.of_listSet ‹listSet _ _ _ = _› ‹listGet _ _ = _› ?_ ?_ hst <;> rfl)

/-- A validator from a deposit has no withdrawable epoch yet. -/
theorem get_validator_from_deposit_withdrawable (p : Preset) (pubkey : BLSPubkey)
    (withdrawal_credentials : Bytes32) (amount : Gwei) (v : Validator)
    (h : get_validator_from_deposit p pubkey withdrawal_credentials amount = .ok v) :
    v.withdrawable_epoch = FAR_FUTURE_EPOCH := by
  unfold get_validator_from_deposit at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; rfl)

/-- `add_validator_to_registry` keeps `ExitDelay`. The new validator has no withdrawable epoch
yet. -/
theorem add_validator_to_registry_exitDelay (p : Preset) (s s' : BeaconState)
    (pubkey : BLSPubkey) (withdrawal_credentials : Bytes32) (amount : Gwei)
    (h : add_validator_to_registry p s pubkey withdrawal_credentials amount = .ok s')
    (hd : ExitDelay p s) : ExitDelay p s' := by
  unfold add_validator_to_registry at h
  obtain ⟨v, hv, h⟩ := specM_bind_ok h
  obtain ⟨l, hl, h⟩ := specM_bind_ok h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first | contradiction | skip
  cases h
  have hw := get_validator_from_deposit_withdrawable p _ _ _ v hv
  unfold set_or_append_list get_index_for_new_validator at hl
  simp only [beq_self_eq_true, if_true, pure, Except.pure, Except.ok.injEq] at hl
  subst hl
  intro u hu
  rcases List.mem_append.mp hu with hu | hu
  · exact hd u hu
  · rw [List.mem_singleton.mp hu]
    exact .inl (Nat.le_of_eq hw.symm)

/-- `apply_pending_deposit` keeps `ExitDelay`. -/
theorem apply_pending_deposit_exitDelay (p : Preset) (o : Oracle) (s s' : BeaconState)
    (deposit : PendingDeposit) (h : apply_pending_deposit p o s deposit = .ok s')
    (hd : ExitDelay p s) : ExitDelay p s' := by
  unfold apply_pending_deposit at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact hd)
    | exact add_validator_to_registry_exitDelay p s s' _ _ _ h hd

/-- `process_pending_deposits` with `apply_pending_deposit` keeps `ExitDelay`. -/
theorem process_pending_deposits_exitDelay (p : Preset) (o : Oracle)
    (total_active_balance : Gwei) (s s' : BeaconState)
    (h : process_pending_deposits p total_active_balance (apply_pending_deposit p o) s = .ok s')
    (hd : ExitDelay p s) : ExitDelay p s' := by
  unfold process_pending_deposits at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨⟨_, _, _, _, st⟩, hr, h⟩ := specM_bind_ok h
  have hP := SlashingsSanity.forIn_invariant
    (fun r : MProd (List PendingDeposit) (MProd Bool (MProd Uint64 (MProd Uint64 BeaconState))) =>
      ExitDelay p r.2.2.2.2) _ ?_ _ _ _ hd hr
  · simp only [bind, Except.bind, pure, Except.pure] at h
    repeat' split at h
    all_goals first
      | contradiction
      | (cases h; exact hP)
  · intro _ ⟨_, _, _, _, _⟩ r hst hb
    simp only [bind, Except.bind, pure, Except.pure] at hb
    repeat' split at hb
    all_goals first
      | contradiction
      | (cases hb; exact hst)
      | (cases hb
         exact apply_pending_deposit_exitDelay p o _ _ _
           ‹apply_pending_deposit _ _ _ _ = _› hst)

/-- `process_effective_balance_updates` keeps `ExitDelay`. It writes only effective
balances. -/
theorem process_effective_balance_updates_exitDelay (p : Preset) (s s' : BeaconState)
    (h : process_effective_balance_updates p s = .ok s') (hd : ExitDelay p s) :
    ExitDelay p s' := by
  rw [process_effective_balance_updates_eq] at h
  cases hm : s.validators.zipIdx.mapM (effectiveBalanceStep p s.balances) with
  | error e => simp [hm, Except.map] at h
  | ok validators =>
    simp only [hm, Except.map, Except.ok.injEq] at h
    subst h
    obtain ⟨hlen, hget⟩ := mapM_ok_getElem _ _ _ hm
    rw [List.length_zipIdx] at hlen
    intro u hu
    obtain ⟨i, hi, rfl⟩ := List.getElem_of_mem hu
    have hv : i < s.validators.length := hlen ▸ hi
    have hs := hget i (by simpa using hv) hi
    rw [List.getElem_zipIdx] at hs
    have hkeep : validators[i].exit_epoch = s.validators[i].exit_epoch ∧
        validators[i].withdrawable_epoch = s.validators[i].withdrawable_epoch := by
      generalize validators[i] = w at hs ⊢
      unfold effectiveBalanceStep at hs
      simp only [bind, Except.bind, pure, Except.pure] at hs
      repeat' split at hs
      all_goals first
        | contradiction
        | (cases hs; exact ⟨rfl, rfl⟩)
    exact (hd _ (List.getElem_mem hv)).mono hkeep.1 (Nat.le_of_eq hkeep.2.symm)

/-- `process_epoch` keeps `ExitDelay`. -/
theorem process_epoch_exitDelay (p : Preset) (o : Oracle) (s s' : BeaconState)
    (h : process_epoch p o s = .ok s') (hd : ExitDelay p s) : ExitDelay p s' := by
  unfold process_epoch at h
  obtain ⟨s1, h1, h⟩ := specM_bind_ok h
  obtain ⟨s2, h2, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨s3, h3, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨s4, h4, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨s5, h5, h⟩ := specM_bind_ok h
  obtain ⟨s6, h6, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨s7, h7, h⟩ := specM_bind_ok h
  obtain ⟨s8, h8, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨s9, h9, h⟩ := specM_bind_ok h
  obtain ⟨s10, h10, h⟩ := specM_bind_ok h
  obtain ⟨s11, h11, h⟩ := specM_bind_ok h
  obtain ⟨s12, h12, h⟩ := specM_bind_ok h
  obtain ⟨s13, h13, h⟩ := specM_bind_ok h
  obtain ⟨s14, h14, h⟩ := specM_bind_ok h
  obtain ⟨s15, h15, h⟩ := specM_bind_ok h
  have d1 := ExitDelay.of_validators_eq
    (process_justification_and_finalization_frame p s s1 h1).1.1 hd
  have d2 := ExitDelay.of_validators_eq (process_inactivity_updates_validators p _ _ h2) d1
  have d3 := ExitDelay.of_validators_eq (process_rewards_and_penalties_validators p _ _ _ h3) d2
  have d4 := process_registry_updates_exitDelay p _ _ _ h4 d3
  have d5 := ExitDelay.of_validators_eq (process_slashings_validators p _ _ _ h5) d4
  have d6 := ExitDelay.of_validators_eq (process_eth1_data_reset_validators p _ _ h6) d5
  have d7 := process_pending_deposits_exitDelay p o _ _ _ h7 d6
  have d8 := ExitDelay.of_validators_eq (process_pending_consolidations_validators p _ _ h8) d7
  have d9 := ExitDelay.of_validators_eq
    (process_builder_pending_payments_validators p _ _ _ h9) d8
  have d10 := process_effective_balance_updates_exitDelay p _ _ h10 d9
  have d11 := ExitDelay.of_validators_eq (process_slashings_reset_frame p _ _ h11).1.1 d10
  have d12 := ExitDelay.of_validators_eq (process_randao_mixes_reset_frame p _ _ h12).1.1 d11
  have d13 := ExitDelay.of_validators_eq
    (process_historical_summaries_update_frame p o _ _ h13).1.1 d12
  have d14 := ExitDelay.of_validators_eq
    (process_sync_committee_updates_frame p o _ _ h14).1.1 d13
  have d15 := ExitDelay.of_validators_eq (process_proposer_lookahead_frame p o _ _ h15).1.1 d14
  exact ExitDelay.of_validators_eq (process_ptc_window_frame p o _ _ h).1.1 d15

/-- `process_slots` with the real `process_epoch` keeps `ExitDelay`. -/
theorem process_slots_exitDelay (p : Preset) (o : Oracle) (s s' : BeaconState) (slot : Slot)
    (h : process_slots p o (process_epoch p o) s slot = .ok s') (hd : ExitDelay p s) :
    ExitDelay p s' :=
  process_slots_invariant p o (process_epoch p o) (ExitDelay p)
    (fun _ _ hs hP => ExitDelay.of_validators_eq (process_slot_frame p o _ _ hs).1.1 hP)
    (fun _ _ hs hP => process_epoch_exitDelay p o _ _ hs hP)
    (fun _ _ hP => hP) s s' slot hd h

end EpochProofs.Spec
