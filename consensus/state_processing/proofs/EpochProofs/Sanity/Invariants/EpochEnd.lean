import EpochProofs.Sanity.Driver
import EpochProofs.Sanity.EffectiveBalanceInvariant

/-!
# Effective balances at the end of `process_epoch`

`EBOk` says that each effective balance is a multiple of the increment and at most
`MAX_EFFECTIVE_BALANCE_ELECTRA`. The epoch steps before the effective balance updates keep
`EBOk`, and new validators from deposits satisfy it. The updates then give
`EffectiveBalanceFloor`, and the later steps keep validators and balances. So on mainnet,
`process_epoch` turns `EBOk` into `EBOk` and `EffectiveBalanceFloor`.
-/

namespace EpochProofs.Spec

/-- The effective balance is a multiple of the increment and at most
`MAX_EFFECTIVE_BALANCE_ELECTRA`. -/
def EBOkV (p : Preset) (v : Validator) : Prop :=
  v.effective_balance % p.EFFECTIVE_BALANCE_INCREMENT = 0 ∧
    v.effective_balance ≤ p.MAX_EFFECTIVE_BALANCE_ELECTRA

/-- Each validator satisfies `EBOkV`. -/
def EBOk (p : Preset) (s : BeaconState) : Prop :=
  ∀ v ∈ s.validators, EBOkV p v

/-- The preset facts that the proofs use. Mainnet holds them. -/
def EBPresetOk (p : Preset) : Prop :=
  PresetIncrementOk p ∧ p.MIN_ACTIVATION_BALANCE ≤ p.MAX_EFFECTIVE_BALANCE_ELECTRA

/-- Mainnet holds `EBPresetOk`. -/
theorem ebPresetOk_mainnet : EBPresetOk Preset.mainnet :=
  ⟨presetIncrementOk_mainnet, by decide⟩

/-- `EBOk` reads only `validators`. -/
theorem EBOk.of_validators {p : Preset} {s s' : BeaconState} (h : EBOk p s)
    (hv : s'.validators = s.validators) : EBOk p s' := by
  intro v hv'
  rw [hv] at hv'
  exact h v hv'

/-- A successful `listGet` returns a member of the list. -/
private theorem listGet_mem {α : Type} {l : List α} {i : Nat} {a : α}
    (h : listGet l i = .ok a) : a ∈ l := by
  unfold listGet at h
  split at h
  · cases h
    exact List.mem_of_getElem? ‹_›
  · cases h

/-- A successful `listSet` keeps a property of all members when the new item has it. -/
private theorem listSet_forall {α : Type} {Q : α → Prop} {l l' : List α} {i : Nat} {a : α}
    (hl : ∀ w ∈ l, Q w) (ha : Q a) (h : listSet l i a = .ok l') : ∀ w ∈ l', Q w := by
  unfold listSet at h
  split at h
  · cases h
    intro w hw
    rcases List.mem_or_eq_of_mem_set hw with hw | rfl
    · exact hl w hw
    · exact ha
  · cases h

/-- A successful `set_or_append_list` keeps a property of all members when the new item has
it. -/
private theorem set_or_append_list_forall {α : Type} {Q : α → Prop} {l l' : List α} {i : Nat}
    {a : α} (hl : ∀ w ∈ l, Q w) (ha : Q a) (h : set_or_append_list l i a = .ok l') :
    ∀ w ∈ l', Q w := by
  unfold set_or_append_list at h
  split at h
  · cases h
    intro w hw
    rcases List.mem_append.mp hw with hw | hw
    · exact hl w hw
    · rw [List.mem_singleton.mp hw]; exact ha
  · exact listSet_forall hl ha h

/-- A `for` loop in `SpecM` keeps every property that each successful pass keeps. -/
private theorem forIn_ok_invariant {α β : Type} (P : β → Prop)
    (f : α → β → SpecM (ForInStep β)) (hf : ∀ a b r, P b → f a b = .ok r → P r.value) :
    ∀ (l : List α) (b b' : β), P b → forIn l b f = .ok b' → P b' := by
  intro l
  induction l with
  | nil => intro b b' hb h; cases h; exact hb
  | cons a as ih =>
    intro b b' hb h
    rw [List.forIn_cons] at h
    obtain ⟨r, hr, h⟩ := specM_bind_ok h
    have := hf a b r hb hr
    cases r with
    | done c => cases h; exact this
    | yield c => exact ih c b' this h

/-- Splits the binds, `if`s and `match`es of a step that does not write `validators`, and
closes each success branch by `rfl`. -/
local macro "vs_tac" h:ident : tactic => `(tactic| (
  simp only [bind, Except.bind, pure, Except.pure] at $h:ident
  repeat' split at $h:ident
  all_goals first
    | contradiction
    | (cases $h:ident; rfl)))

/-! ## Steps that keep `validators` -/

/-- `process_inactivity_updates` keeps `validators`. -/
theorem process_inactivity_updates_validators (p : Preset) (s s' : BeaconState)
    (h : process_inactivity_updates p s = .ok s') : s'.validators = s.validators := by
  unfold process_inactivity_updates at h
  vs_tac h

/-- `process_rewards_and_penalties` keeps `validators`. -/
theorem process_rewards_and_penalties_validators (p : Preset) (total_active_balance : Gwei)
    (s s' : BeaconState) (h : process_rewards_and_penalties p total_active_balance s = .ok s') :
    s'.validators = s.validators := by
  unfold process_rewards_and_penalties at h
  vs_tac h

/-- `process_slashings` keeps `validators`. -/
theorem process_slashings_validators (p : Preset) (total_active_balance : Gwei)
    (s s' : BeaconState) (h : process_slashings p total_active_balance s = .ok s') :
    s'.validators = s.validators := by
  unfold process_slashings at h
  vs_tac h

/-- `process_builder_pending_payments` keeps `validators`. -/
theorem process_builder_pending_payments_validators (p : Preset) (total_active_balance : Gwei)
    (s s' : BeaconState) (h : process_builder_pending_payments p total_active_balance s = .ok s') :
    s'.validators = s.validators := by
  unfold process_builder_pending_payments at h
  vs_tac h

/-- `process_pending_consolidations` keeps `validators`. -/
theorem process_pending_consolidations_validators (p : Preset) (s s' : BeaconState)
    (h : process_pending_consolidations p s = .ok s') : s'.validators = s.validators := by
  unfold process_pending_consolidations at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨⟨_, st⟩, hr, h⟩ := specM_bind_ok h
  have hP := forIn_ok_invariant (fun r : MProd Nat BeaconState => r.snd.validators = s.validators)
    _ ?_ _ _ _ rfl hr
  · cases h
    exact hP
  · intro _ ⟨_, _⟩ r hst hb
    simp only [bind, Except.bind, pure, Except.pure] at hb
    repeat' split at hb
    all_goals first
      | contradiction
      | (cases hb; exact hst)

/-! ## Steps that keep `EBOk` -/

/-- `compute_exit_epoch_and_update_churn` keeps `validators`. -/
theorem compute_exit_epoch_and_update_churn_validators (p : Preset)
    (total_active_balance : Gwei) (s : BeaconState) (exit_balance : Gwei)
    (r : Epoch × BeaconState)
    (h : compute_exit_epoch_and_update_churn p total_active_balance s exit_balance = .ok r) :
    r.2.validators = s.validators := by
  unfold compute_exit_epoch_and_update_churn at h
  vs_tac h

/-- `initiate_validator_exit` keeps `EBOk`. -/
theorem initiate_validator_exit_ebOk (p : Preset) (total_active_balance : Gwei)
    (s s' : BeaconState) (index : ValidatorIndex)
    (h : initiate_validator_exit p total_active_balance s index = .ok s') (hs : EBOk p s) :
    EBOk p s' := by
  unfold initiate_validator_exit at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact hs)
    | (cases h
       have hr := compute_exit_epoch_and_update_churn_validators p total_active_balance s _ _
         ‹compute_exit_epoch_and_update_churn _ _ _ _ = _›
       have hv := listGet_mem ‹listGet s.validators index = _›
       have hset := ‹listSet _ _ _ = _›
       refine listSet_forall (Q := EBOkV p) ?_ ?_ hset
       · intro w hw
         rw [hr] at hw
         exact hs w hw
       · have hw := hs _ hv
         exact ⟨hw.1, hw.2⟩)

/-- `process_registry_updates` keeps `EBOk`. It writes only epochs of validators. -/
theorem process_registry_updates_ebOk (p : Preset) (total_active_balance : Gwei)
    (s s' : BeaconState) (h : process_registry_updates p total_active_balance s = .ok s')
    (hs : EBOk p s) : EBOk p s' := by
  unfold process_registry_updates at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨r, hr, h⟩ := specM_bind_ok h
  cases h
  refine forIn_ok_invariant (EBOk p) _ ?_ _ s _ hs hr
  intro index st r hst hb
  simp only [bind, Except.bind, pure, Except.pure] at hb
  repeat' split at hb
  all_goals first
    | contradiction
    | (cases hb; exact hst)
    | (cases hb
       exact initiate_validator_exit_ebOk p total_active_balance _ _ _
         ‹initiate_validator_exit _ _ _ _ = _› hst)
    | (cases hb
       have hset := ‹listSet _ _ _ = _›
       have hm := listGet_mem ‹listGet _ _ = _›
       refine listSet_forall (Q := EBOkV p) hst ?_ hset
       have hw := hst _ hm
       exact ⟨hw.1, hw.2⟩)

/-- The rounded amount capped by a multiple of the increment is a multiple of the increment. -/
private theorem min_round_mod (amount increment m : Nat) (hm : m % increment = 0) :
    min (amount - amount % increment) m % increment = 0 := by
  rcases Nat.le_total (amount - amount % increment) m with hle | hle
  · rw [Nat.min_eq_left hle]
    exact Nat.sub_mod_eq_zero_of_mod_eq (Nat.mod_mod _ _).symm
  · rw [Nat.min_eq_right hle]
    exact hm

/-- A new validator from a deposit satisfies the `EBOk` bounds. -/
theorem get_validator_from_deposit_ebOk (p : Preset) (hp : EBPresetOk p) (pubkey : BLSPubkey)
    (withdrawal_credentials : Bytes32) (amount : Gwei) (v : Validator)
    (h : get_validator_from_deposit p pubkey withdrawal_credentials amount = .ok v) :
    v.effective_balance % p.EFFECTIVE_BALANCE_INCREMENT = 0 ∧
      v.effective_balance ≤ p.MAX_EFFECTIVE_BALANCE_ELECTRA := by
  unfold get_validator_from_deposit at h
  simp only [uint64Mod, uint64Sub, bind, Except.bind, pure, Except.pure, throw, throwThe,
    MonadExceptOf.throw] at h
  by_cases h0 : p.EFFECTIVE_BALANCE_INCREMENT = 0
  · simp [h0] at h
  · simp only [h0, if_false, Nat.mod_le, if_true, Except.ok.injEq] at h
    subst h
    have hmax : ∀ w : Validator,
        get_max_effective_balance p w ≤ p.MAX_EFFECTIVE_BALANCE_ELECTRA := by
      intro w
      unfold get_max_effective_balance
      split
      · exact Nat.le_refl _
      · exact hp.2
    exact ⟨min_round_mod _ _ _ (get_max_effective_balance_mod p hp.1 _),
      Nat.le_trans (Nat.min_le_right _ _) (hmax _)⟩

/-- `add_validator_to_registry` keeps `EBOk`. -/
theorem add_validator_to_registry_ebOk (p : Preset) (hp : EBPresetOk p) (s s' : BeaconState)
    (pubkey : BLSPubkey) (withdrawal_credentials : Bytes32) (amount : Gwei)
    (h : add_validator_to_registry p s pubkey withdrawal_credentials amount = .ok s')
    (hs : EBOk p s) : EBOk p s' := by
  unfold add_validator_to_registry at h
  obtain ⟨v, hv, h⟩ := specM_bind_ok h
  obtain ⟨l, hl, h⟩ := specM_bind_ok h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h
       exact set_or_append_list_forall (Q := EBOkV p) hs
         (get_validator_from_deposit_ebOk p hp _ _ _ v hv) hl)

/-- `apply_pending_deposit` keeps `EBOk`. -/
theorem apply_pending_deposit_ebOk (p : Preset) (o : Oracle) (hp : EBPresetOk p)
    (s s' : BeaconState) (deposit : PendingDeposit)
    (h : apply_pending_deposit p o s deposit = .ok s') (hs : EBOk p s) : EBOk p s' := by
  unfold apply_pending_deposit at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact hs)
    | exact add_validator_to_registry_ebOk p hp s s' _ _ _ h hs

/-- `process_pending_deposits` with `apply_pending_deposit` keeps `EBOk`. -/
theorem process_pending_deposits_ebOk (p : Preset) (o : Oracle) (hp : EBPresetOk p)
    (total_active_balance : Gwei) (s s' : BeaconState)
    (h : process_pending_deposits p total_active_balance (apply_pending_deposit p o) s = .ok s')
    (hs : EBOk p s) : EBOk p s' := by
  unfold process_pending_deposits at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨⟨_, _, _, _, st⟩, hr, h⟩ := specM_bind_ok h
  have hP := forIn_ok_invariant
    (fun r : MProd (List PendingDeposit) (MProd Bool (MProd Uint64 (MProd Uint64 BeaconState))) =>
      EBOk p r.2.2.2.2) _ ?_ _ _ _ hs hr
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
         exact apply_pending_deposit_ebOk p o hp _ _ _ ‹apply_pending_deposit _ _ _ _ = _› hst)

/-! ## The effective balance updates -/

/-- After `process_effective_balance_updates`, each effective balance is at most
`MAX_EFFECTIVE_BALANCE_ELECTRA`, if it was before. -/
theorem process_effective_balance_updates_le_max (p : Preset) (hp : EBPresetOk p)
    (downward upward : Uint64) (hthresholds : hysteresisThresholds p = .ok (downward, upward))
    (s s' : BeaconState) (h : process_effective_balance_updates p s = .ok s')
    (hle : ∀ v ∈ s.validators, v.effective_balance ≤ p.MAX_EFFECTIVE_BALANCE_ELECTRA) :
    ∀ v ∈ s'.validators, v.effective_balance ≤ p.MAX_EFFECTIVE_BALANCE_ELECTRA := by
  rw [process_effective_balance_updates_eq] at h
  cases hm : s.validators.zipIdx.mapM (effectiveBalanceStep p s.balances) with
  | error e => simp [hm, Except.map] at h
  | ok validators =>
    simp only [hm, Except.map, Except.ok.injEq] at h
    subst h
    obtain ⟨hlen, hget⟩ := mapM_ok_getElem _ _ _ hm
    rw [List.length_zipIdx] at hlen
    intro w hw
    obtain ⟨i, hi, rfl⟩ := List.getElem_of_mem hw
    have hv : i < s.validators.length := hlen ▸ hi
    have hs := hget i (by simpa using hv) hi
    rw [List.getElem_zipIdx] at hs
    obtain ⟨balance, -, hn⟩ :=
      effectiveBalanceStep_ok p s.balances downward upward hthresholds _ _ hs
    by_cases heq : validators[i].effective_balance = s.validators[i].effective_balance
    · rw [heq]
      exact hle _ (List.getElem_mem hv)
    · refine Nat.le_trans (newEffectiveBalance_le_max p downward upward _ balance _ hn heq) ?_
      unfold get_max_effective_balance
      split
      · exact Nat.le_refl _
      · exact hp.2

/-! ## `process_epoch` -/

/-- On mainnet, `process_epoch` keeps `EBOk` and ends with `EffectiveBalanceFloor`: each
validator has a balance, and each effective balance is a multiple of the increment, at most
`MAX_EFFECTIVE_BALANCE_ELECTRA` and at most `4/3` of the balance. -/
theorem process_epoch_effective_balances (o : Oracle) (s s' : BeaconState)
    (h : process_epoch Preset.mainnet o s = .ok s') (hs : EBOk Preset.mainnet s) :
    EBOk Preset.mainnet s' ∧ EffectiveBalanceFloor Preset.mainnet s' := by
  have hp := ebPresetOk_mainnet
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
  have e1 : EBOk _ s1 :=
    hs.of_validators (process_justification_and_finalization_frame _ _ _ h1).1.1
  have e2 : EBOk _ s2 := e1.of_validators (process_inactivity_updates_validators _ _ _ h2)
  have e3 : EBOk _ s3 := e2.of_validators (process_rewards_and_penalties_validators _ _ _ _ h3)
  have e4 : EBOk _ s4 := process_registry_updates_ebOk _ _ _ _ h4 e3
  have e5 : EBOk _ s5 := e4.of_validators (process_slashings_validators _ _ _ _ h5)
  have e6 : EBOk _ s6 := e5.of_validators (process_eth1_data_reset_frame _ _ _ h6).1.1
  have e7 : EBOk _ s7 := process_pending_deposits_ebOk _ o hp _ _ _ h7 e6
  have e8 : EBOk _ s8 := e7.of_validators (process_pending_consolidations_validators _ _ _ h8)
  have e9 : EBOk _ s9 :=
    e8.of_validators (process_builder_pending_payments_validators _ _ _ _ h9)
  have hfloor : EffectiveBalanceFloor _ s10 :=
    process_effective_balance_updates_floor_mainnet s9 s10 h10 fun v hv => (e9 v hv).1
  have hmax := process_effective_balance_updates_le_max _ hp _ _ hysteresisThresholds_mainnet
    s9 s10 h10 fun v hv => (e9 v hv).2
  have e10 : EBOk _ s10 := fun v hv => by
    obtain ⟨i, hi, rfl⟩ := List.getElem_of_mem hv
    exact ⟨(hfloor.2 i hi).1, hmax _ hv⟩
  have f : RegistryFrame s10 s' :=
    (process_slashings_reset_frame _ _ _ h11).1.trans
    ((process_randao_mixes_reset_frame _ _ _ h12).1.trans
    ((process_historical_summaries_update_frame _ o _ _ h13).1.trans
    ((process_participation_flag_updates_frame _).1.trans
    ((process_sync_committee_updates_frame _ o _ _ h14).1.trans
    ((process_proposer_lookahead_frame _ o _ _ h15).1.trans
    (process_ptc_window_frame _ o _ _ h).1)))))
  refine ⟨e10.of_validators f.1, ?_⟩
  obtain ⟨hvs, hbs, -⟩ := f
  unfold EffectiveBalanceFloor
  rw [hvs, hbs]
  exact hfloor

/-- On mainnet, after `process_epoch` the balance list is at least as long as the validator
list. -/
theorem process_epoch_balances_length (o : Oracle) (s s' : BeaconState)
    (h : process_epoch Preset.mainnet o s = .ok s') (hs : EBOk Preset.mainnet s) :
    s'.validators.length ≤ s'.balances.length :=
  (process_epoch_effective_balances o s s' h hs).2.1

end EpochProofs.Spec
