import EpochProofs.Spec.Block.Exits
import EpochProofs.Sanity.Effects
import EpochProofs.Sanity.InactivityUpdates
import EpochProofs.Sanity.PendingDeposits

/-!
# Effects of voluntary exits and BLS to execution changes

`initiate_validator_exit` changes one validator by `ExitChange` and the exit churn fields.
A voluntary exit is one such call. A BLS change sets the withdrawal credentials of one
validator. Both keep effective balances, balances, checkpoints and `ExitOrder`.
-/

namespace EpochProofs.Spec

namespace ExitsSanity

/-- A successful `listSet` is `List.set` at an index in range. -/
theorem listSet_ok {α : Type} {l l' : List α} {i : Nat} {a : α} (h : listSet l i a = .ok l') :
    i < l.length ∧ l' = l.set i a := by
  unfold listSet at h
  split at h
  · cases h; exact ⟨‹_›, rfl⟩
  · cases h

/-- `compute_exit_epoch_and_update_churn` writes only the two churn fields. It returns an
epoch after the current epoch. -/
theorem compute_exit_epoch_and_update_churn_ok (p : Preset) (tab : Gwei) (s s' : BeaconState)
    (b e : Nat) (h : compute_exit_epoch_and_update_churn p tab s b = .ok (e, s')) :
    (∃ x y, s' = { s with earliest_exit_epoch := x, exit_balance_to_consume := y }) ∧
      ∃ cur, get_current_epoch p s = .ok cur ∧ cur < e := by
  unfold compute_exit_epoch_and_update_churn at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  cases hc : get_current_epoch p s with
  | error _ => simp [hc] at h
  | ok cur =>
  simp only [hc] at h
  cases ha : compute_activation_exit_epoch p cur with
  | error _ => simp [ha] at h
  | ok a =>
  simp only [ha] at h
  have ha' := compute_activation_exit_epoch_ok p cur a ha
  cases hl : get_exit_churn_limit p tab with
  | error _ => simp [hl] at h
  | ok c =>
  simp only [hl] at h
  have hmax : cur < max s.earliest_exit_epoch a := by
    rw [ha']
    exact Nat.lt_of_lt_of_le (Nat.lt_of_lt_of_le (Nat.lt_succ_self cur) (Nat.le_add_right _ _))
      (Nat.le_max_right _ _)
  generalize (if s.earliest_exit_epoch < max s.earliest_exit_epoch a then c
    else s.exit_balance_to_consume) = bc at h
  split at h
  · repeat' (split at h)
    all_goals first | (cases h; done) | skip
    rename_i hadd _ _ _ _ _ _ _ _ _
    cases h
    refine ⟨⟨_, _, rfl⟩, cur, rfl, ?_⟩
    rw [uint64Add_ok hadd]
    exact Nat.lt_of_lt_of_le hmax (Nat.le_add_right _ _)
  · split at h
    · cases h
    · cases h
      exact ⟨⟨_, _, rfl⟩, cur, rfl, hmax⟩

end ExitsSanity

open ExitsSanity

/-- How `initiate_validator_exit` can change the validator `v` of state `s`. Either it keeps
`v`, or `v` had no exit and gets an exit epoch after the current epoch. -/
def ExitChange (p : Preset) (s : BeaconState) (v v' : Validator) : Prop :=
  v' = v ∨ (v.exit_epoch = FAR_FUTURE_EPOCH ∧ ∃ cur e, get_current_epoch p s = .ok cur ∧
    cur < e ∧
    v' = { v with
      exit_epoch := e
      withdrawable_epoch := e + p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY })

/-- `initiate_validator_exit` changes only the validator at `index` and the exit churn. -/
theorem initiate_validator_exit_ok (p : Preset) (tab : Gwei) (s s' : BeaconState)
    (index : ValidatorIndex) (h : initiate_validator_exit p tab s index = .ok s') :
    (∃ vs x y, s' =
      { s with validators := vs, earliest_exit_epoch := x, exit_balance_to_consume := y }) ∧
    s'.validators.length = s.validators.length ∧
    (∀ j, j ≠ index → s'.validators[j]? = s.validators[j]?) ∧
    ∃ v v', s.validators[index]? = some v ∧ s'.validators[index]? = some v' ∧
      ExitChange p s v v' := by
  unfold initiate_validator_exit at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  cases hg : listGet s.validators index with
  | error _ => simp [hg] at h
  | ok v =>
  simp only [hg] at h
  obtain ⟨hlt, hv⟩ := listGet_ok hg
  split at h
  · cases h
    exact ⟨⟨_, _, _, rfl⟩, rfl, fun _ _ => rfl, v, v, hv, hv, .inl rfl⟩
  · rename_i hfar
    have hfar : v.exit_epoch = FAR_FUTURE_EPOCH := by simpa using hfar
    cases hc : compute_exit_epoch_and_update_churn p tab s v.effective_balance with
    | error _ => simp [hc] at h
    | ok r =>
    obtain ⟨e, s1⟩ := r
    simp only [hc] at h
    obtain ⟨⟨x, y, rfl⟩, cur, hcur, hlt'⟩ :=
      compute_exit_epoch_and_update_churn_ok p tab s s1 _ e hc
    cases hw : uint64Add e p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY with
    | error _ => simp [hw] at h
    | ok w =>
    simp only [hw] at h
    rw [listSet_of_lt _ hlt] at h
    cases h
    rw [uint64Add_ok hw]
    refine ⟨⟨_, _, _, rfl⟩, List.length_set .., fun j hj => List.getElem?_set_ne (Ne.symm hj),
      v, _, hv, List.getElem?_set_self hlt, .inr ⟨hfar, cur, e, hcur, hlt', rfl⟩⟩

/-- An exit change keeps every validator field except `exit_epoch` and `withdrawable_epoch`. -/
theorem ExitChange.keeps {p : Preset} {s : BeaconState} {v v' : Validator}
    (h : ExitChange p s v v') :
    v'.pubkey = v.pubkey ∧ v'.withdrawal_credentials = v.withdrawal_credentials ∧
      v'.effective_balance = v.effective_balance ∧ v'.slashed = v.slashed ∧
      v'.activation_eligibility_epoch = v.activation_eligibility_epoch ∧
      v'.activation_epoch = v.activation_epoch := by
  rcases h with rfl | ⟨-, -, -, -, -, rfl⟩ <;> exact ⟨rfl, rfl, rfl, rfl, rfl, rfl⟩

/-- An exit change keeps `exit_epoch ≤ withdrawable_epoch`. -/
theorem ExitChange.exit_le {p : Preset} {s : BeaconState} {v v' : Validator}
    (h : ExitChange p s v v') (hv : v.exit_epoch ≤ v.withdrawable_epoch) :
    v'.exit_epoch ≤ v'.withdrawable_epoch := by
  rcases h with rfl | ⟨-, -, -, -, -, rfl⟩
  · exact hv
  · exact Nat.le_add_right _ _

namespace ExitsSanity

/-- `ExitOrder` passes to `s'` when each validator of `s'` comes from a validator of `s` at the
same index by a step that keeps `exit_epoch ≤ withdrawable_epoch`. -/
theorem exitOrder_of_pointwise {s s' : BeaconState}
    (hpt : ∀ (j : Nat) (v' : Validator), s'.validators[j]? = some v' →
      ∃ v : Validator, s.validators[j]? = some v ∧
      (v.exit_epoch ≤ v.withdrawable_epoch → v'.exit_epoch ≤ v'.withdrawable_epoch))
    (hs : ExitOrder s) : ExitOrder s' := by
  intro v' hm
  obtain ⟨j, hj⟩ := List.mem_iff_getElem?.mp hm
  obtain ⟨v, hv, himp⟩ := hpt j v' hj
  exact himp (hs v (List.mem_iff_getElem?.mpr ⟨j, hv⟩))

end ExitsSanity

/-- The effects of `initiate_validator_exit`. -/
theorem initiate_validator_exit_effects (p : Preset) (tab : Gwei) (s s' : BeaconState)
    (index : ValidatorIndex) (h : initiate_validator_exit p tab s index = .ok s') :
    EBStable s s' ∧ CheckpointsStable s s' ∧ (ExitOrder s → ExitOrder s') ∧
      s'.balances = s.balances := by
  obtain ⟨⟨vs, x, y, rfl⟩, hlen, hother, v, v', hv, hv', hch⟩ :=
    initiate_validator_exit_ok p tab s _ index h
  refine ⟨⟨Nat.le_of_eq hlen.symm, fun j u hu => ?_⟩, ⟨rfl, rfl, rfl, rfl⟩,
    ExitsSanity.exitOrder_of_pointwise fun j u' hu' => ?_, rfl⟩
  · by_cases hj : j = index
    · subst hj
      rw [hv] at hu
      cases hu
      exact ⟨v', hv', hch.keeps.2.2.1⟩
    · exact ⟨u, (hother j hj).trans hu, rfl⟩
  · by_cases hj : j = index
    · subst hj
      rw [hv'] at hu'
      cases hu'
      exact ⟨v, hv, hch.exit_le⟩
    · exact ⟨u', (hother j hj).symm.trans hu', id⟩

/-- A successful voluntary exit is `initiate_validator_exit` on the original state, for an
active validator with no exit. -/
theorem process_voluntary_exit_initiate (p : Preset) (o : Oracle) (s s' : BeaconState)
    (e : SignedVoluntaryExit) (h : process_voluntary_exit p o s e = .ok s') :
    ∃ tab cur v, s.validators[e.message.validator_index]? = some v ∧
      get_current_epoch p s = .ok cur ∧ is_active_validator v cur ∧
      v.exit_epoch = FAR_FUTURE_EPOCH ∧
      initiate_validator_exit p tab s e.message.validator_index = .ok s' := by
  unfold process_voluntary_exit at h
  simp only [bind, Except.bind, pure, Except.pure, throw, throwThe, MonadExceptOf.throw] at h
  cases hg : listGet s.validators e.message.validator_index with
  | error _ => simp [hg] at h
  | ok v =>
  simp only [hg] at h
  cases hc : get_current_epoch p s with
  | error _ => simp [hc] at h
  | ok cur =>
  simp only [hc] at h
  repeat' split at h
  all_goals first | (cases h; done) | skip
  exact ⟨_, cur, v, (listGet_ok hg).2, rfl,
    Decidable.of_not_not ‹¬¬is_active_validator v cur = true›,
    Decidable.of_not_not ‹¬¬v.exit_epoch = FAR_FUTURE_EPOCH›, h⟩

/-- The effects of a voluntary exit. Only the exiting validator changes, by `ExitChange`. -/
theorem process_voluntary_exit_effects (p : Preset) (o : Oracle) (s s' : BeaconState)
    (e : SignedVoluntaryExit) (h : process_voluntary_exit p o s e = .ok s') :
    EBStable s s' ∧ CheckpointsStable s s' ∧ (ExitOrder s → ExitOrder s') ∧ BalancesUp s s' ∧
      (∀ j, j ≠ e.message.validator_index → s'.validators[j]? = s.validators[j]?) ∧
      ∃ v v', s.validators[e.message.validator_index]? = some v ∧
        s'.validators[e.message.validator_index]? = some v' ∧ ExitChange p s v v' := by
  obtain ⟨tab, -, -, -, -, -, -, hi⟩ := process_voluntary_exit_initiate p o s s' e h
  obtain ⟨heb, hcp, hord, hbal⟩ := initiate_validator_exit_effects p tab s s' _ hi
  obtain ⟨-, -, hother, hv⟩ := initiate_validator_exit_ok p tab s s' _ hi
  exact ⟨heb, hcp, hord, ⟨by rw [hbal]; exact Nat.le_refl _,
    fun _ b hb => ⟨b, by rw [hbal]; exact hb, Nat.le_refl _⟩⟩, hother, hv⟩

/-- A successful BLS change sets the withdrawal credentials of one validator that had BLS
credentials. It changes no other field of the state. -/
theorem process_bls_to_execution_change_ok (p : Preset) (o : Oracle) (s s' : BeaconState)
    (c : SignedBLSToExecutionChange) (h : process_bls_to_execution_change p o s c = .ok s') :
    ∃ v wc, s.validators[c.message.validator_index]? = some v ∧
      v.withdrawal_credentials.toList.take 1 = [BLS_WITHDRAWAL_PREFIX] ∧
      c.message.validator_index < s.validators.length ∧
      s' = { s with validators :=
        s.validators.set c.message.validator_index { v with withdrawal_credentials := wc } } := by
  unfold process_bls_to_execution_change at h
  simp only [bind, Except.bind, pure, Except.pure, throw, throwThe, MonadExceptOf.throw] at h
  split at h
  · cases h
  · cases hg : listGet s.validators c.message.validator_index with
    | error _ => simp [hg] at h
    | ok v =>
    simp only [hg] at h
    obtain ⟨hlt, hv⟩ := listGet_ok hg
    repeat' split at h
    all_goals first | (cases h; done) | skip
    rename_i hset
    obtain ⟨-, rfl⟩ := ExitsSanity.listSet_ok hset
    cases h
    exact ⟨v, _, hv, Decidable.of_not_not ‹¬¬_ = [BLS_WITHDRAWAL_PREFIX]›, hlt, rfl⟩

/-- The effects of a BLS change. Only `withdrawal_credentials` of one validator changes. -/
theorem process_bls_to_execution_change_effects (p : Preset) (o : Oracle) (s s' : BeaconState)
    (c : SignedBLSToExecutionChange) (h : process_bls_to_execution_change p o s c = .ok s') :
    EBStable s s' ∧ CheckpointsStable s s' ∧ (ExitOrder s → ExitOrder s') ∧ BalancesUp s s' ∧
      s'.validators.length = s.validators.length ∧
      ∀ (j : Nat) (v : Validator), s.validators[j]? = some v → ∃ wc,
        s'.validators[j]? = some { v with withdrawal_credentials := wc } := by
  obtain ⟨v, wc, hv, -, hlt, rfl⟩ := process_bls_to_execution_change_ok p o s s' c h
  have hpt : ∀ (j : Nat) (u : Validator), s.validators[j]? = some u → ∃ wc',
      (s.validators.set c.message.validator_index { v with withdrawal_credentials := wc })[j]? =
        some { u with withdrawal_credentials := wc' } := by
    intro j u hu
    by_cases hj : c.message.validator_index = j
    · subst hj
      rw [hv] at hu
      cases hu
      exact ⟨wc, List.getElem?_set_self hlt⟩
    · exact ⟨u.withdrawal_credentials, by rw [List.getElem?_set_ne hj, hu]⟩
  refine ⟨⟨by simp, fun j u hu => ?_⟩, ⟨rfl, rfl, rfl, rfl⟩,
    ExitsSanity.exitOrder_of_pointwise fun j u' hu' => ?_,
    ⟨Nat.le_refl _, fun _ b hb => ⟨b, hb, Nat.le_refl _⟩⟩, List.length_set .., hpt⟩
  · obtain ⟨wc', h'⟩ := hpt j u hu
    exact ⟨_, h', rfl⟩
  · have hj : j < s.validators.length := by
      simpa using (List.getElem?_eq_some_iff.mp hu').1
    obtain ⟨wc', h'⟩ := hpt j _ (List.getElem?_eq_getElem hj)
    rw [hu'] at h'
    cases h'
    exact ⟨_, List.getElem?_eq_getElem hj, id⟩
