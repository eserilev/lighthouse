import EpochProofs.Spec.Block.Slashings
import EpochProofs.Sanity.Block.Exits

/-!
# Effects of proposer and attester slashings

`slash_validator` changes one validator by `SlashChange`. Its balance drops by at most the
slashing penalty, and no other balance drops. `SlashEffect` and `SlashFields` compose these facts
over the loop of an attester slashing.
-/

namespace EpochProofs.Spec

open ExitsSanity

namespace SlashingsSanity

/-- A successful `uint64Div` is `Nat` division. -/
theorem uint64Div_ok {a b c : Nat} (h : uint64Div a b = .ok c) : c = a / b := by
  unfold uint64Div at h
  split at h
  · cases h
  · cases h; rfl

/-- `saturating_sub b d` is at least `b - d`. -/
theorem le_saturating_sub_add (b d : Nat) : b ≤ saturating_sub b d + d := by
  unfold saturating_sub
  split
  · rename_i hbd
    exact Nat.le_of_eq (Nat.sub_add_cancel (Nat.le_of_lt hbd)).symm
  · rename_i hbd
    rw [Nat.sub_self, Nat.zero_add]
    exact Nat.le_of_not_lt hbd

/-- `decrease_balance` lowers the balance at `k` by at most `d` and keeps the others. -/
theorem decrease_balance_ok {bs bs' : List Gwei} {k : ValidatorIndex} {d : Gwei}
    (h : decrease_balance bs k d = .ok bs') :
    bs'.length = bs.length ∧ ∀ (j : Nat) (b : Gwei), bs[j]? = some b →
      ∃ b' : Gwei, bs'[j]? = some b' ∧ (j ≠ k → b' = b) ∧ b ≤ b' + d := by
  unfold decrease_balance at h
  obtain ⟨c, hc, h⟩ := specM_bind_ok h
  obtain ⟨hlt, hc⟩ := listGet_ok hc
  rw [listSet_of_lt _ hlt] at h
  cases h
  refine ⟨List.length_set .., fun j b hb => ?_⟩
  by_cases hj : k = j
  · subst hj
    rw [hc] at hb
    cases hb
    exact ⟨_, List.getElem?_set_self hlt, fun hne => absurd rfl hne, le_saturating_sub_add _ _⟩
  · exact ⟨b, by rw [List.getElem?_set_ne hj, hb], fun _ => rfl, Nat.le_add_right _ _⟩

/-- `increase_balance` lowers no balance. -/
theorem increase_balance_ok {bs bs' : List Gwei} {k : ValidatorIndex} {d : Gwei}
    (h : increase_balance bs k d = .ok bs') :
    bs'.length = bs.length ∧ ∀ (j : Nat) (b : Gwei), bs[j]? = some b →
      ∃ b' : Gwei, bs'[j]? = some b' ∧ b ≤ b' := by
  unfold increase_balance at h
  obtain ⟨c, hc, h⟩ := specM_bind_ok h
  obtain ⟨e, he, h⟩ := specM_bind_ok h
  obtain ⟨hlt, hc⟩ := listGet_ok hc
  rw [listSet_of_lt _ hlt] at h
  cases h
  refine ⟨List.length_set .., fun j b hb => ?_⟩
  by_cases hj : k = j
  · subst hj
    rw [hc] at hb
    cases hb
    rw [uint64Add_ok he]
    exact ⟨_, List.getElem?_set_self hlt, Nat.le_add_right _ _⟩
  · exact ⟨b, by rw [List.getElem?_set_ne hj, hb], Nat.le_refl _⟩

end SlashingsSanity

open SlashingsSanity

/-- How `slash_validator` changes the slashed validator `v` of state `s`: first an exit change
to `v1`, then `slashed` is set and `withdrawable_epoch` is raised. -/
def SlashChange (p : Preset) (s : BeaconState) (v v' : Validator) : Prop :=
  ∃ v1 cur, ExitChange p s v v1 ∧ get_current_epoch p s = .ok cur ∧
    v' = { v1 with
      slashed := true
      withdrawable_epoch := max v1.withdrawable_epoch (cur + p.EPOCHS_PER_SLASHINGS_VECTOR) }

/-- `initiate_validator_exit` returns the state unchanged for a validator that already exits.
It does not read the total active balance on this path. -/
theorem initiate_validator_exit_of_exiting (p : Preset) (tab : Gwei) (s : BeaconState)
    (i : ValidatorIndex) (v : Validator) (hv : listGet s.validators i = .ok v)
    (hx : v.exit_epoch ≠ FAR_FUTURE_EPOCH) : initiate_validator_exit p tab s i = .ok s := by
  unfold initiate_validator_exit
  simp only [bind, Except.bind, hv, pure, Except.pure]
  rw [if_pos (by simpa using hx)]

/-- The exit step of `slash_validator` equals `initiate_validator_exit` with the total active
balance, whenever the total exists. -/
theorem slash_validator_initiate_eq (p : Preset) (s : BeaconState) (i : ValidatorIndex)
    (tab : Gwei) (htab : get_total_active_balance p s = .ok tab) :
    (do
      let validator ← listGet s.validators i
      if validator.exit_epoch != FAR_FUTURE_EPOCH then pure s
      else initiate_validator_exit p (← get_total_active_balance p s) s i : SpecM BeaconState) =
      initiate_validator_exit p tab s i := by
  cases hv : listGet s.validators i with
  | error e =>
    unfold initiate_validator_exit
    simp only [bind, Except.bind, hv]
  | ok v =>
    simp only [bind, Except.bind, htab]
    by_cases hx : v.exit_epoch = FAR_FUTURE_EPOCH
    · simp only [hx, bne_self_eq_false, Bool.false_eq_true, if_false]
    · rw [if_pos (by simpa using hx), initiate_validator_exit_of_exiting p tab s i v hv hx]
      rfl

namespace SlashingsSanity

/-- Lean moves the rest of a `do` block into both branches of an `if`. This splits it back. -/
theorem bind_if_ok {c : Bool} {s r : BeaconState} {x : SpecM Gwei}
    {f : Gwei → SpecM BeaconState} {k : BeaconState → SpecM BeaconState}
    (h : (if c = true then pure s >>= k else x >>= fun t => f t >>= k) = .ok r) :
    ∃ s1, (if c = true then pure s else x >>= f) = .ok s1 ∧ k s1 = .ok r := by
  by_cases hc : c = true
  · rw [if_pos hc] at h
    exact ⟨s, by rw [if_pos hc]; rfl, h⟩
  · rw [if_neg hc] at h
    obtain ⟨t, ht, h⟩ := specM_bind_ok h
    obtain ⟨s1, hs1, h⟩ := specM_bind_ok h
    refine ⟨s1, ?_, h⟩
    rw [if_neg hc]
    simp only [bind, Except.bind, ht]
    exact hs1

end SlashingsSanity

/-- On success, the exit step of `slash_validator` is `initiate_validator_exit` for some total. -/
theorem slash_validator_initiate_ok (p : Preset) (s s1 : BeaconState) (i : ValidatorIndex)
    (v : Validator) (hv : listGet s.validators i = .ok v)
    (h : (if (v.exit_epoch != FAR_FUTURE_EPOCH) = true then pure s
      else get_total_active_balance p s >>= fun t => initiate_validator_exit p t s i) =
      (.ok s1 : SpecM BeaconState)) :
    ∃ tab, initiate_validator_exit p tab s i = .ok s1 := by
  by_cases hx : v.exit_epoch = FAR_FUTURE_EPOCH
  · simp only [hx, bne_self_eq_false, Bool.false_eq_true, if_false] at h
    obtain ⟨tab, -, h⟩ := specM_bind_ok h
    exact ⟨tab, h⟩
  · rw [if_pos (by simpa using hx)] at h
    cases h
    exact ⟨0, initiate_validator_exit_of_exiting p 0 s i v hv hx⟩

/-- `slash_validator` changes only the validator at `i`, by `SlashChange`. The balance at `i`
drops by at most the slashing penalty. No other balance drops. -/
theorem slash_validator_ok (p : Preset) (s s' : BeaconState) (i : ValidatorIndex)
    (w : Option ValidatorIndex) (h : slash_validator p s i w = .ok s') :
    CheckpointsStable s s' ∧
    s'.validators.length = s.validators.length ∧
    (∀ j, j ≠ i → s'.validators[j]? = s.validators[j]?) ∧
    (∃ v v', s.validators[i]? = some v ∧ s'.validators[i]? = some v' ∧ SlashChange p s v v') ∧
    s'.balances.length = s.balances.length ∧
    ∀ (j : Nat) (b : Gwei), s.balances[j]? = some b → ∃ b' : Gwei, s'.balances[j]? = some b' ∧
      (j ≠ i → b ≤ b') ∧ ∀ v : Validator, s.validators[j]? = some v →
        b ≤ b' + v.effective_balance / p.MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA := by
  unfold slash_validator at h
  obtain ⟨epoch, hc, h⟩ := specM_bind_ok h
  obtain ⟨v0, hv0, h⟩ := specM_bind_ok h
  obtain ⟨s1, hs1, h⟩ := SlashingsSanity.bind_if_ok h
  obtain ⟨tab, hs1⟩ := slash_validator_initiate_ok p s s1 i v0 hv0 hs1
  obtain ⟨v1, hv1, h⟩ := specM_bind_ok h
  obtain ⟨we, hwe, h⟩ := specM_bind_ok h
  obtain ⟨vs2, hvs2, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨pen, hpen, h⟩ := specM_bind_ok h
  obtain ⟨bs1, hbs1, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨bs2, hbs2, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨bs3, hbs3, h⟩ := specM_bind_ok h
  simp only [pure, Except.pure, Except.ok.injEq] at h
  subst h
  dsimp only at hvs2 hpen hbs1 hbs2 hbs3 ⊢
  obtain ⟨⟨vs, x, y, rfl⟩, hlen, hother, v, v1', hv, hv1', hch⟩ :=
    initiate_validator_exit_ok p tab s _ i hs1
  obtain ⟨hlt, hv1⟩ := listGet_ok hv1
  rw [hv1'] at hv1
  cases hv1
  obtain ⟨-, rfl⟩ := listSet_ok hvs2
  rw [uint64Add_ok hwe] at *
  rw [uint64Div_ok hpen] at hbs1
  obtain ⟨hl1, hb1⟩ := decrease_balance_ok hbs1
  obtain ⟨hl2, hb2⟩ := increase_balance_ok hbs2
  obtain ⟨hl3, hb3⟩ := increase_balance_ok hbs3
  refine ⟨⟨rfl, rfl, rfl, rfl⟩, ?_, fun j hj => ?_, ⟨v, _, hv, List.getElem?_set_self hlt,
    _, epoch, hch, hc, rfl⟩, ?_, fun j b hb => ?_⟩
  · simp only [List.length_set]
    exact hlen
  · rw [List.getElem?_set_ne (Ne.symm hj)]
    exact hother j hj
  · simp only [hl3, hl2, hl1]
  · obtain ⟨b1, hb1', hne, hle⟩ := hb1 j b hb
    obtain ⟨b2, hb2', hle2⟩ := hb2 j b1 hb1'
    obtain ⟨b3, hb3', hle3⟩ := hb3 j b2 hb2'
    refine ⟨b3, hb3', fun hj => ?_, fun u hu => ?_⟩
    · rw [← hne hj]
      exact Nat.le_trans hle2 hle3
    · by_cases hj : j = i
      · subst hj
        rw [hv] at hu
        cases hu
        rw [← hch.keeps.2.2.1]
        exact Nat.le_trans hle (Nat.add_le_add_right (Nat.le_trans hle2 hle3) _)
      · rw [← hne hj]
        exact Nat.le_trans (Nat.le_trans hle2 hle3) (Nat.le_add_right _ _)

/-- A slash change keeps the pubkey, the credentials, the effective balance and the activation
epochs, and sets `slashed`. -/
theorem SlashChange.keeps {p : Preset} {s : BeaconState} {v v' : Validator}
    (h : SlashChange p s v v') :
    v'.pubkey = v.pubkey ∧ v'.withdrawal_credentials = v.withdrawal_credentials ∧
      v'.effective_balance = v.effective_balance ∧ v'.slashed = true ∧
      v'.activation_eligibility_epoch = v.activation_eligibility_epoch ∧
      v'.activation_epoch = v.activation_epoch := by
  obtain ⟨v1, -, hch, -, rfl⟩ := h
  obtain ⟨h1, h2, h3, -, h5, h6⟩ := hch.keeps
  exact ⟨h1, h2, h3, rfl, h5, h6⟩

/-- A slash change keeps `exit_epoch ≤ withdrawable_epoch`. -/
theorem SlashChange.exit_le {p : Preset} {s : BeaconState} {v v' : Validator}
    (h : SlashChange p s v v') (hv : v.exit_epoch ≤ v.withdrawable_epoch) :
    v'.exit_epoch ≤ v'.withdrawable_epoch := by
  obtain ⟨v1, -, hch, -, rfl⟩ := h
  exact Nat.le_trans (hch.exit_le hv) (Nat.le_max_left _ _)

/-- A step keeps the pubkey, the credentials, the effective balance and the activation epochs
of a validator. It never clears `slashed`. -/
def ValidatorKeeps (v v' : Validator) : Prop :=
  v'.pubkey = v.pubkey ∧ v'.withdrawal_credentials = v.withdrawal_credentials ∧
    v'.effective_balance = v.effective_balance ∧
    v'.activation_eligibility_epoch = v.activation_eligibility_epoch ∧
    v'.activation_epoch = v.activation_epoch ∧ (v.slashed = true → v'.slashed = true)

/-- Every validator keeps its own fields. -/
theorem ValidatorKeeps.refl (v : Validator) : ValidatorKeeps v v :=
  ⟨rfl, rfl, rfl, rfl, rfl, id⟩

/-- `ValidatorKeeps` composes. -/
theorem ValidatorKeeps.trans {v1 v2 v3 : Validator} (h12 : ValidatorKeeps v1 v2)
    (h23 : ValidatorKeeps v2 v3) : ValidatorKeeps v1 v3 :=
  ⟨h23.1.trans h12.1, h23.2.1.trans h12.2.1, h23.2.2.1.trans h12.2.2.1,
    h23.2.2.2.1.trans h12.2.2.2.1, h23.2.2.2.2.1.trans h12.2.2.2.2.1,
    fun h => h23.2.2.2.2.2 (h12.2.2.2.2.2 h)⟩

/-- The effects of one or more slashings, from `s` to `s'`. Each validator keeps its
`ValidatorKeeps` fields. A balance drops only for a validator that was not slashed in `s` and
is slashed in `s'`, and by at most the slashing penalty. -/
def SlashEffect (p : Preset) (s s' : BeaconState) : Prop :=
  CheckpointsStable s s' ∧ s'.validators.length = s.validators.length ∧
    s'.balances.length = s.balances.length ∧ (ExitOrder s → ExitOrder s') ∧
    (∀ (j : Nat) (v : Validator), s.validators[j]? = some v →
      ∃ v' : Validator, s'.validators[j]? = some v' ∧ ValidatorKeeps v v') ∧
    ∀ (j : Nat) (b : Gwei), s.balances[j]? = some b → ∃ b' : Gwei, s'.balances[j]? = some b' ∧
      (b ≤ b' ∨ ∃ v v' : Validator, s.validators[j]? = some v ∧ s'.validators[j]? = some v' ∧
        v.slashed = false ∧ v'.slashed = true ∧
        b ≤ b' + v.effective_balance / p.MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA)

/-- A state has `SlashEffect` to itself. -/
theorem SlashEffect.refl (p : Preset) (s : BeaconState) : SlashEffect p s s :=
  ⟨CheckpointsStable.refl s, rfl, rfl, id, fun _ v hv => ⟨v, hv, ValidatorKeeps.refl v⟩,
    fun _ b hb => ⟨b, hb, .inl (Nat.le_refl _)⟩⟩

/-- `SlashEffect` composes. A validator is slashed in at most one of the two steps. -/
theorem SlashEffect.trans {p : Preset} {s1 s2 s3 : BeaconState} (h12 : SlashEffect p s1 s2)
    (h23 : SlashEffect p s2 s3) : SlashEffect p s1 s3 := by
  obtain ⟨hc12, hv12, hb12, ho12, hk12, hbal12⟩ := h12
  obtain ⟨hc23, hv23, hb23, ho23, hk23, hbal23⟩ := h23
  refine ⟨hc12.trans hc23, hv23.trans hv12, hb23.trans hb12, fun h => ho23 (ho12 h),
    fun j v hv => ?_, fun j b hb => ?_⟩
  · obtain ⟨v2, hv2, hk⟩ := hk12 j v hv
    obtain ⟨v3, hv3, hk'⟩ := hk23 j v2 hv2
    exact ⟨v3, hv3, hk.trans hk'⟩
  · obtain ⟨b2, hb2, h2⟩ := hbal12 j b hb
    obtain ⟨b3, hb3, h3⟩ := hbal23 j b2 hb2
    refine ⟨b3, hb3, ?_⟩
    rcases h2 with h2 | ⟨v1, v2, hv1, hv2, hs1, hs2, hle2⟩
    · rcases h3 with h3 | ⟨v2, v3, hv2, hv3, hs2, hs3, hle3⟩
      · exact .inl (Nat.le_trans h2 h3)
      · obtain ⟨v1, hv1, hk⟩ : ∃ v1, s1.validators[j]? = some v1 ∧ ValidatorKeeps v1 v2 := by
          have hj : j < s1.validators.length := by
            rw [← hv12]
            exact (List.getElem?_eq_some_iff.mp hv2).1
          obtain ⟨u, hu, hk⟩ := hk12 j _ (List.getElem?_eq_getElem hj)
          rw [hv2] at hu
          cases hu
          exact ⟨_, List.getElem?_eq_getElem hj, hk⟩
        have hs1 : v1.slashed = false := by
          cases h : v1.slashed
          · rfl
          · rw [hk.2.2.2.2.2 h] at hs2
            cases hs2
        refine .inr ⟨v1, v3, hv1, hv3, hs1, hs3, ?_⟩
        rw [hk.2.2.1] at hle3
        exact Nat.le_trans h2 hle3
    · obtain ⟨v3, hv3, hk⟩ := hk23 j v2 hv2
      refine .inr ⟨v1, v3, hv1, hv3, hs1, hk.2.2.2.2.2 hs2, ?_⟩
      rcases h3 with h3 | ⟨u2, -, hu2, -, hu2s, -, -⟩
      · exact Nat.le_trans hle2 (Nat.add_le_add_right h3 _)
      · rw [hv2] at hu2
        cases hu2
        rw [hs2] at hu2s
        cases hu2s

/-- `slash_validator` on a validator that is not slashed has `SlashEffect`. -/
theorem slash_validator_slashEffect (p : Preset) (s s' : BeaconState) (i : ValidatorIndex)
    (w : Option ValidatorIndex) (hns : ∀ v, s.validators[i]? = some v → v.slashed = false)
    (h : slash_validator p s i w = .ok s') : SlashEffect p s s' := by
  obtain ⟨hcp, hlen, hother, ⟨v, v', hv, hv', hch⟩, hblen, hbal⟩ :=
    slash_validator_ok p s s' i w h
  refine ⟨hcp, hlen, hblen, ExitsSanity.exitOrder_of_pointwise fun j u' hu' => ?_,
    fun j u hu => ?_, fun j b hb => ?_⟩
  · by_cases hj : j = i
    · subst hj
      rw [hv'] at hu'
      cases hu'
      exact ⟨v, hv, hch.exit_le⟩
    · exact ⟨u', (hother j hj).symm.trans hu', id⟩
  · by_cases hj : j = i
    · subst hj
      rw [hv] at hu
      cases hu
      obtain ⟨h1, h2, h3, h4, h5, h6⟩ := hch.keeps
      exact ⟨v', hv', h1, h2, h3, h5, h6, fun _ => h4⟩
    · exact ⟨u, (hother j hj).trans hu, ValidatorKeeps.refl u⟩
  · obtain ⟨b', hb', hne, hle⟩ := hbal j b hb
    refine ⟨b', hb', ?_⟩
    by_cases hj : j = i
    · subst hj
      exact .inr ⟨v, v', hv, hv', hns v hv, hch.keeps.2.2.2.1, hle v hv⟩
    · exact .inl (hne hj)

/-- A successful proposer slashing clears zero or one builder payment, then slashes the proposer.
The proposer is slashable before the slashing. -/
theorem process_proposer_slashing_ok (p : Preset) (o : Oracle) (s s' : BeaconState)
    (ps : ProposerSlashing) (h : process_proposer_slashing p o s ps = .ok s') :
    ∃ v cur payments, s.validators[ps.signed_header_1.message.proposer_index]? = some v ∧
      get_current_epoch p s = .ok cur ∧ is_slashable_validator v cur ∧
      slash_validator p { s with builder_pending_payments := payments }
        ps.signed_header_1.message.proposer_index = .ok s' := by
  unfold process_proposer_slashing at h
  simp only [bind, Except.bind, pure, Except.pure, throw, throwThe, MonadExceptOf.throw] at h
  repeat' split at h
  all_goals first | (cases h; done) | skip
  all_goals exact ⟨_, _, _, (listGet_ok ‹listGet s.validators _ = _›).2,
    ‹get_current_epoch p s = _›, Decidable.of_not_not ‹¬¬is_slashable_validator _ _ = true›, h⟩

/-- A `SlashEffect` step keeps effective balances. -/
theorem SlashEffect.ebStable {p : Preset} {s s' : BeaconState} (h : SlashEffect p s s') :
    EBStable s s' := by
  obtain ⟨-, hlen, -, -, hk, -⟩ := h
  refine ⟨Nat.le_of_eq hlen.symm, fun j v hv => ?_⟩
  obtain ⟨v', hv', hkv⟩ := hk j v hv
  exact ⟨v', hv', hkv.2.2.1⟩

/-- A balance that drops in a `SlashEffect` step belongs to a validator that the step slashes. -/
theorem SlashEffect.balance_le {p : Preset} {s s' : BeaconState} (h : SlashEffect p s s')
    (j : Nat) (b : Gwei) (hb : s.balances[j]? = some b) :
    ∃ b' : Gwei, s'.balances[j]? = some b' ∧
      (∀ v v', s.validators[j]? = some v → s'.validators[j]? = some v' →
        ¬ (v.slashed = false ∧ v'.slashed = true) → b ≤ b') ∧
      (∀ v, s.validators[j]? = some v →
        b ≤ b' + v.effective_balance / p.MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA) := by
  obtain ⟨b', hb', hor⟩ := h.2.2.2.2.2 j b hb
  refine ⟨b', hb', fun v v' hv hv' hn => ?_, fun v hv => ?_⟩
  · rcases hor with hle | ⟨u, u', hu, hu', hs, hs', -⟩
    · exact hle
    · rw [hv] at hu
      rw [hv'] at hu'
      cases hu
      cases hu'
      exact absurd ⟨hs, hs'⟩ hn
  · rcases hor with hle | ⟨u, -, hu, -, -, -, hle⟩
    · exact Nat.le_trans hle (Nat.le_add_right _ _)
    · rw [hv] at hu
      cases hu
      exact hle

/-- The effects of a proposer slashing. The proposer `i` was not slashed and is slashed after.
Its balance drops by at most `effective_balance / MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA`. No
other balance drops, and no other validator changes. -/
theorem process_proposer_slashing_effects (p : Preset) (o : Oracle) (s s' : BeaconState)
    (ps : ProposerSlashing) (h : process_proposer_slashing p o s ps = .ok s') :
    EBStable s s' ∧ CheckpointsStable s s' ∧ (ExitOrder s → ExitOrder s') ∧ SlashEffect p s s' ∧
      (∀ j, j ≠ ps.signed_header_1.message.proposer_index →
        s'.validators[j]? = s.validators[j]?) ∧
      (∃ v v', s.validators[ps.signed_header_1.message.proposer_index]? = some v ∧
        s'.validators[ps.signed_header_1.message.proposer_index]? = some v' ∧
        v.slashed = false ∧ v'.slashed = true ∧ SlashChange p s v v') ∧
      ∀ (j : Nat) (b : Gwei), s.balances[j]? = some b → ∃ b' : Gwei, s'.balances[j]? = some b' ∧
        (j ≠ ps.signed_header_1.message.proposer_index → b ≤ b') ∧
        ∀ v : Validator, s.validators[j]? = some v →
          b ≤ b' + v.effective_balance / p.MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA := by
  obtain ⟨v, cur, payments, hv, -, hsl, h⟩ := process_proposer_slashing_ok p o s s' ps h
  have hns : v.slashed = false := by
    unfold is_slashable_validator at hsl
    cases hs : v.slashed
    · rfl
    · simp [hs] at hsl
  have heff := slash_validator_slashEffect p _ s' _ none
    (fun u hu => by rw [show _ = _ from hv] at hu; cases hu; exact hns) h
  obtain ⟨-, -, hother, ⟨u, u', hu, hu', hch⟩, -, hbal⟩ := slash_validator_ok p _ s' _ none h
  change s.validators[_]? = some u at hu
  rw [hv] at hu
  cases hu
  exact ⟨heff.ebStable, heff.1, heff.2.2.2.1, heff, hother, ⟨v, u', hv, hu', hns,
    hch.keeps.2.2.2.1, hch⟩, hbal⟩

namespace SlashingsSanity

/-- A `for` loop in `SpecM` keeps every property that each step keeps. -/
theorem forIn_invariant {α β : Type} (P : β → Prop) (f : α → β → SpecM (ForInStep β))
    (hf : ∀ a b r, P b → f a b = .ok r → P r.value) :
    ∀ (l : List α) (b res : β), P b → forIn l b f = .ok res → P res := by
  intro l
  induction l with
  | nil =>
    intro b res hb h
    cases h
    exact hb
  | cons a l ih =>
    intro b res hb h
    simp only [List.forIn_cons] at h
    obtain ⟨r, hr, h⟩ := specM_bind_ok h
    have hP := hf a b r hb hr
    cases r with
    | done c =>
      cases h
      exact hP
    | yield c => exact ih c res hP h

end SlashingsSanity

/-- `ExitChange` reads only the slot of the state. -/
theorem ExitChange.congr_slot {p : Preset} {s t : BeaconState} {v v' : Validator}
    (hslot : t.slot = s.slot) (h : ExitChange p s v v') : ExitChange p t v v' := by
  unfold ExitChange get_current_epoch at *
  rw [hslot]
  exact h

/-- `SlashChange` reads only the slot of the state. -/
theorem SlashChange.congr_slot {p : Preset} {s t : BeaconState} {v v' : Validator}
    (hslot : t.slot = s.slot) (h : SlashChange p s v v') : SlashChange p t v v' := by
  obtain ⟨v1, cur, hch, hc, rfl⟩ := h
  refine ⟨v1, cur, hch.congr_slot hslot, ?_, rfl⟩
  unfold get_current_epoch at *
  rw [hslot]
  exact hc

/-- From `s` to `s'`, each validator either stays the same, or was not slashed and changes by
one `SlashChange`. -/
def SlashFields (p : Preset) (s s' : BeaconState) : Prop :=
  ∀ (j : Nat) (v v' : Validator), s.validators[j]? = some v → s'.validators[j]? = some v' →
    v' = v ∨ (v.slashed = false ∧ SlashChange p s v v')

/-- A state has `SlashFields` to itself. -/
theorem SlashFields.refl (p : Preset) (s : BeaconState) : SlashFields p s s := by
  intro j v v' hv hv'
  rw [hv] at hv'
  cases hv'
  exact .inl rfl

/-- `SlashFields` composes when the middle state keeps the slot and the registry length. -/
theorem SlashFields.trans {p : Preset} {s1 s2 s3 : BeaconState} (h12 : SlashFields p s1 s2)
    (h23 : SlashFields p s2 s3) (hslot : s2.slot = s1.slot)
    (hlen : s2.validators.length = s1.validators.length) : SlashFields p s1 s3 := by
  intro j v1 v3 hv1 hv3
  have hj : j < s2.validators.length := by
    rw [hlen]
    exact (List.getElem?_eq_some_iff.mp hv1).1
  have hv2 := List.getElem?_eq_getElem hj
  rcases h12 j v1 _ hv1 hv2 with h1 | ⟨hs1, hch1⟩
  · rw [h1] at hv2
    rcases h23 j v1 v3 hv2 hv3 with h2 | ⟨hs2, hch2⟩
    · exact .inl h2
    · exact .inr ⟨hs2, hch2.congr_slot hslot.symm⟩
  · rcases h23 j _ v3 hv2 hv3 with h2 | ⟨hs2, -⟩
    · rw [h2]
      exact .inr ⟨hs1, hch1⟩
    · rw [hch1.keeps.2.2.2.1] at hs2
      cases hs2

/-- `slash_validator` on a validator that is not slashed has `SlashFields`. -/
theorem slash_validator_slashFields (p : Preset) (s s' : BeaconState) (i : ValidatorIndex)
    (w : Option ValidatorIndex) (hns : ∀ v, s.validators[i]? = some v → v.slashed = false)
    (h : slash_validator p s i w = .ok s') : SlashFields p s s' := by
  obtain ⟨-, -, hother, ⟨v, v', hv, hv', hch⟩, -, -⟩ := slash_validator_ok p s s' i w h
  intro j u u' hu hu'
  by_cases hj : j = i
  · subst hj
    rw [hv] at hu
    rw [hv'] at hu'
    cases hu
    cases hu'
    exact .inr ⟨hns v hv, hch⟩
  · rw [hother j hj, hu] at hu'
    cases hu'
    exact .inl rfl

/-- An attester slashing has `SlashEffect` and `SlashFields`. -/
theorem process_attester_slashing_ok (p : Preset) (o : Oracle) (s s' : BeaconState)
    (as : AttesterSlashing) (h : process_attester_slashing p o s as = .ok s') :
    SlashEffect p s s' ∧ SlashFields p s s' := by
  unfold process_attester_slashing at h
  simp only [bind, Except.bind, pure, Except.pure, throw, throwThe, MonadExceptOf.throw] at h
  repeat' split at h
  all_goals first | (cases h; done) | skip
  cases h
  refine SlashingsSanity.forIn_invariant
    (fun r : MProd Bool BeaconState => SlashEffect p s r.snd ∧ SlashFields p s r.snd) _ ?_ _ _ _
    ⟨SlashEffect.refl p s, SlashFields.refl p s⟩ ‹forIn _ _ _ = _›
  intro index r step ⟨hr, hrf⟩ hstep
  obtain ⟨v, hv, hstep⟩ := specM_bind_ok hstep
  obtain ⟨cur, -, hstep⟩ := specM_bind_ok hstep
  split at hstep
  · rename_i hsl
    obtain ⟨r', hr', hstep⟩ := specM_bind_ok hstep
    cases hstep
    have hns : ∀ u, r.snd.validators[index]? = some u → u.slashed = false := by
      intro u hu
      rw [(listGet_ok hv).2] at hu
      cases hu
      unfold is_slashable_validator at hsl
      cases hs : v.slashed
      · rfl
      · simp [hs] at hsl
    have heff := slash_validator_slashEffect p _ _ index none hns hr'
    exact ⟨hr.trans heff, hrf.trans (slash_validator_slashFields p _ _ index none hns hr')
      hr.1.1 hr.2.1⟩
  · cases hstep
    exact ⟨hr, hrf⟩

/-- The effects of an attester slashing. A balance drops only for a validator that was not
slashed before and is slashed after, and by at most
`effective_balance / MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA`. -/
theorem process_attester_slashing_effects (p : Preset) (o : Oracle) (s s' : BeaconState)
    (as : AttesterSlashing) (h : process_attester_slashing p o s as = .ok s') :
    EBStable s s' ∧ CheckpointsStable s s' ∧ (ExitOrder s → ExitOrder s') ∧
      SlashFields p s s' ∧
      ∀ (j : Nat) (b : Gwei), s.balances[j]? = some b → ∃ b' : Gwei, s'.balances[j]? = some b' ∧
        (∀ v v', s.validators[j]? = some v → s'.validators[j]? = some v' →
          ¬ (v.slashed = false ∧ v'.slashed = true) → b ≤ b') ∧
        (∀ v, s.validators[j]? = some v →
          b ≤ b' + v.effective_balance / p.MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA) := by
  obtain ⟨heff, hf⟩ := process_attester_slashing_ok p o s s' as h
  exact ⟨heff.ebStable, heff.1, heff.2.2.2.1, hf, heff.balance_le⟩
