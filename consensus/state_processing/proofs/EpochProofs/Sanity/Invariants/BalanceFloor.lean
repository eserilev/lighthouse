import EpochProofs.Sanity.Invariants.ExitDelay
import EpochProofs.Sanity.LighthouseSinglePass
import EpochProofs.Spec.Block.Block

/-!
# The balance floor at the epoch boundary

After the effective balance updates, `3 * effective_balance ≤ 4 * balance` for every validator
(`AfterEB`). The blocks of the next epoch lower a balance in three ways: one slashing penalty,
the sync committee penalties, and withdrawals. `Budget` bounds the drop since the start of the
epoch. At the next boundary it gives `effective_balance ≤ 256 * balance` for every validator
that is eligible for rewards: the effective balance part of `EpochEntry`.

The sync penalty is the only drop without a fixed bound. `sync_loss_le_mainnet` bounds it
when the total active balance is at most 139 million ETH. `EpochInv` carries the budget through
each `state_transition` of the epoch.
-/

namespace EpochProofs.Spec

/-- A slashable validator becomes withdrawable after the epoch. -/
private theorem lt_withdrawable_of_slashable {v : Validator} {e : Epoch}
    (h : is_slashable_validator v e = true) : e < v.withdrawable_epoch := by
  simp only [is_slashable_validator, Bool.and_eq_true, decide_eq_true_eq] at h
  exact h.2.2

/-! ## Slashings -/

/-- The effect of a slashing at epoch `E`. Each validator stays, or it was not slashed, is
slashed now, keeps its effective balance, activation epoch and `ExitDelayV`, keeps its exit
epoch or exits after `E`, and was withdrawable only after `E`. A balance drops only for a
validator slashed in this step, and by at most
`effective_balance / MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA`. -/
def SlashStep (p : Preset) (E : Epoch) (s s' : BeaconState) : Prop :=
  s'.validators.length = s.validators.length ∧
    (∀ (i : Nat) (v v' : Validator), s.validators[i]? = some v → s'.validators[i]? = some v' →
      v' = v ∨ (v.slashed = false ∧ v'.slashed = true ∧
        v'.effective_balance = v.effective_balance ∧ E < v.withdrawable_epoch ∧
        (ExitDelayV p v → ExitDelayV p v') ∧
        v'.activation_epoch = v.activation_epoch ∧
        (v'.exit_epoch = v.exit_epoch ∨
          (v.exit_epoch = FAR_FUTURE_EPOCH ∧ E < v'.exit_epoch)))) ∧
    ∀ (i : Nat) (b : Gwei), s.balances[i]? = some b → ∃ b', s'.balances[i]? = some b' ∧
      (b ≤ b' ∨ ∃ v v' : Validator, s.validators[i]? = some v ∧ s'.validators[i]? = some v' ∧
        v.slashed = false ∧ v'.slashed = true ∧
        b ≤ b' + v.effective_balance / p.MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA)

/-- A slashing step keeps `ExitDelay`. -/
theorem SlashStep.exitDelay {p : Preset} {E : Epoch} {s s' : BeaconState}
    (h : SlashStep p E s s') (hs : ExitDelay p s) : ExitDelay p s' := by
  refine ExitDelay.of_pointwise (fun j v' hv' => ?_) hs
  have hj : j < s.validators.length := by
    rw [← h.1]; exact (List.getElem?_eq_some_iff.mp hv').1
  refine ⟨_, List.getElem?_eq_getElem hj, fun hd => ?_⟩
  rcases h.2.1 j _ v' (List.getElem?_eq_getElem hj) hv' with rfl | ⟨-, -, -, -, himp, -⟩
  · exact hd
  · exact himp hd

/-- A `SlashChange` at epoch `E` keeps the activation epoch, and keeps the exit epoch or sets it
after `E`. -/
theorem SlashChange.exit_cases {p : Preset} {s : BeaconState} {v v' : Validator}
    (h : SlashChange p s v v') :
    v'.activation_epoch = v.activation_epoch ∧
      (v'.exit_epoch = v.exit_epoch ∨
        (v.exit_epoch = FAR_FUTURE_EPOCH ∧ s.slot / p.SLOTS_PER_EPOCH < v'.exit_epoch)) := by
  obtain ⟨v1, -, hch, -, rfl⟩ := h
  rcases hch with rfl | ⟨hf, cur, e, hc, hlt, rfl⟩
  · exact ⟨rfl, .inl rfl⟩
  · exact ⟨rfl, .inr ⟨hf, get_current_epoch_ok_eq hc ▸ hlt⟩⟩

/-- `process_proposer_slashing` at epoch `E` is a slashing step. The spec checks that the
proposer is slashable. -/
theorem process_proposer_slashing_slashStep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (E : Epoch) (hE : s.slot / p.SLOTS_PER_EPOCH = E) (ps : ProposerSlashing)
    (h : process_proposer_slashing p o s ps = .ok s') : SlashStep p E s s' := by
  obtain ⟨-, -, -, heff, hother, ⟨v, v', hv, hv', hns, hs', hch⟩, hbal⟩ :=
    process_proposer_slashing_effects p o s s' ps h
  obtain ⟨u, cur, -, hu, hcur, hsl, -⟩ := process_proposer_slashing_ok p o s s' ps h
  rw [hv] at hu
  cases hu
  have hlt : E < v.withdrawable_epoch := by
    rw [← hE, ← get_current_epoch_ok_eq hcur]
    exact lt_withdrawable_of_slashable hsl
  refine ⟨heff.2.1, fun j w w' hw hw' => ?_, fun j b hb => ?_⟩
  · by_cases hj : j = ps.signed_header_1.message.proposer_index
    · subst hj
      rw [hv] at hw
      rw [hv'] at hw'
      cases hw
      cases hw'
      exact .inr ⟨hns, hs', hch.keeps.2.2.1, hlt, hch.exitDelay, hch.exit_cases.1,
        hE ▸ hch.exit_cases.2⟩
    · rw [hother j hj, hw] at hw'
      cases hw'
      exact .inl rfl
  · obtain ⟨b', hb', hne, hle⟩ := hbal j b hb
    refine ⟨b', hb', ?_⟩
    by_cases hj : j = ps.signed_header_1.message.proposer_index
    · subst hj
      exact .inr ⟨v, v', hv, hv', hns, hs', hle v hv⟩
    · exact .inl (hne hj)

/-- In an attester slashing, each validator that changes was withdrawable only after the
current epoch. The spec checks `is_slashable_validator` before each `slash_validator`. -/
theorem process_attester_slashing_slashable (p : Preset) (o : Oracle) (s s' : BeaconState)
    (as : AttesterSlashing) (h : process_attester_slashing p o s as = .ok s') :
    ∀ (j : Nat) (v v' : Validator), s.validators[j]? = some v → s'.validators[j]? = some v' →
      v' = v ∨ s.slot / p.SLOTS_PER_EPOCH < v.withdrawable_epoch := by
  unfold process_attester_slashing at h
  simp only [bind, Except.bind, pure, Except.pure, throw, throwThe, MonadExceptOf.throw] at h
  repeat' split at h
  all_goals first | (cases h; done) | skip
  cases h
  refine (SlashingsSanity.forIn_invariant
    (fun r : MProd Bool BeaconState => r.snd.slot = s.slot ∧
      ∀ (j : Nat) (v v' : Validator), s.validators[j]? = some v → r.snd.validators[j]? = some v' →
        v' = v ∨ s.slot / p.SLOTS_PER_EPOCH < v.withdrawable_epoch) _ ?_ _
    (⟨false, s⟩ : MProd Bool BeaconState) _
    ⟨rfl, fun j v v' hv hv' => by rw [hv] at hv'; cases hv'; exact .inl rfl⟩
    ‹forIn _ _ _ = _›).2
  intro index r step ⟨hslot, hr⟩ hstep
  obtain ⟨u, hu, hstep⟩ := specM_bind_ok hstep
  obtain ⟨cur, hcur, hstep⟩ := specM_bind_ok hstep
  split at hstep
  · rename_i hsl
    obtain ⟨r', hr', hstep⟩ := specM_bind_ok hstep
    cases hstep
    obtain ⟨hcp, -, hother, -, -, -⟩ := slash_validator_ok p _ _ index none hr'
    refine ⟨hcp.1.trans hslot, fun j w w' hw hw' => ?_⟩
    by_cases hj : j = index
    · subst hj
      rcases hr j w u hw (listGet_ok hu).2 with rfl | hlt
      · right
        have := lt_withdrawable_of_slashable hsl
        rw [get_current_epoch_ok_eq hcur, hslot] at this
        exact this
      · exact .inr hlt
    · exact hr j w w' hw ((hother j hj).symm.trans hw')
  · cases hstep
    exact ⟨hslot, hr⟩

/-- `process_attester_slashing` at epoch `E` is a slashing step. -/
theorem process_attester_slashing_slashStep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (E : Epoch) (hE : s.slot / p.SLOTS_PER_EPOCH = E) (as : AttesterSlashing)
    (h : process_attester_slashing p o s as = .ok s') : SlashStep p E s s' := by
  obtain ⟨heff, hf⟩ := process_attester_slashing_ok p o s s' as h
  have hsl := process_attester_slashing_slashable p o s s' as h
  refine ⟨heff.2.1, fun j v v' hv hv' => ?_, heff.2.2.2.2.2⟩
  rcases hf j v v' hv hv' with rfl | ⟨hns, hch⟩
  · exact .inl rfl
  · obtain ⟨-, -, heb, hs', -, -⟩ := hch.keeps
    rcases hsl j v v' hv hv' with rfl | hlt
    · rw [hns] at hs'
      cases hs'
    · exact .inr ⟨hns, hs', heb, hE ▸ hlt, hch.exitDelay, hch.exit_cases.1,
        hE ▸ hch.exit_cases.2⟩

/-! ## The budget -/

/-- The slashing penalty that the budget allows: one penalty for a validator that was not
slashed at the start of the epoch and is slashed now. -/
def slashLoss (p : Preset) (v0 v : Validator) : Gwei :=
  if v0.slashed = false ∧ v.slashed = true then
    v0.effective_balance / p.MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA
  else 0

/-- The validator is withdrawable at epoch `E`, and it became withdrawable at least the delay
after its exit. -/
def WithdrawnBy (p : Preset) (E : Epoch) (v : Validator) : Prop :=
  v.withdrawable_epoch ≤ E ∧
    v.exit_epoch + p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY ≤ v.withdrawable_epoch

/-- The balance budget of epoch `E`, from the state `s0` at its start to the state `s`. `n` is
the number of sync aggregates so far, and `L i` bounds the sync penalty of validator `i` in one
sync aggregate.

Each validator of `s0` keeps its effective balance and its slashed flag. It is withdrawn by
`E`, or its balance plus the allowed losses is at least `min b0 MIN_ACTIVATION_BALANCE`. A
withdrawal can lower a balance to `MIN_ACTIVATION_BALANCE`, so the floor is that minimum. -/
def Budget (p : Preset) (s0 : BeaconState) (E : Epoch) (L : Nat → Gwei) (n : Nat)
    (s : BeaconState) : Prop :=
  s.validators.length = s0.validators.length ∧
    ∀ (i : Nat) (v0 : Validator) (b0 : Gwei), s0.validators[i]? = some v0 →
      s0.balances[i]? = some b0 →
      ∃ (v : Validator) (b : Gwei), s.validators[i]? = some v ∧ s.balances[i]? = some b ∧
        v.effective_balance = v0.effective_balance ∧ (v0.slashed = true → v.slashed = true) ∧
        (WithdrawnBy p E v ∨
          min b0 p.MIN_ACTIVATION_BALANCE ≤ b + slashLoss p v0 v + n * L i)

/-- The budget holds at the start of the epoch, with no sync aggregate. -/
theorem Budget.init (p : Preset) (s0 : BeaconState) (E : Epoch) (L : Nat → Gwei) :
    Budget p s0 E L 0 s0 := by
  refine ⟨rfl, fun i v0 b0 h0 hb0 => ⟨v0, b0, h0, hb0, rfl, id, .inr ?_⟩⟩
  simp only [Nat.zero_mul, Nat.add_zero]
  exact Nat.le_trans (Nat.min_le_left _ _) (Nat.le_add_right _ _)

/-- Same validators and balances keep the budget. The slot does not matter. -/
theorem Budget.of_eq {p : Preset} {s0 : BeaconState} {E : Epoch} {L : Nat → Gwei} {n : Nat}
    {s s' : BeaconState} (hv : s'.validators = s.validators) (hbal : s'.balances = s.balances)
    (hb : Budget p s0 E L n s) : Budget p s0 E L n s' := by
  rw [Budget, hv, hbal]
  exact hb

/-- `min b0 m ≤ b + x` and `min b m ≤ b'` give `min b0 m ≤ b' + x`. -/
private theorem floor_step {b0 m b b' x : Nat} (h : min b0 m ≤ b + x) (h' : min b m ≤ b') :
    min b0 m ≤ b' + x := by
  simp only [Nat.min_def] at *
  split at h <;> split at h' <;> split <;> omega

/-- An operation step keeps the budget. A withdrawn validator does not start an exit again. -/
theorem Budget.of_opStep {p : Preset} {s0 : BeaconState} {E : Epoch} {L : Nat → Gwei} {n : Nat}
    {s s' : BeaconState} (hfar : E < FAR_FUTURE_EPOCH) (hop : OpStep p s s')
    (hb : Budget p s0 E L n s) : Budget p s0 E L n s' := by
  obtain ⟨hlen, hall⟩ := hb
  obtain ⟨-, hvlen, hvstep, -, hbstep⟩ := hop
  refine ⟨hvlen.trans hlen, fun i v0 b0 h0 hb0 => ?_⟩
  obtain ⟨v, b, hv, hbb, heb, hsm, hcase⟩ := hall i v0 b0 h0 hb0
  have hi : i < s'.validators.length := by
    rw [hvlen]; exact (List.getElem?_eq_some_iff.mp hv).1
  have hv' := List.getElem?_eq_getElem hi
  have hst := hvstep i v _ hv hv'
  obtain ⟨b', hb', hfl⟩ := hbstep i b hbb
  refine ⟨_, b', hv', hb', hst.1.trans heb, fun h => hst.2.1.trans (hsm h), ?_⟩
  rcases hcase with ⟨hw, hx⟩ | hle
  · obtain ⟨he, hw'⟩ := hst.frozen hw hx hfar
    exact .inl ⟨hw' ▸ hw, by rw [he, hw']; exact hx⟩
  · right
    have hsl : slashLoss p v0 s'.validators[i] = slashLoss p v0 v := by
      unfold slashLoss
      rw [hst.2.1]
    rw [hsl, Nat.add_assoc] at *
    exact floor_step hle hfl

/-- A slashing step keeps the budget. The penalty falls on a validator that was not slashed at
the start of the epoch, so `slashLoss` covers it. -/
theorem Budget.of_slashStep {p : Preset} {s0 : BeaconState} {E : Epoch} {L : Nat → Gwei}
    {n : Nat} {s s' : BeaconState} (hs : SlashStep p E s s') (hb : Budget p s0 E L n s) :
    Budget p s0 E L n s' := by
  obtain ⟨hlen, hall⟩ := hb
  obtain ⟨hvlen, hvstep, hbstep⟩ := hs
  refine ⟨hvlen.trans hlen, fun i v0 b0 h0 hb0 => ?_⟩
  obtain ⟨v, b, hv, hbb, heb, hsm, hcase⟩ := hall i v0 b0 h0 hb0
  have hi : i < s'.validators.length := by
    rw [hvlen]; exact (List.getElem?_eq_some_iff.mp hv).1
  have hv' := List.getElem?_eq_getElem hi
  obtain ⟨b', hb', hbal⟩ := hbstep i b hbb
  generalize s'.validators[i] = v' at hv'
  rcases hvstep i v v' hv hv' with hvv | ⟨hns, hs', heb', hlt, -, -, -⟩
  · rw [hvv] at hv'
    have hle : b ≤ b' := by
      rcases hbal with hle | ⟨u, u', hu, hu', hus, hus', -⟩
      · exact hle
      · rw [hv] at hu
        rw [hv'] at hu'
        cases hu
        cases hu'
        rw [hus] at hus'
        cases hus'
    refine ⟨v, b', hv', hb', heb, hsm, ?_⟩
    rcases hcase with hw | hcase
    · exact .inl hw
    · right
      rw [Nat.add_assoc] at hcase ⊢
      exact Nat.le_trans hcase (Nat.add_le_add_right hle _)
  · have h0s : v0.slashed = false := by
      cases h : v0.slashed
      · rfl
      · rw [hsm h] at hns
        cases hns
    have hle : b ≤ b' + v0.effective_balance / p.MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA := by
      rcases hbal with hle | ⟨u, -, hu, -, -, -, hle⟩
      · exact Nat.le_trans hle (Nat.le_add_right _ _)
      · rw [hv] at hu
        cases hu
        rw [← heb]
        exact hle
    refine ⟨v', b', hv', hb', heb'.trans heb, fun _ => hs', .inr ?_⟩
    rcases hcase with ⟨hw, -⟩ | hcase
    · exact absurd hw (Nat.not_le_of_lt hlt)
    · have hl0 : slashLoss p v0 v = 0 := by simp [slashLoss, hns]
      have hl1 : slashLoss p v0 v' =
          v0.effective_balance / p.MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA := by
        simp [slashLoss, h0s, hs']
      rw [hl0] at hcase
      rw [hl1]
      generalize n * L i = y at hcase ⊢
      simp only [Gwei] at *
      omega

/-- The maximum effective balance of a validator is at least `MIN_ACTIVATION_BALANCE`. -/
theorem min_activation_le_max_effective_balance {p : Preset}
    (hmax : p.MIN_ACTIVATION_BALANCE ≤ p.MAX_EFFECTIVE_BALANCE_ELECTRA) (v : Validator) :
    p.MIN_ACTIVATION_BALANCE ≤ get_max_effective_balance p v := by
  unfold get_max_effective_balance
  split
  · exact hmax
  · exact Nat.le_refl _

/-- `process_withdrawals` at epoch `E` keeps the budget. A partial withdrawal leaves at least
`MIN_ACTIVATION_BALANCE`. A full withdrawal leaves a validator that is withdrawn by `E`. -/
theorem Budget.of_withdrawals {p : Preset} {s0 : BeaconState} {E : Epoch} {L : Nat → Gwei}
    {n : Nat} {s s' : BeaconState} (hE : s.slot / p.SLOTS_PER_EPOCH = E)
    (hfar : E < FAR_FUTURE_EPOCH)
    (hmax : p.MIN_ACTIVATION_BALANCE ≤ p.MAX_EFFECTIVE_BALANCE_ELECTRA)
    (hd : ExitDelay p s) (h : process_withdrawals p s = .ok s')
    (hb : Budget p s0 E L n s) : Budget p s0 E L n s' := by
  obtain ⟨hvs, -, -, -⟩ := process_withdrawals_core p s s' h
  obtain ⟨-, hbal⟩ := process_withdrawals_balances p s s' h
  obtain ⟨hlen, hall⟩ := hb
  refine ⟨by rw [hvs]; exact hlen, fun i v0 b0 h0 hb0 => ?_⟩
  obtain ⟨v, b, hv, hbb, heb, hsm, hcase⟩ := hall i v0 b0 h0 hb0
  obtain ⟨b', hb', -, hor⟩ := hbal i b hbb
  refine ⟨v, b', by rw [hvs]; exact hv, hb', heb, hsm, ?_⟩
  rcases hcase with hw | hcase
  · exact .inl hw
  rcases hor with rfl | ⟨u, hu, hfl⟩
  · exact .inr hcase
  rw [hv] at hu
  cases hu
  have hm : min b0 p.MIN_ACTIVATION_BALANCE ≤ p.MIN_ACTIVATION_BALANCE := Nat.min_le_right _ _
  rcases hfl with hle | hle | ⟨-, hw⟩
  · exact .inr (Nat.le_trans hm (Nat.le_trans hle
      (by rw [Nat.add_assoc]; exact Nat.le_add_right _ _)))
  · have := min_activation_le_max_effective_balance hmax v
    exact .inr (Nat.le_trans hm (Nat.le_trans (Nat.le_trans this hle)
      (by rw [Nat.add_assoc]; exact Nat.le_add_right _ _)))
  · rw [hE] at hw
    left
    refine ⟨hw, ?_⟩
    rcases hd v (List.mem_of_getElem? hv) with hf | hx
    · simp only [Epoch] at *
      omega
    · exact hx

/-- `process_sync_aggregate` keeps the budget with one more sync aggregate, if `L i` bounds the
sync penalty of each validator `i` at the current total active balance. -/
theorem Budget.of_sync {p : Preset} {s0 : BeaconState} {E : Epoch} {L : Nat → Gwei} {n : Nat}
    {o : Oracle} {s s' : BeaconState} {sync_aggregate : SyncAggregate}
    (h : process_sync_aggregate p o s sync_aggregate = .ok s')
    (hL : ∀ tab, get_total_active_balance p s = .ok tab → ∀ i,
      syncPenaltyCount s sync_aggregate.sync_committee_bits i * sync_participant_reward p tab ≤
        L i)
    (hb : Budget p s0 E L n s) : Budget p s0 E L (n + 1) s' := by
  obtain ⟨hfr, -⟩ := process_sync_aggregate_frame p o s s' sync_aggregate h
  have hvs : s'.validators = s.validators := by rw [hfr]
  obtain ⟨tab, htab, hbal⟩ := process_sync_aggregate_balance_bound p o s s' sync_aggregate h
  obtain ⟨hlen, hall⟩ := hb
  refine ⟨by rw [hvs]; exact hlen, fun i v0 b0 h0 hb0 => ?_⟩
  obtain ⟨v, b, hv, hbb, heb, hsm, hcase⟩ := hall i v0 b0 h0 hb0
  obtain ⟨b', hb', hle⟩ := hbal i b hbb
  refine ⟨v, b', by rw [hvs]; exact hv, hb', heb, hsm, ?_⟩
  rcases hcase with hw | hcase
  · exact .inl hw
  · have hl := hL tab htab i
    right
    rw [Nat.succ_mul]
    generalize syncPenaltyCount s sync_aggregate.sync_committee_bits i *
      sync_participant_reward p tab = x at hle hl
    generalize n * L i = y at hcase ⊢
    simp only [Gwei] at *
    omega

/-! ## The start of the epoch -/

/-- The facts at the start of an epoch, right after the effective balance updates. Each
effective balance is a multiple of the increment, at most `4/3` of the balance, and at most
`MAX_EFFECTIVE_BALANCE_ELECTRA`. -/
def AfterEB (p : Preset) (s : BeaconState) : Prop :=
  s.validators.length ≤ s.balances.length ∧
    ∀ i (hi : i < s.validators.length),
      s.validators[i].effective_balance % p.EFFECTIVE_BALANCE_INCREMENT = 0 ∧
      3 * s.validators[i].effective_balance ≤ 4 * s.balances.getD i 0 ∧
      s.validators[i].effective_balance ≤ p.MAX_EFFECTIVE_BALANCE_ELECTRA

/-- Same validators and balances keep `AfterEB`. The epoch steps after the effective balance
updates do not write them. -/
theorem AfterEB.of_eq {p : Preset} {s s' : BeaconState} (hv : s'.validators = s.validators)
    (hb : s'.balances = s.balances) (h : AfterEB p s) : AfterEB p s' := by
  rw [AfterEB, hv, hb]
  exact h

/-- On mainnet, `process_effective_balance_updates` establishes `AfterEB`, if every effective
balance before it is a multiple of the increment and at most `MAX_EFFECTIVE_BALANCE_ELECTRA`. -/
theorem process_effective_balance_updates_afterEB_mainnet (state state' : BeaconState)
    (h : process_effective_balance_updates Preset.mainnet state = .ok state')
    (hmod : ∀ v ∈ state.validators,
      v.effective_balance % Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT = 0)
    (hmax : ∀ v ∈ state.validators,
      v.effective_balance ≤ Preset.mainnet.MAX_EFFECTIVE_BALANCE_ELECTRA) :
    AfterEB Preset.mainnet state' := by
  have hfl := process_effective_balance_updates_floor_mainnet state state' h hmod
  refine ⟨hfl.1, fun i hi => ⟨(hfl.2 i hi).1, (hfl.2 i hi).2, ?_⟩⟩
  rw [process_effective_balance_updates_eq] at h
  cases hm : state.validators.zipIdx.mapM (effectiveBalanceStep Preset.mainnet state.balances) with
  | error e => simp [hm, Except.map] at h
  | ok validators =>
    simp only [hm, Except.map, Except.ok.injEq] at h
    subst h
    obtain ⟨hlen, hget⟩ := mapM_ok_getElem _ _ _ hm
    rw [List.length_zipIdx] at hlen
    have hv : i < state.validators.length := hlen ▸ hi
    have hs := hget i (by simpa using hv) hi
    rw [List.getElem_zipIdx] at hs
    obtain ⟨balance, -, hn⟩ :=
      effectiveBalanceStep_ok _ _ _ _ hysteresisThresholds_mainnet _ _ hs
    obtain ⟨-, -, hle⟩ := newEffectiveBalance_bounds _ presetIncrementOk_mainnet _ _ _ balance _
      hn (hmod _ (List.getElem_mem hv))
    refine Nat.le_trans hle (Nat.max_le.mpr ⟨hmax _ (List.getElem_mem hv), ?_⟩)
    unfold get_max_effective_balance
    split
    · exact Nat.le_refl _
    · decide

/-! ## The effective balance part of `EpochEntry` -/

/-- On mainnet, the budget gives `effective_balance ≤ 256 * balance` for every validator that is
eligible for rewards at `previous_epoch`, if the allowed losses fit. `hfit` says that the sync
penalties of `n` sync aggregates, one slashing penalty and `effective_balance / 256` fit in the
floor `min (3 * effective_balance / 4) MIN_ACTIVATION_BALANCE`. -/
theorem budget_effective_balance_mainnet (s0 s : BeaconState) (E previous_epoch n : Nat)
    (L : Nat → Gwei) (h0 : AfterEB Preset.mainnet s0) (hb : Budget Preset.mainnet s0 E L n s)
    (hfar : E < FAR_FUTURE_EPOCH) (hE : E ≤ previous_epoch + 1)
    (hfit : ∀ (i : Nat) (v0 : Validator), s0.validators[i]? = some v0 →
      0 < v0.effective_balance →
      n * L i + v0.effective_balance / 4096 + v0.effective_balance / 256 ≤
        min (3 * v0.effective_balance / 4) Preset.mainnet.MIN_ACTIVATION_BALANCE) :
    ∀ i (hi : i < s.validators.length),
      rewardsEligible previous_epoch s.validators[i] = .ok true →
        s.validators[i].effective_balance ≤ 256 * s.balances.getD i 0 := by
  intro i hi helig
  obtain ⟨hlen, hall⟩ := hb
  have hi0 : i < s0.validators.length := hlen ▸ hi
  have hib : i < s0.balances.length := Nat.lt_of_lt_of_le hi0 h0.1
  obtain ⟨hmod, h34, -⟩ := h0.2 i hi0
  obtain ⟨v, b, hv, hbb, heb, -, hcase⟩ :=
    hall i _ _ (List.getElem?_eq_getElem hi0) (List.getElem?_eq_getElem hib)
  have hvv : s.validators[i] = v := by
    rw [List.getElem?_eq_getElem hi] at hv
    exact Option.some.inj hv
  have hgd : s.balances.getD i 0 = b := by simp [List.getD_eq_getElem?_getD, hbb]
  have hgd0 : s0.balances.getD i 0 = s0.balances[i] := by
    simp [List.getD_eq_getElem?_getD, List.getElem?_eq_getElem hib]
  rw [hvv] at helig ⊢
  rw [hgd, heb]
  rw [hgd0] at h34
  rcases hcase with ⟨hw, hx⟩ | hle
  · exact absurd helig (rewardsEligible_of_withdrawable (.inr hx) hw hfar (by decide) hE)
  · by_cases h0eb : s0.validators[i].effective_balance = 0
    · rw [h0eb]; exact Nat.zero_le _
    have hf := hfit i _ (List.getElem?_eq_getElem hi0) (Nat.pos_of_ne_zero h0eb)
    have hsl : slashLoss Preset.mainnet s0.validators[i] v ≤
        s0.validators[i].effective_balance / 4096 := by
      unfold slashLoss
      split
      · exact Nat.le_refl _
      · exact Nat.zero_le _
    have hMIN : Preset.mainnet.MIN_ACTIVATION_BALANCE = 32000000000 := rfl
    have hINC : Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT = 1000000000 := rfl
    rw [hMIN] at hle hf
    rw [hINC] at hmod
    generalize n * L i = y at hle hf
    generalize slashLoss Preset.mainnet s0.validators[i] v = z at hle hsl
    generalize s0.validators[i].effective_balance = eb at *
    generalize s0.balances[i] = b0 at *
    simp only [Nat.min_def, Gwei] at *
    split at hle <;> split at hf <;> omega

/-- On mainnet, the losses fit for every effective balance up to `MAX_EFFECTIVE_BALANCE_ELECTRA`,
if there are at most `SLOTS_PER_EPOCH = 32` sync aggregates and one sync aggregate costs at most
23,307,800 Gwei. The tightest case is an effective balance of 1 ETH and a balance of 0.75 ETH:
`32 * 23,307,800 + 244,140 + 3,906,250 = 749,999,990 ≤ 750,000,000`. -/
theorem lossFits_mainnet {n l eb : Nat} (hn : n ≤ 32) (hl : l ≤ 23307800)
    (hmod : eb % 1000000000 = 0) (hpos : 0 < eb) (hmax : eb ≤ 2048000000000) :
    n * l + eb / 4096 + eb / 256 ≤ min (3 * eb / 4) 32000000000 := by
  have hnl : n * l ≤ 32 * 23307800 := Nat.mul_le_mul hn hl
  generalize n * l = y at hnl ⊢
  simp only [Nat.min_def]
  split <;> omega

/-- On mainnet, every validator that is eligible for rewards at the epoch boundary has
`effective_balance ≤ 256 * balance`. The epoch starts at `s0` after the effective balance
updates. Its blocks run at most 32 sync aggregates, and each one costs each validator at most
23,307,800 Gwei (`sync_loss_le_mainnet`). -/
theorem epochEntry_effective_balance_mainnet (s0 s : BeaconState) (E previous_epoch n : Nat)
    (h0 : AfterEB Preset.mainnet s0)
    (hb : Budget Preset.mainnet s0 E (fun _ => 23307800) n s) (hn : n ≤ 32)
    (hfar : E < FAR_FUTURE_EPOCH) (hE : E ≤ previous_epoch + 1) :
    ∀ i (hi : i < s.validators.length),
      rewardsEligible previous_epoch s.validators[i] = .ok true →
        s.validators[i].effective_balance ≤ 256 * s.balances.getD i 0 := by
  refine budget_effective_balance_mainnet s0 s E previous_epoch n _ h0 hb hfar hE ?_
  intro i v0 hv0 hpos
  have hi : i < s0.validators.length := (List.getElem?_eq_some_iff.mp hv0).1
  obtain ⟨hmod, -, hmax⟩ := h0.2 i hi
  rw [List.getElem?_eq_getElem hi] at hv0
  cases hv0
  exact lossFits_mainnet hn (Nat.le_refl _) hmod hpos hmax

/-! ## The sync penalty on mainnet -/

/-- One loop of `integer_squareroot` ends at most at the floor square root, if `y` is the
Newton step of `x`. -/
theorem integer_squareroot_loop_mul_self_le (n : Nat) : ∀ x y : Nat,
    (x = 0 ∨ y = (x + n / x) / 2) →
    integer_squareroot.loop n x y * integer_squareroot.loop n x y ≤ n := by
  intro x
  induction x using Nat.strongRecOn with
  | ind x ih =>
    intro y hy
    rw [integer_squareroot.loop.eq_1]
    split
    · rename_i hlt
      exact ih y hlt _ (.inr rfl)
    · rename_i hge
      rcases hy with rfl | rfl
      · exact Nat.zero_le _
      · by_cases hx0 : x = 0
        · subst hx0; exact Nat.zero_le _
        · have hle : x ≤ n / x := by omega
          exact (Nat.le_div_iff_mul_le (Nat.pos_of_ne_zero hx0)).mp hle

/-- `integer_squareroot n` is at most the floor square root of `n`. With
`integer_squareroot_spec`, it is exactly the floor square root. -/
theorem integer_squareroot_mul_self_le (n : Nat) :
    integer_squareroot n * integer_squareroot n ≤ n := by
  unfold integer_squareroot
  split
  · rename_i h
    have : n = 2 ^ 64 - 1 := by simpa [UINT64_MAX] using h
    subst this
    decide
  · apply integer_squareroot_loop_mul_self_le
    by_cases hn0 : n = 0
    · exact .inl hn0
    · right
      rw [Nat.div_self (Nat.pos_of_ne_zero hn0)]

/-- The seat count of one validator is at most the number of bits. -/
theorem syncPenaltyCount_le_bits (state : BeaconState) (bits : List Bool) (i : ValidatorIndex) :
    syncPenaltyCount state bits i ≤ bits.length := by
  unfold syncPenaltyCount
  refine Nat.le_trans (List.length_filter_le _ _) ?_
  rw [List.length_zip]
  exact Nat.min_le_right _ _

/-- On mainnet, one sync aggregate lowers a balance by at most 23,307,800 Gwei, if it has at most
512 bits and the total active balance is at most 139 million ETH. Then `integer_squareroot` is
at most 372,924,813, and 512 seats cost at most `(372,924,813 + 2) / 16` Gwei. -/
theorem sync_loss_le_mainnet (state : BeaconState) (bits : List Bool) (i : ValidatorIndex)
    (total_active_balance : Gwei) (hbits : bits.length ≤ 512)
    (htab : total_active_balance ≤ 139000000000000000) :
    syncPenaltyCount state bits i * sync_participant_reward Preset.mainnet total_active_balance ≤
      23307800 := by
  have hc := Nat.le_trans (syncPenaltyCount_le_bits state bits i) hbits
  have hpr := sync_participant_reward_mainnet total_active_balance
  have hsq := integer_squareroot_mul_self_le total_active_balance
  generalize integer_squareroot total_active_balance = r at hpr hsq
  have hr : r ≤ 372924813 := by
    refine Nat.le_of_not_lt fun hlt => ?_
    have := Nat.mul_self_le_mul_self (Nat.succ_le_of_lt hlt)
    have h2 : (372924813 + 1) * (372924813 + 1) ≤ total_active_balance :=
      Nat.le_trans this hsq
    exact absurd (Nat.le_trans h2 htab) (by decide)
  generalize syncPenaltyCount state bits i = c at hc ⊢
  generalize sync_participant_reward Preset.mainnet total_active_balance = x at hpr ⊢
  have h1 : c * x * 16 ≤ x * 8192 := by
    calc c * x * 16 ≤ 512 * x * 16 := Nat.mul_le_mul_right _ (Nat.mul_le_mul_right _ hc)
      _ = x * 8192 := by rw [Nat.mul_comm 512 x, Nat.mul_assoc]
  have h2 : c * x * 16 ≤ 372924815 :=
    Nat.le_trans h1 (Nat.le_trans hpr (by simp only [Uint64] at *; omega))
  generalize c * x = y at h2 ⊢
  omega

/-- On mainnet, `process_sync_aggregate` keeps the budget with the uniform loss 23,307,800
Gwei and one more sync aggregate, if it has at most 512 bits and the total active balance is at
most 139 million ETH. -/
theorem Budget.of_sync_mainnet {s0 : BeaconState} {E : Epoch} {n : Nat} {o : Oracle}
    {s s' : BeaconState} {sync_aggregate : SyncAggregate}
    (h : process_sync_aggregate Preset.mainnet o s sync_aggregate = .ok s')
    (hbits : sync_aggregate.sync_committee_bits.length ≤ 512)
    (htab : ∀ tab, get_total_active_balance Preset.mainnet s = .ok tab →
      tab ≤ 139000000000000000)
    (hb : Budget Preset.mainnet s0 E (fun _ => 23307800) n s) :
    Budget Preset.mainnet s0 E (fun _ => 23307800) (n + 1) s' :=
  Budget.of_sync h (fun tab htab' i => sync_loss_le_mainnet s _ i tab hbits (htab tab htab')) hb

/-! ## The total active balance within an epoch -/

/-- `t` has the validators of `s` with the same effective balances and the same activity at
epoch `E`. -/
def ActiveAgree (E : Epoch) (s t : BeaconState) : Prop :=
  t.validators.length = s.validators.length ∧
    ∀ (i : Nat) (v v' : Validator), s.validators[i]? = some v → t.validators[i]? = some v' →
      v'.effective_balance = v.effective_balance ∧
        is_active_validator v' E = is_active_validator v E

/-- A state agrees with itself. -/
theorem ActiveAgree.refl (E : Epoch) (s : BeaconState) : ActiveAgree E s s := by
  refine ⟨rfl, fun i v v' h h' => ?_⟩
  rw [h] at h'
  cases h'
  exact ⟨rfl, rfl⟩

/-- `ActiveAgree` composes. -/
theorem ActiveAgree.trans {E : Epoch} {s1 s2 s3 : BeaconState} (h12 : ActiveAgree E s1 s2)
    (h23 : ActiveAgree E s2 s3) : ActiveAgree E s1 s3 := by
  refine ⟨h23.1.trans h12.1, fun i v1 v3 h1 h3 => ?_⟩
  have hi : i < s2.validators.length := by
    rw [h12.1]; exact (List.getElem?_eq_some_iff.mp h1).1
  have h2 := List.getElem?_eq_getElem hi
  obtain ⟨he12, ha12⟩ := h12.2 i _ _ h1 h2
  obtain ⟨he23, ha23⟩ := h23.2 i _ _ h2 h3
  exact ⟨he23.trans he12, ha23.trans ha12⟩

/-- Same validators agree. -/
theorem ActiveAgree.of_eq {E : Epoch} {s t : BeaconState} (hv : t.validators = s.validators) :
    ActiveAgree E s t := by
  rw [ActiveAgree, hv]
  exact (ActiveAgree.refl E s)

/-- An operation step keeps the activity at the current epoch. -/
theorem OpStep.activeAgree {p : Preset} {s s' : BeaconState} (h : OpStep p s s')
    (hfar : s.slot / p.SLOTS_PER_EPOCH < FAR_FUTURE_EPOCH) :
    ActiveAgree (s.slot / p.SLOTS_PER_EPOCH) s s' :=
  ⟨h.2.1, fun i v v' hv hv' =>
    have hst := h.2.2.1 i v v' hv hv'
    ⟨hst.1, hst.active_eq hfar⟩⟩

/-- A slashing step at `E` keeps the activity at `E`. -/
theorem SlashStep.activeAgree {p : Preset} {E : Epoch} {s s' : BeaconState}
    (h : SlashStep p E s s') (hfar : E < FAR_FUTURE_EPOCH) : ActiveAgree E s s' := by
  refine ⟨h.1, fun i v v' hv hv' => ?_⟩
  rcases h.2.1 i v v' hv hv' with rfl | ⟨-, -, heb, -, -, hact, hex⟩
  · exact ⟨rfl, rfl⟩
  · refine ⟨heb, ?_⟩
    unfold is_active_validator
    rw [hact]
    rcases hex with he | ⟨hf, hlt⟩
    · rw [he]
    · rw [hf]
      simp [hlt, hfar]

/-- `get_active_validator_indices` reads only the activity at the epoch. -/
private theorem active_indices_congr (E : Epoch) :
    ∀ (l l' : List Validator) (k : Nat), l'.length = l.length →
      (∀ (i : Nat) (v v' : Validator), l[i]? = some v → l'[i]? = some v' →
        is_active_validator v' E = is_active_validator v E) →
      ((l'.zipIdx k).filter fun (x : Validator × Nat) => is_active_validator x.1 E).map (·.2) =
        ((l.zipIdx k).filter fun (x : Validator × Nat) => is_active_validator x.1 E).map (·.2)
  | [], [], _, _, _ => rfl
  | [], _ :: _, _, hlen, _ => by simp at hlen
  | _ :: _, [], _, hlen, _ => by simp at hlen
  | v :: l, v' :: l', k, hlen, hact => by
    have h0 := hact 0 v v' rfl rfl
    have ih := active_indices_congr E l l' (k + 1) (by simpa using hlen)
      (fun i u u' hu hu' => hact (i + 1) u u' hu hu')
    simp only [List.zipIdx_cons, List.filter_cons, h0]
    split <;> simp [ih]

/-- `get_total_balance` reads only the effective balances at the indices. -/
private theorem total_balance_congr (p : Preset) (s t : BeaconState)
    (hlen : t.validators.length = s.validators.length)
    (heb : ∀ (i : Nat) (v v' : Validator), s.validators[i]? = some v →
      t.validators[i]? = some v' → v'.effective_balance = v.effective_balance)
    (indices : List ValidatorIndex) :
    get_total_balance p t indices = get_total_balance p s indices := by
  unfold get_total_balance
  have hmap : indices.mapM (fun index => do
        pure (← listGet t.validators index).effective_balance : ValidatorIndex → SpecM Gwei) =
      indices.mapM (fun index => do
        pure (← listGet s.validators index).effective_balance : ValidatorIndex → SpecM Gwei) := by
    congr 1
    funext index
    unfold listGet
    cases hs : s.validators[index]? with
    | none =>
      have : t.validators[index]? = none := by
        rw [List.getElem?_eq_none_iff] at hs ⊢
        omega
      simp [this]
    | some v =>
      have hi : index < t.validators.length := by
        rw [hlen]; exact (List.getElem?_eq_some_iff.mp hs).1
      have ht := List.getElem?_eq_getElem hi
      simp [ht, heb index v _ hs ht]
  rw [hmap]

/-- `get_total_active_balance` is the same for two states at the same epoch whose validators
agree on effective balances and activity. -/
theorem get_total_active_balance_congr (p : Preset) (s t : BeaconState)
    (hslot : t.slot / p.SLOTS_PER_EPOCH = s.slot / p.SLOTS_PER_EPOCH)
    (h : ActiveAgree (s.slot / p.SLOTS_PER_EPOCH) s t) :
    get_total_active_balance p t = get_total_active_balance p s := by
  have hcur : get_current_epoch p t = get_current_epoch p s := by
    simp only [get_current_epoch, compute_epoch_at_slot, uint64Div]
    split
    · rfl
    · rw [hslot]
  unfold get_total_active_balance
  rw [hcur]
  cases hc : get_current_epoch p s with
  | error e => rfl
  | ok e =>
    have he := get_current_epoch_ok_eq hc
    subst he
    simp only [bind, Except.bind]
    unfold get_active_validator_indices
    rw [active_indices_congr _ s.validators t.validators 0 h.1
      (fun i v v' hv hv' => (h.2 i v v' hv hv').2)]
    exact total_balance_congr p s t h.1 (fun i v v' hv hv' => (h.2 i v v' hv hv').1) _

/-! ## One block -/

/-- What one step inside a block keeps, for the budget of epoch `E` that starts at `s0`: the
slot, the activity at `E`, `ExitDelay`, and the budget with `n` sync aggregates. -/
def Keeps (p : Preset) (s0 : BeaconState) (E : Epoch) (L : Nat → Gwei) (n : Nat)
    (s t : BeaconState) : Prop :=
  t.slot = s.slot ∧ ActiveAgree E s t ∧
    (ExitDelay p s → ExitDelay p t ∧ (Budget p s0 E L n s → Budget p s0 E L n t))

/-- A state keeps everything to itself. -/
theorem Keeps.refl (p : Preset) (s0 : BeaconState) (E : Epoch) (L : Nat → Gwei) (n : Nat)
    (s : BeaconState) : Keeps p s0 E L n s s :=
  ⟨rfl, ActiveAgree.refl E s, fun hd => ⟨hd, id⟩⟩

/-- `Keeps` composes. -/
theorem Keeps.trans {p : Preset} {s0 : BeaconState} {E : Epoch} {L : Nat → Gwei} {n : Nat}
    {s1 s2 s3 : BeaconState} (h12 : Keeps p s0 E L n s1 s2) (h23 : Keeps p s0 E L n s2 s3) :
    Keeps p s0 E L n s1 s3 :=
  ⟨h23.1.trans h12.1, h12.2.1.trans h23.2.1, fun hd =>
    have h2 := h12.2.2 hd
    have h3 := h23.2.2 h2.1
    ⟨h3.1, fun hb => h3.2 (h2.2 hb)⟩⟩

/-- An operation step at epoch `E` keeps everything. -/
theorem Keeps.of_opStep {p : Preset} {s0 : BeaconState} {E : Epoch} {L : Nat → Gwei} {n : Nat}
    {s t : BeaconState} (hE : s.slot / p.SLOTS_PER_EPOCH = E) (hfar : E < FAR_FUTURE_EPOCH)
    (h : OpStep p s t) : Keeps p s0 E L n s t :=
  ⟨h.1, hE ▸ h.activeAgree (hE ▸ hfar), fun hd => ⟨h.exitDelay hd, Budget.of_opStep hfar h⟩⟩

/-- A slashing step at epoch `E` keeps everything. -/
theorem Keeps.of_slashStep {p : Preset} {s0 : BeaconState} {E : Epoch} {L : Nat → Gwei}
    {n : Nat} {s t : BeaconState} (hfar : E < FAR_FUTURE_EPOCH) (hslot : t.slot = s.slot)
    (h : SlashStep p E s t) : Keeps p s0 E L n s t :=
  ⟨hslot, h.activeAgree hfar, fun hd => ⟨h.exitDelay hd, Budget.of_slashStep h⟩⟩

/-- `process_withdrawals` at epoch `E` keeps everything. -/
theorem Keeps.of_withdrawals {p : Preset} {s0 : BeaconState} {E : Epoch} {L : Nat → Gwei}
    {n : Nat} {s t : BeaconState} (hE : s.slot / p.SLOTS_PER_EPOCH = E)
    (hfar : E < FAR_FUTURE_EPOCH)
    (hmax : p.MIN_ACTIVATION_BALANCE ≤ p.MAX_EFFECTIVE_BALANCE_ELECTRA)
    (h : process_withdrawals p s = .ok t) : Keeps p s0 E L n s t := by
  obtain ⟨hvs, hcp, -, -⟩ := process_withdrawals_core p s t h
  exact ⟨hcp.1, .of_eq hvs, fun hd =>
    ⟨process_withdrawals_exitDelay p s t h hd, Budget.of_withdrawals hE hfar hmax hd h⟩⟩

/-- A `foldlM` over a block's operations keeps every reflexive and transitive relation that
each operation keeps. -/
theorem foldlM_rel {α : Type} (R : BeaconState → BeaconState → Prop) (hrefl : ∀ s, R s s)
    (htrans : ∀ s1 s2 s3, R s1 s2 → R s2 s3 → R s1 s3)
    (f : BeaconState → α → SpecM BeaconState) (hf : ∀ st a st', f st a = .ok st' → R st st') :
    ∀ (l : List α) (s s' : BeaconState), l.foldlM f s = .ok s' → R s s' := by
  intro l
  induction l with
  | nil =>
    intro s s' h
    cases h
    exact hrefl s
  | cons a l ih =>
    intro s s' h
    rw [List.foldlM_cons] at h
    obtain ⟨s1, h1, h⟩ := specM_bind_ok h
    exact htrans _ _ _ (hf _ _ _ h1) (ih _ _ h)

/-- `Keeps` from a state at epoch `E`. -/
private def KeepsAt (p : Preset) (s0 : BeaconState) (E : Epoch) (L : Nat → Gwei) (n : Nat)
    (s t : BeaconState) : Prop :=
  s.slot / p.SLOTS_PER_EPOCH = E → Keeps p s0 E L n s t

/-- `KeepsAt` composes, since `Keeps` keeps the slot. -/
private theorem KeepsAt.trans {p : Preset} {s0 : BeaconState} {E : Epoch} {L : Nat → Gwei}
    {n : Nat} (s1 s2 s3 : BeaconState) (h12 : KeepsAt p s0 E L n s1 s2)
    (h23 : KeepsAt p s0 E L n s2 s3) : KeepsAt p s0 E L n s1 s3 := fun hE =>
  have k12 := h12 hE
  k12.trans (h23 (by rw [k12.1]; exact hE))

/-- A `foldlM` of steps that each keep everything keeps everything. -/
private theorem foldlM_keeps {α : Type} {p : Preset} {s0 : BeaconState} {E : Epoch}
    {L : Nat → Gwei} {n : Nat} (f : BeaconState → α → SpecM BeaconState)
    (hf : ∀ st a st', f st a = .ok st' → st.slot / p.SLOTS_PER_EPOCH = E →
      Keeps p s0 E L n st st')
    (l : List α) (s s' : BeaconState) (h : l.foldlM f s = .ok s')
    (hE : s.slot / p.SLOTS_PER_EPOCH = E) : Keeps p s0 E L n s s' :=
  foldlM_rel (KeepsAt p s0 E L n) (fun s _ => Keeps.refl p s0 E L n s) KeepsAt.trans f hf l s s'
    h hE

/-- `process_operations` at epoch `E` keeps everything. -/
theorem process_operations_keeps (p : Preset) (o : Oracle) (GLOAS_FORK_EPOCH : Epoch)
    (s0 : BeaconState) (E : Epoch) (L : Nat → Gwei) (n : Nat) (s t : BeaconState)
    (body : BeaconBlockBody) (parent_slot : Slot) (hE : s.slot / p.SLOTS_PER_EPOCH = E)
    (hfar : E < FAR_FUTURE_EPOCH)
    (h : process_operations p o GLOAS_FORK_EPOCH s body parent_slot = .ok t) :
    Keeps p s0 E L n s t := by
  unfold process_operations at h
  simp only [bind, Except.bind, pure, Except.pure, throw, throwThe, MonadExceptOf.throw] at h
  repeat' split at h
  all_goals first | (cases h; done) | skip
  rename_i _ s1 h1 _ s2 h2 _ s3 h3 _ s4 h4 _ s5 h5
  have k1 : Keeps p s0 E L n s s1 := foldlM_keeps _ (fun st a st' hst hE' =>
    Keeps.of_slashStep hfar (process_proposer_slashing_effects p o st st' a hst).2.1.1
      (process_proposer_slashing_slashStep p o st st' E hE' a hst)) _ _ _ h1 hE
  have hE1 : s1.slot / p.SLOTS_PER_EPOCH = E := by rw [k1.1]; exact hE
  have k2 : Keeps p s0 E L n s1 s2 := foldlM_keeps _ (fun st a st' hst hE' =>
    Keeps.of_slashStep hfar (process_attester_slashing_effects p o st st' a hst).2.1.1
      (process_attester_slashing_slashStep p o st st' E hE' a hst)) _ _ _ h2 hE1
  have hE2 : s2.slot / p.SLOTS_PER_EPOCH = E := by rw [k2.1]; exact hE1
  have k3 : Keeps p s0 E L n s2 s3 := foldlM_keeps _ (fun st a st' hst hE' =>
    Keeps.of_opStep hE' hfar (process_attestation_opStep p o st st' a parent_slot hst))
    _ _ _ h3 hE2
  have hE3 : s3.slot / p.SLOTS_PER_EPOCH = E := by rw [k3.1]; exact hE2
  have k4 : Keeps p s0 E L n s3 s4 := foldlM_keeps _ (fun st a st' hst hE' =>
    Keeps.of_opStep hE' hfar (process_voluntary_exit_opStep p o st st' a hst)) _ _ _ h4 hE3
  have hE4 : s4.slot / p.SLOTS_PER_EPOCH = E := by rw [k4.1]; exact hE3
  have k5 : Keeps p s0 E L n s4 s5 := foldlM_keeps _ (fun st a st' hst hE' =>
    Keeps.of_opStep hE' hfar (process_bls_to_execution_change_opStep p o st st' a hst))
    _ _ _ h5 hE4
  have hE5 : s5.slot / p.SLOTS_PER_EPOCH = E := by rw [k5.1]; exact hE4
  have k6 : Keeps p s0 E L n s5 t := foldlM_keeps _ (fun st a st' hst hE' =>
    Keeps.of_opStep hE' hfar
      (process_payload_attestation_opStep p o GLOAS_FORK_EPOCH st st' a hst)) _ _ _ h hE5
  exact k1.trans (k2.trans (k3.trans (k4.trans (k5.trans k6))))

/-- On mainnet, one `process_block` at epoch `E` keeps `ExitDelay`, the slot and the activity
at `E`, and keeps the budget with one more sync aggregate. The sync aggregate has at most 512
bits (it is a `BitVector[SYNC_COMMITTEE_SIZE]`), and the total active balance is at most 139
million ETH. The block does not change the total active balance before the sync aggregate. -/
theorem process_block_budget_mainnet (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (s0 : BeaconState) (E : Epoch) (n : Nat) (s s' : BeaconState)
    (block : BeaconBlock)
    (h : process_block Preset.mainnet o max_blobs_per_block GLOAS_FORK_EPOCH s block = .ok s')
    (hE : s.slot / Preset.mainnet.SLOTS_PER_EPOCH = E) (hfar : E < FAR_FUTURE_EPOCH)
    (hd : ExitDelay Preset.mainnet s)
    (hb : Budget Preset.mainnet s0 E (fun _ => 23307800) n s)
    (hbits : block.body.sync_aggregate.sync_committee_bits.length ≤ 512)
    (htab : ∀ tab, get_total_active_balance Preset.mainnet s = .ok tab →
      tab ≤ 139000000000000000) :
    Budget Preset.mainnet s0 E (fun _ => 23307800) (n + 1) s' ∧ ExitDelay Preset.mainnet s' ∧
      s'.slot = s.slot ∧ ActiveAgree E s s' := by
  unfold process_block at h
  obtain ⟨s1, h1, h⟩ := specM_bind_ok h
  obtain ⟨s2, h2, h⟩ := specM_bind_ok h
  obtain ⟨s3, h3, h⟩ := specM_bind_ok h
  obtain ⟨s4, h4, h⟩ := specM_bind_ok h
  obtain ⟨s5, h5, h⟩ := specM_bind_ok h
  obtain ⟨s6, h6, h⟩ := specM_bind_ok h
  obtain ⟨s7, h7, h⟩ := specM_bind_ok h
  let K := Keeps Preset.mainnet s0 E (fun _ => 23307800) n
  have k1 : K s s1 :=
    .of_opStep hE hfar (process_parent_execution_payload_opStep _ o s s1 block h1)
  have hE1 : s1.slot / Preset.mainnet.SLOTS_PER_EPOCH = E := by rw [k1.1]; exact hE
  have k2 : K s1 s2 := .of_opStep hE1 hfar (process_block_header_opStep _ o s1 s2 block h2)
  have hE2 : s2.slot / Preset.mainnet.SLOTS_PER_EPOCH = E := by rw [k2.1]; exact hE1
  have k3 : K s2 s3 := .of_withdrawals hE2 hfar (by decide) h3
  have hE3 : s3.slot / Preset.mainnet.SLOTS_PER_EPOCH = E := by rw [k3.1]; exact hE2
  have k4 : K s3 s4 :=
    .of_opStep hE3 hfar (process_execution_payload_bid_opStep _ o _ s3 s4 _ h4)
  have hE4 : s4.slot / Preset.mainnet.SLOTS_PER_EPOCH = E := by rw [k4.1]; exact hE3
  have k5 : K s4 s5 := .of_opStep hE4 hfar (process_randao_opStep _ o s4 s5 _ h5)
  have hE5 : s5.slot / Preset.mainnet.SLOTS_PER_EPOCH = E := by rw [k5.1]; exact hE4
  have k6 : K s5 s6 := .of_opStep hE5 hfar (process_eth1_data_opStep _ s5 s6 _ h6)
  have hE6 : s6.slot / Preset.mainnet.SLOTS_PER_EPOCH = E := by rw [k6.1]; exact hE5
  have k7 : K s6 s7 :=
    process_operations_keeps _ o GLOAS_FORK_EPOCH s0 E _ n s6 s7 _ _ hE6 hfar h7
  have k : K s s7 := k1.trans (k2.trans (k3.trans (k4.trans (k5.trans (k6.trans k7)))))
  obtain ⟨hslot, hagree, hkeep⟩ := k
  obtain ⟨hd7, hb7⟩ := hkeep hd
  have htab7 : ∀ tab, get_total_active_balance Preset.mainnet s7 = .ok tab →
      tab ≤ 139000000000000000 := by
    intro tab ht
    rw [get_total_active_balance_congr Preset.mainnet s s7 (by rw [hslot]) (hE ▸ hagree)] at ht
    exact htab tab ht
  obtain ⟨hfr, -⟩ := process_sync_aggregate_frame _ o s7 s' _ h
  have hvs : s'.validators = s7.validators := by rw [hfr]
  have hsl : s'.slot = s7.slot := by rw [hfr]
  exact ⟨Budget.of_sync_mainnet h hbits htab7 (hb7 hb),
    process_sync_aggregate_exitDelay _ o s7 s' _ h hd7, hsl.trans hslot,
    hagree.trans (.of_eq hvs)⟩

/-! ## One epoch of blocks on mainnet -/

/-- A successful `uint64Mod` is the `Nat` remainder. -/
private theorem uint64Mod_ok' {a b c : Nat} (h : uint64Mod a b = .ok c) : c = a % b := by
  unfold uint64Mod at h
  split at h
  · cases h
  · cases h; rfl

/-- The `process_slots` loop with the real `process_epoch` keeps `P`, if each step keeps `P`
while the slot is below the target. -/
theorem process_slots_loop_guard (p : Preset) (o : Oracle) (slot : Slot)
    (P : BeaconState → Prop)
    (hslot : ∀ s s', process_slot p o s = .ok s' → s.slot < slot → P s → P s')
    (hepoch : ∀ s s', s.slot < slot → (s.slot + 1) % p.SLOTS_PER_EPOCH = 0 →
      process_epoch p o s = .ok s' → P s → P s')
    (hbump : ∀ s, s.slot < slot → P s → P { s with slot := s.slot + 1 }) :
    ∀ fuel st st', P st →
      process_slots_loop p o (process_epoch p o) slot fuel st = .ok st' → P st' := by
  intro fuel
  induction fuel with
  | zero =>
    intro st st' hP h
    cases h
    exact hP
  | succ n ih =>
    intro st st' hP h
    simp only [process_slots_loop] at h
    split at h
    · rename_i hlt
      obtain ⟨s1, h1, h⟩ := specM_bind_ok h
      have hs1 := (process_slot_frame p o _ _ h1).2
      have hP1 := hslot _ _ h1 hlt hP
      have hlt1 : s1.slot < slot := hs1 ▸ hlt
      obtain ⟨a, ha, h⟩ := specM_bind_ok h
      obtain ⟨m, hm, h⟩ := specM_bind_ok h
      rw [uint64Add_ok ha] at hm
      have hm' := uint64Mod_ok' hm
      split at h
      · rename_i hz
        have hz' : (s1.slot + 1) % p.SLOTS_PER_EPOCH = 0 := by
          rw [← hm']; simpa using hz
        obtain ⟨s2, h2, h⟩ := specM_bind_ok h
        have hP2 := hepoch _ _ hlt1 hz' h2 hP1
        have hs2 := process_epoch_slot p o _ _ h2
        obtain ⟨c, hc, h⟩ := specM_bind_ok h
        rw [uint64Add_ok hc] at h
        exact ih _ _ (hbump _ (hs2 ▸ hlt1) hP2) h
      · obtain ⟨_, h2, h⟩ := specM_bind_ok h
        cases h2
        obtain ⟨c, hc, h⟩ := specM_bind_ok h
        rw [uint64Add_ok hc] at h
        exact ih _ _ (hbump _ hlt1 hP1) h
    · cases h
      exact hP

/-- The state of the blocks of epoch `E` on mainnet, from the start `s0` of the epoch. `r` is a
state of epoch `E` with the validators of the epoch. `n` sync aggregates ran, and there were at
most as many slots in the epoch so far. -/
def EpochInv (s0 r : BeaconState) (E n : Nat) (t : BeaconState) : Prop :=
  Budget Preset.mainnet s0 E (fun _ => 23307800) n t ∧ ExitDelay Preset.mainnet t ∧
    t.slot / Preset.mainnet.SLOTS_PER_EPOCH = E ∧ ActiveAgree E r t ∧
    n + E * Preset.mainnet.SLOTS_PER_EPOCH ≤ t.slot + 1

/-- At the start of epoch `E`, with the state `t` right after the slot moves into `E`.
`t` keeps the validators and balances of `s0`. -/
theorem EpochInv.start (s0 t : BeaconState) (E : Nat) (hd : ExitDelay Preset.mainnet s0)
    (hv : t.validators = s0.validators) (hb : t.balances = s0.balances)
    (hslot : t.slot = E * Preset.mainnet.SLOTS_PER_EPOCH) : EpochInv s0 t E 0 t := by
  refine ⟨Budget.of_eq hv hb (Budget.init _ s0 E _), ExitDelay.of_validators_eq hv hd, ?_,
    ActiveAgree.refl E t, ?_⟩
  · rw [hslot]; simp [Preset.mainnet]
  · rw [hslot]; omega

/-- `process_slots` inside epoch `E` keeps `EpochInv`. It runs no `process_epoch`, because the
target slot is in `E`. -/
theorem process_slots_epochInv (o : Oracle) (s0 r : BeaconState) (E n : Nat)
    (s s' : BeaconState) (slot : Slot) (hslotE : slot / Preset.mainnet.SLOTS_PER_EPOCH = E)
    (h : process_slots Preset.mainnet o (process_epoch Preset.mainnet o) s slot = .ok s')
    (hinv : EpochInv s0 r E n s) : EpochInv s0 r E n s' := by
  unfold process_slots at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  split at h
  · cases h
  refine process_slots_loop_guard _ o slot (EpochInv s0 r E n) ?_ ?_ ?_ _ _ _ hinv h
  · intro t t' ht _ hP
    obtain ⟨hf, hsl⟩ := process_slot_frame _ o t t' ht
    obtain ⟨h1, h2, h3, h4, h5⟩ := hP
    exact ⟨Budget.of_eq hf.1 hf.2.1 h1, ExitDelay.of_validators_eq hf.1 h2, hsl ▸ h3,
      h4.trans (.of_eq hf.1), hsl ▸ h5⟩
  · intro t t' hlt hz _ hP
    exfalso
    have h3 := hP.2.2.1
    have hS : Preset.mainnet.SLOTS_PER_EPOCH = 32 := rfl
    rw [hS] at h3 hz hslotE
    simp only [Slot] at *
    omega
  · intro t hlt hP
    obtain ⟨h1, h2, h3, h4, h5⟩ := hP
    refine ⟨h1, h2, ?_, h4, ?_⟩
    · show (t.slot + 1) / Preset.mainnet.SLOTS_PER_EPOCH = E
      have hS : Preset.mainnet.SLOTS_PER_EPOCH = 32 := rfl
      rw [hS] at h3 hslotE ⊢
      simp only [Slot] at *
      omega
    · show n + E * Preset.mainnet.SLOTS_PER_EPOCH ≤ t.slot + 1 + 1
      simp only [Slot] at *
      omega

/-- On mainnet, one `state_transition` inside epoch `E` keeps `EpochInv` with one more sync
aggregate. The block's slot is in `E`, its sync aggregate has at most 512 bits, and the total
active balance of `E` is at most 139 million ETH. -/
theorem state_transition_epochInv (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (s0 r : BeaconState) (E n : Nat) (s s' : BeaconState)
    (signed_block : SignedBeaconBlock) (validate_result : Bool)
    (hblockE : signed_block.message.slot / Preset.mainnet.SLOTS_PER_EPOCH = E)
    (hfar : E < FAR_FUTURE_EPOCH)
    (hbits : signed_block.message.body.sync_aggregate.sync_committee_bits.length ≤ 512)
    (hr : r.slot / Preset.mainnet.SLOTS_PER_EPOCH = E)
    (htab : ∀ tab, get_total_active_balance Preset.mainnet r = .ok tab →
      tab ≤ 139000000000000000)
    (h : state_transition Preset.mainnet o max_blobs_per_block GLOAS_FORK_EPOCH s signed_block
      validate_result = .ok s')
    (hinv : EpochInv s0 r E n s) : EpochInv s0 r E (n + 1) s' := by
  unfold state_transition at h
  obtain ⟨s1, h1, h⟩ := specM_bind_ok h
  have hlt : s.slot < signed_block.message.slot := by
    unfold process_slots at h1
    simp only [bind, Except.bind, pure, Except.pure] at h1
    split at h1
    · cases h1
    · exact Decidable.of_not_not ‹_›
  have hs1 := process_slots_process_epoch_slot _ o s s1 _ h1
  have hn0 := hinv.2.2.2.2
  have hinv1 := process_slots_epochInv o s0 r E n s s1 _ hblockE h1 hinv
  obtain ⟨hb1, hd1, hE1, hag1, hn1⟩ := hinv1
  have htab1 : ∀ tab, get_total_active_balance Preset.mainnet s1 = .ok tab →
      tab ≤ 139000000000000000 := by
    intro tab ht
    rw [get_total_active_balance_congr _ r s1 (by rw [hE1, hr]) (hr ▸ hag1)] at ht
    exact htab tab ht
  have hblock : ∀ s2, process_block Preset.mainnet o max_blobs_per_block GLOAS_FORK_EPOCH s1
      signed_block.message = .ok s2 → EpochInv s0 r E (n + 1) s2 := by
    intro s2 h2
    obtain ⟨hb2, hd2, hsl2, hag2⟩ :=
      process_block_budget_mainnet o _ _ s0 E n s1 s2 _ h2 hE1 hfar hd1 hb1 hbits htab1
    refine ⟨hb2, hd2, hsl2 ▸ hE1, hag1.trans hag2, ?_⟩
    rw [hsl2, hs1]
    simp only [Slot] at *
    omega
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact hblock _ ‹_›)

/-- On mainnet, at the end of epoch `E` the state `t` that enters `process_epoch` has
`effective_balance ≤ 256 * balance` for every validator that is eligible for rewards at the
previous epoch `E - 1`. `s0` is the state after the effective balance updates of epoch
`E - 1`. -/
theorem boundary_effective_balance_mainnet (s0 r t : BeaconState) (E n previous_epoch : Nat)
    (h0 : AfterEB Preset.mainnet s0) (hinv : EpochInv s0 r E n t)
    (hfar : E < FAR_FUTURE_EPOCH) (hE : E ≤ previous_epoch + 1) :
    ∀ i (hi : i < t.validators.length),
      rewardsEligible previous_epoch t.validators[i] = .ok true →
        t.validators[i].effective_balance ≤ 256 * t.balances.getD i 0 := by
  obtain ⟨hb, -, hslot, -, hn⟩ := hinv
  have hn32 : n ≤ 32 := by
    have hS : Preset.mainnet.SLOTS_PER_EPOCH = 32 := rfl
    rw [hS] at hslot hn
    simp only [Slot] at *
    omega
  exact epochEntry_effective_balance_mainnet s0 t E previous_epoch n h0 hb hn32 hfar hE

end EpochProofs.Spec
