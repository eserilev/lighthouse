import EpochProofs.Spec.Block.ExecutionRequests
import EpochProofs.Sanity.Effects

/-!
# Effects of the execution requests

`RequestEffect` is the effect of one request on validators, balances and checkpoints. A
validator keeps its effective balance. A balance stays, or drops to `MIN_ACTIVATION_BALANCE`
from above it. Exit and withdrawable epochs stay, or end in order. Builder requests do not
touch validators or balances.
-/

namespace EpochProofs.Spec

/-- A validator keeps its effective balance. Its exit and withdrawable epochs stay, or the new
exit epoch is at most the new withdrawable epoch. -/
def ValidatorStep (v v' : Validator) : Prop :=
  v'.effective_balance = v.effective_balance ∧
    ((v'.exit_epoch = v.exit_epoch ∧ v'.withdrawable_epoch = v.withdrawable_epoch) ∨
      v'.exit_epoch ≤ v'.withdrawable_epoch)

/-- A balance stays, or drops to `MIN_ACTIVATION_BALANCE` from above it. -/
def BalanceStep (p : Preset) (b b' : Gwei) : Prop :=
  b' = b ∨ (b' = p.MIN_ACTIVATION_BALANCE ∧ p.MIN_ACTIVATION_BALANCE < b)

/-- The effect of execution requests on validators, balances, the slot and the checkpoints. -/
def RequestEffect (p : Preset) (s s' : BeaconState) : Prop :=
  s'.validators.length = s.validators.length ∧
    (∀ (i : Nat) (v : Validator), s.validators[i]? = some v →
      ∃ v', s'.validators[i]? = some v' ∧ ValidatorStep v v') ∧
    s'.balances.length = s.balances.length ∧
    (∀ (i : Nat) (b : Gwei), s.balances[i]? = some b →
      ∃ b', s'.balances[i]? = some b' ∧ BalanceStep p b b') ∧
    CheckpointsStable s s'

/-- A validator that does not change takes a validator step. -/
theorem ValidatorStep.refl (v : Validator) : ValidatorStep v v := ⟨rfl, .inl ⟨rfl, rfl⟩⟩

/-- Two validator steps compose. -/
theorem ValidatorStep.trans {v1 v2 v3 : Validator} (h12 : ValidatorStep v1 v2)
    (h23 : ValidatorStep v2 v3) : ValidatorStep v1 v3 := by
  refine ⟨h23.1.trans h12.1, ?_⟩
  rcases h23.2 with ⟨he, hw⟩ | h
  · rcases h12.2 with ⟨he', hw'⟩ | h'
    · exact .inl ⟨he.trans he', hw.trans hw'⟩
    · exact .inr (by rw [he, hw]; exact h')
  · exact .inr h

/-- A balance that does not change takes a balance step. -/
theorem BalanceStep.refl (p : Preset) (b : Gwei) : BalanceStep p b b := .inl rfl

/-- Two balance steps compose. A balance at `MIN_ACTIVATION_BALANCE` does not drop again. -/
theorem BalanceStep.trans {p : Preset} {b1 b2 b3 : Gwei} (h12 : BalanceStep p b1 b2)
    (h23 : BalanceStep p b2 b3) : BalanceStep p b1 b3 := by
  rcases h23 with h | ⟨h, hlt⟩
  · rw [h]; exact h12
  · rcases h12 with h' | ⟨h', -⟩
    · rw [← h']; exact .inr ⟨h, hlt⟩
    · rw [h'] at hlt; exact absurd hlt (Nat.lt_irrefl _)

/-- A balance step never goes below `min b MIN_ACTIVATION_BALANCE` and never goes up. -/
theorem BalanceStep.bounds {p : Preset} {b b' : Gwei} (h : BalanceStep p b b') :
    min b p.MIN_ACTIVATION_BALANCE ≤ b' ∧ b' ≤ b := by
  rcases h with h | ⟨h, hlt⟩
  · subst h; exact ⟨Nat.min_le_left _ _, Nat.le_refl _⟩
  · subst h; exact ⟨Nat.min_le_right _ _, Nat.le_of_lt hlt⟩

/-- No change is a request effect. -/
theorem RequestEffect.refl (p : Preset) (s : BeaconState) : RequestEffect p s s :=
  ⟨rfl, fun _ v h => ⟨v, h, .refl v⟩, rfl, fun _ b h => ⟨b, h, .refl p b⟩,
    CheckpointsStable.refl s⟩

/-- Request effects compose. -/
theorem RequestEffect.trans {p : Preset} {s1 s2 s3 : BeaconState} (h12 : RequestEffect p s1 s2)
    (h23 : RequestEffect p s2 s3) : RequestEffect p s1 s3 := by
  refine ⟨h23.1.trans h12.1, fun i v h => ?_, h23.2.2.1.trans h12.2.2.1, fun i b h => ?_,
    h12.2.2.2.2.trans h23.2.2.2.2⟩
  · obtain ⟨v2, h2, hs2⟩ := h12.2.1 i v h
    obtain ⟨v3, h3, hs3⟩ := h23.2.1 i v2 h2
    exact ⟨v3, h3, hs2.trans hs3⟩
  · obtain ⟨b2, h2, hs2⟩ := h12.2.2.2.1 i b h
    obtain ⟨b3, h3, hs3⟩ := h23.2.2.2.1 i b2 h2
    exact ⟨b3, h3, hs2.trans hs3⟩

/-- Execution requests keep the registry size and all effective balances. -/
theorem RequestEffect.ebStable {p : Preset} {s s' : BeaconState} (h : RequestEffect p s s') :
    EBStable s s' := by
  refine ⟨Nat.le_of_eq h.1.symm, fun i v hv => ?_⟩
  obtain ⟨v', hv', hs⟩ := h.2.1 i v hv
  exact ⟨v', hv', hs.1⟩

/-- A request effect keeps the exit order. -/
theorem RequestEffect.exitOrder {p : Preset} {s s' : BeaconState} (h : RequestEffect p s s')
    (hs : ExitOrder s) : ExitOrder s' := by
  intro v' hmem
  obtain ⟨i, hi, hget⟩ := List.getElem_of_mem hmem
  have hi' : i < s.validators.length := h.1 ▸ hi
  obtain ⟨v2, hv2, hstep⟩ := h.2.1 i s.validators[i] (List.getElem?_eq_getElem hi')
  have hv' : v2 = v' := by
    rw [List.getElem?_eq_getElem hi, hget] at hv2
    exact (Option.some.inj hv2).symm
  subst hv'
  rcases hstep.2 with ⟨he, hw⟩ | hle
  · rw [he, hw]; exact hs _ (List.getElem_mem hi')
  · exact hle

/-- Each balance after a request is at least `min old MIN_ACTIVATION_BALANCE` and at most the
old balance. -/
theorem RequestEffect.balance_bounds {p : Preset} {s s' : BeaconState}
    (h : RequestEffect p s s') (i : Nat) (b : Gwei) (hb : s.balances[i]? = some b) :
    ∃ b', s'.balances[i]? = some b' ∧ min b p.MIN_ACTIVATION_BALANCE ≤ b' ∧ b' ≤ b := by
  obtain ⟨b', hb', hs⟩ := h.2.2.2.1 i b hb
  exact ⟨b', hb', hs.bounds⟩

/-- Same validators, balances and checkpoints give a request effect. -/
theorem RequestEffect.of_eq {p : Preset} {s s' : BeaconState} (hv : s'.validators = s.validators)
    (hb : s'.balances = s.balances) (hc : CheckpointsStable s s') : RequestEffect p s s' := by
  refine ⟨by rw [hv], fun i v h => ⟨v, by rw [hv]; exact h, .refl v⟩, by rw [hb],
    fun i b h => ⟨b, by rw [hb]; exact h, .refl p b⟩, hc⟩

/-- One validator write with a validator step gives a request effect. -/
theorem RequestEffect.of_set_validator {p : Preset} {s s' : BeaconState} {i : Nat}
    {v v' : Validator} (hv : s.validators[i]? = some v)
    (hset : s'.validators = s.validators.set i v') (hstep : ValidatorStep v v')
    (hb : s'.balances = s.balances) (hc : CheckpointsStable s s') : RequestEffect p s s' := by
  refine ⟨by rw [hset, List.length_set], fun j w hw => ?_, by rw [hb],
    fun j b h => ⟨b, by rw [hb]; exact h, .refl p b⟩, hc⟩
  rw [hset, List.getElem?_set]
  by_cases hij : i = j
  · subst hij
    rw [hv] at hw
    cases hw
    have hlt : i < s.validators.length := (List.getElem?_eq_some_iff.mp hv).1
    exact ⟨v', by simp [hlt], hstep⟩
  · exact ⟨w, by simp [hij, hw], .refl w⟩

/-- One balance write with a balance step gives a request effect. -/
theorem RequestEffect.of_set_balance {p : Preset} {s s' : BeaconState} {i : Nat} {b b' : Gwei}
    (hb : s.balances[i]? = some b) (hv : s'.validators = s.validators)
    (hset : s'.balances = s.balances.set i b') (hstep : BalanceStep p b b')
    (hc : CheckpointsStable s s') : RequestEffect p s s' := by
  refine ⟨by rw [hv], fun j w h => ⟨w, by rw [hv]; exact h, .refl w⟩, by rw [hset, List.length_set],
    fun j c hc' => ?_, hc⟩
  rw [hset, List.getElem?_set]
  by_cases hij : i = j
  · subst hij
    rw [hb] at hc'
    cases hc'
    have hlt : i < s.balances.length := (List.getElem?_eq_some_iff.mp hb).1
    exact ⟨b', by simp [hlt], hstep⟩
  · exact ⟨c, by simp [hij, hc'], .refl p c⟩

/-- `uint64Add` succeeds exactly when the sum fits. -/
private theorem uint64Add_iff {a b c : Nat} :
    uint64Add a b = .ok c ↔ a + b < UINT64_SIZE ∧ c = a + b := by
  unfold uint64Add
  by_cases hlt : a + b < UINT64_SIZE
  · simp [hlt, pure, Except.pure, eq_comm]
  · simp [hlt, throw, throwThe, MonadExceptOf.throw]

/-- A successful `listGet` reads the list. -/
private theorem listGet_some {α : Type} {l : List α} {i : Nat} {a : α}
    (h : listGet l i = .ok a) : l[i]? = some a := by
  unfold listGet at h
  cases hl : l[i]? with
  | none => simp [hl, throw, throwThe, MonadExceptOf.throw] at h
  | some b => simp only [hl, pure, Except.pure, Except.ok.injEq] at h; rw [h]

/-- A successful `listSet` is `List.set`. -/
private theorem listSet_eq {α : Type} {l l' : List α} {i : Nat} {a : α}
    (h : listSet l i a = .ok l') : l' = l.set i a := by
  unfold listSet at h
  split at h
  · cases h; rfl
  · cases h

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

/-- `compute_activation_exit_epoch` is `epoch + 1 + MAX_SEED_LOOKAHEAD`. -/
private theorem compute_activation_exit_epoch_eq {p : Preset} {c a : Nat}
    (h : compute_activation_exit_epoch p c = .ok a) : a = c + 1 + p.MAX_SEED_LOOKAHEAD := by
  unfold compute_activation_exit_epoch at h
  obtain ⟨b, hb, h⟩ := specM_bind_ok h
  rw [uint64Add_iff] at hb h
  omega

/-- `compute_exit_epoch_and_update_churn` returns an epoch at least `current + 1 +
MAX_SEED_LOOKAHEAD`. It only writes the two exit churn fields. -/
theorem compute_exit_epoch_and_update_churn_bound (p : Preset) (tab : Gwei)
    (s s' : BeaconState) (exit_balance e : Nat)
    (h : compute_exit_epoch_and_update_churn p tab s exit_balance = .ok (e, s')) :
    ∃ c : Nat, get_current_epoch p s = .ok c ∧ c + 1 + p.MAX_SEED_LOOKAHEAD ≤ e ∧
      s'.validators = s.validators ∧ s'.balances = s.balances ∧ CheckpointsStable s s' := by
  unfold compute_exit_epoch_and_update_churn at h
  obtain ⟨c, hc, h⟩ := specM_bind_ok h
  obtain ⟨a, ha, h⟩ := specM_bind_ok h
  obtain ⟨churn, -, h⟩ := specM_bind_ok h
  have hmax : (c : Nat) + 1 + p.MAX_SEED_LOOKAHEAD ≤ (max s.earliest_exit_epoch a : Nat) :=
    (compute_activation_exit_epoch_eq ha) ▸ Nat.le_max_right _ _
  generalize max s.earliest_exit_epoch a = m at h hmax
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | (cases h; done)
    | (cases h
       refine ⟨c, hc, Nat.le_trans hmax ?_, rfl, rfl, CheckpointsStable.refl _⟩
       simp only [uint64Add_iff] at *
       omega)

/-- `compute_consolidation_epoch_and_update_churn` returns an epoch at least `current + 1 +
MAX_SEED_LOOKAHEAD`. It only writes the two consolidation churn fields. -/
theorem compute_consolidation_epoch_and_update_churn_bound (p : Preset) (tab : Gwei)
    (s s' : BeaconState) (consolidation_balance e : Nat)
    (h : compute_consolidation_epoch_and_update_churn p tab s consolidation_balance
      = .ok (e, s')) :
    ∃ c : Nat, get_current_epoch p s = .ok c ∧ c + 1 + p.MAX_SEED_LOOKAHEAD ≤ e ∧
      s'.validators = s.validators ∧ s'.balances = s.balances ∧ CheckpointsStable s s' := by
  unfold compute_consolidation_epoch_and_update_churn at h
  obtain ⟨c, hc, h⟩ := specM_bind_ok h
  obtain ⟨a, ha, h⟩ := specM_bind_ok h
  obtain ⟨churn, -, h⟩ := specM_bind_ok h
  have hmax : (c : Nat) + 1 + p.MAX_SEED_LOOKAHEAD
      ≤ (max s.earliest_consolidation_epoch a : Nat) :=
    (compute_activation_exit_epoch_eq ha) ▸ Nat.le_max_right _ _
  generalize max s.earliest_consolidation_epoch a = m at h hmax
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | (cases h; done)
    | (cases h
       refine ⟨c, hc, Nat.le_trans hmax ?_, rfl, rfl, CheckpointsStable.refl _⟩
       simp only [uint64Add_iff] at *
       omega)

/-- `initiate_validator_exit` keeps balances and checkpoints. It changes nothing, or it sets
the exit epoch of one validator with `exit_epoch = FAR_FUTURE_EPOCH` to an epoch `e` at least
`current + 1 + MAX_SEED_LOOKAHEAD`, and its withdrawable epoch to
`e + MIN_VALIDATOR_WITHDRAWABILITY_DELAY`. -/
theorem initiate_validator_exit_cases (p : Preset) (tab : Gwei) (s s' : BeaconState)
    (index : Nat) (h : initiate_validator_exit p tab s index = .ok s') :
    s'.balances = s.balances ∧ CheckpointsStable s s' ∧
      (s'.validators = s.validators ∨
        ∃ (v : Validator) (c e : Nat), s.validators[index]? = some v ∧
          v.exit_epoch = FAR_FUTURE_EPOCH ∧ get_current_epoch p s = .ok c ∧
          c + 1 + p.MAX_SEED_LOOKAHEAD ≤ e ∧
          s'.validators = s.validators.set index { v with
            exit_epoch := e
            withdrawable_epoch := e + p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY }) := by
  unfold initiate_validator_exit at h
  obtain ⟨v, hv, h⟩ := specM_bind_ok h
  by_cases hfar : v.exit_epoch = FAR_FUTURE_EPOCH
  · simp only [hfar, bne_self_eq_false, Bool.false_eq_true, if_false] at h
    obtain ⟨_, -, h⟩ := specM_bind_ok h
    obtain ⟨⟨e, s1⟩, hx, h⟩ := specM_bind_ok h
    obtain ⟨w, hw, h⟩ := specM_bind_ok h
    obtain ⟨l, hl, h⟩ := specM_bind_ok h
    cases h
    obtain ⟨c, hc, hle, hvs, hbs, hcs⟩ :=
      compute_exit_epoch_and_update_churn_bound p tab s s1 _ e hx
    rw [uint64Add_iff] at hw
    refine ⟨hbs, hcs, .inr ⟨v, c, e, listGet_some hv, hfar, hc, hle, ?_⟩⟩
    rw [listSet_eq hl, hvs, hw.2]
  · have hne : (v.exit_epoch != FAR_FUTURE_EPOCH) = true := by simp [hfar]
    simp only [hne, if_true, pure, Except.pure, Except.ok.injEq] at h
    subst h
    exact ⟨rfl, CheckpointsStable.refl _, .inl rfl⟩

/-- `initiate_validator_exit` keeps balances and has a request effect. -/
theorem initiate_validator_exit_effect {p : Preset} {tab : Gwei} {s s' : BeaconState}
    {index : Nat} (h : initiate_validator_exit p tab s index = .ok s') :
    s'.balances = s.balances ∧ RequestEffect p s s' := by
  obtain ⟨hb, hc, hv | ⟨v, c, e, hv, -, -, -, hset⟩⟩ :=
    initiate_validator_exit_cases p tab s s' index h
  · exact ⟨hb, .of_eq hv hb hc⟩
  · refine ⟨hb, .of_set_validator hv hset ⟨rfl, .inr ?_⟩ hb hc⟩
    exact Nat.le_add_right _ _

/-- A churn update and then a write to another field of the state has a request effect. -/
private theorem exit_churn_effect {p : Preset} {tab : Gwei} {s : BeaconState} {x : Nat}
    {r : Epoch × BeaconState} (h : compute_exit_epoch_and_update_churn p tab s x = .ok r)
    (f : BeaconState → BeaconState) (hfv : ∀ t, (f t).validators = t.validators)
    (hfb : ∀ t, (f t).balances = t.balances) (hfc : ∀ t, CheckpointsStable t (f t)) :
    (f r.2).balances = s.balances ∧ RequestEffect p s (f r.2) := by
  obtain ⟨e, s1⟩ := r
  obtain ⟨-, -, -, hv, hb, hc⟩ := compute_exit_epoch_and_update_churn_bound p tab s s1 x e h
  refine ⟨(hfb s1).trans hb, .of_eq ((hfv s1).trans hv) ((hfb s1).trans hb) (hc.trans (hfc s1))⟩

/-- `process_deposit_request` only appends to `pending_deposits`. -/
theorem process_deposit_request_effect (p : Preset) (s s' : BeaconState)
    (deposit_request : DepositRequest)
    (h : process_deposit_request s deposit_request = .ok s') :
    s'.balances = s.balances ∧ RequestEffect p s s' := by
  cases h
  exact ⟨rfl, .of_eq rfl rfl (CheckpointsStable.refl _)⟩

/-- `process_withdrawal_request` keeps balances. A full exit goes through
`initiate_validator_exit`. A partial withdrawal only writes the exit churn and the queue. -/
theorem process_withdrawal_request_effect (p : Preset) (s s' : BeaconState)
    (withdrawal_request : WithdrawalRequest)
    (h : process_withdrawal_request p s withdrawal_request = .ok s') :
    s'.balances = s.balances ∧ RequestEffect p s s' := by
  unfold process_withdrawal_request at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | (cases h; done)
    | (cases h; exact ⟨rfl, RequestEffect.refl p _⟩)
    | (cases h
       exact exit_churn_effect ‹compute_exit_epoch_and_update_churn _ _ _ _ = .ok _›
         (fun t => { t with pending_partial_withdrawals := _ }) (fun _ => rfl) (fun _ => rfl)
         (fun _ => CheckpointsStable.refl _))
    | (cases h; exact initiate_validator_exit_effect ‹initiate_validator_exit _ _ _ _ = .ok _›)

/-- `queue_excess_active_balance` keeps validators. It keeps the balance at `index`, or it
lowers it from above `MIN_ACTIVATION_BALANCE` to `MIN_ACTIVATION_BALANCE`. -/
theorem queue_excess_active_balance_cases (p : Preset) (s s' : BeaconState) (index : Nat)
    (h : queue_excess_active_balance p s index = .ok s') :
    s'.validators = s.validators ∧ CheckpointsStable s s' ∧
      (s'.balances = s.balances ∨
        ∃ b, s.balances[index]? = some b ∧ p.MIN_ACTIVATION_BALANCE < b ∧
          s'.balances = s.balances.set index p.MIN_ACTIVATION_BALANCE) := by
  unfold queue_excess_active_balance at h
  obtain ⟨b, hb, h⟩ := specM_bind_ok h
  split at h
  · obtain ⟨_, -, h⟩ := specM_bind_ok h
    obtain ⟨l, hl, h⟩ := specM_bind_ok h
    obtain ⟨_, -, h⟩ := specM_bind_ok h
    cases h
    exact ⟨rfl, CheckpointsStable.refl _, .inr ⟨b, listGet_some hb, ‹_›, listSet_eq hl⟩⟩
  · cases h
    exact ⟨rfl, CheckpointsStable.refl _, .inl rfl⟩

/-- `queue_excess_active_balance` has a request effect. -/
theorem queue_excess_active_balance_effect {p : Preset} {s s' : BeaconState} {index : Nat}
    (h : queue_excess_active_balance p s index = .ok s') : RequestEffect p s s' := by
  obtain ⟨hv, hc, hb | ⟨b, hb, hlt, hset⟩⟩ := queue_excess_active_balance_cases p s s' index h
  · exact .of_eq hv hb hc
  · exact .of_set_balance hb hv hset (.inr ⟨rfl, hlt⟩) hc

/-- `switch_to_compounding_validator` writes the withdrawal credentials of one validator and
then runs `queue_excess_active_balance`. -/
theorem switch_to_compounding_validator_effect {p : Preset} {s s' : BeaconState} {index : Nat}
    (h : switch_to_compounding_validator p s index = .ok s') : RequestEffect p s s' := by
  unfold switch_to_compounding_validator at h
  obtain ⟨v, hv, h⟩ := specM_bind_ok h
  obtain ⟨l, hl, h⟩ := specM_bind_ok h
  refine RequestEffect.trans ?_ (queue_excess_active_balance_effect h)
  exact .of_set_validator (listGet_some hv) (listSet_eq hl) ⟨rfl, .inl ⟨rfl, rfl⟩⟩ rfl
    (CheckpointsStable.refl _)

/-- The last step of a consolidation: the churn update, the exit and withdrawable epochs of the
source, and the append to the queue. -/
private theorem consolidation_exit_effect {p : Preset} {tab : Gwei} {s : BeaconState} {x i : Nat}
    {v v' : Validator} {r : Epoch × BeaconState} {w : Uint64} {l : List Validator}
    (hr : compute_consolidation_epoch_and_update_churn p tab s x = .ok r)
    (hw : uint64Add r.1 p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY = .ok w)
    (hl : listSet r.2.validators i v' = .ok l) (hv : listGet s.validators i = .ok v)
    (hv' : v'.effective_balance = v.effective_balance ∧ v'.exit_epoch = r.1 ∧
      v'.withdrawable_epoch = w)
    (pc : List PendingConsolidation) :
    RequestEffect p s { r.2 with validators := l, pending_consolidations := pc } := by
  obtain ⟨e, s1⟩ := r
  obtain ⟨-, -, -, hvs, hbs, hcs⟩ :=
    compute_consolidation_epoch_and_update_churn_bound p tab s s1 x e hr
  rw [uint64Add_iff] at hw
  rw [hvs] at hl
  refine .of_set_validator (listGet_some hv) (listSet_eq hl) ⟨hv'.1, .inr ?_⟩ hbs hcs
  rw [hv'.2.1, hv'.2.2, hw.2]
  exact Nat.le_add_right _ _

/-- `process_consolidation_request` has a request effect. A switch to compounding lowers at
most one balance to `MIN_ACTIVATION_BALANCE`. A consolidation sets the exit and withdrawable
epochs of the source in order. -/
theorem process_consolidation_request_effect (p : Preset) (s s' : BeaconState)
    (consolidation_request : ConsolidationRequest)
    (h : process_consolidation_request p s consolidation_request = .ok s') :
    RequestEffect p s s' := by
  unfold process_consolidation_request at h
  peel_ok h
  all_goals first
    | (cases h; exact RequestEffect.refl p _)
    | exact switch_to_compounding_validator_effect h
    | (cases h
       exact consolidation_exit_effect
         ‹compute_consolidation_epoch_and_update_churn _ _ _ _ = .ok _› ‹uint64Add _ _ = .ok _›
         ‹listSet _ _ _ = .ok _› ‹listGet _ _ = .ok _› (by exact ⟨rfl, rfl, rfl⟩) _)


/-- Validators, balances, inactivity scores, the slot and the checkpoints stay. -/
def BuilderFrame (s s' : BeaconState) : Prop := RegistryFrame s s' ∧ CheckpointsStable s s'

/-- A builder frame is a request effect. -/
theorem BuilderFrame.requestEffect {p : Preset} {s s' : BeaconState} (h : BuilderFrame s s') :
    RequestEffect p s s' :=
  .of_eq h.1.1 h.1.2.1 h.2

/-- Closes a goal `BuilderFrame s { s with ... }` where the update leaves the frame fields. -/
local macro "builder_frame_rfl" : tactic =>
  `(tactic| exact ⟨⟨rfl, rfl, rfl⟩, CheckpointsStable.refl _⟩)

/-- `add_builder_to_registry` only writes `builders`. -/
theorem add_builder_to_registry_frame (p : Preset) (s s' : BeaconState) (pubkey : BLSPubkey)
    (version : UInt8) (execution_address : ExecutionAddress) (amount : Gwei) (slot : Slot)
    (h : add_builder_to_registry p s pubkey version execution_address amount slot = .ok s') :
    BuilderFrame s s' := by
  unfold add_builder_to_registry at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | (cases h; done)
    | (cases h; builder_frame_rfl)

/-- `initiate_builder_exit` only writes `builders`. -/
theorem initiate_builder_exit_frame (p : Preset) (s s' : BeaconState) (builder_index : Nat)
    (h : initiate_builder_exit p s builder_index = .ok s') : BuilderFrame s s' := by
  unfold initiate_builder_exit at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | (cases h; done)
    | (cases h; builder_frame_rfl)

/-- `process_builder_deposit_request` does not touch validators or their balances. -/
theorem process_builder_deposit_request_frame (p : Preset) (o : Oracle) (s s' : BeaconState)
    (request : BuilderDepositRequest)
    (h : process_builder_deposit_request p o s request = .ok s') : BuilderFrame s s' := by
  unfold process_builder_deposit_request at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | (cases h; done)
    | (cases h; builder_frame_rfl)
    | exact add_builder_to_registry_frame p s s' _ _ _ _ _ h

/-- `process_builder_exit_request` does not touch validators or their balances. -/
theorem process_builder_exit_request_frame (p : Preset) (s s' : BeaconState)
    (request : BuilderExitRequest)
    (h : process_builder_exit_request p s request = .ok s') : BuilderFrame s s' := by
  unfold process_builder_exit_request at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | (cases h; done)
    | (cases h; builder_frame_rfl)
    | exact initiate_builder_exit_frame p s s' _ h

/-- Each validator keeps its exit and withdrawable epochs, or it had not started an exit and
now has exit epoch `e ≥ current + 1 + MAX_SEED_LOOKAHEAD` and withdrawable epoch
`e + MIN_VALIDATOR_WITHDRAWABILITY_DELAY`. -/
def ExitShape (p : Preset) (s s' : BeaconState) : Prop :=
  ∀ (i : Nat) (v v' : Validator), s.validators[i]? = some v → s'.validators[i]? = some v' →
    (v'.exit_epoch = v.exit_epoch ∧ v'.withdrawable_epoch = v.withdrawable_epoch) ∨
      (v.exit_epoch = FAR_FUTURE_EPOCH ∧ ∃ c : Nat, get_current_epoch p s = .ok c ∧
        c + 1 + p.MAX_SEED_LOOKAHEAD ≤ v'.exit_epoch ∧
        v'.withdrawable_epoch = v'.exit_epoch + p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY)

/-- Same validators give the exit shape. -/
theorem ExitShape.of_eq {p : Preset} {s s' : BeaconState} (hv : s'.validators = s.validators) :
    ExitShape p s s' := by
  intro i v v' h h'
  rw [hv, h] at h'
  cases h'
  exact .inl ⟨rfl, rfl⟩

/-- One validator write that keeps the exit and withdrawable epochs. -/
theorem ExitShape.of_set_same {p : Preset} {s s' : BeaconState} {i : Nat} {v' : Validator}
    (hset : s'.validators = s.validators.set i v')
    (hsame : ∀ v, s.validators[i]? = some v →
      v'.exit_epoch = v.exit_epoch ∧ v'.withdrawable_epoch = v.withdrawable_epoch) :
    ExitShape p s s' := by
  intro j w w' h h'
  rw [hset, List.getElem?_set] at h'
  by_cases hij : i = j
  · subst hij
    have hlt : i < s.validators.length := (List.getElem?_eq_some_iff.mp h).1
    simp only [hlt, if_true, Option.some.injEq] at h'
    subst h'
    exact .inl (hsame w h)
  · simp only [hij, if_false, h, Option.some.injEq] at h'
    subst h'
    exact .inl ⟨rfl, rfl⟩

/-- One validator write that starts an exit. -/
theorem ExitShape.of_set_exit {p : Preset} {s s' : BeaconState} {i c : Nat} {v v' : Validator}
    (hv : s.validators[i]? = some v) (hset : s'.validators = s.validators.set i v')
    (hfar : v.exit_epoch = FAR_FUTURE_EPOCH) (hc : get_current_epoch p s = .ok c)
    (hle : c + 1 + p.MAX_SEED_LOOKAHEAD ≤ v'.exit_epoch)
    (hw : v'.withdrawable_epoch = v'.exit_epoch + p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY) :
    ExitShape p s s' := by
  intro j w w' h h'
  rw [hset, List.getElem?_set] at h'
  by_cases hij : i = j
  · subst hij
    have hlt : i < s.validators.length := (List.getElem?_eq_some_iff.mp h).1
    simp only [hlt, if_true, Option.some.injEq] at h'
    subst h'
    rw [hv] at h
    cases h
    exact .inr ⟨hfar, c, hc, hle, hw⟩
  · simp only [hij, if_false, h, Option.some.injEq] at h'
    subst h'
    exact .inl ⟨rfl, rfl⟩

/-- `initiate_validator_exit` has the exit shape. -/
theorem initiate_validator_exit_exitShape {p : Preset} {tab : Gwei} {s s' : BeaconState}
    {index : Nat} (h : initiate_validator_exit p tab s index = .ok s') : ExitShape p s s' := by
  obtain ⟨-, -, hv | ⟨v, c, e, hv, hfar, hc, hle, hset⟩⟩ :=
    initiate_validator_exit_cases p tab s s' index h
  · exact .of_eq hv
  · exact .of_set_exit hv hset hfar hc hle rfl

/-- `process_withdrawal_request` only changes exit epochs through `initiate_validator_exit`. -/
theorem process_withdrawal_request_exitShape (p : Preset) (s s' : BeaconState)
    (withdrawal_request : WithdrawalRequest)
    (h : process_withdrawal_request p s withdrawal_request = .ok s') : ExitShape p s s' := by
  unfold process_withdrawal_request at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | (cases h; done)
    | (cases h; exact .of_eq rfl)
    | exact initiate_validator_exit_exitShape ‹initiate_validator_exit _ _ _ _ = .ok _›
    | (cases h; exact initiate_validator_exit_exitShape ‹initiate_validator_exit _ _ _ _ = .ok _›)
    | (cases h
       obtain ⟨-, -, -, hv, -, -⟩ := compute_exit_epoch_and_update_churn_bound p _ s _ _ _
         ‹compute_exit_epoch_and_update_churn _ _ _ _ = .ok _›
       exact .of_eq hv)

/-- `switch_to_compounding_validator` keeps all exit and withdrawable epochs. -/
theorem switch_to_compounding_validator_exitShape {p : Preset} {s s' : BeaconState}
    {index : Nat} (h : switch_to_compounding_validator p s index = .ok s') :
    ExitShape p s s' := by
  unfold switch_to_compounding_validator at h
  obtain ⟨v, hv, h⟩ := specM_bind_ok h
  obtain ⟨l, hl, h⟩ := specM_bind_ok h
  obtain ⟨hvs, -, -⟩ := queue_excess_active_balance_cases p _ s' index h
  refine .of_set_same (hvs.trans (listSet_eq hl)) fun w hw => ?_
  rw [listGet_some hv] at hw
  cases hw
  exact ⟨rfl, rfl⟩

/-- The last step of a consolidation has the exit shape. -/
private theorem consolidation_exit_exitShape {p : Preset} {tab : Gwei} {s : BeaconState}
    {x i : Nat} {v v' : Validator} {r : Epoch × BeaconState} {w : Uint64} {l : List Validator}
    (hr : compute_consolidation_epoch_and_update_churn p tab s x = .ok r)
    (hw : uint64Add r.1 p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY = .ok w)
    (hl : listSet r.2.validators i v' = .ok l) (hv : listGet s.validators i = .ok v)
    (hv' : v'.exit_epoch = r.1 ∧ v'.withdrawable_epoch = w)
    (hfar : ¬(v.exit_epoch != FAR_FUTURE_EPOCH) = true)
    (pc : List PendingConsolidation) :
    ExitShape p s { r.2 with validators := l, pending_consolidations := pc } := by
  obtain ⟨e, s1⟩ := r
  obtain ⟨c, hc, hle, hvs, -, -⟩ :=
    compute_consolidation_epoch_and_update_churn_bound p tab s s1 x e hr
  rw [uint64Add_iff] at hw
  rw [hvs] at hl
  refine .of_set_exit (listGet_some hv) (listSet_eq hl) ?_ hc (by rw [hv'.1]; exact hle)
    (by rw [hv'.1, hv'.2, hw.2])
  simpa using hfar

/-- `process_consolidation_request` only changes the exit epoch of a source that had not
started an exit. The new exit epoch is at least `current + 1 + MAX_SEED_LOOKAHEAD`, and the
withdrawable epoch is the exit epoch plus `MIN_VALIDATOR_WITHDRAWABILITY_DELAY`. -/
theorem process_consolidation_request_exitShape (p : Preset) (s s' : BeaconState)
    (consolidation_request : ConsolidationRequest)
    (h : process_consolidation_request p s consolidation_request = .ok s') :
    ExitShape p s s' := by
  unfold process_consolidation_request at h
  peel_ok h
  all_goals first
    | (cases h; exact .of_eq rfl)
    | exact switch_to_compounding_validator_exitShape h
    | (cases h
       exact consolidation_exit_exitShape
         ‹compute_consolidation_epoch_and_update_churn _ _ _ _ = .ok _› ‹uint64Add _ _ = .ok _›
         ‹listSet _ _ _ = .ok _› ‹listGet _ _ = .ok _› (by exact ⟨rfl, rfl⟩)
         ‹¬(_ != FAR_FUTURE_EPOCH) = true› _)

/-- A request effect keeps effective balances, the slot, the checkpoints and the exit order. -/
theorem RequestEffect.stable {p : Preset} {s s' : BeaconState} (h : RequestEffect p s s') :
    EBStable s s' ∧ CheckpointsStable s s' ∧ (ExitOrder s → ExitOrder s') :=
  ⟨h.ebStable, h.2.2.2.2, h.exitOrder⟩

/-- Same balances give `BalancesUp`. -/
private theorem balancesUp_of_eq {s s' : BeaconState} (h : s'.balances = s.balances) :
    BalancesUp s s' :=
  ⟨by rw [h]; exact Nat.le_refl _, fun _ b hb => ⟨b, by rw [h]; exact hb, Nat.le_refl _⟩⟩

/-- `process_deposit_request` keeps effective balances, balances, the slot, the checkpoints
and the exit order. -/
theorem process_deposit_request_stable (p : Preset) (s s' : BeaconState)
    (deposit_request : DepositRequest)
    (h : process_deposit_request s deposit_request = .ok s') :
    EBStable s s' ∧ BalancesUp s s' ∧ CheckpointsStable s s' ∧ (ExitOrder s → ExitOrder s') :=
  have he := process_deposit_request_effect p s s' deposit_request h
  ⟨he.2.stable.1, balancesUp_of_eq he.1, he.2.stable.2⟩

/-- `process_withdrawal_request` keeps effective balances, balances, the slot, the
checkpoints and the exit order. -/
theorem process_withdrawal_request_stable (p : Preset) (s s' : BeaconState)
    (withdrawal_request : WithdrawalRequest)
    (h : process_withdrawal_request p s withdrawal_request = .ok s') :
    EBStable s s' ∧ BalancesUp s s' ∧ CheckpointsStable s s' ∧ (ExitOrder s → ExitOrder s') :=
  have he := process_withdrawal_request_effect p s s' withdrawal_request h
  ⟨he.2.stable.1, balancesUp_of_eq he.1, he.2.stable.2⟩

/-- `process_consolidation_request` keeps effective balances, the slot, the checkpoints and
the exit order. Each balance ends between `min old MIN_ACTIVATION_BALANCE` and the old
balance. -/
theorem process_consolidation_request_stable (p : Preset) (s s' : BeaconState)
    (consolidation_request : ConsolidationRequest)
    (h : process_consolidation_request p s consolidation_request = .ok s') :
    EBStable s s' ∧ CheckpointsStable s s' ∧ (ExitOrder s → ExitOrder s') ∧
      ∀ (i : Nat) (b : Gwei), s.balances[i]? = some b →
        ∃ b', s'.balances[i]? = some b' ∧ min b p.MIN_ACTIVATION_BALANCE ≤ b' ∧ b' ≤ b :=
  have he := process_consolidation_request_effect p s s' consolidation_request h
  ⟨he.stable.1, he.stable.2.1, he.stable.2.2, he.balance_bounds⟩

/-- `process_builder_deposit_request` keeps effective balances, balances, the slot, the
checkpoints and the exit order. -/
theorem process_builder_deposit_request_stable (p : Preset) (o : Oracle) (s s' : BeaconState)
    (request : BuilderDepositRequest)
    (h : process_builder_deposit_request p o s request = .ok s') :
    EBStable s s' ∧ BalancesUp s s' ∧ CheckpointsStable s s' ∧ (ExitOrder s → ExitOrder s') :=
  have hf := process_builder_deposit_request_frame p o s s' request h
  ⟨hf.1.ebStable, hf.1.balancesUp, (hf.requestEffect (p := p)).stable.2⟩

/-- `process_builder_exit_request` keeps effective balances, balances, the slot, the
checkpoints and the exit order. -/
theorem process_builder_exit_request_stable (p : Preset) (s s' : BeaconState)
    (request : BuilderExitRequest)
    (h : process_builder_exit_request p s request = .ok s') :
    EBStable s s' ∧ BalancesUp s s' ∧ CheckpointsStable s s' ∧ (ExitOrder s → ExitOrder s') :=
  have hf := process_builder_exit_request_frame p s s' request h
  ⟨hf.1.ebStable, hf.1.balancesUp, (hf.requestEffect (p := p)).stable.2⟩

end EpochProofs.Spec
