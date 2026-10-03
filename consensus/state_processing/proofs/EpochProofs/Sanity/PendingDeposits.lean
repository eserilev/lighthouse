import EpochProofs.Spec.PendingDeposits
import EpochProofs.Sanity.RegistryUpdates

/-!
# Sanity theorems for the `process_pending_deposits` reference

`depositDecisions` is the control flow of the `process_pending_deposits` loop, over one view per
deposit: its slot, its amount, and whether its validator is exited or withdrawn. It returns
`next_deposit_index`, the new `deposit_balance_to_consume`, and for each handled deposit whether
it is postponed.

Lighthouse computes the exited flag before `process_registry_updates` runs, and predicts the
ejections that it makes. `predictedStatus_eq_post_registry` shows that the prediction equals the
flags after the spec's registry update, on valid states.
-/

namespace EpochProofs.Spec

/-- What the loop reads from one deposit and its validator. -/
structure DepositSpecView where
  slot : Slot
  amount : Gwei
  eth1_bridge_blocked : Bool
  is_exited : Bool
  is_withdrawn : Bool
  deriving DecidableEq, Repr

structure DepositLoopState where
  processed_amount : Gwei
  next_deposit_index : Nat
  postponed : List Bool
  deriving DecidableEq, Repr

/-- `stopped` is `true` when the loop breaks. `churn_reached` is `is_churn_limit_reached`. -/
structure DepositLoopResult where
  state : DepositLoopState
  stopped : Bool
  churn_reached : Bool
  deriving DecidableEq, Repr

/-- One iteration of the loop. -/
def depositStep (p : Preset) (finalized_slot available_for_processing : Nat)
    (s : DepositLoopState) (v : DepositSpecView) : SpecM DepositLoopResult := do
  if v.eth1_bridge_blocked || v.slot > finalized_slot
      || s.next_deposit_index ≥ p.MAX_PENDING_DEPOSITS_PER_EPOCH then
    pure ⟨s, true, false⟩
  else if v.is_withdrawn then
    pure ⟨⟨s.processed_amount, s.next_deposit_index + 1, s.postponed ++ [false]⟩, false, false⟩
  else if v.is_exited then
    pure ⟨⟨s.processed_amount, s.next_deposit_index + 1, s.postponed ++ [true]⟩, false, false⟩
  else do
    let total ← uint64Add s.processed_amount v.amount
    if total > available_for_processing then
      pure ⟨s, true, true⟩
    else
      pure ⟨⟨total, s.next_deposit_index + 1, s.postponed ++ [false]⟩, false, false⟩

/-- The loop: run `depositStep` until it stops or the views run out. -/
def depositLoop (p : Preset) (finalized_slot available_for_processing : Nat) :
    DepositLoopState → List DepositSpecView → SpecM DepositLoopResult
  | s, [] => pure ⟨s, false, false⟩
  | s, v :: vs => do
    let r ← depositStep p finalized_slot available_for_processing s v
    if r.stopped then pure r else depositLoop p finalized_slot available_for_processing r.state vs

/-- The decisions of `process_pending_deposits`. -/
def depositDecisions (p : Preset) (finalized_slot deposit_balance_to_consume
    activation_churn_limit : Nat) (views : List DepositSpecView) :
    SpecM (Nat × Gwei × List Bool) := do
  let available_for_processing ← uint64Add deposit_balance_to_consume activation_churn_limit
  let r ← depositLoop p finalized_slot available_for_processing ⟨0, 0, []⟩ views
  let new_deposit_balance_to_consume ←
    if r.churn_reached then uint64Sub available_for_processing r.state.processed_amount
    else pure 0
  pure (r.state.next_deposit_index, new_deposit_balance_to_consume, r.state.postponed)

theorem depositLoop_append (p : Preset) (finalized_slot available : Nat)
    (s : DepositLoopState) (l1 l2 : List DepositSpecView) :
    depositLoop p finalized_slot available s (l1 ++ l2) = (do
      let r ← depositLoop p finalized_slot available s l1
      if r.stopped then pure r else depositLoop p finalized_slot available r.state l2) := by
  induction l1 generalizing s with
  | nil => simp [depositLoop, pure, Except.pure, bind, Except.bind]
  | cons v vs ih =>
    simp only [List.cons_append, depositLoop, bind, Except.bind]
    cases depositStep p finalized_slot available s v with
    | error e => rfl
    | ok r =>
      simp only
      by_cases h : r.stopped = true
      · simp [h, pure, Except.pure]
      · simp only [h, Bool.false_eq_true, if_false]
        rw [ih]
        rfl

theorem bind_ok_inv {α β : Type} {m : SpecM α} {K : α → SpecM β} {x : β}
    (h : (match m with
      | Except.error e => Except.error e
      | Except.ok v => K v) = Except.ok x) : ∃ v, m = Except.ok v ∧ K v = Except.ok x := by
  cases m with
  | error e => cases h
  | ok v => exact ⟨v, rfl, h⟩

theorem uint64Add_ok {a b c : Nat} (h : uint64Add a b = .ok c) : c = a + b := by
  unfold uint64Add at h; split at h <;> simp_all [pure, Except.pure, throw, throwThe,
    MonadExceptOf.throw]

theorem compute_activation_exit_epoch_ok (p : Preset) (epoch r : Nat)
    (h : compute_activation_exit_epoch p epoch = .ok r) : r = epoch + 1 + p.MAX_SEED_LOOKAHEAD := by
  unfold compute_activation_exit_epoch at h
  simp only [bind, Except.bind] at h
  split at h
  · cases h
  · rename_i v hv
    rw [uint64Add_ok h, uint64Add_ok hv]

theorem exitChurnStep_ge (p : Preset) (total_active_balance current_epoch exit_balance
    earliest_exit_epoch exit_balance_to_consume : Nat) (e e' b : Nat)
    (h : exitChurnStep p total_active_balance current_epoch exit_balance earliest_exit_epoch
      exit_balance_to_consume = .ok (e, e', b)) : current_epoch + 1 ≤ e := by
  unfold exitChurnStep at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  split at h
  · cases h
  rename_i act hact
  have hact' := compute_activation_exit_epoch_ok p current_epoch act hact
  split at h
  · cases h
  rename_i churn hchurn
  split at h <;> split at h
  all_goals first
    | (split at h
       · cases h
       split at h
       · cases h
       split at h
       · cases h
       split at h
       · cases h
       split at h
       · cases h
       rename_i newe hnewe
       split at h
       · cases h
       split at h
       · cases h
       split at h
       · cases h
       cases h
       rw [uint64Add_ok hnewe, hact']
       omega)
    | (split at h
       · cases h
       cases h
       rw [hact']
       omega)

/-- Lighthouse's flags for a deposit's validator, read before the registry update. -/
def predictedStatus (p : Preset) (registry_updates is_known : Bool) (f : RegistryFields)
    (effective_balance current_epoch next_epoch : Nat) : Bool × Bool :=
  if !is_known then (false, false)
  else
    let already_exited := decide (f.exit_epoch < FAR_FUTURE_EPOCH)
    let is_active := decide (f.activation_epoch ≤ current_epoch)
      && decide (current_epoch < f.exit_epoch)
    let will_be_exited := registry_updates && is_active
      && decide (effective_balance ≤ p.EJECTION_BALANCE)
    (already_exited || will_be_exited, decide (f.withdrawable_epoch < next_epoch))

/-- The spec's flags, read after the registry update. -/
def postRegistryStatus (g : RegistryFields) (next_epoch : Nat) : Bool × Bool :=
  (decide (g.exit_epoch < FAR_FUTURE_EPOCH), decide (g.withdrawable_epoch < next_epoch))

theorem exitStep_cases (p : Preset) (total_active_balance current_epoch : Nat)
    (effective_balance : Gwei) (f g : RegistryFields)
    (h : exitStep p total_active_balance current_epoch effective_balance f = .ok g) :
    (f.exit_epoch ≠ FAR_FUTURE_EPOCH ∧ g = f)
    ∨ (f.exit_epoch = FAR_FUTURE_EPOCH ∧ current_epoch + 1 ≤ g.exit_epoch
        ∧ g.withdrawable_epoch = g.exit_epoch + p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY) := by
  unfold exitStep at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  split at h
  · rename_i hne
    cases h
    exact Or.inl ⟨by simpa using hne, rfl⟩
  · rename_i heq
    split at h
    · cases h
    rename_i x hx
    obtain ⟨e, e', b⟩ := x
    split at h
    · cases h
    rename_i w hw
    cases h
    refine Or.inr ⟨by simpa using heq, exitChurnStep_ge p _ _ _ _ _ e e' b hx, ?_⟩
    exact uint64Add_ok hw

/-- On valid states, Lighthouse's prediction equals the spec's flags after the registry
update. The conditions: an ejected validator cannot also be eligible for the queue, a validator
that has not exited is not withdrawable, the next epoch is below `FAR_FUTURE_EPOCH`, and an
ejected validator gets an exit epoch below `FAR_FUTURE_EPOCH`. -/
theorem predictedStatus_eq_post_registry (p : Preset)
    (total_active_balance current_epoch finalized_epoch activation_epoch : Nat)
    (effective_balance : Gwei) (f g : RegistryFields)
    (hreg : registryStepExclusive p total_active_balance current_epoch finalized_epoch
      activation_epoch effective_balance f = .ok g)
    (hbalances : p.EJECTION_BALANCE < p.MIN_ACTIVATION_BALANCE)
    (hexit : f.exit_epoch ≤ FAR_FUTURE_EPOCH)
    (hvalid : f.exit_epoch = FAR_FUTURE_EPOCH → f.withdrawable_epoch = FAR_FUTURE_EPOCH)
    (hnext : current_epoch + 1 < FAR_FUTURE_EPOCH)
    (hejected : f.activation_epoch ≤ current_epoch → current_epoch < f.exit_epoch →
      effective_balance ≤ p.EJECTION_BALANCE → f.exit_epoch = FAR_FUTURE_EPOCH →
      g.exit_epoch < FAR_FUTURE_EPOCH) :
    predictedStatus p true true f effective_balance current_epoch (current_epoch + 1) =
      postRegistryStatus g (current_epoch + 1) := by
  unfold registryStepExclusive at hreg
  unfold predictedStatus postRegistryStatus
  simp only [Bool.not_true, Bool.false_eq_true, if_false, Bool.true_and]
  split at hreg
  · -- The queue step. Exit and withdrawable epochs do not change, and no ejection is predicted.
    rename_i hq
    simp only [Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq, ge_iff_le] at hq
    have hnej : ¬ effective_balance ≤ p.EJECTION_BALANCE :=
      Nat.not_le.mpr (Nat.lt_of_lt_of_le hbalances hq.2)
    simp only [bind, Except.bind, pure, Except.pure] at hreg
    split at hreg
    · cases hreg
    cases hreg
    simp [hnej]
  · split at hreg
    · -- The ejection step.
      rename_i _ hej
      simp only [Bool.and_eq_true, decide_eq_true_eq] at hej
      rcases exitStep_cases p _ _ _ f g hreg with ⟨hne, hgf⟩ | ⟨hfar, hge, hwd⟩
      · have hlt : f.exit_epoch < FAR_FUTURE_EPOCH := Nat.lt_of_le_of_ne hexit hne
        rw [hgf]
        simp [hlt]
      · have hg := hejected hej.1.1 hej.1.2 hej.2 hfar
        have hwf := hvalid hfar
        have hn1 : ¬ g.withdrawable_epoch < current_epoch + 1 := by
          rw [hwd]; exact Nat.not_lt.mpr (Nat.le_trans hge (Nat.le_add_right _ _))
        have hn2 : ¬ FAR_FUTURE_EPOCH < current_epoch + 1 := Nat.not_lt.mpr (Nat.le_of_lt hnext)
        simp [hg, hwf, hn1, hn2, hej.1.1, hej.1.2, hej.2]
    · -- The activation step or nothing. Exit and withdrawable epochs do not change.
      rename_i _ hnej
      split at hreg <;> (simp only [pure, Except.pure, Except.ok.injEq] at hreg; subst hreg)
      all_goals
        simp only [Bool.and_eq_true, decide_eq_true_eq, not_and] at hnej
        by_cases ha : f.activation_epoch ≤ current_epoch
        · by_cases hb : current_epoch < f.exit_epoch
          · have := hnej ⟨ha, hb⟩
            simp [ha, hb, this]
          · simp [hb]
        · simp [ha]

theorem depositLoop_split (p : Preset) (fs avail : Nat) (init st : DepositLoopState)
    (views : List DepositSpecView) (i : Nat) (v : DepositSpecView) (hget : views[i]? = some v)
    (hst : depositLoop p fs avail init (views.take i) = .ok ⟨st, false, false⟩) :
    depositLoop p fs avail init views = (do
      let r ← depositStep p fs avail st v
      if r.stopped then pure r else depositLoop p fs avail r.state (views.drop (i + 1))) := by
  have hlt : i < views.length := (List.getElem?_eq_some_iff.mp hget).1
  have hv : views[i] = v := (List.getElem?_eq_some_iff.mp hget).2
  have hsplit : views = views.take i ++ (v :: views.drop (i + 1)) := by
    rw [← hv, ← List.drop_eq_getElem_cons hlt, List.take_append_drop]
  conv => lhs; rw [hsplit]
  rw [depositLoop_append, hst]
  simp only [bind, Except.bind, Bool.false_eq_true, if_false, depositLoop]

theorem depositLoop_stop_at (p : Preset) (fs avail : Nat) (init st : DepositLoopState)
    (views : List DepositSpecView) (i : Nat) (v : DepositSpecView) (hget : views[i]? = some v)
    (hst : depositLoop p fs avail init (views.take i) = .ok ⟨st, false, false⟩)
    (r : DepositLoopResult) (hr : depositStep p fs avail st v = .ok r) (hstop : r.stopped = true) :
    depositLoop p fs avail init views = .ok r := by
  rw [depositLoop_split p fs avail init st views i v hget hst, hr]
  simp [bind, Except.bind, hstop, pure, Except.pure]

theorem depositLoop_error_at (p : Preset) (fs avail : Nat) (init st : DepositLoopState)
    (views : List DepositSpecView) (i : Nat) (v : DepositSpecView) (hget : views[i]? = some v)
    (hst : depositLoop p fs avail init (views.take i) = .ok ⟨st, false, false⟩)
    (e : SpecError) (hr : depositStep p fs avail st v = .error e) :
    depositLoop p fs avail init views = .error e := by
  rw [depositLoop_split p fs avail init st views i v hget hst, hr]
  rfl

theorem depositLoop_next_at (p : Preset) (fs avail : Nat) (init st : DepositLoopState)
    (views : List DepositSpecView) (i : Nat) (v : DepositSpecView) (hget : views[i]? = some v)
    (hst : depositLoop p fs avail init (views.take i) = .ok ⟨st, false, false⟩)
    (st' : DepositLoopState) (hr : depositStep p fs avail st v = .ok ⟨st', false, false⟩) :
    depositLoop p fs avail init (views.take (i + 1)) = .ok ⟨st', false, false⟩ := by
  have hlt : i < views.length := (List.getElem?_eq_some_iff.mp hget).1
  have hv : views[i] = v := (List.getElem?_eq_some_iff.mp hget).2
  rw [List.take_add_one, List.getElem?_eq_getElem hlt, hv, Option.toList_some,
    depositLoop_append, hst]
  simp [bind, Except.bind, depositLoop, hr, pure, Except.pure]

theorem depositLoop_end (p : Preset) (fs avail : Nat) (init : DepositLoopState)
    (views : List DepositSpecView) (i : Nat) (hi : views.length ≤ i) (x : DepositLoopResult)
    (hst : depositLoop p fs avail init (views.take i) = .ok x) :
    depositLoop p fs avail init views = .ok x := by
  rw [List.take_of_length_le hi] at hst
  exact hst

end EpochProofs.Spec
