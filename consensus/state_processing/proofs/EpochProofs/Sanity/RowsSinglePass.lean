import EpochProofs.Sanity.RowsChain
import EpochProofs.Sanity.RowsInactivity
import EpochProofs.Sanity.RowsRewards
import EpochProofs.Sanity.RowsRegistry

/-!
# One pass equals four passes

The spec runs inactivity updates, rewards and penalties, registry updates and slashings as
four passes over the validators. `separate_passes_eq_single_pass` shows that one pass, which runs the four
row steps on each validator in turn, gives the same `ok` results.
-/

namespace EpochProofs.Spec

theorem SameOk.bind_map {α β γ : Type} {x : SpecM α} {y : SpecM β} {f : β → α}
    {k : α → SpecM γ} {l : β → SpecM γ} (h : SameOk x (f <$> y))
    (hk : ∀ b, y = .ok b → SameOk (k (f b)) (l b)) : SameOk (x >>= k) (y >>= l) := by
  cases hy : y with
  | error e =>
    cases hx : x with
    | error e' => exact SameOk.errors
    | ok v => exact absurd ((h v).mp hx) (by simp [hy, Functor.map, Except.map])
  | ok b =>
    have hx : x = .ok (f b) := (h (f b)).mpr (by simp [hy, Functor.map, Except.map])
    rw [hx]
    exact hk b hy

/-- Two passes, then the rest, equal one pass of both steps, then the rest. -/
theorem passM_seq_bothSteps {A B R γ : Type} (f : A → R → SpecM (A × R))
    (g : B → R → SpecM (B × R)) (a : A) (b : B) (rs : List R)
    (K : A → B × List R → SpecM γ) :
    SameOk (do let x ← passM f a rs; let y ← passM g b x.2; K x.1 y)
      (do let z ← passM (bothSteps f g) (a, b) rs; K z.1.1 (z.1.2, z.2)) := by
  have h : (do let x ← passM f a rs; let y ← passM g b x.2; K x.1 y) =
      (twoPasses f g a b rs >>= fun z => K z.1.1 (z.1.2, z.2)) := by
    simp only [twoPasses, bind, Except.bind, pure, Except.pure]
    cases passM f a rs with
    | error e => rfl
    | ok x =>
      dsimp only
      cases passM g b x.2 <;> rfl
  rw [h]
  exact SameOk.bind (two_passes_eq_one_pass f g a b rs) (fun _ _ => SameOk.refl _)

/-- A step that keeps the row key keeps the keys of a whole pass. -/
theorem passM_rowKey {A : Type} (f : A → Row → SpecM (A × Row))
    (hf : ∀ a r x, f a r = .ok x → rowKey x.2 = rowKey r) (a : A) (rows : List Row)
    (y : A × List Row) (h : passM f a rows = .ok y) : y.2.map rowKey = rows.map rowKey :=
  passM_map_eq f rowKey hf a rows y h

theorem withRows_withRows (state : BeaconState) (rows rows' : List Row) :
    (state.withRows rows).withRows rows' = state.withRows rows' := rfl

theorem rowsOk_withRows_of_key (state : BeaconState) (rows : List Row) (hrows : RowsOk state)
    (hkey : rows.map rowKey = (rowsOf state).map rowKey) : RowsOk (state.withRows rows) := by
  apply rowsOk_withRows state rows hrows
  rw [← rowsOf_length, ← List.length_map (f := rowKey), hkey, List.length_map]

/-- The four row steps, run on one row in turn. -/
def singlePassStep (p : Preset) (total_active_balance : Gwei) (previous_epoch : Epoch)
    (in_leak : Bool) (ctx : RewardsContext) (current_epoch finalized_epoch activation_epoch : Nat)
    (target per : Uint64) :
    ((Unit × Unit) × (Epoch × Gwei)) × Unit → Row →
      SpecM ((((Unit × Unit) × (Epoch × Gwei)) × Unit) × Row) :=
  bothSteps (bothSteps (bothSteps (inactivityRowStep p previous_epoch in_leak) (rewardsRowStep p ctx))
    (registryRowStep p total_active_balance current_epoch finalized_epoch activation_epoch))
    (slashingsRowStep p target per)

theorem rewardsContextOf_withRows (p : Preset) (total_active_balance : Gwei)
    (state : BeaconState) (rows : List Row) (hv : rows.map (·.validator) = state.validators) :
    rewardsContextOf p total_active_balance (state.withRows rows) =
      rewardsContextOf p total_active_balance state := by
  have h : state.withRows rows = { state with
      balances := rows.map (·.balance)
      inactivity_scores := rows.map (·.inactivity_score) } := by
    simp [BeaconState.withRows, hv]
  rw [h]
  rfl

/-- Write back the rows and the churn of the single pass. -/
def BeaconState.withRowsChurn (state : BeaconState) (churn : Epoch × Gwei) (rows : List Row) :
    BeaconState :=
  { state.withRows rows with
    earliest_exit_epoch := churn.1
    exit_balance_to_consume := churn.2 }

/-- The four passes, each over the rows that the previous pass wrote. -/
def fourPasses (p : Preset) (total_active_balance : Gwei) (previous_epoch : Epoch)
    (in_leak : Bool) (ctx : RewardsContext) (current_epoch activation_epoch : Nat)
    (target per : Uint64) (state : BeaconState) : SpecM BeaconState := do
  let y1 ← passM (inactivityRowStep p previous_epoch in_leak) () (rowsOf state)
  let y2 ← passM (rewardsRowStep p ctx) () y1.2
  let y3 ← passM (registryRowStep p total_active_balance current_epoch
    state.finalized_checkpoint.epoch activation_epoch)
    (state.earliest_exit_epoch, state.exit_balance_to_consume) y2.2
  let y4 ← passM (slashingsRowStep p target per) () y3.2
  pure (state.withRowsChurn y3.1 y4.2)

theorem spec_passes_eq_rows (p : Preset) (total_active_balance : Gwei) (state : BeaconState)
    (hrows : RowsOk state) (current_epoch previous_epoch : Epoch) (in_leak : Bool)
    (hcurrent : get_current_epoch p state = .ok current_epoch)
    (hgenesis : current_epoch ≠ GENESIS_EPOCH)
    (hprevious : get_previous_epoch p state = .ok previous_epoch)
    (hleak : is_in_inactivity_leak p state = .ok in_leak)
    (ctx : RewardsContext) (hctx : rewardsContextOf p total_active_balance state = .ok ctx)
    (activation_epoch : Epoch)
    (hactivation : compute_activation_exit_epoch p current_epoch = .ok activation_epoch)
    (htarget : current_epoch + p.EPOCHS_PER_SLASHINGS_VECTOR / 2 < UINT64_SIZE)
    (adjusted per : Gwei)
    (hpre : slashingsPreamble p total_active_balance state.slashings = .ok (adjusted, per)) :
    SameOk (do
      let s1 ← process_inactivity_updates p state
      let s2 ← process_rewards_and_penalties p total_active_balance s1
      let s3 ← process_registry_updates p total_active_balance s2
      process_slashings p total_active_balance s3)
    (fourPasses p total_active_balance previous_epoch in_leak ctx current_epoch activation_epoch
      (current_epoch + p.EPOCHS_PER_SLASHINGS_VECTOR / 2) per state) := by
  unfold fourPasses
  apply SameOk.bind_map (process_inactivity_updates_rows p state current_epoch previous_epoch
    in_leak hrows hcurrent hgenesis hprevious hleak)
  intro y1 hy1
  have hk1 := passM_rowKey _ (fun a r x h => by
    obtain ⟨u, r'⟩ := x
    rw [inactivityRowStep_preserves p previous_epoch in_leak a u r r' h]; rfl) () _ y1 hy1
  have hv1 : y1.2.map (·.validator) = state.validators := by
    rw [passM_map_eq _ (·.validator) (fun a r x h => by
      obtain ⟨u, r'⟩ := x
      rw [inactivityRowStep_preserves p previous_epoch in_leak a u r r' h]) () _ y1 hy1,
      rowsOf_map_validator]
  have hR1 : rowsOf (state.withRows y1.2) = y1.2 := rowsOf_withRows state y1.2 hk1
  have hok1 := rowsOk_withRows_of_key state y1.2 hrows hk1
  have hctx1 : rewardsContextOf p total_active_balance (state.withRows y1.2) = .ok ctx := by
    rw [rewardsContextOf_withRows p total_active_balance state y1.2 hv1, hctx]
  have h2 := process_rewards_and_penalties_rows p total_active_balance (state.withRows y1.2)
    hok1 current_epoch hcurrent hgenesis ctx hctx1
  rw [hR1] at h2
  apply SameOk.bind_map h2
  intro y2 hy2
  have hk2 : y2.2.map rowKey = (rowsOf state).map rowKey := by
    rw [passM_rowKey _ (fun a r x h => by
      rw [rewardsRowStep_preserves p ctx a r x h]; rfl) () _ y2 hy2, hk1]
  rw [withRows_withRows]
  have hR2 : rowsOf (state.withRows y2.2) = y2.2 := rowsOf_withRows state y2.2 hk2
  have hok2 := rowsOk_withRows_of_key state y2.2 hrows hk2
  have h3 := process_registry_updates_rows p total_active_balance (state.withRows y2.2)
    current_epoch activation_epoch hok2 hcurrent hactivation
  rw [hR2] at h3
  apply SameOk.bind_map h3
  intro y3 hy3
  have hk3 : y3.2.map rowKey = (rowsOf state).map rowKey := by
    rw [passM_rowKey _ (fun a r x h => by
      rw [registryRowStep_preserves p total_active_balance current_epoch
        state.finalized_checkpoint.epoch activation_epoch a r x h]; rfl) _ _ y3 hy3, hk2]
  have hR3 : rowsOf (state.withRowsChurn y3.1 y3.2) = y3.2 := rowsOf_withRows state y3.2 hk3
  have hok3 : RowsOk (state.withRowsChurn y3.1 y3.2) := rowsOk_withRows_of_key state y3.2 hrows hk3
  have h4 := process_slashings_rows_sameOk p total_active_balance
    (state.withRowsChurn y3.1 y3.2) current_epoch hcurrent htarget adjusted per hpre hok3
  rw [hR3] at h4
  refine SameOk.trans h4 (SameOk.of_eq ?_)
  simp only [Functor.map, Except.map, bind, Except.bind, pure, Except.pure]
  cases passM (slashingsRowStep p (current_epoch + p.EPOCHS_PER_SLASHINGS_VECTOR / 2) per) ()
    y3.2 <;> rfl

theorem fourPasses_eq_single_pass (p : Preset) (total_active_balance : Gwei) (previous_epoch : Epoch)
    (in_leak : Bool) (ctx : RewardsContext) (current_epoch activation_epoch : Nat)
    (target per : Uint64) (state : BeaconState) :
    SameOk (fourPasses p total_active_balance previous_epoch in_leak ctx current_epoch
        activation_epoch target per state)
      ((fun z => state.withRowsChurn z.1.1.2 z.2) <$>
        passM (singlePassStep p total_active_balance previous_epoch in_leak ctx current_epoch
          state.finalized_checkpoint.epoch activation_epoch target per)
          ((((), ()), (state.earliest_exit_epoch, state.exit_balance_to_consume)), ())
          (rowsOf state)) := by
  unfold fourPasses singlePassStep
  refine SameOk.trans (passM_seq_bothSteps _ _ () () (rowsOf state) (fun _ y2 => do
    let y3 ← passM (registryRowStep p total_active_balance current_epoch
      state.finalized_checkpoint.epoch activation_epoch)
      (state.earliest_exit_epoch, state.exit_balance_to_consume) y2.2
    let y4 ← passM (slashingsRowStep p target per) () y3.2
    pure (state.withRowsChurn y3.1 y4.2))) ?_
  refine SameOk.trans (passM_seq_bothSteps _ _ ((), ())
    (state.earliest_exit_epoch, state.exit_balance_to_consume) (rowsOf state) (fun _ y3 => do
    let y4 ← passM (slashingsRowStep p target per) () y3.2
    pure (state.withRowsChurn y3.1 y4.2))) ?_
  refine SameOk.trans (passM_seq_bothSteps _ _ (((), ()),
    (state.earliest_exit_epoch, state.exit_balance_to_consume)) () (rowsOf state)
    (fun x y4 => pure (state.withRowsChurn x.2 y4.2))) ?_
  apply SameOk.of_eq
  simp only [Functor.map, Except.map, bind, Except.bind, pure, Except.pure]

/-- The spec's four passes give the same `ok` results as one pass of `singlePassStep`. -/
theorem separate_passes_eq_single_pass (p : Preset) (total_active_balance : Gwei) (state : BeaconState)
    (hrows : RowsOk state) (current_epoch previous_epoch : Epoch) (in_leak : Bool)
    (hcurrent : get_current_epoch p state = .ok current_epoch)
    (hgenesis : current_epoch ≠ GENESIS_EPOCH)
    (hprevious : get_previous_epoch p state = .ok previous_epoch)
    (hleak : is_in_inactivity_leak p state = .ok in_leak)
    (ctx : RewardsContext) (hctx : rewardsContextOf p total_active_balance state = .ok ctx)
    (activation_epoch : Epoch)
    (hactivation : compute_activation_exit_epoch p current_epoch = .ok activation_epoch)
    (htarget : current_epoch + p.EPOCHS_PER_SLASHINGS_VECTOR / 2 < UINT64_SIZE)
    (adjusted per : Gwei)
    (hpre : slashingsPreamble p total_active_balance state.slashings = .ok (adjusted, per)) :
    SameOk (do
      let s1 ← process_inactivity_updates p state
      let s2 ← process_rewards_and_penalties p total_active_balance s1
      let s3 ← process_registry_updates p total_active_balance s2
      process_slashings p total_active_balance s3)
    ((fun z => state.withRowsChurn z.1.1.2 z.2) <$>
      passM (singlePassStep p total_active_balance previous_epoch in_leak ctx current_epoch
        state.finalized_checkpoint.epoch activation_epoch
        (current_epoch + p.EPOCHS_PER_SLASHINGS_VECTOR / 2) per)
        ((((), ()), (state.earliest_exit_epoch, state.exit_balance_to_consume)), ())
        (rowsOf state)) :=
  SameOk.trans (spec_passes_eq_rows p total_active_balance state hrows current_epoch
    previous_epoch in_leak hcurrent hgenesis hprevious hleak ctx hctx activation_epoch
    hactivation htarget adjusted per hpre)
    (fourPasses_eq_single_pass p total_active_balance previous_epoch in_leak ctx current_epoch
      activation_epoch _ per state)

end EpochProofs.Spec
