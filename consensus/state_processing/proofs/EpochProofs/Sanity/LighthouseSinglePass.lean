import EpochProofs.Sanity.RewardsBound
import EpochProofs.Sanity.EffectiveBalanceInvariant
import EpochProofs.Sanity.PassCongr

/-!
# The Lighthouse single pass equals the spec passes

`lighthouse_single_pass_eq_spec` shows that one pass of `lhRowStep` over the validators gives
the same `ok` results as the spec's inactivity, rewards, registry and slashings passes. The
hypotheses are facts about the state at the start of epoch processing, not about single rows.
-/

namespace EpochProofs.Spec

/-- A pass with a padded accumulator equals the pass with the plain accumulator. -/
theorem passM_proj {A B R : Type} (f : B → R → SpecM (B × R)) (emb : A → B) (proj : B → A)
    (hep : ∀ b, emb (proj b) = b) (hpe : ∀ a, proj (emb a) = a) (a : A) (rows : List R) :
    passM (fun a r => (fun x => (proj x.1, x.2)) <$> f (emb a) r) a rows =
      (fun y => (proj y.1, y.2)) <$> passM f (emb a) rows := by
  induction rows generalizing a with
  | nil => simp [passM, hpe, Functor.map, Except.map, pure, Except.pure]
  | cons r rs ih =>
    simp only [passM_cons]
    cases hf : f (emb a) r with
    | error e => simp [Functor.map, Except.map, bind, Except.bind]
    | ok x =>
      simp only [Functor.map, Except.map, bind, Except.bind]
      have h := ih (proj x.1)
      rw [hep] at h
      simp only [Functor.map, Except.map] at h
      rw [h]
      cases passM f x.1 rs <;> rfl

/-- The per-validator facts that hold at the start of epoch processing. -/
structure EpochEntry (state : BeaconState) : Prop where
  /-- Hysteresis keeps the effective balance close to the balance. -/
  effective_balance : ∀ i (h : i < state.validators.length),
    state.validators[i].effective_balance ≤ 256 * state.balances.getD i 0
  /-- Balances are far below 2^64 Gwei. -/
  supply : ∀ i (h : i < state.validators.length),
    state.balances.getD i 0 + state.validators[i].effective_balance < 2 ^ 64
  /-- Exit epochs are `u64` values. -/
  exit_epoch : ∀ i (h : i < state.validators.length),
    state.validators[i].exit_epoch ≤ FAR_FUTURE_EPOCH

/-- The effective balance update establishes the effective balance part of `EpochEntry`. -/
theorem EpochEntry.effective_balance_of_floor (state : BeaconState)
    (h : EffectiveBalanceFloor Preset.mainnet state) :
    ∀ i (hi : i < state.validators.length),
      state.validators[i].effective_balance ≤ 256 * state.balances.getD i 0 :=
  fun i hi => EffectiveBalanceFloor.le_mul _ _ h i hi

/-- The Lighthouse step context for this state. -/
def lhContextOf (state : BeaconState) (total_active_balance : Gwei)
    (current_epoch : Epoch) (rctx : RewardsContext) (per : Gwei) : LhStepContext :=
  { current_epoch
    previous_epoch := rctx.previous_epoch
    finalized_epoch := state.finalized_checkpoint.epoch
    in_leak := rctx.in_leak
    source_increments := rctx.source_increments
    target_increments := rctx.target_increments
    head_increments := rctx.head_increments
    active_increments := rctx.active_increments
    total_active_balance
    slashings_target := current_epoch + Preset.mainnet.EPOCHS_PER_SLASHINGS_VECTOR / 2
    penalty_per_increment := per }

/-- On mainnet, one pass of `lhRowStep` gives the same `ok` results as the spec's inactivity,
rewards, registry and slashings passes.

The hypotheses are facts about the state at the start of epoch processing:
- `RowsOk`, `EpochEntry`: SSZ lengths, the effective balance floor, the supply bound, `u64`
  exit epochs.
- `hfinalized`, `hcurrent64`: the finalized epoch is not after the current epoch, and the next
  epoch fits in a `u64`.
- `htotal`: `get_total_active_balance` is at least one increment, by its definition.
- `hincrements`: previous-epoch participation is at most 256 times the active balance.
- `hbase`: `base_reward` is `get_base_reward` for each eligible row. Lighthouse reads it from
  the epoch cache. -/
theorem lighthouse_single_pass_eq_spec (total_active_balance : Gwei) (state : BeaconState)
    (hrows : RowsOk state) (hentry : EpochEntry state) (current_epoch : Epoch)
    (hcurrent : get_current_epoch Preset.mainnet state = .ok current_epoch)
    (hgenesis : current_epoch ≠ GENESIS_EPOCH)
    (rctx : RewardsContext)
    (hctx : rewardsContextOf Preset.mainnet total_active_balance state = .ok rctx)
    (activation_epoch : Epoch)
    (hactivation : compute_activation_exit_epoch Preset.mainnet current_epoch =
      .ok activation_epoch)
    (htarget : current_epoch + Preset.mainnet.EPOCHS_PER_SLASHINGS_VECTOR / 2 < UINT64_SIZE)
    (adjusted per : Gwei)
    (hpre : slashingsPreamble Preset.mainnet total_active_balance state.slashings =
      .ok (adjusted, per))
    (hfinalized : state.finalized_checkpoint.epoch ≤ current_epoch)
    (hcurrent64 : current_epoch + 1 < UINT64_SIZE)
    (htotal : Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT ≤ total_active_balance)
    (hincrements : rctx.source_increments ≤ 256 * rctx.active_increments ∧
      rctx.target_increments ≤ 256 * rctx.active_increments ∧
      rctx.head_increments ≤ 256 * rctx.active_increments)
    (base_reward : Row → Gwei)
    (hbase : ∀ r ∈ rowsOf state, rewardsEligible rctx.previous_epoch r.validator = .ok true →
      rewardsBaseReward Preset.mainnet total_active_balance r.validator = .ok (base_reward r)) :
    SameOk (do
      let s1 ← process_inactivity_updates Preset.mainnet state
      let s2 ← process_rewards_and_penalties Preset.mainnet total_active_balance s1
      let s3 ← process_registry_updates Preset.mainnet total_active_balance s2
      process_slashings Preset.mainnet total_active_balance s3)
    ((fun y => state.withRowsChurn y.1 y.2) <$>
      passM (fun churn r => lhRowStep Preset.mainnet
          (lhContextOf state total_active_balance current_epoch rctx per) (base_reward r) churn r)
        (state.earliest_exit_epoch, state.exit_balance_to_consume) (rowsOf state)) := by
  obtain ⟨hprevious, -, -, -, -, hleak, htab⟩ :=
    rewardsContextOf_ok Preset.mainnet total_active_balance state rctx hctx
  refine SameOk.trans (separate_passes_eq_single_pass Preset.mainnet total_active_balance state
    hrows current_epoch rctx.previous_epoch rctx.in_leak hcurrent hgenesis hprevious hleak rctx
    hctx activation_epoch hactivation htarget adjusted per hpre) (SameOk.of_eq ?_)
  have hstep : ∀ churn r, r ∈ rowsOf state →
      lhRowStep Preset.mainnet (lhContextOf state total_active_balance current_epoch rctx per)
        (base_reward r) churn r =
      (fun x => (x.1.1.2, x.2)) <$> singlePassStep Preset.mainnet total_active_balance
        rctx.previous_epoch rctx.in_leak rctx current_epoch state.finalized_checkpoint.epoch
        activation_epoch (current_epoch + Preset.mainnet.EPOCHS_PER_SLASHINGS_VECTOR / 2) per
        ((((), ()), churn), ()) r := by
    intro churn r hr
    obtain ⟨i, hi, hri⟩ := List.mem_iff_getElem.mp hr
    have hiv : i < state.validators.length := by rw [← rowsOf_length]; exact hi
    have hrow := rowsOf_getElem state i hi
    rw [hri] at hrow
    subst hrow
    exact lhRowStep_eq_singlePassStep_of_bounds _ rctx _ churn (base_reward _) activation_epoch
      rfl rfl rfl rfl rfl rfl
      (by rw [htab]; exact hbase _ hr) (by rw [htab]; exact htotal) hincrements
      (hentry.effective_balance i hiv) (hentry.supply i hiv) hfinalized hcurrent64 hactivation
      (hentry.exit_epoch i hiv)
  rw [passM_congr _ _ (rowsOf state) (fun churn r hr => hstep churn r hr)]
  rw [passM_proj (singlePassStep Preset.mainnet total_active_balance rctx.previous_epoch
    rctx.in_leak rctx current_epoch state.finalized_checkpoint.epoch activation_epoch
    (current_epoch + Preset.mainnet.EPOCHS_PER_SLASHINGS_VECTOR / 2) per)
    (fun c => ((((), ()), c), ())) (fun b => b.1.2) (fun _ => rfl) (fun _ => rfl)]
  cases passM _ _ (rowsOf state) <;> rfl

end EpochProofs.Spec
