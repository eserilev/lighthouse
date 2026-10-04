import EpochProofs.Sanity.Invariants.LighthouseTailLhNF
import EpochProofs.Sanity.Invariants.LighthouseTailSpecNF
import EpochProofs.Sanity.Invariants.ConsolidationIndices
import EpochProofs.Sanity.Invariants.WithdrawableEpochs

/-!
# The Lighthouse epoch tail equals the spec

`process_epoch_lh_full` runs the pending deposits and the effective balance updates inside the
single pass, as Lighthouse does. On mainnet it gives the same `ok` results as `process_epoch`.
Both tails reach `tailNF`: `lhFullTail_nf` and `epochRest_nf` show this.
-/

namespace EpochProofs.Spec

/-! ## Balances after the single pass -/

/-- `rewardsCombined` returns a `u64` value. -/
private theorem rewardsCombined_lt (b : Gwei) (ds : List (Gwei × Gwei)) (x : Gwei)
    (h : rewardsCombined b ds = .ok x) : x < 2 ^ 64 := by
  unfold rewardsCombined at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨y, hy, h⟩ := specM_bind_ok h
  simp only [pure, Except.pure, Except.ok.injEq] at h
  subst h
  unfold uint64Add at hy
  split at hy
  · rename_i hlt
    cases hy
    have hlt' : @LT.lt Nat _ (b + _) (2 ^ 64) := hlt
    show @LT.lt Nat _ (saturating_sub _ _) (2 ^ 64)
    unfold saturating_sub
    split <;> omega
  · cases hy

/-- The slashing penalty does not raise the balance. -/
private theorem lhSlashingBalanceStep_le (target per : Uint64) (v : Validator) (b x : Gwei)
    (h : slashingBalanceStep Preset.mainnet target per v b = .ok x) : x ≤ b := by
  unfold slashingBalanceStep at h
  split at h
  · obtain ⟨_, -, h⟩ := specM_bind_ok h
    obtain ⟨_, -, h⟩ := specM_bind_ok h
    simp only [pure, Except.pure, Except.ok.injEq] at h
    subst h
    unfold saturating_sub
    split <;> exact Nat.sub_le _ _
  · cases h
    exact Nat.le_refl _

/-- One Lighthouse row step keeps the balance below `2^64`. -/
private theorem lhRowStep_balance_lt (ctx : LhStepContext) (base : Gwei) (churn : Epoch × Gwei)
    (r : Row) (x : (Epoch × Gwei) × Row) (hb : r.balance < 2 ^ 64)
    (h : lhRowStep Preset.mainnet ctx base churn r = .ok x) : x.2.balance < 2 ^ 64 := by
  unfold lhRowStep at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h
       exact Nat.lt_of_le_of_lt (lhSlashingBalanceStep_le _ _ _ _ _
         ‹slashingBalanceStep _ _ _ _ _ = _›)
         (by first
           | exact rewardsCombined_lt _ _ _ ‹rewardsCombined _ _ = _›
           | exact hb))

/-- After the single pass every balance is below `2^64`, if every balance was. -/
private theorem pass_balances_lt (ctx : LhStepContext) (bf : Row → Gwei) (s : BeaconState)
    (churn0 : Epoch × Gwei) (y : (Epoch × Gwei) × List Row)
    (hb : ∀ i, i < s.validators.length → s.balances.getD i 0 < 2 ^ 64)
    (h : passM (fun c r => lhRowStep Preset.mainnet ctx (bf r) c r) churn0 (rowsOf s) = .ok y) :
    ∀ r ∈ y.2, r.balance < 2 ^ 64 := by
  obtain ⟨hlen, hget⟩ := passM_rows _ _ _ _ h
  intro r hr
  obtain ⟨i, hi, rfl⟩ := List.getElem_of_mem hr
  have hi' : i < (rowsOf s).length := by rw [← hlen]; exact hi
  obtain ⟨_, x, hx, hxe⟩ := hget i hi' hi
  rw [← hxe]
  have hiv : i < s.validators.length := by rw [← rowsOf_length]; exact hi'
  refine lhRowStep_balance_lt _ _ _ _ _ ?_ hx
  rw [rowsOf_getElem]
  exact hb i hiv

/-! ## The total active balance after the four steps -/

/-- An `ok` value followed by more code. -/
private theorem ok_bind'' {α β : Type} (a : α) (f : α → SpecM β) :
    (Except.ok a >>= f) = f a := rfl

/-- The current epoch reads only the slot. -/
private theorem current_epoch_of_slot' {s s' : BeaconState} {e : Epoch}
    (h : get_current_epoch Preset.mainnet s = .ok e) (hslot : s'.slot = s.slot) :
    get_current_epoch Preset.mainnet s' = .ok e := by
  rw [← h]; simp only [get_current_epoch, hslot]

/-- The four spec steps with one total keep the total active balance. -/
private theorem fourSteps_total (s1 : BeaconState) (total_active_balance : Gwei)
    (htab : get_total_active_balance Preset.mainnet s1 = .ok total_active_balance)
    (hrows : RowsOk s1) (current_epoch activation_epoch : Epoch)
    (hcurrent : get_current_epoch Preset.mainnet s1 = .ok current_epoch)
    (hactivation : compute_activation_exit_epoch Preset.mainnet current_epoch =
      .ok activation_epoch)
    (hcur : current_epoch < FAR_FUTURE_EPOCH) (S : BeaconState)
    (h : (do
      let s2 ← process_inactivity_updates Preset.mainnet s1
      let s3 ← process_rewards_and_penalties Preset.mainnet total_active_balance s2
      let s4 ← process_registry_updates Preset.mainnet total_active_balance s3
      process_slashings Preset.mainnet total_active_balance s4) = .ok S) :
    get_total_active_balance Preset.mainnet S = .ok total_active_balance := by
  obtain ⟨s2, h2, h⟩ := specM_bind_ok h
  obtain ⟨s3, h3, h⟩ := specM_bind_ok h
  obtain ⟨s4, h4, h5⟩ := specM_bind_ok h
  have hv2 := process_inactivity_updates_validators _ _ _ h2
  have hsl2 := (process_inactivity_updates_checkpointsStable _ _ _ h2).1
  have hv3 := process_rewards_and_penalties_validators _ _ _ _ h3
  have hsl3 := (process_rewards_and_penalties_checkpointsStable _ _ _ _ h3).1
  have hsl4 := (process_registry_updates_checkpointsStable _ _ _ _ h4).1
  have hv5 := process_slashings_validators _ _ _ _ h5
  have hsl5 := (process_slashings_checkpointsStable _ _ _ _ h5).1
  have hc2 := current_epoch_of_slot' hcurrent hsl2
  have hc3 := current_epoch_of_slot' hc2 hsl3
  have hc4 := current_epoch_of_slot' hc3 hsl4
  have hrows3 : RowsOk s3 :=
    ((process_inactivity_updates_len _ _ _ h2).trans
      (process_rewards_and_penalties_len _ _ _ _ h3)).rowsOk hrows
  have hkey := process_registry_updates_activeKey _ _ s3 s4 hrows3 current_epoch
    activation_epoch hc3 hactivation hcur h4
  rw [get_total_active_balance_congr_key _ s4 S current_epoch hc4 hsl5 (by rw [hv5]),
    get_total_active_balance_congr_key _ s3 s4 current_epoch hc3 hsl4 hkey,
    get_total_active_balance_congr_key _ s2 s3 current_epoch hc2 hsl3 (by rw [hv3]),
    get_total_active_balance_congr_key _ s1 s2 current_epoch hcurrent hsl2 (by rw [hv2])]
  exact htab

/-! ## The whole epoch -/

/-- `saturating_sub e 1` is `e - 1`. -/
private theorem saturating_sub_one' (e : Nat) : saturating_sub e 1 = e - 1 := by
  show (if e > 1 then e - 1 else e - e) = e - 1
  split <;> omega

/-- A `map` followed by a `bind` is one `bind`. -/
private theorem map_bind' {α β γ : Type} (x : SpecM α) (f : α → β) (g : β → SpecM γ) :
    ((f <$> x) >>= g) = (x >>= fun a => g (f a)) := by
  cases x <;> rfl

/-- On mainnet, at a state that `process_epoch` starts from, the Lighthouse epoch with the
deposits and effective balance updates in the single pass gives the same `ok` results as the
spec epoch. Beyond the hypotheses of `process_epoch_lh_sameOk'` it needs `WithdrawableValid`,
for the deposit statuses that Lighthouse predicts, and `ConsolidationsInRange`, so that the
validators that consolidations name are existing validators. -/
theorem process_epoch_lh_full_sameOk' (o : Oracle) (t : BeaconState) (hb : BoundaryOk t)
    (hslot : t.slot < 2 ^ 64) (heb : EBOk Preset.mainnet t) (hexitb : ExitBounds t)
    (hsupply_t : EpochSupply t) (hvalid : WithdrawableValid t)
    (hcons : ConsolidationsInRange t) :
    SameOk (process_epoch_lh_full o t) (process_epoch Preset.mainnet o t) := by
  obtain ⟨hinv, hrows, heffective⟩ := hb
  obtain ⟨hbalance, hsupply, hslashings⟩ := hsupply_t
  unfold process_epoch_lh_full
  split
  · exact SameOk.refl _
  rename_i hgen
  have hcurrent : get_current_epoch Preset.mainnet t =
      .ok (t.slot / Preset.mainnet.SLOTS_PER_EPOCH) := by
    simp [get_current_epoch, compute_epoch_at_slot, uint64Div, Preset.mainnet, pure,
      Except.pure]
  have hepoch : t.slot / Preset.mainnet.SLOTS_PER_EPOCH + 4096 < UINT64_SIZE := by
    have h64 : @LT.lt Nat _ t.slot (2 ^ 64) := hslot
    show @LT.lt Nat _ (t.slot / 32 + 4096) (2 ^ 64)
    omega
  unfold EntryEB at heffective
  generalize hce : t.slot / Preset.mainnet.SLOTS_PER_EPOCH = current_epoch
  rw [hce] at hcurrent hepoch hgen heffective
  rw [process_epoch_parts]
  refine SameOk.bind (SameOk.refl _) (fun s1 h1 => ?_)
  obtain ⟨_, _, _, _, hs1⟩ := process_justification_and_finalization_shape _ _ _ h1
  have hcur1 : get_current_epoch Preset.mainnet s1 = .ok current_epoch := by
    rw [hs1]; exact hcurrent
  have hfin := process_justification_and_finalization_finalized _ t s1 current_epoch hcurrent
    hgen hinv h1
  have hrows1 : RowsOk s1 := by rw [hs1]; exact hrows
  have heff1 : ∀ i (h : i < s1.validators.length),
      rewardsEligible (current_epoch - 1) s1.validators[i] = .ok true →
        s1.validators[i].effective_balance ≤ 256 * s1.balances.getD i 0 := by
    rw [hs1]; exact heffective
  have hexit1 : ∀ i (h : i < s1.validators.length),
      s1.validators[i].exit_epoch ≤ FAR_FUTURE_EPOCH := by
    rw [hs1]; exact fun i h => hexitb _ (List.getElem_mem h)
  have hmax1 : ∀ v ∈ s1.validators,
      v.effective_balance ≤ Preset.mainnet.MAX_EFFECTIVE_BALANCE_ELECTRA := by
    rw [hs1]; exact fun v hv => (heb v hv).2
  have hbal1 : ∀ i (h : i < s1.validators.length), s1.balances.getD i 0 < 2 ^ 62 := by
    rw [hs1]; exact hbalance
  have hvalid1 : ∀ v ∈ s1.validators, v.exit_epoch = FAR_FUTURE_EPOCH →
      v.withdrawable_epoch = FAR_FUTURE_EPOCH := by
    rw [hs1]; exact hvalid
  have hnamed : ∀ i ∈ consolidationIndices s1, i < s1.validators.length := by
    intro i hi
    have hi' : i ∈ (s1.pending_consolidations.flatMap fun c =>
        [c.source_index, c.target_index]).eraseDups :=
      (List.mem_mergeSort).mp hi
    have hi'' := List.mem_eraseDups.mp hi'
    obtain ⟨c, hc, hic⟩ := List.mem_flatMap.mp hi''
    have hc' : c ∈ t.pending_consolidations := by rw [hs1] at hc; exact hc
    have hlen : s1.validators.length = t.validators.length := by rw [hs1]
    rw [hlen]
    rcases List.mem_cons.mp hic with rfl | hic
    · exact (hcons c hc').1
    · rcases List.mem_singleton.mp hic with rfl
      exact (hcons c hc').2
  have hS1 : totalEffectiveBalance s1 = totalEffectiveBalance t := by rw [hs1]; rfl
  have hsl1 : s1.slashings = t.slashings := by rw [hs1]
  rw [← hS1] at hsupply
  rw [← hsl1] at hslashings
  simp only [GENESIS_EPOCH] at hgen
  have hS64 : totalEffectiveBalance s1 < 2 ^ 64 := Nat.lt_trans hsupply (by decide)
  obtain ⟨tab, htab, htab_lo, -⟩ :=
    get_total_active_balance_bounds Preset.mainnet s1 current_epoch hcur1 hS64
  have hprev : get_previous_epoch Preset.mainnet s1 = .ok (current_epoch - 1) := by
    have hsat : saturating_sub current_epoch 1 = current_epoch - 1 :=
      saturating_sub_one' current_epoch
    simp only [get_previous_epoch, hcur1, ok_bind'', hsat]
    rfl
  obtain ⟨rctx, hctx, hsrc, htgt, hhd⟩ := rewardsContextOf_bounds Preset.mainnet tab s1 hrows1
    current_epoch (current_epoch - 1) hcur1 hprev (by epoch_omega) (by decide) hS64
    (by epoch_omega)
  obtain ⟨hprev', -, -, -, hactive, -, -⟩ := rewardsContextOf_ok _ _ _ rctx hctx
  rw [hprev] at hprev'
  have hpe : rctx.previous_epoch = current_epoch - 1 := (Except.ok.inj hprev').symm
  have hact_eq : rctx.active_increments = tab / Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT := by
    simp only [uint64Div] at hactive
    split at hactive
    · cases hactive
    · exact (Except.ok.inj hactive).symm
  have hactivation := compute_activation_exit_epoch_of_lt Preset.mainnet current_epoch
    (by simp only [Preset.mainnet]; epoch_omega)
  obtain ⟨adj, per, hpre⟩ := slashingsPreamble_ok tab s1.slashings hslashings htab_lo
  have hk : max Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT (totalEffectiveBalance s1) /
      Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT ≤ 1152921504 := by
    have : max Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT (totalEffectiveBalance s1) <
        2 ^ 60 := Nat.max_lt.mpr ⟨by decide, hsupply⟩
    have hINC : Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT = 1000000000 := rfl
    obtain ⟨(m : Nat), hm⟩ : ∃ m : Nat,
        max Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT (totalEffectiveBalance s1) = m :=
      ⟨_, rfl⟩
    rw [hm] at this ⊢
    rw [hINC]
    have h60 : m < 1152921504606846976 := this
    show m / 1000000000 ≤ 1152921504
    omega
  have hact1 : 1 ≤ rctx.active_increments := by
    rw [hact_eq]
    exact (Nat.le_div_iff_mul_le (by decide)).mpr (by simpa using htab_lo)
  have hle : ∀ x, x ≤ max Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT (totalEffectiveBalance s1) /
      Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT →
      x ≤ max Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT (totalEffectiveBalance s1) /
        Preset.mainnet.EFFECTIVE_BALANCE_INCREMENT * rctx.active_increments :=
    fun x hx => Nat.le_trans hx (Nat.le_mul_of_pos_right _ hact1)
  have hcfar : current_epoch < FAR_FUTURE_EPOCH := by
    have h64 : @LT.lt Nat _ (current_epoch + 4096) (2 ^ 64) := hepoch
    show @LT.lt Nat _ current_epoch (2 ^ 64 - 1)
    omega
  have hnext : current_epoch + 1 < FAR_FUTURE_EPOCH := by
    have h64 : @LT.lt Nat _ (current_epoch + 4096) (2 ^ 64) := hepoch
    show @LT.lt Nat _ (current_epoch + 1) (2 ^ 64 - 1)
    omega
  have h4096 : Preset.mainnet.EPOCHS_PER_SLASHINGS_VECTOR / 2 = 4096 := rfl
  -- The four spec steps equal the first Lighthouse pass.
  have hfour := SameOk.trans (specFourSteps_sameOk Preset.mainnet s1 tab htab hrows1
    current_epoch _ hcur1 hactivation hcfar)
    (lighthouse_single_pass_eq_spec_supply tab s1 hrows1 current_epoch hcur1
    (by simpa [GENESIS_EPOCH] using hgen) rctx hctx (hpe ▸ heff1) hexit1 hmax1 hbal1 _
    hactivation (by rw [h4096]; exact hepoch) adj per hpre
    (Nat.le_of_succ_le hfin) (Nat.lt_of_le_of_lt (Nat.add_le_add_left (by decide) _) hepoch)
    htab_lo
    _ hk ⟨hle _ hsrc, hle _ htgt, hle _ hhd⟩ (lhBaseReward tab) (by
      intro r hr _
      obtain ⟨b, hb⟩ := rewardsBaseReward_ok_mainnet tab r.validator htab_lo
        (hmax1 _ (by rw [← rowsOf_map_validator]; exact List.mem_map_of_mem hr))
      simp only [lhBaseReward, hb]))
  -- The Lighthouse tail reaches `tailNF`.
  refine SameOk.trans (lhFullTail_nf o s1 current_epoch tab rctx adj per _ _ hcur1 htab hctx
    hpre hysteresisThresholds_mainnet hrows1) ?_
  -- The spec tail reaches `tailNF`.
  refine SameOk.flip ?_
  refine SameOk.trans (SameOk.bind hfour (fun _ _ => SameOk.refl _)) ?_
  rw [map_bind']
  refine SameOk.bind (SameOk.refl _) (fun x hx => ?_)
  have hspec := (hfour (s1.withRowsChurn x.1 x.2)).mpr (by rw [hx]; rfl)
  have htab4 := fourSteps_total s1 tab htab hrows1 current_epoch _ hcur1 hactivation hcfar _
    ((specFourSteps_sameOk Preset.mainnet s1 tab htab hrows1 current_epoch _ hcur1 hactivation
      hcfar _).mp hspec)
  exact epochRest_nf o s1 current_epoch tab _ _ x.1 x.2 _ hx rfl rfl hcur1
    (Nat.le_of_succ_le hfin) hnext hactivation hexit1 hvalid1 htab4
    (pass_balances_lt _ _ s1 _ x
      (fun i h => Nat.lt_trans (hbal1 i h) (by decide)) hx) hnamed


/-- `process_epoch_lh_full_sameOk'` in the form of the `hf` hypothesis of
`reachable_state_transition_sameOk'''`, with `EpochSupply` as the epoch condition. -/
theorem process_epoch_lh_full_hf (o : Oracle) :
    ∀ t, EpochInputOk'' EpochSupply t →
      SameOk (process_epoch_lh_full o t) (process_epoch Preset.mainnet o t) :=
  fun t ⟨⟨⟨hb, hslot, heb, hexit, hsupply⟩, _, hcons⟩, _, hvalid⟩ =>
    process_epoch_lh_full_sameOk' o t hb hslot heb hexit hsupply hvalid hcons

end EpochProofs.Spec
