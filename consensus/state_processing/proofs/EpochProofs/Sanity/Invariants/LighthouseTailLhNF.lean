import EpochProofs.Sanity.Invariants.LighthouseTailNF

/-!
# The Lighthouse tail in the common form

`lhFullTail_nf` shows that `lhFullTail` gives the same `ok` results as the first pass followed by
`tailNF`. The deposit plan does not depend on the pass, so it moves after the first pass. The
full pass splits into the first pass and the top-up and update pass, which is a fold over the
validator indices.
-/

namespace EpochProofs.Spec

/-- The steps of `lhFullTail` after the pass, for a plan and the result of the pass. -/
def lhAfterPass (o : Oracle) (s : BeaconState) (total_active_balance : Gwei)
    (plan : LhDepositPlan) (x : (Epoch × Gwei) × List Row) : SpecM BeaconState := do
  let s1 ← process_eth1_data_reset Preset.mainnet (s.withRowsChurn x.1 x.2)
  let s2 := { s1 with
    pending_deposits := s.pending_deposits.drop plan.next_deposit_index ++ plan.postponed
    deposit_balance_to_consume := plan.deposit_balance_to_consume }
  let s3 ← plan.new_validator_deposits.foldlM (apply_pending_deposit Preset.mainnet o) s2
  let s4 ← (List.range' s2.validators.length (s3.validators.length - s2.validators.length)).foldlM
    ebUpdateAt s3
  let s5 ← process_pending_consolidations Preset.mainnet s4
  let s6 ← (consolidationIndices s).foldlM ebUpdateAt s5
  let s7 ← process_builder_pending_payments Preset.mainnet total_active_balance s6
  epochRest2 o s7

/-- `lhFullTail` once the context values are known. -/
theorem lhFullTail_eq (o : Oracle) (s1 : BeaconState) (current_epoch : Epoch)
    (total_active_balance : Gwei) (rctx : RewardsContext) (adjusted per : Gwei)
    (downward upward : Uint64)
    (hcur : get_current_epoch Preset.mainnet s1 = .ok current_epoch)
    (htab : get_total_active_balance Preset.mainnet s1 = .ok total_active_balance)
    (hctx : rewardsContextOf Preset.mainnet total_active_balance s1 = .ok rctx)
    (hpre : slashingsPreamble Preset.mainnet total_active_balance s1.slashings =
      .ok (adjusted, per))
    (hthr : hysteresisThresholds Preset.mainnet = .ok (downward, upward)) :
    lhFullTail o s1 = (lhDepositPlan s1 total_active_balance current_epoch >>= fun plan =>
      passM (fun churn r => lhRowStepFull Preset.mainnet
          (lhContextOf s1 total_active_balance current_epoch rctx per) downward upward
          ⟨lhBaseReward total_active_balance r, plan.topups.getD r.index 0,
            decide (r.index ∈ consolidationIndices s1)⟩ churn r)
        (s1.earliest_exit_epoch, s1.exit_balance_to_consume) (rowsOf s1) >>=
      lhAfterPass o s1 total_active_balance plan) := by
  unfold lhFullTail
  rw [hcur, ok_bind', htab, ok_bind', hctx, ok_bind', hpre, ok_bind']
  simp only [hthr, ok_bind']
  rfl

/-- The deposit decisions of the plan. -/
def planDec (s : BeaconState) (total_active_balance : Gwei) (current_epoch : Epoch) :
    SpecM (Nat × Gwei × List Bool) := do
  let finalized_slot ←
    compute_start_slot_at_epoch Preset.mainnet s.finalized_checkpoint.epoch
  depositDecisions Preset.mainnet finalized_slot s.deposit_balance_to_consume
    (← get_activation_churn_limit Preset.mainnet total_active_balance)
    ((s.pending_deposits.take Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH).map
      (lhDepositView s current_epoch))

/-- The handled deposits for a decision. -/
def handledOfDec (s : BeaconState) (dec : Nat × Gwei × List Bool) :
    List (PendingDeposit × Bool) :=
  (s.pending_deposits.take dec.1).zip dec.2.2

/-- The top-up sums for a decision. -/
def sumsOfDec (s : BeaconState) (dec : Nat × Gwei × List Bool) : SpecM (List Gwei) :=
  (knownTopups (s.validators.map (·.pubkey)) (handledOfDec s dec)).foldlM
    (fun acc x => increase_balance acc x.1 x.2) (List.replicate s.validators.length 0)

/-- The plan for a decision and its top-up sums. -/
def planOf (s : BeaconState) (dec : Nat × Gwei × List Bool) (sums : List Gwei) :
    LhDepositPlan :=
  ⟨dec.1, dec.2.1, sums, ((handledOfDec s dec).filter (·.2)).map (·.1),
    newDeposits (s.validators.map (·.pubkey)) (handledOfDec s dec)⟩

/-- `lhDepositPlan` is the decisions, then the top-up sums. -/
theorem lhDepositPlan_eq (s : BeaconState) (total_active_balance : Gwei)
    (current_epoch : Epoch) :
    lhDepositPlan s total_active_balance current_epoch =
      (planDec s total_active_balance current_epoch >>= fun dec =>
        planOf s dec <$> sumsOfDec s dec) := by
  unfold lhDepositPlan planDec
  cases compute_start_slot_at_epoch Preset.mainnet s.finalized_checkpoint.epoch with
  | error e => rfl
  | ok fs =>
    simp only [ok_bind']
    cases get_activation_churn_limit Preset.mainnet total_active_balance with
    | error e => rfl
    | ok cl =>
      simp only [ok_bind']
      cases depositDecisions Preset.mainnet fs s.deposit_balance_to_consume cl
          ((s.pending_deposits.take Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH).map
            (lhDepositView s current_epoch)) with
      | error e => rfl
      | ok dec =>
        obtain ⟨next, dbtc, pp⟩ := dec
        simp only [ok_bind']
        rw [lhPlan_fold_eq]
        unfold sumsOfDec handledOfDec
        cases (knownTopups (s.validators.map (·.pubkey))
            ((s.pending_deposits.take next).zip pp)).foldlM
            (fun acc x => increase_balance acc x.1 x.2) (List.replicate s.validators.length 0) with
        | error e => rfl
        | ok sums => rfl

/-- The steps of `tailNF` after the top-up sums. -/
def nfAfter (o : Oracle) (s1 S4 : BeaconState) (total_active_balance : Gwei)
    (dec : Nat × Gwei × List Bool) (sums : List Gwei) : SpecM BeaconState := do
  let handled := handledOfDec s1 dec
  let pubkeys := s1.validators.map (·.pubkey)
  let n := s1.validators.length
  let named := consolidationIndices s1
  let t1 ← addSums S4 sums
  let t2 ← ((List.range n).filter (fun i => !decide (i ∈ named))).foldlM ebUpdateAt t1
  let t3 ← process_eth1_data_reset Preset.mainnet t2
  let t4 ← writeQueue (s1.pending_deposits.drop dec.1 ++ (handled.filter (·.2)).map (·.1))
    dec.2.1 t3
  let t5 ← (newDeposits pubkeys handled).foldlM (apply_pending_deposit Preset.mainnet o) t4
  let t6 ← (List.range' n (t5.validators.length - n)).foldlM ebUpdateAt t5
  let t7 ← process_pending_consolidations Preset.mainnet t6
  let t8 ← named.foldlM ebUpdateAt t7
  let t9 ← process_builder_pending_payments Preset.mainnet total_active_balance t8
  epochRest2 o t9

/-- `tailNF` is the decisions, the top-up sums, then `nfAfter`. -/
theorem tailNF_eq (o : Oracle) (s1 S4 : BeaconState) (total_active_balance : Gwei)
    (current_epoch : Epoch) :
    tailNF o s1 S4 total_active_balance current_epoch =
      (planDec s1 total_active_balance current_epoch >>= fun dec =>
        sumsOfDec s1 dec >>= fun sums => nfAfter o s1 S4 total_active_balance dec sums) := by
  unfold tailNF planDec
  cases compute_start_slot_at_epoch Preset.mainnet s1.finalized_checkpoint.epoch with
  | error e => rfl
  | ok fs =>
    simp only [ok_bind']
    cases get_activation_churn_limit Preset.mainnet total_active_balance with
    | error e => rfl
    | ok cl =>
      simp only [ok_bind']
      cases depositDecisions Preset.mainnet fs s1.deposit_balance_to_consume cl
          ((s1.pending_deposits.take Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH).map
            (lhDepositView s1 current_epoch)) with
      | error e => rfl
      | ok dec => rfl

/-! ## Moving the plan after the first pass -/

/-- Two computations that do not depend on each other give the same `ok` results in either
order. -/
theorem sameOk_bind_swap {α β γ : Type} (x : SpecM α) (y : SpecM β) (k : α → β → SpecM γ) :
    SameOk (x >>= fun a => y >>= fun b => k a b) (y >>= fun b => x >>= fun a => k a b) := by
  intro v
  cases x <;> cases y <;> simp [bind, Except.bind]

/-- `lhRowStep` keeps the row key. -/
theorem lhRowStep_rowKey (ctx : LhStepContext) (base : Gwei) (churn : Epoch × Gwei) (r : Row)
    (x : (Epoch × Gwei) × Row) (h : lhRowStep Preset.mainnet ctx base churn r = .ok x) :
    rowKey x.2 = rowKey r := by
  unfold lhRowStep at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; rfl)

/-! ## After the pass -/

/-- The steps of `lhAfterPass` from the state after the pass. -/
def lhRest (o : Oracle) (s : BeaconState) (total_active_balance : Gwei)
    (plan : LhDepositPlan) (T : BeaconState) : SpecM BeaconState := do
  let s1 ← process_eth1_data_reset Preset.mainnet T
  let s2 := { s1 with
    pending_deposits := s.pending_deposits.drop plan.next_deposit_index ++ plan.postponed
    deposit_balance_to_consume := plan.deposit_balance_to_consume }
  let s3 ← plan.new_validator_deposits.foldlM (apply_pending_deposit Preset.mainnet o) s2
  let s4 ← (List.range' s2.validators.length (s3.validators.length - s2.validators.length)).foldlM
    ebUpdateAt s3
  let s5 ← process_pending_consolidations Preset.mainnet s4
  let s6 ← (consolidationIndices s).foldlM ebUpdateAt s5
  let s7 ← process_builder_pending_payments Preset.mainnet total_active_balance s6
  epochRest2 o s7

/-- The steps of `nfAfter` from the state after the updates of the validators that are not
named. -/
def nfRest (o : Oracle) (s1 : BeaconState) (total_active_balance : Gwei)
    (dec : Nat × Gwei × List Bool) (t2 : BeaconState) : SpecM BeaconState := do
  let handled := handledOfDec s1 dec
  let n := s1.validators.length
  let t3 ← process_eth1_data_reset Preset.mainnet t2
  let t4 ← writeQueue (s1.pending_deposits.drop dec.1 ++ (handled.filter (·.2)).map (·.1))
    dec.2.1 t3
  let t5 ← (newDeposits (s1.validators.map (·.pubkey)) handled).foldlM
    (apply_pending_deposit Preset.mainnet o) t4
  let t6 ← (List.range' n (t5.validators.length - n)).foldlM ebUpdateAt t5
  let t7 ← process_pending_consolidations Preset.mainnet t6
  let t8 ← (consolidationIndices s1).foldlM ebUpdateAt t7
  let t9 ← process_builder_pending_payments Preset.mainnet total_active_balance t8
  epochRest2 o t9

/-- With a state of `n` validators, the two rests are equal. -/
theorem lhRest_eq_nfRest (o : Oracle) (s1 : BeaconState) (total_active_balance : Gwei)
    (dec : Nat × Gwei × List Bool) (sums : List Gwei) (T : BeaconState)
    (hT : T.validators.length = s1.validators.length) :
    lhRest o s1 total_active_balance (planOf s1 dec sums) T =
      nfRest o s1 total_active_balance dec T := by
  unfold lhRest nfRest
  cases he : process_eth1_data_reset Preset.mainnet T with
  | error e => rfl
  | ok t3 =>
    have hv := (eth1_frame T t3 he).1
    simp only [ok_bind', writeQueue]
    have hl : t3.validators.length = s1.validators.length := by rw [hv, hT]
    simp only [hl]
    rfl

/-- A fold of top-ups keeps the validators. -/
theorem increaseFold_validators (td : Nat → Gwei) :
    ∀ (l : List Nat) (T T' : BeaconState),
      l.foldlM (fun t i => increaseAt i (td i) t) T = .ok T' → T'.validators = T.validators
  | [], T, T', h => by cases h; rfl
  | i :: l, T, T', h => by
    rw [List.foldlM_cons] at h
    obtain ⟨T1, h1, h⟩ := specM_bind_ok h
    rw [increaseFold_validators td l T1 T' h, increaseAt_validators h1]

/-- A fold of effective balance updates keeps the number of validators. -/
theorem ebFold_length :
    ∀ (l : List Nat) (T T' : BeaconState),
      l.foldlM ebUpdateAt T = .ok T' → T'.validators.length = T.validators.length
  | [], T, T', h => by cases h; rfl
  | i :: l, T, T', h => by
    rw [List.foldlM_cons] at h
    obtain ⟨T1, h1, h⟩ := specM_bind_ok h
    obtain ⟨_, _, _, -, -, -, rfl⟩ := (ebUpdateAt_ok T T1 i).mp h1
    rw [ebFold_length l _ T' h, setEB_length]

/-! ## The main theorem -/

/-- After the first pass, the second pass and the rest of `lhFullTail` give the same `ok`
results as `nfAfter`. -/
theorem lhAfter_pass2_nf (o : Oracle) (s1 : BeaconState) (total_active_balance : Gwei)
    (downward upward : Uint64)
    (hthr : hysteresisThresholds Preset.mainnet = .ok (downward, upward)) (hrows : RowsOk s1)
    (dec : Nat × Gwei × List Bool) (sums : List Gwei) (x : (Epoch × Gwei) × List Row)
    (hkey : x.2.map rowKey = (rowsOf s1).map rowKey) :
    SameOk (passM (fun _ r => (fun r' => ((), r')) <$> lhRowStep2 downward upward
        (sums.getD r.index 0) (decide (r.index ∈ consolidationIndices s1)) r) () x.2 >>=
        fun y => lhAfterPass o s1 total_active_balance (planOf s1 dec sums) (x.1, y.2))
      (nfAfter o s1 (s1.withRowsChurn x.1 x.2) total_active_balance dec sums) := by
  let S4 := s1.withRowsChurn x.1 x.2
  let td := fun i => sums.getD i 0
  let nm := fun i => decide (i ∈ consolidationIndices s1)
  have hrowsS4 : rowsOf S4 = x.2 := rowsOf_withRows s1 x.2 hkey
  have hokS4 : RowsOk S4 := rowsOk_withRows_of_key s1 x.2 hrows hkey
  have hlen2 : x.2.length = s1.validators.length := by
    rw [← rowsOf_length, ← List.length_map (f := rowKey), hkey, List.length_map]
  have hlen : S4.validators.length = s1.validators.length := by
    show (x.2.map (·.validator)).length = _
    rw [List.length_map, hlen2]
  have hL : (passM (fun _ r => (fun r' => ((), r')) <$> lhRowStep2 downward upward
        (td r.index) (nm r.index) r) () x.2 >>=
        fun y => lhAfterPass o s1 total_active_balance (planOf s1 dec sums) (x.1, y.2)) =
      ((fun y => S4.withRows y.2) <$> passM (fun _ r => (fun r' => ((), r')) <$>
        lhRowStep2 downward upward (td r.index) (nm r.index) r) () (rowsOf S4)) >>=
        lhRest o s1 total_active_balance (planOf s1 dec sums) := by
    rw [bind_map_left, hrowsS4]
    rfl
  have hR : nfAfter o s1 S4 total_active_balance dec sums =
      (((List.range S4.validators.length).foldlM (fun t i => increaseAt i (td i) t) S4) >>=
        fun t => ((List.range S4.validators.length).filter (fun i => !nm i)).foldlM
          ebUpdateAt t) >>= nfRest o s1 total_active_balance dec := by
    simp only [nfAfter, addSums]
    rw [hlen, bind_assoc]
    rfl
  show SameOk (passM (fun _ r => (fun r' => ((), r')) <$> lhRowStep2 downward upward
      (td r.index) (nm r.index) r) () x.2 >>=
      fun y => lhAfterPass o s1 total_active_balance (planOf s1 dec sums) (x.1, y.2))
    (nfAfter o s1 S4 total_active_balance dec sums)
  rw [hL, hR]
  refine SameOk.bind ((rows_pass2_fold S4 hokS4 downward upward hthr td nm).trans
    (fold_interleave td nm _ S4)) (fun T2 hT2 => SameOk.of_eq ?_)
  apply lhRest_eq_nfRest
  obtain ⟨y, hy, rfl⟩ := (map_ok_iff' _ _ _).mp hT2
  have hyl := (passM_rows _ _ _ _ hy).1
  show (y.2.map (·.validator)).length = _
  rw [List.length_map, hyl, rowsOf_length, hlen]

/-- `lhFullTail` gives the same `ok` results as the first pass followed by `tailNF`. -/
theorem lhFullTail_nf (o : Oracle) (s1 : BeaconState) (current_epoch : Epoch)
    (total_active_balance : Gwei) (rctx : RewardsContext) (adjusted per : Gwei)
    (downward upward : Uint64)
    (hcur : get_current_epoch Preset.mainnet s1 = .ok current_epoch)
    (htab : get_total_active_balance Preset.mainnet s1 = .ok total_active_balance)
    (hctx : rewardsContextOf Preset.mainnet total_active_balance s1 = .ok rctx)
    (hpre : slashingsPreamble Preset.mainnet total_active_balance s1.slashings =
      .ok (adjusted, per))
    (hthr : hysteresisThresholds Preset.mainnet = .ok (downward, upward))
    (hrows : RowsOk s1) :
    SameOk (lhFullTail o s1)
      (do
        let x ← passM (fun c r => lhRowStep Preset.mainnet
            (lhContextOf s1 total_active_balance current_epoch rctx per)
            (lhBaseReward total_active_balance r) c r)
          (s1.earliest_exit_epoch, s1.exit_balance_to_consume) (rowsOf s1)
        tailNF o s1 (s1.withRowsChurn x.1 x.2) total_active_balance current_epoch) := by
  let ctx := lhContextOf s1 total_active_balance current_epoch rctx per
  let c0 := (s1.earliest_exit_epoch, s1.exit_balance_to_consume)
  let pass1 := passM (fun c r => lhRowStep Preset.mainnet ctx
    (lhBaseReward total_active_balance r) c r) c0 (rowsOf s1)
  rw [lhFullTail_eq o s1 current_epoch total_active_balance rctx adjusted per downward upward
    hcur htab hctx hpre hthr, lhDepositPlan_eq]
  simp only [tailNF_eq, bind_assoc, bind_map_left]
  refine SameOk.trans ?_ (sameOk_bind_swap (planDec s1 total_active_balance current_epoch)
    pass1 (fun dec x => sumsOfDec s1 dec >>= fun sums =>
      nfAfter o s1 (s1.withRowsChurn x.1 x.2) total_active_balance dec sums))
  refine SameOk.bind (SameOk.refl _) (fun dec _ => ?_)
  refine SameOk.trans ?_ (sameOk_bind_swap (sumsOfDec s1 dec) pass1 (fun sums x =>
      nfAfter o s1 (s1.withRowsChurn x.1 x.2) total_active_balance dec sums))
  refine SameOk.bind (SameOk.refl _) (fun sums _ => ?_)
  refine SameOk.trans (SameOk.bind (lhRowStepFull_split ctx downward upward
    (lhBaseReward total_active_balance) (fun i => sums.getD i 0)
    (fun i => decide (i ∈ consolidationIndices s1)) c0 (rowsOf s1))
    (fun _ _ => SameOk.refl _)) ?_
  simp only [bind_assoc, pure_bind]
  refine SameOk.bind (SameOk.refl _) (fun x hx => ?_)
  exact lhAfter_pass2_nf o s1 total_active_balance downward upward hthr hrows dec sums x
    (passM_rowKey _ (fun c r y h => lhRowStep_rowKey ctx _ c r y h) c0 (rowsOf s1) x hx)

end EpochProofs.Spec
