import EpochProofs.Sanity.Rows
import EpochProofs.Sanity.RewardsAndPenalties

/-!
# `process_rewards_and_penalties` as a pass over rows

`rewardsRowStep` computes the four deltas of one row with `rewardDeltas` and applies them with
`rewardsSequential`. `process_rewards_and_penalties_rows` shows that the spec pass gives the same
`ok` values as a `passM` of this step over `rowsOf state`.
-/

namespace EpochProofs.Spec

/-- The epoch-wide values that each row of the rewards pass reads. -/
structure RewardsContext where
  previous_epoch : Epoch
  in_leak : Bool
  total_active_balance : Gwei
  source_increments : Uint64
  target_increments : Uint64
  head_increments : Uint64
  active_increments : Uint64

/-- `unslashed_participating_increments` of `get_flag_index_deltas` for one flag. -/
def rewardsFlagIncrements (p : Preset) (state : BeaconState) (previous_epoch : Epoch)
    (flag_index : Nat) : SpecM Uint64 := do
  let indices ← get_unslashed_participating_indices p state flag_index previous_epoch
  uint64Div (← get_total_balance p state indices) p.EFFECTIVE_BALANCE_INCREMENT

/-- The context, computed with the spec functions. -/
def rewardsContextOf (p : Preset) (total_active_balance : Gwei) (state : BeaconState) :
    SpecM RewardsContext := do
  let previous_epoch ← get_previous_epoch p state
  let source_increments ← rewardsFlagIncrements p state previous_epoch TIMELY_SOURCE_FLAG_INDEX
  let target_increments ← rewardsFlagIncrements p state previous_epoch TIMELY_TARGET_FLAG_INDEX
  let head_increments ← rewardsFlagIncrements p state previous_epoch TIMELY_HEAD_FLAG_INDEX
  let active_increments ← uint64Div total_active_balance p.EFFECTIVE_BALANCE_INCREMENT
  let in_leak ← is_in_inactivity_leak p state
  pure ⟨previous_epoch, in_leak, total_active_balance, source_increments, target_increments,
    head_increments, active_increments⟩

/-- The test of `get_eligible_validator_indices` for one validator. -/
def rewardsEligible (previous_epoch : Epoch) (v : Validator) : SpecM Bool :=
  if is_active_validator v previous_epoch then pure true
  else if v.slashed then do pure (decide ((← uint64Add previous_epoch 1) < v.withdrawable_epoch))
  else pure false

/-- `has_flag` for a flag index below 8. -/
def rewardsHasFlag (flags : ParticipationFlags) (flag_index : Nat) : Bool :=
  flags &&& UInt8.ofNat (2 ^ flag_index) == UInt8.ofNat (2 ^ flag_index)

/-- The row is in `get_unslashed_participating_indices` for the flag and the previous epoch. -/
def rewardsParticipating (previous_epoch : Epoch) (r : Row) (flag_index : Nat) : Bool :=
  is_active_validator r.validator previous_epoch
    && rewardsHasFlag r.previous_participation flag_index && !r.validator.slashed

/-- `get_base_reward` for one validator. -/
def rewardsBaseReward (p : Preset) (total_active_balance : Gwei) (v : Validator) :
    SpecM Gwei := do
  let increments ← uint64Div v.effective_balance p.EFFECTIVE_BALANCE_INCREMENT
  uint64Mul increments (← get_base_reward_per_increment p total_active_balance)

/-- The four deltas of one row. A row that is not eligible gets four zero deltas. -/
def rewardsRowDeltas (p : Preset) (ctx : RewardsContext) (r : Row) :
    SpecM (List (Gwei × Gwei)) := do
  if (← rewardsEligible ctx.previous_epoch r.validator) then
    rewardDeltas p (← rewardsBaseReward p ctx.total_active_balance r.validator)
      r.validator.effective_balance r.inactivity_score
      (rewardsParticipating ctx.previous_epoch r TIMELY_SOURCE_FLAG_INDEX)
      (rewardsParticipating ctx.previous_epoch r TIMELY_TARGET_FLAG_INDEX)
      (rewardsParticipating ctx.previous_epoch r TIMELY_HEAD_FLAG_INDEX)
      ctx.in_leak ctx.source_increments ctx.target_increments ctx.head_increments
      ctx.active_increments
  else pure [(0, 0), (0, 0), (0, 0), (0, 0)]

/-- One row of `process_rewards_and_penalties`. Only `balance` changes. -/
def rewardsRowStep (p : Preset) (ctx : RewardsContext) : Unit → Row → SpecM (Unit × Row) :=
  fun _ r => do
    let deltas ← rewardsRowDeltas p ctx r
    let balance ← rewardsSequential r.balance deltas
    pure ((), { r with balance })

/-- The step changes only `balance`. -/
theorem rewardsRowStep_preserves (p : Preset) (ctx : RewardsContext) (u : Unit) (r : Row)
    (x : Unit × Row) (h : rewardsRowStep p ctx u r = .ok x) :
    x.2 = { r with balance := x.2.balance } := by
  simp only [rewardsRowStep, bind, Except.bind] at h
  cases hd : rewardsRowDeltas p ctx r with
  | error e => simp [hd] at h
  | ok d =>
    simp only [hd] at h
    cases hb : rewardsSequential r.balance d with
    | error e => simp [hb] at h
    | ok b =>
      simp only [hb, pure, Except.pure, Except.ok.injEq] at h
      subst h
      rfl

/-! ## General lemmas -/

/-- A bind succeeds exactly when both parts succeed. -/
private theorem bind_ok_iff {α β : Type} (x : SpecM α) (k : α → SpecM β) (v : β) :
    (x >>= k) = .ok v ↔ ∃ a, x = .ok a ∧ k a = .ok v := by
  cases x with
  | error e => simp [bind, Except.bind]
  | ok a => simp [bind, Except.bind]

/-- A map succeeds exactly when its argument succeeds. -/
private theorem map_ok_iff {α β : Type} (x : SpecM α) (g : α → β) (v : β) :
    (g <$> x) = .ok v ↔ ∃ a, x = .ok a ∧ g a = v := by
  cases x with
  | error e => simp [Functor.map, Except.map]
  | ok a => simp [Functor.map, Except.map]

/-- `mapM` succeeds only if each element succeeds with the matching value. -/
private theorem mapM_ok_get {α β : Type} (f : α → SpecM β) :
    ∀ (l : List α) (ys : List β), l.mapM f = .ok ys →
      ys.length = l.length ∧ ∀ i (h1 : i < l.length) (h2 : i < ys.length), f l[i] = .ok ys[i]
  | [], ys, h => by
    simp only [List.mapM_nil, pure, Except.pure, Except.ok.injEq] at h
    subst h
    simp
  | a :: l, ys, h => by
    rw [List.mapM_cons] at h
    cases hf : f a with
    | error e => simp [hf, bind, Except.bind] at h
    | ok y =>
      cases hl : l.mapM f with
      | error e => simp [hf, hl, bind, Except.bind] at h
      | ok ys' =>
        simp only [hf, hl, bind, Except.bind, pure, Except.pure, Except.ok.injEq] at h
        subst h
        obtain ⟨hlen, hget⟩ := mapM_ok_get f l ys' hl
        refine ⟨by simp [hlen], ?_⟩
        intro i h1 h2
        cases i with
        | zero => simpa using hf
        | succ j => simpa using hget j (by simpa using h1) (by simpa using h2)

/-- `mapM` succeeds if each element succeeds with the matching value. -/
private theorem mapM_ok_of {α β : Type} (f : α → SpecM β) :
    ∀ (l : List α) (ys : List β), ys.length = l.length →
      (∀ i (h1 : i < l.length) (h2 : i < ys.length), f l[i] = .ok ys[i]) → l.mapM f = .ok ys
  | [], ys, hlen, _ => by
    cases ys with
    | nil => rfl
    | cons y ys => simp at hlen
  | a :: l, ys, hlen, hget => by
    cases ys with
    | nil => simp at hlen
    | cons y ys =>
      have ha : f a = .ok y := hget 0 (by simp) (by simp)
      have hl : l.mapM f = .ok ys := mapM_ok_of f l ys (by simpa using hlen)
        (fun i h1 h2 => hget (i + 1) (by simpa using h1) (by simpa using h2))
      simp [List.mapM_cons, ha, hl, bind, Except.bind, pure, Except.pure]

/-- `mapM` succeeds with `ys` exactly when each element succeeds with the matching value. -/
private theorem mapM_ok_iff {α β : Type} (f : α → SpecM β) (l : List α) (ys : List β) :
    l.mapM f = .ok ys ↔
      ys.length = l.length ∧ ∀ i (h1 : i < l.length) (h2 : i < ys.length), f l[i] = .ok ys[i] :=
  ⟨mapM_ok_get f l ys, fun h => mapM_ok_of f l ys h.1 h.2⟩

/-- `mapM` succeeds if each element succeeds. -/
private theorem mapM_exists {α β : Type} (f : α → SpecM β) :
    ∀ (l : List α), (∀ a ∈ l, ∃ y, f a = .ok y) → ∃ ys, l.mapM f = .ok ys
  | [], _ => ⟨[], rfl⟩
  | a :: l, h => by
    obtain ⟨y, hy⟩ := h a (by simp)
    obtain ⟨ys, hys⟩ := mapM_exists f l (fun b hb => h b (by simp [hb]))
    exact ⟨y :: ys, by simp [List.mapM_cons, hy, hys, bind, Except.bind, pure, Except.pure]⟩

/-- `mapM` fails if one element fails. -/
private theorem mapM_not_ok {α β : Type} (f : α → SpecM β) (l : List α) (a : α) (ha : a ∈ l)
    (hf : ∀ y, f a ≠ .ok y) (ys : List β) : l.mapM f ≠ .ok ys := by
  intro h
  obtain ⟨hlen, hget⟩ := mapM_ok_get f l ys h
  obtain ⟨i, hi, rfl⟩ := List.getElem_of_mem ha
  exact hf _ (hget i hi (by omega))

/-- A pass with a `Unit` accumulator is a `mapM`. -/
private theorem passM_unit {R : Type} (f : Unit → R → SpecM (Unit × R)) :
    ∀ (rs : List R), passM f () rs = (fun ys => ((), ys)) <$> rs.mapM (fun r => Prod.snd <$> f () r)
  | [] => rfl
  | r :: rs => by
    rw [passM_cons, List.mapM_cons]
    cases hf : f () r with
    | error e => rfl
    | ok x =>
      obtain ⟨⟨⟩, x2⟩ := x
      show (passM f () rs >>= fun y => pure (y.fst, x2 :: y.snd)) =
        (fun ys => ((), ys)) <$> ((Prod.snd <$> Except.ok ((), x2)) >>= fun a =>
          rs.mapM (fun r => Prod.snd <$> f () r) >>= fun b => pure (a :: b))
      rw [passM_unit f rs]
      generalize rs.mapM (fun r => Prod.snd <$> f () r) = m
      cases m <;> rfl

/-- A loop over the indices of the rows that pass `Ev` is a `mapM` over all rows, if the body
reads and writes only the entry at its index, and that entry starts at `zero`. -/
private theorem forIn_filter_rows {S D : Type} (enc : List D → S) (zero : D) (Q : Row → Prop)
    (Ev : Row → Bool) (c : Row → SpecM D) (body : Nat → S → SpecM (ForInStep S))
    (hbody : ∀ (r : Row) (pre suf : List D), Q r → Ev r = true → r.index = pre.length →
      body r.index (enc (pre ++ zero :: suf)) =
        (fun d => ForInStep.yield (enc (pre ++ d :: suf))) <$> c r) :
    ∀ (rows : List Row) (pre : List D), (∀ r ∈ rows, Q r) →
      rows.map (·.index) = List.range' pre.length rows.length →
      forIn ((rows.filter Ev).map (·.index)) (enc (pre ++ List.replicate rows.length zero)) body =
        (fun ds => enc (pre ++ ds)) <$> rows.mapM (fun r => if Ev r then c r else pure zero)
  | [], pre, _, _ => by simp [pure, Except.pure, Functor.map, Except.map]
  | r :: rs, pre, hQ, hidx => by
    simp only [List.map_cons, List.length_cons, List.range'_succ, List.cons.injEq] at hidx
    obtain ⟨hr, hrs⟩ := hidx
    have hQs : ∀ r ∈ rs, Q r := fun r' h => hQ r' (by simp [h])
    rw [List.length_cons, List.replicate_succ, List.mapM_cons]
    by_cases he : Ev r = true
    · rw [List.filter_cons_of_pos he, List.map_cons, List.forIn_cons,
        hbody r pre _ (hQ r (by simp)) he hr]
      simp only [he, if_true]
      cases hc : c r with
      | error e => rfl
      | ok d =>
        have ih := forIn_filter_rows enc zero Q Ev c body hbody rs (pre ++ [d]) hQs
          (by simpa using hrs)
        simp only [List.append_assoc, List.cons_append, List.nil_append] at ih
        simp only [Functor.map, Except.map, bind, Except.bind, ih]
        cases rs.mapM (fun r => if Ev r = true then c r else pure zero) <;> simp [pure, Except.pure]
    · rw [List.filter_cons_of_neg he]
      have ih := forIn_filter_rows enc zero Q Ev c body hbody rs (pre ++ [zero]) hQs
        (by simpa using hrs)
      simp only [List.append_assoc, List.cons_append, List.nil_append] at ih
      rw [ih]
      simp only [he, Bool.false_eq_true, if_false, Functor.map, Except.map, bind, Except.bind,
        pure, Except.pure]
      cases rs.mapM (fun r => if Ev r = true then c r else Except.ok zero) <;> simp

/-- A loop over `range' k m` on a list, where the body at index `i` reads and writes only entry
`i`, is a `mapM` over the entries. -/
private theorem forIn_range_local {B : Type} (N : Nat) (F : Nat → B → SpecM B)
    (body : Nat → List B → SpecM (ForInStep (List B)))
    (hbody : ∀ (pre suf : List B) (b : B), (pre ++ b :: suf).length = N →
      body pre.length (pre ++ b :: suf) =
        (fun b' => ForInStep.yield (pre ++ b' :: suf)) <$> F pre.length b) :
    ∀ (suf pre : List B), (pre ++ suf).length = N →
      forIn (List.range' pre.length suf.length) (pre ++ suf) body =
        (fun ys => pre ++ ys) <$> (suf.zipIdx pre.length).mapM (fun x => F x.2 x.1)
  | [], pre, _ => by simp [pure, Except.pure, Functor.map, Except.map]
  | b :: suf, pre, hlen => by
    rw [List.length_cons, List.range'_succ, List.forIn_cons, hbody pre suf b hlen,
      List.zipIdx_cons, List.mapM_cons]
    cases hF : F pre.length b with
    | error e => rfl
    | ok b' =>
      have ih := forIn_range_local N F body hbody suf (pre ++ [b'])
        (by simp only [List.length_append, List.length_cons] at hlen ⊢; simp; omega)
      simp only [List.append_assoc, List.cons_append, List.nil_append, List.length_append,
        List.length_singleton] at ih
      simp only [Functor.map, Except.map, bind, Except.bind, ih]
      cases (suf.zipIdx (pre.length + 1)).mapM (fun x => F x.2 x.1) <;> simp [pure, Except.pure]

/-- A filtering loop where each test succeeds is `List.filter`. -/
private theorem forIn_filter_ok {X Y : Type} (P : X → SpecM Bool) (Q : X → Bool) (out : X → Y)
    (body : X → List Y → SpecM (ForInStep (List Y)))
    (hbody : ∀ x acc, body x acc =
      (fun b => ForInStep.yield (if b then acc ++ [out x] else acc)) <$> P x) :
    ∀ (l : List X) (acc : List Y), (∀ x ∈ l, P x = .ok (Q x)) →
      forIn l acc body = .ok (acc ++ (l.filter Q).map out)
  | [], acc, _ => by simp [pure, Except.pure]
  | x :: l, acc, h => by
    rw [List.forIn_cons, hbody, h x (by simp)]
    simp only [Functor.map, Except.map, bind, Except.bind]
    rw [forIn_filter_ok P Q out body hbody l _ (fun y hy => h y (by simp [hy]))]
    cases hq : Q x <;> simp [hq]

/-- A filtering loop fails if one test fails. -/
private theorem forIn_filter_not_ok {X Y : Type} (P : X → SpecM Bool) (out : X → Y)
    (body : X → List Y → SpecM (ForInStep (List Y)))
    (hbody : ∀ x acc, body x acc =
      (fun b => ForInStep.yield (if b then acc ++ [out x] else acc)) <$> P x) :
    ∀ (l : List X) (acc : List Y) (x : X), x ∈ l → (∀ b, P x ≠ .ok b) →
      ∀ v, forIn l acc body ≠ .ok v
  | [], _, _, hx, _ => by simp at hx
  | y :: l, acc, x, hx, hP => by
    intro v hv
    rw [List.forIn_cons, hbody] at hv
    cases hy : P y with
    | error e => simp [hy, Functor.map, Except.map, bind, Except.bind] at hv
    | ok b =>
      simp only [hy, Functor.map, Except.map, bind, Except.bind] at hv
      rcases List.mem_cons.mp hx with hxy | hxl
      · subst hxy; exact hP b hy
      · exact forIn_filter_not_ok P out body hbody l _ x hxl hP v hv

/-- The row at position `i` has index `i`. -/
private theorem rowsOf_index_getElem (state : BeaconState) (i : Nat)
    (h : i < (rowsOf state).length) : (rowsOf state)[i].index = i := by
  simp [rowsOf_getElem]

/-- The indices of the rows are `0, 1, 2, ...`. -/
private theorem rowsOf_indices (state : BeaconState) :
    (rowsOf state).map (·.index) = List.range' 0 (rowsOf state).length := by
  apply List.ext_getElem
  · simp
  · intro i h1 h2
    simp [rowsOf_getElem]

/-- A row of the state, as a position. -/
private theorem mem_rowsOf_iff {state : BeaconState} {r : Row} (h : r ∈ rowsOf state) :
    ∃ i, ∃ hi : i < (rowsOf state).length, (rowsOf state)[i] = r :=
  List.getElem_of_mem h

/-- The index of a row is a valid validator index. -/
private theorem rowsOf_index_lt {state : BeaconState} {r : Row} (h : r ∈ rowsOf state) :
    r.index < state.validators.length := by
  obtain ⟨i, hi, rfl⟩ := mem_rowsOf_iff h
  rw [rowsOf_index_getElem]
  simpa [rowsOf_length] using hi

/-- The validator at the index of a row is the validator of the row. -/
private theorem listGet_validators_row {state : BeaconState} {r : Row} (h : r ∈ rowsOf state) :
    listGet state.validators r.index = .ok r.validator := by
  obtain ⟨i, hi, rfl⟩ := mem_rowsOf_iff h
  have hi' : i < state.validators.length := by simpa [rowsOf_length] using hi
  simp [rowsOf_getElem, listGet, hi', pure, Except.pure]

/-- A list with one entry per validator has an entry at the index of each row. -/
private theorem getD_row {α : Type} (l : List α) (d : α) (n : Nat) (hl : l.length = n)
    {state : BeaconState} (hn : state.validators.length = n) {r : Row} (h : r ∈ rowsOf state) :
    listGet l r.index = .ok (l.getD r.index d) := by
  have := rowsOf_index_lt h
  have hlt : r.index < l.length := by omega
  simp [listGet, hlt, List.getD_eq_getElem?_getD, pure, Except.pure]

/-- The previous participation at the index of a row is the field of the row. -/
private theorem previous_participation_row {state : BeaconState} {r : Row} (h : r ∈ rowsOf state) :
    state.previous_epoch_participation.getD r.index 0 = r.previous_participation := by
  obtain ⟨i, hi, rfl⟩ := mem_rowsOf_iff h
  simp [rowsOf_getElem]

/-- The inactivity score at the index of a row is the field of the row. -/
private theorem inactivity_score_row {state : BeaconState} {r : Row} (h : r ∈ rowsOf state) :
    state.inactivity_scores.getD r.index 0 = r.inactivity_score := by
  obtain ⟨i, hi, rfl⟩ := mem_rowsOf_iff h
  simp [rowsOf_getElem]

/-- `has_flag` is `rewardsHasFlag` for a flag index below 8. -/
private theorem has_flag_eq (flags : ParticipationFlags) (flag_index : Nat)
    (h : 2 ^ flag_index < 256) : has_flag flags flag_index = .ok (rewardsHasFlag flags flag_index) := by
  simp [has_flag, rewardsHasFlag, h, pure, Except.pure]

/-- Two loop bodies that agree give the same loop. -/
private theorem forIn_ext {X S : Type} (l : List X) (s : S)
    (B C : X → S → SpecM (ForInStep S)) (h : ∀ x s, B x s = C x s) :
    forIn l s B = forIn l s C := by
  rw [show B = C from funext fun x => funext fun s => h x s]

/-- The loop body of `get_eligible_validator_indices`, with the test as `rewardsEligible`. -/
private def eligibleBody (previous_epoch : Epoch) (x : Validator × Nat) (acc : List Nat) :
    SpecM (ForInStep (List Nat)) :=
  (fun b => ForInStep.yield (if b then acc ++ [x.2] else acc)) <$>
    rewardsEligible previous_epoch x.1

/-- `get_eligible_validator_indices` is a loop of `eligibleBody`. -/
private theorem eligible_eq (p : Preset) (state : BeaconState) (previous_epoch : Epoch)
    (hprevious : get_previous_epoch p state = .ok previous_epoch) :
    get_eligible_validator_indices p state =
      forIn state.validators.zipIdx [] (eligibleBody previous_epoch) := by
  unfold get_eligible_validator_indices
  simp only [bind, Except.bind, hprevious]
  rw [forIn_ext _ _ _ (eligibleBody previous_epoch)]
  · cases forIn state.validators.zipIdx [] (eligibleBody previous_epoch) <;> rfl
  · rintro ⟨v, index⟩ acc
    simp only [eligibleBody, rewardsEligible]
    by_cases ha : is_active_validator v previous_epoch = true
    · simp [ha, pure, Except.pure, Functor.map, Except.map]
    · by_cases hs : v.slashed = true
      · cases uint64Add previous_epoch 1 with
        | error e => simp [ha, hs, bind, Except.bind, Functor.map, Except.map]
        | ok d =>
          by_cases hd : d < v.withdrawable_epoch <;>
            simp [ha, hs, hd, bind, Except.bind, pure, Except.pure, Functor.map, Except.map]
      · simp [ha, hs, pure, Except.pure, Functor.map, Except.map]

/-- `get_eligible_validator_indices` is the indices of the eligible rows, if each test
succeeds. -/
private theorem eligible_ok (p : Preset) (state : BeaconState) (previous_epoch : Epoch)
    (hprevious : get_previous_epoch p state = .ok previous_epoch) (Ev : Validator → Bool)
    (h : ∀ r ∈ rowsOf state, rewardsEligible previous_epoch r.validator = .ok (Ev r.validator)) :
    get_eligible_validator_indices p state =
      .ok (((rowsOf state).filter fun r => Ev r.validator).map (·.index)) := by
  rw [eligible_eq p state previous_epoch hprevious,
    forIn_filter_ok (fun x => rewardsEligible previous_epoch x.1) (fun x => Ev x.1) (·.2)]
  · simp [rowsOf, List.filter_map, Function.comp_def]
  · intro x acc; rfl
  · intro x hx
    exact h _ (List.mem_map.mpr ⟨x, hx, rfl⟩)

/-- `get_eligible_validator_indices` fails if the test fails for one row. -/
private theorem eligible_not_ok (p : Preset) (state : BeaconState) (previous_epoch : Epoch)
    (hprevious : get_previous_epoch p state = .ok previous_epoch) (r : Row)
    (hr : r ∈ rowsOf state) (hbad : ∀ b, rewardsEligible previous_epoch r.validator ≠ .ok b) :
    ∀ v, get_eligible_validator_indices p state ≠ .ok v := by
  rw [eligible_eq p state previous_epoch hprevious]
  apply forIn_filter_not_ok (fun x => rewardsEligible previous_epoch x.1) (·.2) _
    (fun x acc => rfl) _ _ (r.validator, r.index)
  · obtain ⟨i, hi, rfl⟩ := mem_rowsOf_iff hr
    have hi' : i < state.validators.length := by simpa [rowsOf_length] using hi
    rw [List.mem_iff_getElem]
    exact ⟨i, by simpa using hi', by simp [rowsOf_getElem]⟩
  · exact hbad

/-- `get_active_validator_indices` is the indices of the active rows. -/
private theorem active_indices_rows (state : BeaconState) (epoch : Epoch) :
    get_active_validator_indices state epoch =
      ((rowsOf state).filter fun r => is_active_validator r.validator epoch).map (·.index) := by
  simp [get_active_validator_indices, rowsOf, List.filter_map, Function.comp_def]

/-- `get_unslashed_participating_indices` for the previous epoch is the indices of the
participating rows. -/
private theorem participating_ok (p : Preset) (state : BeaconState) (hrows : RowsOk state)
    (current_epoch previous_epoch : Epoch) (flag_index : Nat)
    (hcurrent : get_current_epoch p state = .ok current_epoch)
    (hprevious : get_previous_epoch p state = .ok previous_epoch)
    (hne : previous_epoch ≠ current_epoch) (hflag : 2 ^ flag_index < 256) :
    get_unslashed_participating_indices p state flag_index previous_epoch =
      .ok (((rowsOf state).filter fun r => rewardsParticipating previous_epoch r flag_index).map
        (·.index)) := by
  have hne' : (previous_epoch == current_epoch) = false := by simpa using hne
  unfold get_unslashed_participating_indices
  simp only [bind, Except.bind, hprevious, hcurrent, BEq.rfl, Bool.true_or, Bool.not_true,
    Bool.false_eq_true, if_false, pure, Except.pure, hne']
  rw [active_indices_rows]
  rw [forIn_filter_ok
    (fun i => do has_flag (← listGet state.previous_epoch_participation i) flag_index)
    (fun i => rewardsHasFlag (state.previous_epoch_participation.getD i 0) flag_index) id]
  rotate_left
  · intro i acc
    simp only [bind, Except.bind]
    cases listGet state.previous_epoch_participation i with
    | error e => rfl
    | ok f =>
      cases hf : has_flag f flag_index with
      | error e => simp [hf, Functor.map, Except.map]
      | ok b => cases b <;> simp [hf, Functor.map, Except.map]
  · intro i hi
    obtain ⟨r, hr, rfl⟩ := List.mem_map.mp hi
    have hr' := (List.mem_filter.mp hr).1
    rw [getD_row _ 0 state.validators.length hrows.2.2.1 rfl hr']
    exact has_flag_eq _ _ hflag
  dsimp only
  rw [forIn_filter_ok (fun i => (fun v => !v.slashed) <$> listGet state.validators i)
    (fun i => ((state.validators[i]?).map (!·.slashed)).getD false) id]
  rotate_left
  · intro i acc
    cases listGet state.validators i with
    | error e => rfl
    | ok v => cases hs : v.slashed <;> simp [hs, Functor.map, Except.map]
  · intro i hi
    simp only [List.nil_append, List.map_id_fun, id_eq, List.mem_filter, List.mem_map] at hi
    obtain ⟨⟨r, hr, rfl⟩, -⟩ := hi
    have hlt := rowsOf_index_lt hr.1
    simp [listGet, hlt, pure, Except.pure, Functor.map, Except.map]
  simp only [List.nil_append, List.map_id_fun, id_eq, List.filter_map, List.filter_filter,
    Function.comp_def]
  congr 2
  apply List.filter_congr
  intro r hr
  have hlt := rowsOf_index_lt hr
  rw [previous_participation_row hr]
  obtain ⟨i, hi, rfl⟩ := mem_rowsOf_iff hr
  simp only [rewardsParticipating, rowsOf_getElem] at hlt ⊢
  simp only [hlt, getElem?_pos, Option.map_some, Option.getD_some]
  cases is_active_validator state.validators[i] previous_epoch <;>
    cases rewardsHasFlag (state.previous_epoch_participation.getD i 0) flag_index <;>
    cases state.validators[i].slashed <;> rfl

/-- `mapM` only reads the function on the members of the list. -/
private theorem mapM_congr_mem {α β : Type} (f g : α → SpecM β) :
    ∀ (l : List α), (∀ a ∈ l, f a = g a) → l.mapM f = l.mapM g
  | [], _ => rfl
  | a :: l, h => by
    rw [List.mapM_cons, List.mapM_cons, h a (by simp),
      mapM_congr_mem f g l (fun b hb => h b (by simp [hb]))]

/-- A row of the state is at the position of its index. -/
private theorem rowsOf_getElem_index {state : BeaconState} {r : Row} (h : r ∈ rowsOf state) :
    ∃ hi : r.index < (rowsOf state).length, (rowsOf state)[r.index] = r := by
  obtain ⟨i, hi, rfl⟩ := mem_rowsOf_iff h
  rw [rowsOf_index_getElem]
  exact ⟨hi, rfl⟩

/-- A row index is in the index list of the rows that pass `P` exactly when its row passes. -/
private theorem index_mem_filter {state : BeaconState} (P : Row → Bool) {r : Row}
    (h : r ∈ rowsOf state) :
    r.index ∈ ((rowsOf state).filter P).map (·.index) ↔ P r = true := by
  constructor
  · intro hm
    obtain ⟨r', hr', hidx⟩ := List.mem_map.mp hm
    obtain ⟨hr'1, hr'2⟩ := List.mem_filter.mp hr'
    obtain ⟨hi, hget⟩ := rowsOf_getElem_index h
    obtain ⟨hi', hget'⟩ := rowsOf_getElem_index hr'1
    have : r' = r := by
      rw [← hget, ← hget']
      simp only [hidx]
    rw [← this]; exact hr'2
  · intro hp
    exact List.mem_map.mpr ⟨r, List.mem_filter.mpr ⟨h, hp⟩, rfl⟩

/-- Read the entry after a prefix. -/
private theorem listGet_map_mid {D B : Type} (g : D → B) (pre suf : List D) (d : D) (i : Nat)
    (hi : i = pre.length) : listGet ((pre ++ d :: suf).map g) i = .ok (g d) := by
  subst hi
  simp [listGet, pure, Except.pure]

/-- Write the entry after a prefix. -/
private theorem listSet_map_mid {D B : Type} (g : D → B) (pre suf : List D) (d : D) (b : B)
    (i : Nat) (hi : i = pre.length) :
    listSet ((pre ++ d :: suf).map g) i b = .ok (pre.map g ++ b :: suf.map g) := by
  subst hi
  simp [listSet, pure, Except.pure, List.set_append_right]

/-- Adding to zero succeeds for a value that fits in `uint64`. -/
private theorem uint64Add_zero_left (x : Uint64) (h : x < UINT64_SIZE) :
    uint64Add 0 x = .ok x := by
  simp [uint64Add, h, pure, Except.pure]

/-- A successful product fits in `uint64`. -/
private theorem uint64Mul_lt {a b c : Uint64} (h : uint64Mul a b = .ok c) : c < UINT64_SIZE := by
  unfold uint64Mul at h
  split at h
  · rename_i hlt
    simp only [pure, Except.pure, Except.ok.injEq] at h
    rw [← h]; exact hlt
  · simp [throw, throwThe, MonadExceptOf.throw] at h

/-- A quotient of a value that fits in `uint64` also fits. -/
private theorem uint64Div_lt {a b c : Uint64} (h : uint64Div a b = .ok c) (ha : a < UINT64_SIZE) :
    c < UINT64_SIZE := by
  unfold uint64Div at h
  split at h
  · simp [throw, throwThe, MonadExceptOf.throw] at h
  · simp only [pure, Except.pure, Except.ok.injEq] at h
    rw [← h]
    exact Nat.lt_of_le_of_lt (Nat.div_le_self a b) ha

/-- The flag delta of one eligible row, as `get_flag_index_deltas` computes it. -/
private def flagCore (p : Preset) (ctx : RewardsContext) (weight : Uint64) (flag_index : Nat)
    (increments : Uint64) (r : Row) : SpecM (Gwei × Gwei) := do
  flagDelta (← rewardsBaseReward p ctx.total_active_balance r.validator) weight
    (rewardsParticipating ctx.previous_epoch r flag_index)
    (flag_index == TIMELY_HEAD_FLAG_INDEX) ctx.in_leak increments ctx.active_increments

/-- The flag delta of one row. A row that is not eligible gets a zero delta. -/
private def flagRow (p : Preset) (ctx : RewardsContext) (weight : Uint64) (flag_index : Nat)
    (increments : Uint64) (r : Row) : SpecM (Gwei × Gwei) := do
  if (← rewardsEligible ctx.previous_epoch r.validator) then
    flagCore p ctx weight flag_index increments r
  else pure (0, 0)

/-- `get_base_reward` at the index of a row is `rewardsBaseReward` of the row. -/
private theorem get_base_reward_row (p : Preset) (total_active_balance : Gwei)
    {state : BeaconState} {r : Row} (h : r ∈ rowsOf state) :
    get_base_reward p total_active_balance state r.index =
      rewardsBaseReward p total_active_balance r.validator := by
  unfold get_base_reward rewardsBaseReward
  rw [listGet_validators_row h]
  rfl

/-- `get_flag_index_deltas` gives the same `ok` values as `flagRow` over the rows. -/
private theorem flag_deltas_sameOk (p : Preset) (state : BeaconState) (hrows : RowsOk state)
    (ctx : RewardsContext) (current_epoch : Epoch)
    (hcurrent : get_current_epoch p state = .ok current_epoch)
    (hprevious : get_previous_epoch p state = .ok ctx.previous_epoch)
    (hne : ctx.previous_epoch ≠ current_epoch)
    (hleak : is_in_inactivity_leak p state = .ok ctx.in_leak)
    (hactive : uint64Div ctx.total_active_balance p.EFFECTIVE_BALANCE_INCREMENT =
      .ok ctx.active_increments)
    (flag_index : Nat) (weight increments : Uint64) (hflag : flag_index < 3)
    (hweight : listGet PARTICIPATION_FLAG_WEIGHTS flag_index = .ok weight)
    (hinc : rewardsFlagIncrements p state ctx.previous_epoch flag_index = .ok increments) :
    SameOk (get_flag_index_deltas p ctx.total_active_balance state flag_index)
      ((fun ds => (ds.map (·.1), ds.map (·.2))) <$>
        (rowsOf state).mapM (flagRow p ctx weight flag_index increments)) := by
  have hpow : 2 ^ flag_index < 256 := by
    have : 2 ^ flag_index ≤ 2 ^ 2 := Nat.pow_le_pow_right (by decide) (by omega)
    omega
  have hpart := participating_ok p state hrows current_epoch ctx.previous_epoch flag_index
    hcurrent hprevious hne hpow
  unfold rewardsFlagIncrements at hinc
  simp only [hpart, bind, Except.bind] at hinc
  cases htotal : get_total_balance p state (((rowsOf state).filter fun r =>
      rewardsParticipating ctx.previous_epoch r flag_index).map (·.index)) with
  | error e => simp [htotal] at hinc
  | ok total =>
  simp only [htotal] at hinc
  unfold get_flag_index_deltas
  simp only [bind, Except.bind, hprevious, hpart, hweight, htotal, hinc, hactive, hleak]
  by_cases hall : ∀ r ∈ rowsOf state, ∃ b, rewardsEligible ctx.previous_epoch r.validator = .ok b
  · let Ev : Validator → Bool := fun v =>
      match rewardsEligible ctx.previous_epoch v with
      | .ok b => b
      | .error _ => false
    have hEv : ∀ r ∈ rowsOf state,
        rewardsEligible ctx.previous_epoch r.validator = .ok (Ev r.validator) := by
      intro r hr
      obtain ⟨b, hb⟩ := hall r hr
      simp [Ev, hb]
    rw [eligible_ok p state _ hprevious Ev hEv]
    dsimp only
    have hinit : (⟨List.replicate state.validators.length 0,
        List.replicate state.validators.length 0⟩ : MProd (List Gwei) (List Gwei)) =
        (fun ds : List (Gwei × Gwei) =>
          (⟨ds.map (·.2), ds.map (·.1)⟩ : MProd (List Gwei) (List Gwei)))
          ([] ++ List.replicate (rowsOf state).length (0, 0)) := by
      simp [rowsOf_length]
    rw [hinit, forIn_filter_rows (fun ds : List (Gwei × Gwei) =>
        (⟨ds.map (·.2), ds.map (·.1)⟩ : MProd (List Gwei) (List Gwei))) (0, 0)
        (· ∈ rowsOf state) (fun r => Ev r.validator) (flagCore p ctx weight flag_index increments)]
    · have hmap : (rowsOf state).mapM (flagRow p ctx weight flag_index increments) =
          (rowsOf state).mapM (fun r => if Ev r.validator then
            flagCore p ctx weight flag_index increments r else pure (0, 0)) :=
        mapM_congr_mem _ _ _ (fun r hr => by simp [flagRow, hEv r hr, bind, Except.bind])
      rw [hmap]
      apply SameOk.of_eq
      cases (rowsOf state).mapM (fun r => if Ev r.validator then
        flagCore p ctx weight flag_index increments r else pure (0, 0)) <;> rfl
    · intro r pre suf hr _ hidx
      rw [get_base_reward_row _ _ hr]
      dsimp only
      simp only [listGet_map_mid _ pre suf (0, 0) r.index hidx,
        listSet_map_mid _ pre suf (0, 0) _ r.index hidx]
      unfold flagCore
      cases hb : rewardsBaseReward p ctx.total_active_balance r.validator with
      | error e => rfl
      | ok br =>
        simp only [bind, Except.bind]
        have hmem := index_mem_filter (fun r => rewardsParticipating ctx.previous_epoch r flag_index) hr
        by_cases hp : rewardsParticipating ctx.previous_epoch r flag_index = true
        · rw [if_pos (hmem.mpr hp)]
          simp only [hp, flagDelta, if_true]
          cases ctx.in_leak
          · simp only [Bool.not_false, if_true]
            cases hm1 : uint64Mul br weight with
            | error e => simp [Functor.map, Except.map, bind, Except.bind]
            | ok m1 =>
            simp only [bind, Except.bind]
            cases hm2 : uint64Mul m1 increments with
            | error e => simp [Functor.map, Except.map]
            | ok m2 =>
            dsimp only
            cases hm3 : uint64Mul ctx.active_increments WEIGHT_DENOMINATOR with
            | error e => simp [Functor.map, Except.map]
            | ok m3 =>
            dsimp only
            cases hd : uint64Div m2 m3 with
            | error e => simp [Functor.map, Except.map]
            | ok reward =>
            simp [uint64Add_zero_left _ (uint64Div_lt hd (uint64Mul_lt hm2)), pure, Except.pure,
              Functor.map, Except.map]
          · simp [pure, Except.pure, Functor.map, Except.map]
        · rw [if_neg (fun h => hp (hmem.mp h))]
          simp only [hp, flagDelta, Bool.false_eq_true, if_false]
          by_cases hh : flag_index = TIMELY_HEAD_FLAG_INDEX
          · simp [hh, pure, Except.pure, Functor.map, Except.map]
          · have hne' : (flag_index != TIMELY_HEAD_FLAG_INDEX) = true := by simpa using hh
            have heq' : (flag_index == TIMELY_HEAD_FLAG_INDEX) = false := by simpa using hh
            simp only [hne', heq', Bool.not_false, if_true]
            cases hm1 : uint64Mul br weight with
            | error e => simp [Functor.map, Except.map, bind, Except.bind]
            | ok m1 =>
            simp only [bind, Except.bind]
            cases hd : uint64Div m1 WEIGHT_DENOMINATOR with
            | error e => simp [Functor.map, Except.map]
            | ok penalty =>
            simp [uint64Add_zero_left _ (uint64Div_lt hd (uint64Mul_lt hm1)), pure, Except.pure,
              Functor.map, Except.map]
    · exact fun r hr => hr
    · exact rowsOf_indices state
  · have hbad : ∃ r ∈ rowsOf state, ∀ b, rewardsEligible ctx.previous_epoch r.validator ≠ .ok b := by
      apply Classical.byContradiction
      intro hno
      apply hall
      intro r hr
      cases hq : rewardsEligible ctx.previous_epoch r.validator with
      | ok b => exact ⟨b, rfl⟩
      | error e => exact absurd ⟨r, hr, fun b => by simp [hq]⟩ hno
    obtain ⟨r, hr, hbad⟩ := hbad
    have hE := eligible_not_ok p state _ hprevious r hr hbad
    intro v
    constructor
    · intro h
      cases hget : get_eligible_validator_indices p state with
      | error e => simp [hget] at h
      | ok E => exact absurd hget (hE E)
    · intro h
      obtain ⟨ds, hds, -⟩ := (map_ok_iff _ _ _).mp h
      refine absurd hds (mapM_not_ok _ _ r hr ?_ ds)
      intro y hy
      simp only [flagRow, bind, Except.bind] at hy
      cases hq : rewardsEligible ctx.previous_epoch r.validator with
      | error e => simp [hq] at hy
      | ok b => exact hbad b hq

/-- The inactivity penalty of one row. A row that is not eligible gets a zero penalty. -/
private def inactivityRow (p : Preset) (ctx : RewardsContext) (r : Row) : SpecM Gwei := do
  if (← rewardsEligible ctx.previous_epoch r.validator) then
    inactivityDelta p r.validator.effective_balance r.inactivity_score
      (rewardsParticipating ctx.previous_epoch r TIMELY_TARGET_FLAG_INDEX)
  else pure 0

/-- `get_inactivity_penalty_deltas` gives the same `ok` values as `inactivityRow` over the
rows. -/
private theorem inactivity_deltas_sameOk (p : Preset) (state : BeaconState)
    (hrows : RowsOk state) (ctx : RewardsContext) (current_epoch : Epoch)
    (hcurrent : get_current_epoch p state = .ok current_epoch)
    (hprevious : get_previous_epoch p state = .ok ctx.previous_epoch)
    (hne : ctx.previous_epoch ≠ current_epoch) :
    SameOk (get_inactivity_penalty_deltas p state)
      ((fun ds => (List.replicate state.validators.length 0, ds)) <$>
        (rowsOf state).mapM (inactivityRow p ctx)) := by
  have hpart := participating_ok p state hrows current_epoch ctx.previous_epoch
    TIMELY_TARGET_FLAG_INDEX hcurrent hprevious hne (by decide)
  unfold get_inactivity_penalty_deltas
  simp only [bind, Except.bind, hprevious, hpart]
  by_cases hall : ∀ r ∈ rowsOf state, ∃ b, rewardsEligible ctx.previous_epoch r.validator = .ok b
  · let Ev : Validator → Bool := fun v =>
      match rewardsEligible ctx.previous_epoch v with
      | .ok b => b
      | .error _ => false
    have hEv : ∀ r ∈ rowsOf state,
        rewardsEligible ctx.previous_epoch r.validator = .ok (Ev r.validator) := by
      intro r hr
      obtain ⟨b, hb⟩ := hall r hr
      simp [Ev, hb]
    rw [eligible_ok p state _ hprevious Ev hEv]
    dsimp only
    have hinit : List.replicate state.validators.length (0 : Gwei) =
        (fun ds : List Gwei => ds) ([] ++ List.replicate (rowsOf state).length 0) := by
      simp [rowsOf_length]
    rw [hinit, forIn_filter_rows (fun ds : List Gwei => ds) 0
        (· ∈ rowsOf state) (fun r => Ev r.validator)
        (fun r => inactivityDelta p r.validator.effective_balance r.inactivity_score
          (rewardsParticipating ctx.previous_epoch r TIMELY_TARGET_FLAG_INDEX))]
    · have hmap : (rowsOf state).mapM (inactivityRow p ctx) =
          (rowsOf state).mapM (fun r => if Ev r.validator then
            inactivityDelta p r.validator.effective_balance r.inactivity_score
              (rewardsParticipating ctx.previous_epoch r TIMELY_TARGET_FLAG_INDEX)
            else pure 0) :=
        mapM_congr_mem _ _ _ (fun r hr => by simp [inactivityRow, hEv r hr, bind, Except.bind])
      rw [hmap]
      apply SameOk.of_eq
      cases (rowsOf state).mapM (fun r => if Ev r.validator then
            inactivityDelta p r.validator.effective_balance r.inactivity_score
              (rewardsParticipating ctx.previous_epoch r TIMELY_TARGET_FLAG_INDEX)
            else pure 0) <;> rfl
    · intro r pre suf hr _ hidx
      have hscore : listGet state.inactivity_scores r.index = .ok r.inactivity_score := by
        rw [getD_row _ 0 state.validators.length hrows.2.1 rfl hr, inactivity_score_row hr]
      have hmem := index_mem_filter
        (fun r => rewardsParticipating ctx.previous_epoch r TIMELY_TARGET_FLAG_INDEX) hr
      have hget : listGet (pre ++ 0 :: suf) r.index = .ok 0 := by
        simpa using listGet_map_mid id pre suf 0 r.index hidx
      have hset : ∀ b, listSet (pre ++ 0 :: suf) r.index b = .ok (pre ++ b :: suf) := by
        intro b
        simpa using listSet_map_mid id pre suf 0 b r.index hidx
      simp only [listGet_validators_row hr, hscore, hget, hset]
      by_cases hp : rewardsParticipating ctx.previous_epoch r TIMELY_TARGET_FLAG_INDEX = true
      · rw [if_neg (fun h => h (hmem.mpr hp))]
        simp [inactivityDelta, hp, pure, Except.pure, Functor.map, Except.map]
      · rw [if_pos (fun h => hp (hmem.mp h))]
        simp only [inactivityDelta, hp, Bool.not_false, if_true]
        cases hm1 : uint64Mul r.validator.effective_balance r.inactivity_score with
        | error e => simp [Functor.map, Except.map, bind, Except.bind]
        | ok m1 =>
        simp only [bind, Except.bind]
        cases hm2 : uint64Mul p.INACTIVITY_SCORE_BIAS p.INACTIVITY_PENALTY_QUOTIENT_BELLATRIX with
        | error e => simp [Functor.map, Except.map]
        | ok m2 =>
        dsimp only
        cases hd : uint64Div m1 m2 with
        | error e => simp [Functor.map, Except.map]
        | ok penalty =>
        simp [uint64Add_zero_left _ (uint64Div_lt hd (uint64Mul_lt hm1)), pure, Except.pure,
          Functor.map, Except.map]
    · exact fun r hr => hr
    · exact rowsOf_indices state
  · have hbad : ∃ r ∈ rowsOf state, ∀ b, rewardsEligible ctx.previous_epoch r.validator ≠ .ok b := by
      apply Classical.byContradiction
      intro hno
      apply hall
      intro r hr
      cases hq : rewardsEligible ctx.previous_epoch r.validator with
      | ok b => exact ⟨b, rfl⟩
      | error e => exact absurd ⟨r, hr, fun b => by simp [hq]⟩ hno
    obtain ⟨r, hr, hbad⟩ := hbad
    have hE := eligible_not_ok p state _ hprevious r hr hbad
    intro v
    constructor
    · intro h
      cases hget : get_eligible_validator_indices p state with
      | error e => simp [hget] at h
      | ok E => exact absurd hget (hE E)
    · intro h
      obtain ⟨ds, hds, -⟩ := (map_ok_iff _ _ _).mp h
      refine absurd hds (mapM_not_ok _ _ r hr ?_ ds)
      intro y hy
      simp only [inactivityRow, bind, Except.bind] at hy
      cases hq : rewardsEligible ctx.previous_epoch r.validator with
      | error e => simp [hq] at hy
      | ok b => exact hbad b hq

/-- `forIn_range_local` from the start of the list. -/
private theorem forIn_range_local0 {B : Type} (N : Nat) (F : Nat → B → SpecM B)
    (body : Nat → List B → SpecM (ForInStep (List B)))
    (hbody : ∀ (pre suf : List B) (b : B), (pre ++ b :: suf).length = N →
      body pre.length (pre ++ b :: suf) =
        (fun b' => ForInStep.yield (pre ++ b' :: suf)) <$> F pre.length b)
    (l : List B) (hlen : l.length = N) :
    forIn (List.range N) l body = l.zipIdx.mapM (fun x => F x.2 x.1) := by
  have h := forIn_range_local N F body hbody l [] (by simpa using hlen)
  subst hlen
  simp only [List.length_nil, List.nil_append] at h
  rw [List.range_eq_range', h]
  cases l.zipIdx.mapM (fun x => F x.2 x.1) <;> rfl

/-- `rewardsSequential` applies the first delta, then the rest. -/
private theorem rewardsSequential_cons (b : Gwei) (d : Gwei × Gwei) (ds : List (Gwei × Gwei)) :
    rewardsSequential b (d :: ds) =
      (uint64Add b d.1 >>= fun b' => rewardsSequential (saturating_sub b' d.2) ds) := by
  simp only [rewardsSequential, List.foldlM_cons, bind, Except.bind, pure, Except.pure]
  cases uint64Add b d.1 <;> rfl

/-- One round of the balance loop at one position: add the reward, then subtract the
penalty. -/
private def roundEntry (R P : List Gwei) (x : Gwei × Nat) : SpecM Gwei := do
  pure (saturating_sub (← uint64Add x.1 (R.getD x.2 0)) (P.getD x.2 0))

/-- The deltas of all rounds at position `i`. -/
private def deltasAt (Ds : List (List Gwei × List Gwei)) (i : Nat) : List (Gwei × Gwei) :=
  Ds.map fun d => (d.1.getD i 0, d.2.getD i 0)

/-- The balance loop over all rounds succeeds with `bal` exactly when `rewardsSequential`
succeeds with `bal[i]` at each position `i`. -/
private theorem balance_loop_ok_iff (n : Nat)
    (body : List Gwei × List Gwei → List Gwei → SpecM (ForInStep (List Gwei)))
    (hbody : ∀ R P bal, R.length = n → P.length = n → bal.length = n →
      body (R, P) bal = (fun b => ForInStep.yield b) <$> bal.zipIdx.mapM (roundEntry R P)) :
    ∀ (Ds : List (List Gwei × List Gwei)) (bal0 bal : List Gwei), bal0.length = n →
      (∀ d ∈ Ds, d.1.length = n ∧ d.2.length = n) →
      (forIn Ds bal0 body = .ok bal ↔ bal.length = n ∧
        ∀ i (h1 : i < bal0.length) (h2 : i < bal.length),
          rewardsSequential bal0[i] (deltasAt Ds i) = .ok bal[i])
  | [], bal0, bal, hlen, _ => by
    simp only [List.forIn_nil, pure, Except.pure, Except.ok.injEq, deltasAt, List.map_nil]
    constructor
    · intro h
      subst h
      exact ⟨hlen, fun i _ _ => rfl⟩
    · intro ⟨hb, h⟩
      apply List.ext_getElem (by omega)
      intro i h1 h2
      have := h i h1 h2
      simp only [rewardsSequential, List.foldlM_nil, pure, Except.pure, Except.ok.injEq] at this
      exact this
  | (R, P) :: Ds, bal0, bal, hlen, hDs => by
    obtain ⟨hR, hP⟩ := hDs (R, P) (by simp)
    have hDs' : ∀ d ∈ Ds, d.1.length = n ∧ d.2.length = n := fun d hd => hDs d (by simp [hd])
    rw [List.forIn_cons, hbody R P bal0 hR hP hlen]
    have hstep : ∀ i (h1 : i < bal0.length),
        roundEntry R P (bal0.zipIdx[i]'(by simpa using h1)) =
          (uint64Add bal0[i] (R.getD i 0) >>= fun b' => .ok (saturating_sub b' (P.getD i 0))) := by
      intro i h1
      simp [roundEntry, List.getElem_zipIdx, pure, Except.pure]
    constructor
    · intro h
      cases hm : bal0.zipIdx.mapM (roundEntry R P) with
      | error e => simp [hm, Functor.map, Except.map, bind, Except.bind] at h
      | ok mid =>
        simp only [hm, Functor.map, Except.map, bind, Except.bind] at h
        obtain ⟨hmlen, hmget⟩ := mapM_ok_get _ _ _ hm
        rw [List.length_zipIdx] at hmlen
        obtain ⟨hblen, hrest⟩ :=
          (balance_loop_ok_iff n body hbody Ds mid bal (by omega) hDs').mp h
        refine ⟨hblen, fun i h1 h2 => ?_⟩
        have hm' := hmget i (by simpa using h1) (by omega)
        rw [hstep i h1] at hm'
        simp only [deltasAt, List.map_cons, rewardsSequential_cons]
        cases hadd : uint64Add bal0[i] (R.getD i 0) with
        | error e => rw [hadd] at hm'; cases hm'
        | ok b' =>
          rw [hadd] at hm'
          simp only [bind, Except.bind, Except.ok.injEq] at hm' ⊢
          rw [hm']
          exact hrest i (by omega) h2
    · intro ⟨hblen, hall⟩
      have hex : ∀ x ∈ bal0.zipIdx, ∃ y, roundEntry R P x = .ok y := by
        intro x hx
        obtain ⟨i, hi, rfl⟩ := List.getElem_of_mem hx
        have hi' : i < bal0.length := by simpa using hi
        have := hall i hi' (by omega)
        simp only [deltasAt, List.map_cons, rewardsSequential_cons] at this
        rw [hstep i hi']
        cases hadd : uint64Add bal0[i] (R.getD i 0) with
        | error e => rw [hadd] at this; cases this
        | ok b' => exact ⟨_, rfl⟩
      obtain ⟨mid, hm⟩ := mapM_exists _ _ hex
      obtain ⟨hmlen, hmget⟩ := mapM_ok_get _ _ _ hm
      rw [List.length_zipIdx] at hmlen
      simp only [hm, Functor.map, Except.map, bind, Except.bind]
      refine (balance_loop_ok_iff n body hbody Ds mid bal (by omega) hDs').mpr ⟨hblen, ?_⟩
      intro i h1 h2
      have hi : i < bal0.length := by omega
      have hm' := hmget i (by simpa using hi) h1
      rw [hstep i hi] at hm'
      have := hall i hi h2
      simp only [deltasAt, List.map_cons, rewardsSequential_cons] at this
      cases hadd : uint64Add bal0[i] (R.getD i 0) with
      | error e => rw [hadd] at hm'; cases hm'
      | ok b' =>
        rw [hadd] at hm' this
        simp only [bind, Except.bind, Except.ok.injEq] at hm' this
        rw [← hm']
        exact this

/-- The four deltas of a row, one flag at a time. -/
private theorem rowDeltas_ok_iff (p : Preset) (ctx : RewardsContext) (r : Row)
    (ds : List (Gwei × Gwei)) :
    rewardsRowDeltas p ctx r = .ok ds ↔ ∃ a b c d,
      flagRow p ctx TIMELY_SOURCE_WEIGHT TIMELY_SOURCE_FLAG_INDEX ctx.source_increments r = .ok a
      ∧ flagRow p ctx TIMELY_TARGET_WEIGHT TIMELY_TARGET_FLAG_INDEX ctx.target_increments r = .ok b
      ∧ flagRow p ctx TIMELY_HEAD_WEIGHT TIMELY_HEAD_FLAG_INDEX ctx.head_increments r = .ok c
      ∧ inactivityRow p ctx r = .ok d ∧ ds = [a, b, c, (0, d)] := by
  have hs : (TIMELY_SOURCE_FLAG_INDEX == TIMELY_HEAD_FLAG_INDEX) = false := by decide
  have ht : (TIMELY_TARGET_FLAG_INDEX == TIMELY_HEAD_FLAG_INDEX) = false := by decide
  have hh : (TIMELY_HEAD_FLAG_INDEX == TIMELY_HEAD_FLAG_INDEX) = true := by decide
  unfold rewardsRowDeltas flagRow inactivityRow flagCore
  cases he : rewardsEligible ctx.previous_epoch r.validator with
  | error e => simp [bind, Except.bind]
  | ok el =>
    cases el with
    | false =>
      simp only [bind, Except.bind, Bool.false_eq_true, if_false, pure, Except.pure,
        Except.ok.injEq]
      constructor
      · intro h; exact ⟨_, _, _, _, rfl, rfl, rfl, rfl, h.symm⟩
      · intro ⟨a, b, c, d, ha, hb, hc, hd, h⟩
        subst ha hb hc hd; exact h.symm
    | true =>
      simp only [bind, Except.bind, if_true, hs, ht, hh]
      cases hb : rewardsBaseReward p ctx.total_active_balance r.validator with
      | error e => simp
      | ok br =>
        simp only [rewardDeltas, bind, Except.bind]
        cases h1 : flagDelta br TIMELY_SOURCE_WEIGHT
            (rewardsParticipating ctx.previous_epoch r TIMELY_SOURCE_FLAG_INDEX) false ctx.in_leak
            ctx.source_increments ctx.active_increments with
        | error e => simp
        | ok a =>
        cases h2 : flagDelta br TIMELY_TARGET_WEIGHT
            (rewardsParticipating ctx.previous_epoch r TIMELY_TARGET_FLAG_INDEX) false ctx.in_leak
            ctx.target_increments ctx.active_increments with
        | error e => simp
        | ok b =>
        cases h3 : flagDelta br TIMELY_HEAD_WEIGHT
            (rewardsParticipating ctx.previous_epoch r TIMELY_HEAD_FLAG_INDEX) true ctx.in_leak
            ctx.head_increments ctx.active_increments with
        | error e => simp
        | ok c =>
        cases h4 : inactivityDelta p r.validator.effective_balance r.inactivity_score
            (rewardsParticipating ctx.previous_epoch r TIMELY_TARGET_FLAG_INDEX) with
        | error e => simp
        | ok d =>
          simp only [pure, Except.pure, Except.ok.injEq]
          constructor
          · intro h; exact ⟨_, _, _, _, rfl, rfl, rfl, rfl, h.symm⟩
          · intro ⟨a, b, c, d, ha, hb, hc, hd, h⟩
            subst ha hb hc hd; exact h.symm

/-- The new balance of one row. -/
private def rowResult (p : Preset) (ctx : RewardsContext) (r : Row) : SpecM Gwei :=
  rewardsRowDeltas p ctx r >>= rewardsSequential r.balance

/-- The `ok` values of the pass: the state with new balances, where each new balance is the
`rowResult` of its row. -/
private def RewardsGood (p : Preset) (ctx : RewardsContext) (state : BeaconState)
    (v : BeaconState) : Prop :=
  ∃ bal : List Gwei, v = { state with balances := bal } ∧ bal.length = (rowsOf state).length ∧
    ∀ i (h1 : i < (rowsOf state).length) (h2 : i < bal.length),
      rowResult p ctx (rowsOf state)[i] = .ok bal[i]

/-- The new row of the step is the row with the `rowResult` balance. -/
private theorem rowStep_snd (p : Preset) (ctx : RewardsContext) (r : Row) :
    Prod.snd <$> rewardsRowStep p ctx () r =
      (fun b => { r with balance := b }) <$> rowResult p ctx r := by
  simp only [rewardsRowStep, rowResult, bind, Except.bind]
  cases rewardsRowDeltas p ctx r with
  | error e => rfl
  | ok ds =>
    dsimp only
    cases rewardsSequential r.balance ds <;> rfl

/-- Rows that keep the validator and the inactivity score of the state rows write back only
the balances. -/
private theorem withRows_balances (state : BeaconState) (hrows : RowsOk state) (ys : List Row)
    (hlen : ys.length = (rowsOf state).length)
    (hkeep : ∀ i (h1 : i < ys.length) (h2 : i < (rowsOf state).length),
      ys[i].validator = (rowsOf state)[i].validator
      ∧ ys[i].inactivity_score = (rowsOf state)[i].inactivity_score) :
    state.withRows ys = { state with balances := ys.map (·.balance) } := by
  have hV : ys.map (·.validator) = state.validators := by
    apply List.ext_getElem
    · simp [hlen, rowsOf_length]
    · intro i h1 h2
      simp only [List.getElem_map]
      rw [(hkeep i (by simpa using h1) (by simpa [rowsOf_length] using h2)).1, rowsOf_getElem]
  have hS : ys.map (·.inactivity_score) = state.inactivity_scores := by
    apply List.ext_getElem
    · simp [hlen, rowsOf_length, hrows.2.1]
    · intro i h1 h2
      simp only [List.getElem_map]
      rw [(hkeep i (by simpa using h1) (by simpa [hlen] using h1)).2, rowsOf_getElem]
      simp [List.getD_eq_getElem?_getD, h2]
  simp only [BeaconState.withRows, hV, hS]

/-- The single pass succeeds with `v` exactly when `v` is good. -/
private theorem single_pass_ok_iff (p : Preset) (ctx : RewardsContext) (state : BeaconState)
    (hrows : RowsOk state) (v : BeaconState) :
    (fun x => state.withRows x.2) <$> passM (rewardsRowStep p ctx) () (rowsOf state) = .ok v ↔
      RewardsGood p ctx state v := by
  rw [passM_unit, map_ok_iff]
  constructor
  · intro ⟨x, hx, hv⟩
    obtain ⟨ys, hys, rfl⟩ := (map_ok_iff _ _ _).mp hx
    obtain ⟨hlen, hget⟩ := mapM_ok_get _ _ _ hys
    have hrow : ∀ i (h1 : i < (rowsOf state).length) (h2 : i < ys.length),
        ∃ b, rowResult p ctx (rowsOf state)[i] = .ok b
          ∧ { (rowsOf state)[i] with balance := b } = ys[i] := by
      intro i h1 h2
      have := hget i h1 h2
      rw [rowStep_snd] at this
      exact (map_ok_iff _ _ _).mp this
    refine ⟨ys.map (·.balance), ?_, by simp [hlen], ?_⟩
    · rw [← hv]
      apply withRows_balances state hrows ys hlen
      intro i h1 h2
      obtain ⟨b, -, hb⟩ := hrow i h2 h1
      rw [← hb]
      exact ⟨rfl, rfl⟩
    · intro i h1 h2
      obtain ⟨b, hb, hys'⟩ := hrow i h1 (by simpa using h2)
      simp only [List.getElem_map, ← hys']
      exact hb
  · intro ⟨bal, hv, hlen, hgood⟩
    let ys := ((rowsOf state).zip bal).map fun x => { x.1 with balance := x.2 }
    have hylen : ys.length = (rowsOf state).length := by simp [ys, hlen]
    have hyget : ∀ i (h : i < ys.length), ys[i] =
        { (rowsOf state)[i]'(by omega) with balance := bal[i]'(by omega) } := by
      intro i h
      simp [ys]
    have hmap : (rowsOf state).mapM (fun r => Prod.snd <$> rewardsRowStep p ctx () r) =
        .ok ys := by
      apply mapM_ok_of _ _ _ hylen
      intro i h1 h2
      rw [rowStep_snd, hgood i h1 (by omega), hyget i h2]
      rfl
    refine ⟨((), ys), by rw [hmap]; rfl, ?_⟩
    rw [hv, withRows_balances state hrows ys hylen]
    · congr 1
      apply List.ext_getElem
      · simp [hylen, hlen]
      · intro i h1 h2
        simp [hyget i (by simpa using h1)]
    · intro i h1 h2
      rw [hyget i h1]
      exact ⟨rfl, rfl⟩

/-- What a successful context computation gives. -/
private theorem rewardsContextOf_ok (p : Preset) (total_active_balance : Gwei)
    (state : BeaconState) (ctx : RewardsContext)
    (h : rewardsContextOf p total_active_balance state = .ok ctx) :
    get_previous_epoch p state = .ok ctx.previous_epoch
    ∧ rewardsFlagIncrements p state ctx.previous_epoch TIMELY_SOURCE_FLAG_INDEX =
      .ok ctx.source_increments
    ∧ rewardsFlagIncrements p state ctx.previous_epoch TIMELY_TARGET_FLAG_INDEX =
      .ok ctx.target_increments
    ∧ rewardsFlagIncrements p state ctx.previous_epoch TIMELY_HEAD_FLAG_INDEX =
      .ok ctx.head_increments
    ∧ uint64Div total_active_balance p.EFFECTIVE_BALANCE_INCREMENT = .ok ctx.active_increments
    ∧ is_in_inactivity_leak p state = .ok ctx.in_leak
    ∧ ctx.total_active_balance = total_active_balance := by
  simp only [rewardsContextOf, bind_ok_iff] at h
  obtain ⟨pe, h1, s, h2, t, h3, hd, h4, a, h5, l, h6, h7⟩ := h
  simp only [pure, Except.pure, Except.ok.injEq] at h7
  subst h7
  exact ⟨h1, h2, h3, h4, h5, h6, rfl⟩

/-- The deltas at a position are the four row deltas at that position. -/
private theorem deltasAt_eq (n : Nat) (m0 m1 m2 : List (Gwei × Gwei)) (mi : List Gwei)
    (i : Nat) (h0 : i < m0.length) (h1 : i < m1.length) (h2 : i < m2.length)
    (hi : i < mi.length) (hn : i < n) :
    deltasAt [(m0.map (·.1), m0.map (·.2)), (m1.map (·.1), m1.map (·.2)),
        (m2.map (·.1), m2.map (·.2)), (List.replicate n 0, mi)] i =
      [m0[i], m1[i], m2[i], (0, mi[i])] := by
  simp [deltasAt, List.getD_eq_getElem?_getD, h0, h1, h2, hi, hn]

/-- The balance of a row is the state balance at its position. -/
private theorem balance_rowsOf (state : BeaconState) (i : Nat)
    (h1 : i < (rowsOf state).length) (h2 : i < state.balances.length) :
    (rowsOf state)[i].balance = state.balances[i] := by
  simp [rowsOf_getElem, List.getD_eq_getElem?_getD, h2]

/-- The spec pass, after the genesis check, succeeds with `v` exactly when `v` is good.
`body` is the loop body that applies the deltas. -/
private theorem spec_ok_iff (p : Preset) (state : BeaconState) (hrows : RowsOk state)
    (ctx : RewardsContext) (current_epoch : Epoch)
    (hcurrent : get_current_epoch p state = .ok current_epoch)
    (hprevious : get_previous_epoch p state = .ok ctx.previous_epoch)
    (hne : ctx.previous_epoch ≠ current_epoch)
    (hleak : is_in_inactivity_leak p state = .ok ctx.in_leak)
    (hactive : uint64Div ctx.total_active_balance p.EFFECTIVE_BALANCE_INCREMENT =
      .ok ctx.active_increments)
    (hsource : rewardsFlagIncrements p state ctx.previous_epoch TIMELY_SOURCE_FLAG_INDEX =
      .ok ctx.source_increments)
    (htarget : rewardsFlagIncrements p state ctx.previous_epoch TIMELY_TARGET_FLAG_INDEX =
      .ok ctx.target_increments)
    (hhead : rewardsFlagIncrements p state ctx.previous_epoch TIMELY_HEAD_FLAG_INDEX =
      .ok ctx.head_increments)
    (body : List Gwei × List Gwei → List Gwei → SpecM (ForInStep (List Gwei)))
    (hbody : ∀ R P bal, R.length = state.validators.length → P.length = state.validators.length →
      bal.length = state.validators.length →
      body (R, P) bal = (fun b => ForInStep.yield b) <$> bal.zipIdx.mapM (roundEntry R P))
    (v : BeaconState) :
    (do
      pure PUnit.unit
      let flag_deltas ← [0, 1, 2].mapM fun flag_index =>
        get_flag_index_deltas p ctx.total_active_balance state flag_index
      let inactivity ← get_inactivity_penalty_deltas p state
      let balances ← forIn (flag_deltas ++ [inactivity]) state.balances body
      pure { state with balances }) = .ok v ↔ RewardsGood p ctx state v := by
  have hF0 := flag_deltas_sameOk p state hrows ctx current_epoch hcurrent hprevious hne hleak
    hactive TIMELY_SOURCE_FLAG_INDEX TIMELY_SOURCE_WEIGHT ctx.source_increments (by decide) rfl
    hsource
  have hF1 := flag_deltas_sameOk p state hrows ctx current_epoch hcurrent hprevious hne hleak
    hactive TIMELY_TARGET_FLAG_INDEX TIMELY_TARGET_WEIGHT ctx.target_increments (by decide) rfl
    htarget
  have hF2 := flag_deltas_sameOk p state hrows ctx current_epoch hcurrent hprevious hne hleak
    hactive TIMELY_HEAD_FLAG_INDEX TIMELY_HEAD_WEIGHT ctx.head_increments (by decide) rfl hhead
  have hI := inactivity_deltas_sameOk p state hrows ctx current_epoch hcurrent hprevious hne
  have hn : (rowsOf state).length = state.validators.length := rowsOf_length state
  have hbal0 : state.balances.length = state.validators.length := hrows.1
  simp only [List.mapM_cons, List.mapM_nil, bind_ok_iff, pure, Except.pure, Except.ok.injEq]
  constructor
  · intro h
    obtain ⟨_, -, fd, ⟨d0, h0, _, ⟨d1, h1, _, ⟨d2, h2, _, rfl, rfl⟩, rfl⟩, rfl⟩, di, hdi, bal,
      hloop, rfl⟩ := h
    obtain ⟨m0, hm0, rfl⟩ := (map_ok_iff _ _ _).mp ((hF0 d0).mp h0)
    obtain ⟨m1, hm1, rfl⟩ := (map_ok_iff _ _ _).mp ((hF1 d1).mp h1)
    obtain ⟨m2, hm2, rfl⟩ := (map_ok_iff _ _ _).mp ((hF2 d2).mp h2)
    obtain ⟨mi, hmi, rfl⟩ := (map_ok_iff _ _ _).mp ((hI di).mp hdi)
    obtain ⟨hl0, hg0⟩ := mapM_ok_get _ _ _ hm0
    obtain ⟨hl1, hg1⟩ := mapM_ok_get _ _ _ hm1
    obtain ⟨hl2, hg2⟩ := mapM_ok_get _ _ _ hm2
    obtain ⟨hli, hgi⟩ := mapM_ok_get _ _ _ hmi
    obtain ⟨hlen, hres⟩ := (balance_loop_ok_iff state.validators.length body hbody _
      state.balances bal hbal0 (by
        intro d hd
        simp only [List.cons_append, List.nil_append, List.mem_cons, List.not_mem_nil,
          or_false] at hd
        rcases hd with rfl | rfl | rfl | rfl <;> simp [hl0, hl1, hl2, hli, hn])).mp hloop
    refine ⟨bal, rfl, by omega, ?_⟩
    intro i hi1 hi2
    simp only [rowResult, bind_ok_iff]
    refine ⟨[m0[i]'(by omega), m1[i]'(by omega), m2[i]'(by omega), (0, mi[i]'(by omega))],
      (rowDeltas_ok_iff _ _ _ _).mpr ⟨_, _, _, _, hg0 i hi1 (by omega), hg1 i hi1 (by omega),
        hg2 i hi1 (by omega), hgi i hi1 (by omega), rfl⟩, ?_⟩
    have := hres i (by omega) hi2
    simp only [List.cons_append, List.nil_append] at this
    rw [deltasAt_eq _ m0 m1 m2 mi i (by omega) (by omega) (by omega) (by omega) (by omega)]
      at this
    rw [balance_rowsOf state i hi1 (by omega)]
    exact this
  · intro ⟨bal, hv, hlen, hgood⟩
    have hdec : ∀ i (h1 : i < (rowsOf state).length), ∃ a b c d,
        flagRow p ctx TIMELY_SOURCE_WEIGHT TIMELY_SOURCE_FLAG_INDEX ctx.source_increments
          (rowsOf state)[i] = .ok a
        ∧ flagRow p ctx TIMELY_TARGET_WEIGHT TIMELY_TARGET_FLAG_INDEX ctx.target_increments
          (rowsOf state)[i] = .ok b
        ∧ flagRow p ctx TIMELY_HEAD_WEIGHT TIMELY_HEAD_FLAG_INDEX ctx.head_increments
          (rowsOf state)[i] = .ok c
        ∧ inactivityRow p ctx (rowsOf state)[i] = .ok d
        ∧ rewardsSequential (rowsOf state)[i].balance [a, b, c, (0, d)] =
          .ok (bal[i]'(by omega)) := by
      intro i h1
      have := hgood i h1 (by omega)
      simp only [rowResult, bind_ok_iff] at this
      obtain ⟨ds, hds, hseq⟩ := this
      obtain ⟨a, b, c, d, ha, hb, hc, hd, rfl⟩ := (rowDeltas_ok_iff _ _ _ _).mp hds
      exact ⟨a, b, c, d, ha, hb, hc, hd, hseq⟩
    obtain ⟨m0, hm0⟩ := mapM_exists
      (flagRow p ctx TIMELY_SOURCE_WEIGHT TIMELY_SOURCE_FLAG_INDEX ctx.source_increments)
      (rowsOf state) (fun r hr => by
        obtain ⟨i, hi, rfl⟩ := List.getElem_of_mem hr
        obtain ⟨a, -, -, -, ha, -⟩ := hdec i hi
        exact ⟨a, ha⟩)
    obtain ⟨m1, hm1⟩ := mapM_exists
      (flagRow p ctx TIMELY_TARGET_WEIGHT TIMELY_TARGET_FLAG_INDEX ctx.target_increments)
      (rowsOf state) (fun r hr => by
        obtain ⟨i, hi, rfl⟩ := List.getElem_of_mem hr
        obtain ⟨-, b, -, -, -, hb, -⟩ := hdec i hi
        exact ⟨b, hb⟩)
    obtain ⟨m2, hm2⟩ := mapM_exists
      (flagRow p ctx TIMELY_HEAD_WEIGHT TIMELY_HEAD_FLAG_INDEX ctx.head_increments)
      (rowsOf state) (fun r hr => by
        obtain ⟨i, hi, rfl⟩ := List.getElem_of_mem hr
        obtain ⟨-, -, c, -, -, -, hc, -⟩ := hdec i hi
        exact ⟨c, hc⟩)
    obtain ⟨mi, hmi⟩ := mapM_exists (inactivityRow p ctx) (rowsOf state) (fun r hr => by
        obtain ⟨i, hi, rfl⟩ := List.getElem_of_mem hr
        obtain ⟨-, -, -, d, -, -, -, hd, -⟩ := hdec i hi
        exact ⟨d, hd⟩)
    obtain ⟨hl0, hg0⟩ := mapM_ok_get _ _ _ hm0
    obtain ⟨hl1, hg1⟩ := mapM_ok_get _ _ _ hm1
    obtain ⟨hl2, hg2⟩ := mapM_ok_get _ _ _ hm2
    obtain ⟨hli, hgi⟩ := mapM_ok_get _ _ _ hmi
    refine ⟨(), trivial, _,
      ⟨(m0.map (·.1), m0.map (·.2)), (hF0 _).mpr (by rw [hm0]; rfl), _,
        ⟨(m1.map (·.1), m1.map (·.2)), (hF1 _).mpr (by rw [hm1]; rfl), _,
          ⟨(m2.map (·.1), m2.map (·.2)), (hF2 _).mpr (by rw [hm2]; rfl), _, rfl, rfl⟩, rfl⟩,
        rfl⟩,
      (List.replicate state.validators.length 0, mi), (hI _).mpr (by rw [hmi]; rfl), bal, ?_,
      hv.symm⟩
    refine (balance_loop_ok_iff state.validators.length body hbody _ state.balances bal hbal0 (by
        intro d hd
        simp only [List.cons_append, List.nil_append, List.mem_cons, List.not_mem_nil,
          or_false] at hd
        rcases hd with rfl | rfl | rfl | rfl <;> simp [hl0, hl1, hl2, hli, hn])).mpr
      ⟨by omega, ?_⟩
    intro i hi1 hi2
    obtain ⟨a, b, c, d, ha, hb, hc, hd, hseq⟩ := hdec i (by omega)
    have e0 := hg0 i (by omega) (by omega)
    have e1 := hg1 i (by omega) (by omega)
    have e2 := hg2 i (by omega) (by omega)
    have ei := hgi i (by omega) (by omega)
    rw [ha] at e0
    rw [hb] at e1
    rw [hc] at e2
    rw [hd] at ei
    injection e0 with e0
    injection e1 with e1
    injection e2 with e2
    injection ei with ei
    simp only [List.cons_append, List.nil_append]
    rw [deltasAt_eq _ m0 m1 m2 mi i (by omega) (by omega) (by omega) (by omega) (by omega),
      ← e0, ← e1, ← e2, ← ei, ← balance_rowsOf state i (by omega) hi1]
    exact hseq

/-- Bind on a success is the continuation. -/
private theorem ok_bind {α β : Type} (a : α) (f : α → SpecM β) :
    (Except.ok a >>= f) = f a := rfl

/-- `process_rewards_and_penalties` gives the same `ok` values as a pass of `rewardsRowStep` over
the rows. `SameOk`, not equality: the spec computes all deltas before it changes any balance, so
it can fail on a later row first. -/
theorem process_rewards_and_penalties_rows (p : Preset) (total_active_balance : Gwei)
    (state : BeaconState) (hrows : RowsOk state) (current_epoch : Epoch)
    (hcurrent : get_current_epoch p state = .ok current_epoch)
    (hgenesis : current_epoch ≠ GENESIS_EPOCH) (ctx : RewardsContext)
    (hctx : rewardsContextOf p total_active_balance state = .ok ctx) :
    SameOk (process_rewards_and_penalties p total_active_balance state)
      ((fun x => state.withRows x.2) <$> passM (rewardsRowStep p ctx) () (rowsOf state)) := by
  obtain ⟨hprevious, hsource, htarget, hhead, hactive, hleak, htab⟩ :=
    rewardsContextOf_ok p total_active_balance state ctx hctx
  subst htab
  have hne : ctx.previous_epoch ≠ current_epoch := by
    have h := hprevious
    simp only [get_previous_epoch, hcurrent, ok_bind, pure, Except.pure, Except.ok.injEq] at h
    have h0 : current_epoch ≠ 0 := hgenesis
    intro heq
    rw [heq] at h
    unfold saturating_sub at h
    split at h
    · exact absurd h (Nat.ne_of_lt (Nat.sub_lt (Nat.pos_of_ne_zero h0) Nat.one_pos))
    · simp only [Nat.sub_self] at h
      exact h0 h.symm
  have hg : (current_epoch == GENESIS_EPOCH) = false := by simpa using hgenesis
  unfold process_rewards_and_penalties
  simp only [hcurrent, ok_bind, hg, Bool.false_eq_true, if_false]
  rw [show List.range PARTICIPATION_FLAG_WEIGHTS.length = [0, 1, 2] from rfl]
  intro v
  rw [single_pass_ok_iff p ctx state hrows v]
  refine spec_ok_iff p state hrows ctx current_epoch hcurrent hprevious hne hleak hactive hsource
    htarget hhead _ ?_ v
  intro R P bal hR hP hb
  dsimp only
  rw [forIn_range_local0 state.validators.length (fun i b => roundEntry R P (b, i)) _ ?_ bal hb]
  · cases bal.zipIdx.mapM (roundEntry R P) <;> rfl
  · intro pre suf b hlen
    have hlt : pre.length < state.validators.length := by
      simp only [List.length_append, List.length_cons] at hlen; omega
    have hgR : listGet R pre.length = .ok (R.getD pre.length 0) := by
      simp [listGet, List.getD_eq_getElem?_getD, show pre.length < R.length by omega, pure,
        Except.pure]
    have hgP : listGet P pre.length = .ok (P.getD pre.length 0) := by
      simp [listGet, List.getD_eq_getElem?_getD, show pre.length < P.length by omega, pure,
        Except.pure]
    have hget : ∀ x : Gwei, listGet (pre ++ x :: suf) pre.length = .ok x := fun x => by
      simpa using listGet_map_mid id pre suf x pre.length rfl
    have hset : ∀ x y : Gwei, listSet (pre ++ x :: suf) pre.length y = .ok (pre ++ y :: suf) :=
      fun x y => by simpa using listSet_map_mid id pre suf x y pre.length rfl
    simp only [increase_balance, decrease_balance, hgR, hgP, hget, hset, ok_bind]
    unfold roundEntry
    cases uint64Add b (R.getD pre.length 0) with
    | error e => rfl
    | ok x => simp [hget, hset, ok_bind, pure, Except.pure, Functor.map, Except.map]

/-- If `get_flag_index_deltas` succeeds, the increments of its flag and the active increments
succeed. -/
private theorem flag_deltas_ok_parts (p : Preset) (total_active_balance : Gwei)
    (state : BeaconState) (previous_epoch : Epoch)
    (hprevious : get_previous_epoch p state = .ok previous_epoch) (flag_index : Nat)
    (d : List Gwei × List Gwei)
    (h : get_flag_index_deltas p total_active_balance state flag_index = .ok d) :
    (∃ x, rewardsFlagIncrements p state previous_epoch flag_index = .ok x)
    ∧ ∃ x, uint64Div total_active_balance p.EFFECTIVE_BALANCE_INCREMENT = .ok x := by
  unfold get_flag_index_deltas at h
  simp only [hprevious, ok_bind, bind_ok_iff] at h
  obtain ⟨U, hU, w, -, tb, htb, inc, hinc, act, hact, -⟩ := h
  refine ⟨⟨inc, ?_⟩, ⟨act, hact⟩⟩
  simp only [rewardsFlagIncrements, hU, htb, ok_bind, hinc]

/-- If the context computation fails, the spec pass fails too. The hypothesis `hleak` is
necessary: the spec reads `is_in_inactivity_leak` only for an eligible row that participates in a
flag. If no row does, a failing leak test does not stop the spec, but it stops the context. -/
theorem process_rewards_and_penalties_context_error (p : Preset) (total_active_balance : Gwei)
    (state : BeaconState) (current_epoch : Epoch)
    (hcurrent : get_current_epoch p state = .ok current_epoch)
    (hgenesis : current_epoch ≠ GENESIS_EPOCH) (in_leak : Bool)
    (hleak : is_in_inactivity_leak p state = .ok in_leak) (err : SpecError)
    (hctx : rewardsContextOf p total_active_balance state = .error err) :
    ∀ v, process_rewards_and_penalties p total_active_balance state ≠ .ok v := by
  intro v hv
  have hg : (current_epoch == GENESIS_EPOCH) = false := by simpa using hgenesis
  have hprevious : get_previous_epoch p state = .ok (saturating_sub current_epoch 1) := by
    simp [get_previous_epoch, hcurrent, ok_bind, pure, Except.pure]
  unfold process_rewards_and_penalties at hv
  simp only [hcurrent, ok_bind, hg, Bool.false_eq_true, if_false] at hv
  rw [show List.range PARTICIPATION_FLAG_WEIGHTS.length = [0, 1, 2] from rfl] at hv
  simp only [List.mapM_cons, List.mapM_nil, bind_ok_iff] at hv
  obtain ⟨_, -, _, ⟨d0, h0, _, ⟨d1, h1, _, ⟨d2, h2, -⟩, -⟩, -⟩, -⟩ := hv
  obtain ⟨⟨s, hs⟩, ⟨a, ha⟩⟩ := flag_deltas_ok_parts p total_active_balance state _ hprevious _ _ h0
  obtain ⟨⟨t, ht⟩, -⟩ := flag_deltas_ok_parts p total_active_balance state _ hprevious _ _ h1
  obtain ⟨⟨h, hh⟩, -⟩ := flag_deltas_ok_parts p total_active_balance state _ hprevious _ _ h2
  simp only [rewardsContextOf, hprevious, ok_bind] at hctx
  rw [show TIMELY_SOURCE_FLAG_INDEX = 0 from rfl, hs, ok_bind,
    show TIMELY_TARGET_FLAG_INDEX = 1 from rfl, ht, ok_bind,
    show TIMELY_HEAD_FLAG_INDEX = 2 from rfl, hh, ok_bind, ha, ok_bind, hleak, ok_bind] at hctx
  cases hctx

end EpochProofs.Spec
