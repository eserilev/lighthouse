import Mathlib.Data.Nat.Sqrt
import Mathlib.Tactic
import CacheProofs.Generated
import CacheProofs.Spec.EpochCache

/-!
# `EpochCache` and `PreEpochCache` equal the spec

The pure functions are in `consensus/types/src/state/{total_active_balance,base_rewards}.rs`.
-/

namespace CacheProofs.EpochCache

open Aeneas Aeneas.Std Aeneas.Std.WP Result types
open CacheProofs.Spec CacheProofs.Spec.EpochCache

/-! ## Integer square root -/

theorem iter_eq_sqrt (n : Nat) (hn : 1 ≤ n) : Nat.sqrt.iter n n = Nat.sqrt n := by
  apply Nat.eq_sqrt.mpr
  refine ⟨Nat.sqrt.iter_sq_le n n, Nat.sqrt.lt_iter_succ_sq n n ?_⟩
  nlinarith

theorem uint64_max_sqrt : Nat.sqrt (2 ^ 64 - 1) = 4294967295 := by
  symm; apply Nat.eq_sqrt.mpr; norm_num

theorem newton_step_pos (n x : Nat) (hn : 1 ≤ n) (hx : 1 ≤ x) : 1 ≤ (x + n / x) / 2 := by
  rcases Nat.lt_or_ge x 2 with h | h
  · have : x = 1 := by omega
    subst this; simp; omega
  · have := Nat.zero_le (n / x)
    exact (Nat.le_div_iff_mul_le (by norm_num)).mpr (by omega)

theorem newton_step_fit (n y : Nat) (hn : n ≤ 2 ^ 64 - 2) (hy : 1 ≤ y)
    (hy2 : y ≤ (n + 1) / 2) : y + n / y < 2 ^ 64 := by
  rcases Nat.lt_or_ge y 2 with h | h
  · have : y = 1 := by omega
    subst this; simp; omega
  · have : n / y ≤ n / 2 := Nat.div_le_div_left h (by norm_num)
    omega

/-- The spec loop computes `Nat.sqrt.iter`. -/
theorem integer_squareroot_loop_eq (n : Nat) (hn : 1 ≤ n) (hn' : n ≤ 2 ^ 64 - 2) :
    ∀ x y, 1 ≤ x → y = (x + n / x) / 2 → (y < x → y ≤ (n + 1) / 2) →
      integer_squareroot_loop n x y = .ok (Nat.sqrt.iter n x) := by
  intro x
  induction x using Nat.strong_induction_on with
  | _ x ih =>
    intro y hx hy hbound
    rw [integer_squareroot_loop, Nat.sqrt.iter]
    by_cases hlt : y < x
    · have hy1 : 1 ≤ y := by rw [hy]; exact newton_step_pos n x hn hx
      have hfit := newton_step_fit n y hn' hy1 (hbound hlt)
      have hne : y ≠ 0 := by omega
      have hlt' : (x + n / x) / 2 < x := hy ▸ hlt
      simp only [hlt, dif_pos, uint64Div, hne, if_false, uint64Add, UINT64_SIZE, ← hy]
      show (if y + n / y < 2 ^ 64 then Except.ok (y + n / y) else throw SpecError.overflow) >>=
        (fun s => integer_squareroot_loop n y (s / 2)) = _
      simp only [hfit, if_true]
      exact ih y hlt ((y + n / y) / 2) hy1 rfl (fun h => by omega)
    · have hlt' : ¬ (x + n / x) / 2 < x := hy ▸ hlt
      simp only [hlt, hlt', dif_neg, not_false_eq_true]
      rfl

theorem integer_squareroot_eq (n : Nat) (hn : n < 2 ^ 64) :
    integer_squareroot n = .ok (Nat.sqrt n) := by
  unfold integer_squareroot
  by_cases hmax : n = UINT64_MAX
  · simp only [hmax, if_true, UINT64_MAX, UINT64_MAX_SQRT, uint64_max_sqrt]; rfl
  · simp only [hmax, if_false]
    have hn' : n ≤ 2 ^ 64 - 2 := by simp [UINT64_MAX] at hmax; omega
    have hfit : n + 1 < UINT64_SIZE := by simp [UINT64_SIZE]; omega
    simp only [uint64Add, hfit, if_true]
    show integer_squareroot_loop n n ((n + 1) / 2) = _
    rcases Nat.eq_zero_or_pos n with h0 | hpos
    · subst h0; rw [integer_squareroot_loop]; simp; rfl
    · rw [integer_squareroot_loop_eq n hpos hn' n ((n + 1) / 2) hpos
        (by rw [Nat.div_self hpos]) (fun _ => le_refl _)]
      rw [iter_eq_sqrt n hpos]

/-! ### Scalar helpers -/

theorem u64_max_val : U64.max = 18446744073709551615 := by simp [U64.max_eq]

theorem u64_max_lt : U64.max < 2 ^ 64 := by rw [u64_max_val]; norm_num

theorem div_two (a : U64) : ∃ z : U64, a / 2#u64 = ok z ∧ z.val = a.val / 2 :=
  UScalar.div_spec a (by simp)

theorem saturating_add_val (a b : U64) (h : a.val + b.val ≤ U64.max) :
    (core.num.U64.saturating_add a b).val = a.val + b.val := by
  simp only [core.num.U64.saturating_add, UScalar.saturating_add, UScalar.val,
    BitVec.toNat_ofNat]
  have hm : UScalar.max .U64 = U64.max := by simp [U64.max_eq]
  rw [hm]
  change min U64.max (a.val + b.val) % _ = a.val + b.val
  rw [Nat.min_eq_right h]
  apply Nat.mod_eq_of_lt
  have : U64.max < 2 ^ UScalarTy.U64.numBits := by simp [U64.max_eq]
  omega

theorem checked_div_ok (a b : U64) (h : b.val ≠ 0) :
    ∃ q : U64, U64.checked_div a b = some q ∧ q.val = a.val / b.val := by
  have hs := U64.checked_div_bv_spec a b
  cases hc : U64.checked_div a b with
  | none => simp only [hc] at hs; exact absurd hs h
  | some q => simp only [hc] at hs; exact ⟨q, rfl, hs.2.1⟩

/-- The Rust loop computes `Nat.sqrt.iter`. -/
theorem integer_sqrt_loop_spec (n x y : U64) (hn : 1 ≤ n.val) (hn' : n.val ≤ 2 ^ 64 - 2)
    (hx : 1 ≤ x.val) (hy : y.val = (x.val + n.val / x.val) / 2)
    (hb : y.val < x.val → y.val ≤ (n.val + 1) / 2) :
    ∃ r, state.base_rewards.integer_sqrt_loop n x y = ok r ∧
      r.val = Nat.sqrt.iter n.val x.val := by
  suffices h : state.base_rewards.integer_sqrt_loop n x y ⦃ r =>
      r.val = Nat.sqrt.iter n.val x.val ⦄ by
    exact spec_imp_exists h
  unfold state.base_rewards.integer_sqrt_loop
  apply loop.spec_decr_nat
    (measure := fun (s : U64 × U64) => s.1.val)
    (inv := fun (s : U64 × U64) => 1 ≤ s.1.val ∧ s.2.val = (s.1.val + n.val / s.1.val) / 2 ∧
      (s.2.val < s.1.val → s.2.val ≤ (n.val + 1) / 2) ∧
      Nat.sqrt.iter n.val s.1.val = Nat.sqrt.iter n.val x.val)
  · rintro ⟨a, b⟩ ⟨ha, hb', hbb, hit⟩
    dsimp only at ha hb' hbb hit
    unfold state.base_rewards.integer_sqrt_loop.body
    by_cases hlt : b < a
    · have hlt' : b.val < a.val := hlt
      have hb1 : 1 ≤ b.val := by rw [hb']; exact newton_step_pos _ _ hn ha
      have hfit := newton_step_fit n.val b.val hn' hb1 (hbb hlt')
      obtain ⟨q, hq, hqv⟩ := checked_div_ok n b (by omega)
      have hsat : (core.num.U64.saturating_add b q).val = b.val + q.val :=
        saturating_add_val b q (by rw [u64_max_val, hqv]; omega)
      obtain ⟨z, hz, hzv⟩ := div_two (core.num.U64.saturating_add b q)
      simp only [hlt, if_true, lift, hq, bind_tc_ok, core.option.Option.unwrap_or, hz,
        spec_ok]
      refine ⟨⟨hb1, by rw [hzv, hsat, hqv], ?_, ?_⟩, hlt'⟩
      · intro h; rw [hzv, hsat, hqv] at h ⊢; omega
      · rw [← hit, Nat.sqrt.iter.eq_1 n.val a.val]
        have : (a.val + n.val / a.val) / 2 < a.val := hb' ▸ hlt'
        simp only [← hb']
        rw [dif_pos hlt']
    · have hlt' : ¬ b.val < a.val := hlt
      simp only [hlt, if_false, spec_ok]
      rw [← hit, Nat.sqrt.iter.eq_1 n.val a.val]
      have : ¬ (a.val + n.val / a.val) / 2 < a.val := hb' ▸ hlt'
      simp only [this, dif_neg, not_false_eq_true]
  · exact ⟨hx, hy, hb, rfl⟩

theorem integer_sqrt_spec (n : U64) :
    ∃ r, state.base_rewards.integer_sqrt n = ok r ∧ r.val = Nat.sqrt n.val := by
  unfold state.base_rewards.integer_sqrt
  by_cases hmax : n = core.num.U64.MAX
  · have hv : n.val = 2 ^ 64 - 1 := by rw [hmax]; simp [core.num.U64.MAX, U64.rMax]
    simp only [hmax, if_true]
    refine ⟨_, rfl, ?_⟩
    rw [hmax] at hv; rw [hv, uint64_max_sqrt]; rfl
  · have hn' : n.val ≤ 2 ^ 64 - 2 := by
      have h1 : n.val ≠ 2 ^ 64 - 1 := by
        intro h; apply hmax; apply UScalar.eq_of_val_eq; simp [core.num.U64.MAX, U64.rMax, h]
      have h2 := n.hBounds
      simp at h2
      omega
    have hsat : (core.num.U64.saturating_add n 1#u64).val = n.val + 1 :=
      saturating_add_val n 1#u64 (by simp [u64_max_val]; omega)
    obtain ⟨y, hy, hyv⟩ := div_two (core.num.U64.saturating_add n 1#u64)
    simp only [hmax, if_false, lift, bind_tc_ok, hy]
    rcases Nat.eq_zero_or_pos n.val with h0 | hpos
    · have hy0 : y.val = 0 := by rw [hyv, hsat, h0]
      unfold state.base_rewards.integer_sqrt_loop
      rw [loop.eq_def]
      unfold state.base_rewards.integer_sqrt_loop.body
      have : ¬ y < n := by
        intro h; have : y.val < n.val := h; omega
      simp only [this, if_false]
      exact ⟨n, rfl, by rw [h0]; rfl⟩
    · obtain ⟨r, hr, hrv⟩ := integer_sqrt_loop_spec n n y hpos hn' hpos
        (by rw [hyv, hsat, Nat.div_self hpos]) (fun _ => by rw [hyv, hsat])
      exact ⟨r, hr, by rw [hrv, iter_eq_sqrt _ hpos]⟩

/-- The Rust `integer_sqrt` equals the spec `integer_squareroot`. -/
theorem integer_sqrt_equiv (n : U64) :
    ∃ r, state.base_rewards.integer_sqrt n = ok r ∧
      integer_squareroot n.val = .ok r.val := by
  obtain ⟨r, hr, hrv⟩ := integer_sqrt_spec n
  refine ⟨r, hr, ?_⟩
  rw [integer_squareroot_eq _ (by have := n.hBounds; simpa using this), hrv]

/-! ## Safe arithmetic helpers -/

theorem safe_add_ok (a b : U64) (h : a.val + b.val ≤ U64.max) :
    ∃ c : U64, U64.Insts.Safe_arithSafeArithU64.safe_add a b = ok (.Ok c) ∧
      c.val = a.val + b.val := by
  unfold U64.Insts.Safe_arithSafeArithU64.safe_add
  have hs := U64.checked_add_bv_spec a b
  cases hc : U64.checked_add a b with
  | none => simp only [hc] at hs; omega
  | some c =>
    simp only [hc] at hs
    exact ⟨c, by simp [lift, core.option.Option.ok_or], hs.2.1⟩

theorem safe_add_err (a b : U64) (h : U64.max < a.val + b.val) :
    U64.Insts.Safe_arithSafeArithU64.safe_add a b = ok (.Err .Overflow) := by
  unfold U64.Insts.Safe_arithSafeArithU64.safe_add
  have hs := U64.checked_add_bv_spec a b
  cases hc : U64.checked_add a b with
  | none => simp [lift, core.option.Option.ok_or]
  | some c => simp only [hc] at hs; omega

theorem safe_sub_ok (a b : U64) (h : b.val ≤ a.val) :
    ∃ c : U64, U64.Insts.Safe_arithSafeArithU64.safe_sub a b = ok (.Ok c) ∧
      c.val = a.val - b.val := by
  unfold U64.Insts.Safe_arithSafeArithU64.safe_sub
  have hs := U64.checked_sub_bv_spec a b
  cases hc : U64.checked_sub a b with
  | none => simp only [hc] at hs; omega
  | some c =>
    simp only [hc] at hs
    exact ⟨c, by simp [lift, core.option.Option.ok_or], hs.2.1⟩

theorem safe_mul_ok (a b : U64) (h : a.val * b.val ≤ U64.max) :
    ∃ c : U64, U64.Insts.Safe_arithSafeArithU64.safe_mul a b = ok (.Ok c) ∧
      c.val = a.val * b.val := by
  unfold U64.Insts.Safe_arithSafeArithU64.safe_mul
  have hs := U64.checked_mul_bv_spec a b
  cases hc : U64.checked_mul a b with
  | none => simp only [hc] at hs; omega
  | some c =>
    simp only [hc] at hs
    exact ⟨c, by simp [lift, core.option.Option.ok_or], hs.2.1⟩

theorem safe_div_ok (a b : U64) (h : b.val ≠ 0) :
    ∃ c : U64, U64.Insts.Safe_arithSafeArithU64.safe_div a b = ok (.Ok c) ∧
      c.val = a.val / b.val := by
  unfold U64.Insts.Safe_arithSafeArithU64.safe_div
  obtain ⟨q, hq, hqv⟩ := checked_div_ok a b h
  exact ⟨q, by simp [lift, core.option.Option.ok_or, hq], hqv⟩

theorem safe_div_zero (a b : U64) (h : b.val = 0) :
    U64.Insts.Safe_arithSafeArithU64.safe_div a b = ok (.Err .DivisionByZero) := by
  unfold U64.Insts.Safe_arithSafeArithU64.safe_div
  have hs := U64.checked_div_bv_spec a b
  cases hc : U64.checked_div a b with
  | none => simp [lift, core.option.Option.ok_or]
  | some c => simp only [hc] at hs; exact absurd h hs.1

theorem branch_ok {T E : Type} (v : T) :
    core.result.Result.Insts.CoreOpsTry.branch (E := E) (.Ok v) =
      ok (core.ops.control_flow.ControlFlow.Continue v) :=
  rfl

theorem branch_err {T E : Type} (e : E) :
    core.result.Result.Insts.CoreOpsTry.branch (T := T) (.Err e) =
      ok (core.ops.control_flow.ControlFlow.Break (.Err e)) :=
  rfl

/-! ## Total active balance -/

/-- Sum of `ebs[j]` over the positions `j` with `act (k + j)`. -/
def activeSumFrom (act : Nat → Bool) : Nat → List Nat → Nat
  | _, [] => 0
  | k, e :: es => (if act k then e else 0) + activeSumFrom act (k + 1) es

/-- Sum of the effective balances of the validators with `act i`. -/
def activeSum (act : Nat → Bool) (ebs : List Nat) : Nat := activeSumFrom act 0 ebs

/-- The list after `update_effective_balance` at an index that is not out of bounds. -/
def updateList (l : List Nat) (i e : Nat) : List Nat :=
  if i = l.length then l ++ [e] else l.set i e

theorem activeSumFrom_snoc (act : Nat → Bool) (k : Nat) (l : List Nat) (e : Nat) :
    activeSumFrom act k (l ++ [e]) =
      activeSumFrom act k l + (if act (k + l.length) then e else 0) := by
  induction l generalizing k with
  | nil => simp [activeSumFrom]
  | cons x t ih =>
    simp only [List.cons_append, activeSumFrom, ih, List.length_cons]
    rw [show k + 1 + t.length = k + (t.length + 1) by omega]
    omega

theorem activeSumFrom_set (act : Nat → Bool) (k : Nat) (l : List Nat) (i e : Nat)
    (hi : i < l.length) :
    activeSumFrom act k (l.set i e) + (if act (k + i) then l[i] else 0) =
      activeSumFrom act k l + (if act (k + i) then e else 0) := by
  induction l generalizing k i with
  | nil => simp at hi
  | cons x t ih =>
    cases i with
    | zero => simp [activeSumFrom]; omega
    | succ j =>
      simp only [List.set_cons_succ, activeSumFrom, List.getElem_cons_succ]
      have := ih (k + 1) j (by simpa using hi)
      rw [show k + 1 + j = k + (j + 1) by omega] at this
      omega

theorem activeSumFrom_le (act : Nat → Bool) (k : Nat) (l : List Nat) (i : Nat)
    (hi : i < l.length) (ha : act (k + i)) : l[i] ≤ activeSumFrom act k l := by
  induction l generalizing k i with
  | nil => simp at hi
  | cons x t ih =>
    cases i with
    | zero => simp [activeSumFrom] at ha ⊢; simp [ha]
    | succ j =>
      simp only [activeSumFrom, List.getElem_cons_succ]
      have := ih (k + 1) j (by simpa using hi) (by rw [show k + 1 + j = k + (j + 1) by omega]; exact ha)
      omega

theorem list_set_opt_some {α : Type} (l : List α) (i : Nat) (x : α) :
    l.set_opt i (some x) = l.set i x := by
  induction l generalizing i with
  | nil => simp
  | cons h t ih =>
    cases i with
    | zero => simp
    | succ j => simp [List.set_opt, ih]

theorem list_set_opt_none {α : Type} (l : List α) (i : Nat) : l.set_opt i none = l := by
  induction l generalizing i with
  | nil => simp
  | cons h t ih =>
    cases i with
    | zero => simp
    | succ j => simp [List.set_opt, ih]

/-- `PreEpochCache::update_effective_balance` keeps `total = activeSum act ebs` and does not fail
if the index is at most the length and the running total does not overflow. -/
theorem update_effective_balance_spec (ebs : alloc.vec.Vec U64) (total : U64) (i : Usize)
    (eb : U64) (active : Bool) (act : Nat → Bool)
    (hinv : total.val = activeSum act (ebs.val.map (·.val)))
    (hact : act i.val = active)
    (hi : i.val ≤ ebs.val.length)
    (hlen : ebs.val.length < Usize.max)
    (hfit : active → total.val + eb.val ≤ U64.max) :
    ∃ ebs' total',
      state.total_active_balance.update_effective_balance ebs total i eb active =
        ok (.Ok true, ebs', total') ∧
      ebs'.val.map (·.val) = updateList (ebs.val.map (·.val)) i.val eb.val ∧
      total'.val = activeSum act (ebs'.val.map (·.val)) := by
  unfold state.total_active_balance.update_effective_balance
  by_cases heq : i.val = ebs.val.length
  · have hi' : i = alloc.vec.Vec.len ebs := by
      apply UScalar.eq_of_val_eq; rw [alloc.vec.Vec.len_val]; exact heq
    obtain ⟨v1, hv1, hv1v⟩ := spec_imp_exists (alloc.vec.Vec.push_spec ebs eb hlen)
    have hmap : v1.val.map (·.val) = updateList (ebs.val.map (·.val)) i.val eb.val := by
      simp [updateList, hv1v, heq]
    have hsum : activeSum act (v1.val.map (·.val)) =
        activeSum act (ebs.val.map (·.val)) + (if active then eb.val else 0) := by
      rw [hv1v, List.map_append]
      simp only [List.map_cons, List.map_nil]
      rw [activeSum, activeSumFrom_snoc]
      simp only [List.length_map, Nat.zero_add]
      rw [← heq, hact]; rfl
    rw [if_pos hi']
    simp only [hv1, bind_tc_ok]
    cases active with
    | true =>
      obtain ⟨c, hc, hcv⟩ := safe_add_ok total eb (hfit rfl)
      simp only [if_true, hc, bind_tc_ok, branch_ok]
      refine ⟨v1, c, rfl, hmap, ?_⟩
      rw [hsum, hcv, hinv]; simp
    | false =>
      simp only [Bool.false_eq_true, if_false]
      refine ⟨v1, total, rfl, hmap, ?_⟩
      rw [hsum, hinv]; simp
  · have hne : ¬ i = alloc.vec.Vec.len ebs := by
      intro h; apply heq; rw [h, alloc.vec.Vec.len_val]
    have hlt : i.val < ebs.val.length := by omega
    have hget : ebs.val[i.val]? = some ebs.val[i.val] := List.getElem?_eq_getElem hlt
    have hmap : (ebs.val.set i.val eb).map (·.val) =
        updateList (ebs.val.map (·.val)) i.val eb.val := by
      simp [updateList, heq, List.map_set]
    have hset := activeSumFrom_set act 0 (ebs.val.map (·.val)) i.val eb.val (by simpa using hlt)
    simp only [Nat.zero_add, List.getElem_map, hact] at hset
    rw [if_neg hne]
    generalize hd : alloc.vec.Vec.deref_mut ebs = p
    obtain ⟨sl, back⟩ := p
    have hs : sl.val = ebs.val := by simp [alloc.vec.Vec.deref_mut] at hd; rw [← hd.1]
    have hback : ∀ t : Slice U64, (back t).val = t.val := by
      simp [alloc.vec.Vec.deref_mut] at hd; intro t; rw [← hd.2]
    have hget2 : sl.val[i.val]? = some ebs.val[i.val] := by
      rw [hs]; exact List.getElem?_eq_getElem hlt
    have hnew : ((back (sl.set_opt i (some eb))).val).map (·.val) =
        updateList (ebs.val.map (·.val)) i.val eb.val := by
      rw [hback, Slice.set_opt_val_eq, list_set_opt_some, hs]; exact hmap
    have hsetsum : activeSum act ((back (sl.set_opt i (some eb))).val.map (·.val)) +
        (if active then ebs.val[i.val].val else 0) =
        activeSum act (ebs.val.map (·.val)) + (if active then eb.val else 0) := by
      rw [hback, Slice.set_opt_val_eq, list_set_opt_some, hs, List.map_set]
      simpa [activeSum] using hset
    cases active with
    | true =>
      have hle : ebs.val[i.val].val ≤ total.val + eb.val := by
        have := activeSumFrom_le act 0 (ebs.val.map (·.val)) i.val (by simpa using hlt)
          (by simpa using hact)
        simp only [List.getElem_map] at this
        rw [hinv]; simp only [activeSum]; omega
      obtain ⟨c, hc, hcv⟩ := safe_add_ok total eb (hfit rfl)
      obtain ⟨c2, hc2, hc2v⟩ := safe_sub_ok c ebs.val[i.val] (by rw [hcv]; exact hle)
      simp [lift, core.slice.Slice.get_mut, core.slice.index.Usize.get_mut, hget2, hc, hc2,
        branch_ok]
      refine ⟨hnew, ?_⟩
      simp only [if_true] at hsetsum
      rw [hc2v, hcv, hinv]
      omega
    | false =>
      simp [lift, core.slice.Slice.get_mut, core.slice.index.Usize.get_mut, hget2]
      refine ⟨hnew, ?_⟩
      simp only [Bool.false_eq_true, if_false] at hsetsum
      rw [hinv]
      omega

/-- If the index is past the end, `update_effective_balance` reports it and changes nothing. -/
theorem update_effective_balance_out_of_bounds (ebs : alloc.vec.Vec U64) (total : U64)
    (i : Usize) (eb : U64) (active : Bool) (hi : ebs.val.length < i.val) :
    ∃ ebs', state.total_active_balance.update_effective_balance ebs total i eb active =
      ok (.Ok false, ebs', total) ∧ ebs'.val = ebs.val := by
  unfold state.total_active_balance.update_effective_balance
  have hne : ¬ i = alloc.vec.Vec.len ebs := by
    intro h; have := congrArg (·.val) h; simp at this; omega
  rw [if_neg hne]
  generalize hd : alloc.vec.Vec.deref_mut ebs = p
  obtain ⟨sl, back⟩ := p
  have hs : sl.val = ebs.val := by simp [alloc.vec.Vec.deref_mut] at hd; rw [← hd.1]
  have hback : ∀ t : Slice U64, (back t).val = t.val := by
    simp [alloc.vec.Vec.deref_mut] at hd; intro t; rw [← hd.2]
  have hget2 : sl.val[i.val]? = none := by rw [hs]; exact List.getElem?_eq_none (by omega)
  simp [lift, core.slice.Slice.get_mut, core.slice.index.Usize.get_mut, hget2]
  rw [hback, Slice.set_opt_val_eq]
  simp [hs, list_set_opt_none]

/-- A call `update_effective_balance(i, eb, active)` as `single_pass.rs` makes it. -/
abbrev Call := Usize × U64 × Bool

/-- The `single_pass.rs` glue: it calls `update_effective_balance` for each call in order and
stops at the first error (`?`). `none` is an error. -/
def lhRun (s : alloc.vec.Vec U64 × U64) : List Call → Result (Option (alloc.vec.Vec U64 × U64))
  | [] => ok (some s)
  | (i, eb, active) :: cs => do
    let (r, ebs', total') ←
      state.total_active_balance.update_effective_balance s.1 s.2 i eb active
    match r with
    | .Ok true => lhRun (ebs', total') cs
    | _ => ok none

/-- The calls are valid for `act`: each index is at most the current length, each flag equals
`act` of its index, and each running total plus the new balance fits in a `u64`. -/
def ValidCalls (act : Nat → Bool) : List Nat → List Call → Prop
  | _, [] => True
  | l, (i, eb, active) :: cs =>
    i.val ≤ l.length ∧ l.length < Usize.max ∧ act i.val = active ∧
      (active → activeSum act l + eb.val ≤ U64.max) ∧
      ValidCalls act (updateList l i.val eb.val) cs

/-- The effective balances after the calls. -/
def modelRun (l : List Nat) (cs : List Call) : List Nat :=
  cs.foldl (fun l c => updateList l c.1.val c.2.1.val) l

/-- Preservation over a whole call sequence: the glue does not fail, and the total stays the sum
of the active effective balances. -/
theorem lhRun_spec (act : Nat → Bool) (cs : List Call) :
    ∀ (ebs : alloc.vec.Vec U64) (total : U64),
      total.val = activeSum act (ebs.val.map (·.val)) →
      ValidCalls act (ebs.val.map (·.val)) cs →
      ∃ ebs' total', lhRun (ebs, total) cs = ok (some (ebs', total')) ∧
        ebs'.val.map (·.val) = modelRun (ebs.val.map (·.val)) cs ∧
        total'.val = activeSum act (ebs'.val.map (·.val)) := by
  induction cs with
  | nil =>
    intro ebs total hinv _
    exact ⟨ebs, total, rfl, rfl, hinv⟩
  | cons c cs ih =>
    obtain ⟨i, eb, active⟩ := c
    intro ebs total hinv hvalid
    obtain ⟨hi, hlen, hact, hfit, hrest⟩ := hvalid
    simp only [List.length_map] at hi hlen
    obtain ⟨ebs1, total1, hrun, hmap1, hinv1⟩ := update_effective_balance_spec ebs total i eb
      active act hinv hact hi hlen (fun h => by rw [hinv]; exact hfit h)
    rw [← hmap1] at hrest
    obtain ⟨ebs2, total2, hrun2, hmap2, hinv2⟩ := ih ebs1 total1 hinv1 hrest
    refine ⟨ebs2, total2, ?_, ?_, hinv2⟩
    · simp only [lhRun, hrun, bind_tc_ok]; exact hrun2
    · rw [hmap2, hmap1]; rfl

/-- Build: `PreEpochCache::new_for_next_epoch` starts from an empty vector and a zero total. -/
theorem lhRun_build (act : Nat → Bool) (cs : List Call) (ValidCalls_cs : ValidCalls act [] cs) :
    ∃ ebs' total', lhRun (alloc.vec.Vec.new U64, 0#u64) cs = ok (some (ebs', total')) ∧
      ebs'.val.map (·.val) = modelRun [] cs ∧
      total'.val = activeSum act (ebs'.val.map (·.val)) :=
  lhRun_spec act cs (alloc.vec.Vec.new U64) 0#u64 (by simp [activeSum, activeSumFrom])
    (by simpa using ValidCalls_cs)

/-! ### The spec total active balance -/

/-- `is_active_validator(state.validators[i], epoch)`, and `false` past the end. -/
def activeAt (vs : List Validator) (epoch : Nat) (i : Nat) : Bool :=
  match vs[i]? with
  | some v => is_active_validator v epoch
  | none => false

theorem sumUint64_eq (xs : List Nat) : ∀ a, a + xs.sum < 2 ^ 64 →
    xs.foldlM uint64Add a = .ok (a + xs.sum) := by
  induction xs with
  | nil => intro a _; simp; rfl
  | cons x t ih =>
    intro a h
    simp only [List.sum_cons] at h
    have hfit : a + x < UINT64_SIZE := by simp [UINT64_SIZE]; omega
    simp only [List.foldlM_cons, uint64Add, hfit, if_true]
    show List.foldlM uint64Add (a + x) t = _
    rw [ih (a + x) (by omega)]
    simp [List.sum_cons, Nat.add_assoc]

theorem active_balances_eq (W : List Validator) (e e' : Nat) :
    ∀ (l : List Validator) (k : Nat), W.drop k = l →
      ∃ bs, ((l.zipIdx k).filterMap fun (v, i) =>
          if is_active_validator v e then some i else none).mapM
          (fun index => do
            let v ← getValidator ⟨W, e'⟩ index
            pure v.effective_balance) = .ok bs ∧
        bs.sum = activeSumFrom (activeAt W e) k (l.map (·.effective_balance)) := by
  intro l
  induction l with
  | nil => intro k _; exact ⟨[], rfl, by simp [activeSumFrom]⟩
  | cons v t ih =>
    intro k hk
    have hget : W[k]? = some v := by
      have := congrArg (·[0]?) hk
      simpa [List.getElem?_drop] using this
    have hk' : W.drop (k + 1) = t := by
      rw [← List.drop_drop, hk]; rfl
    obtain ⟨bs, hbs, hsum⟩ := ih (k + 1) hk'
    have hact : activeAt W e k = is_active_validator v e := by simp [activeAt, hget]
    have hval : getValidator ⟨W, e'⟩ k = .ok v := by simp [getValidator, hget]; rfl
    by_cases ha : is_active_validator v e
    · refine ⟨v.effective_balance :: bs, ?_, ?_⟩
      · simp only [List.zipIdx_cons, List.filterMap_cons, ha, if_true, List.mapM_cons, hval, hbs]
        rfl
      · simp [activeSumFrom, hact, ha, hsum]
    · refine ⟨bs, ?_, ?_⟩
      · simp only [List.zipIdx_cons, List.filterMap_cons, ha, Bool.false_eq_true, if_false]
        exact hbs
      · simp [activeSumFrom, hact, ha, hsum]

/-- The spec `get_total_active_balance` is the floored sum of the active effective balances. -/
theorem get_total_active_balance_eq (cfg : Config) (vs : List Validator) (epoch : Nat)
    (h : activeSum (activeAt vs epoch) (vs.map (·.effective_balance)) < 2 ^ 64) :
    get_total_active_balance cfg ⟨vs, epoch⟩ =
      .ok (max cfg.EFFECTIVE_BALANCE_INCREMENT
        (activeSum (activeAt vs epoch) (vs.map (·.effective_balance)))) := by
  obtain ⟨bs, hbs, hsum⟩ := active_balances_eq vs epoch epoch vs 0 rfl
  unfold get_total_active_balance get_total_balance get_active_validator_indices
    get_current_epoch
  dsimp only
  rw [hbs]
  show (do let total ← sumUint64 bs; pure (max cfg.EFFECTIVE_BALANCE_INCREMENT total)) = _
  rw [sumUint64, sumUint64_eq bs 0 (by rw [hsum]; simpa [activeSum] using h)]
  simp [hsum, activeSum]

theorem floor_total_active_balance_spec (total inc : U64) :
    ∃ r, state.total_active_balance.floor_total_active_balance total inc = ok r ∧
      r.val = max inc.val total.val := by
  unfold state.total_active_balance.floor_total_active_balance
  by_cases h : total > inc
  · have h' : inc.val < total.val := h
    simp only [h, if_true]
    exact ⟨total, rfl, by omega⟩
  · have h' : ¬ inc.val < total.val := h
    simp only [h, if_false]
    exact ⟨inc, rfl, by omega⟩

/-- Read agreement for the total: if the cache holds the effective balances of `vs` and the
coherent total for `epoch`, the floored total equals the spec `get_total_active_balance`. -/
theorem total_active_balance_equiv (cfg : Config) (vs : List Validator) (epoch : Nat)
    (ebs : List Nat) (total inc : U64)
    (hinc : inc.val = cfg.EFFECTIVE_BALANCE_INCREMENT)
    (hebs : ebs = vs.map (·.effective_balance))
    (hinv : total.val = activeSum (activeAt vs epoch) ebs) :
    ∃ r, state.total_active_balance.floor_total_active_balance total inc = ok r ∧
      get_total_active_balance cfg ⟨vs, epoch⟩ = .ok r.val := by
  obtain ⟨r, hr, hrv⟩ := floor_total_active_balance_spec total inc
  refine ⟨r, hr, ?_⟩
  have hlt : activeSum (activeAt vs epoch) (vs.map (·.effective_balance)) < 2 ^ 64 := by
    rw [← hebs, ← hinv]; have := total.hBounds; simpa using this
  rw [get_total_active_balance_eq cfg vs epoch hlt, hrv, hinc, ← hebs, ← hinv]

/-- The `single_pass.rs` sequence, end to end: from an empty `PreEpochCache`, the calls give the
effective balances of `vs`, and the floored total equals the spec `get_total_active_balance` of
the state at `epoch` (the next epoch). -/
theorem single_pass_total_active_balance (cfg : Config) (vs : List Validator) (epoch : Nat)
    (cs : List Call) (inc : U64) (hinc : inc.val = cfg.EFFECTIVE_BALANCE_INCREMENT)
    (hvalid : ValidCalls (activeAt vs epoch) [] cs)
    (hfinal : modelRun [] cs = vs.map (·.effective_balance)) :
    ∃ ebs total, lhRun (alloc.vec.Vec.new U64, 0#u64) cs = ok (some (ebs, total)) ∧
      ebs.val.map (·.val) = vs.map (·.effective_balance) ∧
      ∃ r, state.total_active_balance.floor_total_active_balance total inc = ok r ∧
        get_total_active_balance cfg ⟨vs, epoch⟩ = .ok r.val := by
  obtain ⟨ebs, total, hrun, hmap, hinv⟩ := lhRun_build (activeAt vs epoch) cs hvalid
  rw [hfinal] at hmap
  refine ⟨ebs, total, hrun, hmap, ?_⟩
  exact total_active_balance_equiv cfg vs epoch _ total inc hinc hmap hinv

/-! ### `BeaconState::compute_total_active_balance_slow` -/

/-- The loop of `compute_total_active_balance_slow` (glue, by hand): `safe_add_assign` over the
active validators. `none` is an overflow. -/
def lhSlowSum (epoch : Nat) : Nat → List Validator → Option Nat
  | acc, [] => some acc
  | acc, v :: vs =>
    if is_active_validator v epoch then
      if acc + v.effective_balance ≤ 18446744073709551615 then
        lhSlowSum epoch (acc + v.effective_balance) vs
      else none
    else lhSlowSum epoch acc vs

theorem lhSlowSum_eq (W : List Validator) (epoch : Nat) :
    ∀ (l : List Validator) (k acc : Nat), W.drop k = l →
      acc + activeSumFrom (activeAt W epoch) k (l.map (·.effective_balance)) < 2 ^ 64 →
      lhSlowSum epoch acc l =
        some (acc + activeSumFrom (activeAt W epoch) k (l.map (·.effective_balance))) := by
  intro l
  induction l with
  | nil => intro k acc _ _; simp [lhSlowSum, activeSumFrom]
  | cons v t ih =>
    intro k acc hk hlt
    have hget : W[k]? = some v := by
      have := congrArg (·[0]?) hk
      simpa [List.getElem?_drop] using this
    have hk' : W.drop (k + 1) = t := by rw [← List.drop_drop, hk]; rfl
    have hact : activeAt W epoch k = is_active_validator v epoch := by simp [activeAt, hget]
    simp only [List.map_cons, activeSumFrom, hact] at hlt ⊢
    by_cases ha : is_active_validator v epoch
    · simp only [ha, if_true] at hlt ⊢
      have hfit : acc + v.effective_balance ≤ 18446744073709551615 := by omega
      simp only [lhSlowSum, ha, if_true, hfit]
      rw [ih (k + 1) _ hk' (by omega)]
      congr 1; omega
    · simp only [ha, Bool.false_eq_true, if_false] at hlt ⊢
      simp only [lhSlowSum, ha, Bool.false_eq_true, if_false]
      rw [ih (k + 1) _ hk' (by omega)]
      congr 1; omega

/-- Build for the `BeaconState` total active balance cache: if the spec sum does not overflow,
the slow loop does not overflow, and the floored result equals `get_total_active_balance`. -/
theorem compute_total_active_balance_slow_equiv (cfg : Config) (vs : List Validator)
    (epoch : Nat) (inc : U64) (hinc : inc.val = cfg.EFFECTIVE_BALANCE_INCREMENT)
    (h : activeSum (activeAt vs epoch) (vs.map (·.effective_balance)) < 2 ^ 64) :
    ∃ total : U64, lhSlowSum epoch 0 vs = some total.val ∧
      ∃ r, state.total_active_balance.floor_total_active_balance total inc = ok r ∧
        get_total_active_balance cfg ⟨vs, epoch⟩ = .ok r.val := by
  have hs := lhSlowSum_eq vs epoch vs 0 0 rfl (by simpa [activeSum] using h)
  let total : U64 := ⟨BitVec.ofNat _ (activeSum (activeAt vs epoch) (vs.map (·.effective_balance)))⟩
  have htv : total.val = activeSum (activeAt vs epoch) (vs.map (·.effective_balance)) := by
    simp only [total, UScalar.val, BitVec.toNat_ofNat]
    apply Nat.mod_eq_of_lt; simpa using h
  refine ⟨total, ?_, total_active_balance_equiv cfg vs epoch _ total inc hinc rfl htv⟩
  rw [hs, htv]; simp [activeSum]

/-! ## Base rewards -/

theorem sqrt_pos (n : Nat) (h : 1 ≤ n) : 1 ≤ Nat.sqrt n := Nat.le_sqrt.mpr (by simpa using h)

theorem base_reward_per_increment_spec (total inc factor : U64) (htotal : 1 ≤ total.val)
    (hmul : inc.val * factor.val ≤ U64.max) :
    ∃ c : U64, state.base_rewards.base_reward_per_increment total inc factor =
      ok (.Ok c) ∧ c.val = inc.val * factor.val / Nat.sqrt total.val := by
  unfold state.base_rewards.base_reward_per_increment
  obtain ⟨a, ha, hav⟩ := safe_mul_ok inc factor hmul
  obtain ⟨sq, hsq, hsqv⟩ := integer_sqrt_spec total
  have hpos := sqrt_pos total.val htotal
  obtain ⟨c, hc, hcv⟩ := safe_div_ok a sq (by omega)
  simp only [ha, bind_tc_ok, branch_ok, hsq, hc]
  exact ⟨c, rfl, by rw [hcv, hav, hsqv]⟩

theorem altair_base_reward_spec (eb inc brpi : U64) (hinc : inc.val ≠ 0)
    (hmul : eb.val / inc.val * brpi.val ≤ U64.max) :
    ∃ c : U64, state.base_rewards.altair_base_reward eb inc brpi = ok (.Ok c) ∧
      c.val = eb.val / inc.val * brpi.val := by
  unfold state.base_rewards.altair_base_reward
  obtain ⟨a, ha, hav⟩ := safe_div_ok eb inc hinc
  obtain ⟨c, hc, hcv⟩ := safe_mul_ok a brpi (by rw [hav]; exact hmul)
  simp only [ha, bind_tc_ok, branch_ok, hc]
  exact ⟨c, rfl, by rw [hcv, hav]⟩

theorem phase0_base_reward_spec (eb sq factor bpe : U64) (hsq : sq.val ≠ 0) (hbpe : bpe.val ≠ 0)
    (hmul : eb.val * factor.val ≤ U64.max) :
    ∃ c : U64, state.base_rewards.phase0_base_reward eb sq factor bpe =
      ok (.Ok c) ∧ c.val = eb.val * factor.val / sq.val / bpe.val := by
  unfold state.base_rewards.phase0_base_reward
  obtain ⟨a, ha, hav⟩ := safe_mul_ok eb factor hmul
  obtain ⟨b, hb, hbv⟩ := safe_div_ok a sq hsq
  obtain ⟨c, hc, hcv⟩ := safe_div_ok b bpe hbpe
  simp only [ha, bind_tc_ok, branch_ok, hb, hc]
  exact ⟨c, rfl, by rw [hcv, hbv, hav]⟩

/-- The table entry for `k` increments. -/
def tableEntry (phase0 : Bool) (inc sq brpi factor bpe k : Nat) : Nat :=
  if phase0 then k * inc * factor / sq / bpe else k * inc / inc * brpi

theorem base_reward_at_spec (k inc sq brpi factor bpe : U64) (phase0 : Bool)
    (hk : k.val * inc.val ≤ U64.max) (hinc : inc.val ≠ 0) (hsq : sq.val ≠ 0)
    (hbpe : bpe.val ≠ 0) (hp0 : k.val * inc.val * factor.val ≤ U64.max)
    (hal : k.val * brpi.val ≤ U64.max) :
    ∃ c : U64, state.base_rewards.base_reward_at k inc phase0 sq brpi factor bpe =
      ok (.Ok c) ∧ c.val = tableEntry phase0 inc.val sq.val brpi.val factor.val bpe.val k.val := by
  unfold state.base_rewards.base_reward_at
  obtain ⟨e, he, hev⟩ := safe_mul_ok k inc hk
  simp only [he, bind_tc_ok, branch_ok]
  cases phase0 with
  | true =>
    obtain ⟨c, hc, hcv⟩ := phase0_base_reward_spec e sq factor bpe hsq hbpe (by rw [hev]; exact hp0)
    simp only [if_true, hc, bind_tc_ok, branch_ok]
    exact ⟨c, rfl, by rw [hcv, hev]; rfl⟩
  | false =>
    have hdiv : e.val / inc.val = k.val := by rw [hev]; exact Nat.mul_div_cancel _ (by omega)
    obtain ⟨c, hc, hcv⟩ := altair_base_reward_spec e inc brpi hinc (by rw [hdiv]; exact hal)
    simp only [Bool.false_eq_true, if_false, hc, bind_tc_ok]
    exact ⟨c, rfl, by rw [hcv, hev]; rfl⟩

theorem usize_max_ge : 4294967295 ≤ Usize.max := by
  rcases Usize.bounds_eq with h | h <;> rw [h] <;> simp [U32.max_eq, U64.max_eq]

theorem cast_usize_val (x : U64) (h : x.val < 4294967296) :
    (UScalar.cast .Usize x).val = x.val := by
  rw [UScalar.cast_val_eq]; apply Nat.mod_eq_of_lt
  cases System.Platform.numBits_eq <;> simp [*]; omega

/-- The table loop of `base_rewards`: it appends `tableEntry k` for `k` from `j` to `m`. -/
theorem base_rewards_loop_spec (inc factor bpe : U64) (phase0 : Bool) (sq brpi m : U64)
    (hinc : inc.val ≠ 0) (hsq : sq.val ≠ 0) (hbpe : bpe.val ≠ 0) (hm : m.val < 4294967295)
    (hfit : ∀ k ≤ m.val, k * inc.val ≤ U64.max ∧ k * inc.val * factor.val ≤ U64.max ∧
      k * brpi.val ≤ U64.max)
    (v : alloc.vec.Vec U64) (j : U64) (hj : j.val ≤ m.val + 1)
    (hv : v.val.map (·.val) =
      (List.range j.val).map (tableEntry phase0 inc.val sq.val brpi.val factor.val bpe.val)) :
    ∃ v', state.base_rewards.base_rewards_loop inc factor bpe phase0 sq brpi m v
        none j = ok (v', none) ∧
      v'.val.map (·.val) =
        (List.range (m.val + 1)).map (tableEntry phase0 inc.val sq.val brpi.val factor.val bpe.val) := by
  suffices h : state.base_rewards.base_rewards_loop inc factor bpe phase0 sq brpi m v
      none j ⦃ r => r.2 = none ∧ r.1.val.map (·.val) =
        (List.range (m.val + 1)).map (tableEntry phase0 inc.val sq.val brpi.val factor.val bpe.val) ⦄ by
    obtain ⟨⟨v', e'⟩, hr, he, hv'⟩ := spec_imp_exists h
    dsimp only at he hv'
    subst he
    exact ⟨v', hr, hv'⟩
  unfold state.base_rewards.base_rewards_loop
  apply loop.spec_decr_nat
    (measure := fun (x : alloc.vec.Vec U64 × Option state.base_rewards.BaseRewardsError × U64) =>
      m.val + 1 - x.2.2.val)
    (inv := fun (x : alloc.vec.Vec U64 × Option state.base_rewards.BaseRewardsError × U64) =>
      x.2.1 = none ∧ x.2.2.val ≤ m.val + 1 ∧ x.1.val.map (·.val) =
        (List.range x.2.2.val).map (tableEntry phase0 inc.val sq.val brpi.val factor.val bpe.val))
  · rintro ⟨w, err, k⟩ ⟨herr, hk, hw⟩
    dsimp only at herr hk hw ⊢
    cases err with
    | some e => simp at herr
    | none =>
    unfold state.base_rewards.base_rewards_loop.body
    by_cases hle : k ≤ m
    · have hle' : k.val ≤ m.val := hle
      obtain ⟨h1, h2, h3⟩ := hfit k.val hle'
      obtain ⟨c, hc, hcv⟩ := base_reward_at_spec k inc sq brpi factor bpe phase0 h1 hinc hsq hbpe
        h2 h3
      have hwlen : w.val.length < Usize.max := by
        have := congrArg List.length hw
        simp only [List.length_map, List.length_range] at this
        have := usize_max_ge
        omega
      obtain ⟨w1, hw1, hw1v⟩ := spec_imp_exists (alloc.vec.Vec.push_spec w c hwlen)
      have hsat : (core.num.U64.saturating_add k 1#u64).val = k.val + 1 :=
        saturating_add_val k 1#u64 (by simp [u64_max_val]; omega)
      simp only [hle, if_true, core.option.Option.is_none, Option.isNone_none, hc, bind_tc_ok,
        hw1, lift]
      refine ⟨⟨rfl, by rw [hsat]; omega, ?_⟩, by rw [hsat]; omega⟩
      rw [hw1v, hsat, List.map_append, hw, List.range_succ, List.map_append]
      simp [hcv]
    · have hlt : m.val < k.val := by
        have : ¬ k.val ≤ m.val := hle
        omega
      have hkm : k.val = m.val + 1 := by omega
      simp only [hle, if_false, spec_ok]
      exact ⟨by simp, by rw [hw, hkm]⟩
  · exact ⟨rfl, hj, hv⟩

/-- The total after the floor, its square root and the altair base reward per increment. -/
def flooredTotal (inc total : Nat) : Nat := max inc total
def sqrtTotal (inc total : Nat) : Nat := Nat.sqrt (flooredTotal inc total)
def perIncrement (inc factor total : Nat) : Nat := inc * factor / sqrtTotal inc total

/-- Build: `base_rewards` (the body of `PreEpochCache::into_epoch_cache`) returns one entry for
each `k` in `0..=max_effective_balance / increment`, and entry `k` is `tableEntry k`. -/
theorem base_rewards_spec (total inc maxEb factor bpe : U64) (phase0 : Bool)
    (hinc : 1 ≤ inc.val) (hbpe : 1 ≤ bpe.val)
    (hincf : inc.val * factor.val ≤ U64.max) (hfit : maxEb.val * factor.val ≤ U64.max)
    (hcap : maxEb.val / inc.val < 4294967295) :
    ∃ tbl, state.base_rewards.base_rewards total inc maxEb factor bpe phase0 =
        ok (.Ok tbl) ∧
      tbl.val.map (·.val) = (List.range (maxEb.val / inc.val + 1)).map
        (tableEntry phase0 inc.val (sqrtTotal inc.val total.val)
          (perIncrement inc.val factor.val total.val) factor.val bpe.val) := by
  unfold state.base_rewards.base_rewards
  obtain ⟨t1, ht1, ht1v⟩ := floor_total_active_balance_spec total inc
  have ht1pos : 1 ≤ t1.val := by rw [ht1v]; omega
  obtain ⟨sq, hsq, hsqv⟩ := integer_sqrt_spec t1
  obtain ⟨brpi, hbrpi, hbrpiv⟩ := base_reward_per_increment_spec t1 inc factor ht1pos hincf
  obtain ⟨m, hm, hmv⟩ := safe_div_ok maxEb inc (by omega)
  obtain ⟨m1, hm1, hm1v⟩ := safe_add_ok m 1#u64 (by simp [u64_max_val]; omega)
  have hsqpos : sq.val ≠ 0 := by have := sqrt_pos t1.val ht1pos; omega
  have hsqv' : sq.val = sqrtTotal inc.val total.val := by
    rw [hsqv, ht1v]; rfl
  have hbrpiv' : brpi.val = perIncrement inc.val factor.val total.val := by
    rw [hbrpiv, ← hsqv, hsqv']; rfl
  have hbrpi_le : brpi.val ≤ inc.val * factor.val := by rw [hbrpiv]; exact Nat.div_le_self _ _
  have hmEb : m.val * inc.val ≤ maxEb.val := by rw [hmv]; exact Nat.div_mul_le_self _ _
  have hentries : ∀ k ≤ m.val, k * inc.val ≤ U64.max ∧ k * inc.val * factor.val ≤ U64.max ∧
      k * brpi.val ≤ U64.max := by
    intro k hk
    have hkinc : k * inc.val ≤ maxEb.val := le_trans (Nat.mul_le_mul_right _ hk) hmEb
    have hkf : k * inc.val * factor.val ≤ maxEb.val * factor.val := Nat.mul_le_mul_right _ hkinc
    have hmax : maxEb.val ≤ U64.max := by have := maxEb.hBounds; simp [u64_max_val] at this ⊢; omega
    refine ⟨le_trans hkinc hmax, le_trans hkf hfit, ?_⟩
    calc k * brpi.val ≤ k * (inc.val * factor.val) := Nat.mul_le_mul_left _ hbrpi_le
      _ = k * inc.val * factor.val := by rw [Nat.mul_assoc]
      _ ≤ U64.max := le_trans hkf hfit
  obtain ⟨tbl, hloop, htbl⟩ := base_rewards_loop_spec inc factor bpe phase0 sq brpi m
    (by omega) hsqpos (by omega) (by rw [hmv]; exact hcap) hentries
    (alloc.vec.Vec.with_capacity U64 (UScalar.cast .Usize m1)) 0#u64 (by simp)
    (by simp [alloc.vec.Vec.with_capacity, alloc.vec.Vec.new])
  simp only [ht1, bind_tc_ok, hsq, hbrpi, branch_ok, hm, hm1, lift, hloop]
  refine ⟨tbl, rfl, ?_⟩
  rw [htbl, hmv, hsqv', hbrpiv']

/-! ## Reads -/

theorem slice_get (sl : Slice U64) (i : Usize) :
    core.slice.Slice.get (core.slice.index.SliceIndexUsizeSlice U64) sl i = ok sl.val[i.val]? :=
  rfl

/-- `get_effective_balance` returns the entry, and an error only past the end. -/
theorem get_effective_balance_spec (ebs : Slice U64) (index : Usize) :
    state.base_rewards.get_effective_balance ebs index =
      ok (match ebs.val[index.val]? with
        | some eb => .Ok eb
        | none => .Err .ValidatorIndexOutOfBounds) := by
  unfold state.base_rewards.get_effective_balance
  rw [slice_get]
  cases ebs.val[index.val]? <;> rfl

theorem get_base_reward_spec (ebs table : Slice U64) (inc : U64) (index : Usize)
    (hindex : index.val < ebs.val.length) (hinc : inc.val ≠ 0)
    (heth : ebs.val[index.val].val / inc.val < table.val.length)
    (hcast : ebs.val[index.val].val / inc.val < 4294967296) :
    state.base_rewards.get_base_reward ebs table inc index =
      ok (.Ok table.val[ebs.val[index.val].val / inc.val]) := by
  unfold state.base_rewards.get_base_reward
  rw [get_effective_balance_spec, List.getElem?_eq_getElem hindex]
  obtain ⟨q, hq, hqv⟩ := safe_div_ok ebs.val[index.val] inc hinc
  have hc := cast_usize_val q (by rw [hqv]; exact hcast)
  simp only [bind_tc_ok, branch_ok, hq, lift, slice_get, hc, hqv]
  rw [List.getElem?_eq_getElem (by simpa [hc, hqv] using heth)]

/-! ## The spec base reward -/

theorem spec_get_base_reward_phase0_eq (cfg : Config) (st : BeaconState) (index : Nat)
    (T : Nat) (v : Validator)
    (htotal : get_total_active_balance cfg st = .ok T) (hT : 1 ≤ T) (hT64 : T < 2 ^ 64)
    (hv : st.validators[index]? = some v)
    (hmul : v.effective_balance * cfg.BASE_REWARD_FACTOR < 2 ^ 64)
    (hbpe : cfg.BASE_REWARDS_PER_EPOCH ≠ 0) :
    get_base_reward_phase0 cfg st index =
      .ok (v.effective_balance * cfg.BASE_REWARD_FACTOR / Nat.sqrt T /
        cfg.BASE_REWARDS_PER_EPOCH) := by
  have hsq := sqrt_pos T hT
  have hsq0 : Nat.sqrt T ≠ 0 := by omega
  unfold get_base_reward_phase0
  rw [htotal]
  simp only [getValidator, hv, uint64Mul, UINT64_SIZE, uint64Div, Bind.bind, Except.bind,
    Pure.pure, Except.pure]
  simp only [hmul, if_true, integer_squareroot_eq T hT64, hsq0, hbpe, if_false]

theorem spec_get_base_reward_altair_eq (cfg : Config) (st : BeaconState) (index : Nat)
    (T : Nat) (v : Validator)
    (htotal : get_total_active_balance cfg st = .ok T) (hT : 1 ≤ T) (hT64 : T < 2 ^ 64)
    (hv : st.validators[index]? = some v)
    (hinc : cfg.EFFECTIVE_BALANCE_INCREMENT ≠ 0)
    (hmul1 : cfg.EFFECTIVE_BALANCE_INCREMENT * cfg.BASE_REWARD_FACTOR < 2 ^ 64)
    (hmul2 : v.effective_balance / cfg.EFFECTIVE_BALANCE_INCREMENT *
      (cfg.EFFECTIVE_BALANCE_INCREMENT * cfg.BASE_REWARD_FACTOR / Nat.sqrt T) < 2 ^ 64) :
    get_base_reward_altair cfg st index =
      .ok (v.effective_balance / cfg.EFFECTIVE_BALANCE_INCREMENT *
        (cfg.EFFECTIVE_BALANCE_INCREMENT * cfg.BASE_REWARD_FACTOR / Nat.sqrt T)) := by
  have hsq := sqrt_pos T hT
  have hsq0 : Nat.sqrt T ≠ 0 := by omega
  unfold get_base_reward_altair get_base_reward_per_increment
  rw [htotal]
  simp only [getValidator, hv, uint64Mul, UINT64_SIZE, uint64Div, Bind.bind, Except.bind,
    Pure.pure, Except.pure]
  simp only [hmul1, hmul2, if_true, integer_squareroot_eq T hT64, hsq0, hinc, if_false]

/-! ## Read agreement, end to end -/

/-- Read agreement for `EpochCache::get_base_reward`. Build the table with `base_rewards` from a
total whose floor is the spec total active balance. Then for every validator whose effective
balance is a multiple of the increment and at most `max_effective_balance`, the cached read
returns `ok`, and it equals the spec `get_base_reward` (phase0 or altair). -/
theorem get_base_reward_equiv (cfg : Config) (vs : List Validator) (epoch : Nat)
    (total inc maxEb factor bpe : U64) (phase0 : Bool)
    (hinc : inc.val = cfg.EFFECTIVE_BALANCE_INCREMENT)
    (hfactor : factor.val = cfg.BASE_REWARD_FACTOR)
    (hbpeq : bpe.val = cfg.BASE_REWARDS_PER_EPOCH)
    (hinc1 : 1 ≤ inc.val) (hbpe1 : 1 ≤ bpe.val)
    (hincf : inc.val * factor.val ≤ U64.max) (hfit : maxEb.val * factor.val ≤ U64.max)
    (hcap : maxEb.val / inc.val < 4294967295)
    (htotal : get_total_active_balance cfg ⟨vs, epoch⟩ = .ok (max inc.val total.val))
    (ebs : Slice U64) (hebs : ebs.val.map (·.val) = vs.map (·.effective_balance))
    (index : Usize) (v : Validator) (hv : vs[index.val]? = some v)
    (hmult : v.effective_balance % inc.val = 0) (hle : v.effective_balance ≤ maxEb.val) :
    ∃ tbl, state.base_rewards.base_rewards total inc maxEb factor bpe phase0 =
        ok (.Ok tbl) ∧
      ∃ r, state.base_rewards.get_base_reward ebs (alloc.vec.Vec.deref tbl) inc
          index = ok (.Ok r) ∧
        (if phase0 then get_base_reward_phase0 cfg ⟨vs, epoch⟩ index.val
          else get_base_reward_altair cfg ⟨vs, epoch⟩ index.val) = .ok r.val := by
  obtain ⟨tbl, htbl, htblv⟩ := base_rewards_spec total inc maxEb factor bpe phase0 hinc1 hbpe1
    hincf hfit hcap
  refine ⟨tbl, htbl, ?_⟩
  have hlenv : index.val < vs.length := by
    rcases Nat.lt_or_ge index.val vs.length with h | h
    · exact h
    · rw [List.getElem?_eq_none h] at hv; cases hv
  have hlen : ebs.val.length = vs.length := by
    have := congrArg List.length hebs; simpa using this
  have hindex : index.val < ebs.val.length := by omega
  have hvget : vs[index.val] = v := by
    rw [List.getElem?_eq_getElem hlenv] at hv; exact Option.some.inj hv
  have hebv : ebs.val[index.val].val = v.effective_balance := by
    have := congrArg (·[index.val]?) hebs
    simp only [List.getElem?_map, List.getElem?_eq_getElem hindex,
      List.getElem?_eq_getElem hlenv, Option.map_some, hvget] at this
    exact Option.some.inj this
  obtain ⟨m, hm⟩ : ∃ m : Nat, m = maxEb.val / inc.val := ⟨_, rfl⟩
  obtain ⟨eth, heth_def⟩ : ∃ e : Nat, e = v.effective_balance / inc.val := ⟨_, rfl⟩
  have heth : eth ≤ m := by rw [hm, heth_def]; exact Nat.div_le_div_right hle
  rw [← hm] at htblv hcap
  have htlen : tbl.val.length = m + 1 := by
    have := congrArg List.length htblv; simpa using this
  have hethlt : eth < tbl.val.length := by omega
  obtain ⟨r, hr⟩ : ∃ r, r = tbl.val[eth]'hethlt := ⟨_, rfl⟩
  have hrv : r.val = tableEntry phase0 inc.val (sqrtTotal inc.val total.val)
      (perIncrement inc.val factor.val total.val) factor.val bpe.val eth := by
    have := congrArg (·[eth]?) htblv
    simp only [List.getElem?_map, List.getElem?_eq_getElem hethlt,
      List.getElem?_range (show eth < m + 1 by omega), Option.map_some] at this
    rw [hr]; exact Option.some.inj this
  refine ⟨r, ?_, ?_⟩
  · rw [get_base_reward_spec ebs (alloc.vec.Vec.deref tbl) inc index hindex (by omega)
      (by simp only [alloc.vec.Vec.deref, hebv, ← heth_def]; omega)
      (by rw [hebv, ← heth_def]; omega)]
    simp only [alloc.vec.Vec.deref, hebv, ← heth_def, hr]
  · have hT : 1 ≤ max inc.val total.val := by omega
    have hT64 : max inc.val total.val < 2 ^ 64 := by
      have := inc.hBounds; have := total.hBounds; simp at *; omega
    have hmulti : eth * inc.val = v.effective_balance := by
      rw [heth_def]; exact Nat.div_mul_cancel (Nat.dvd_of_mod_eq_zero hmult)
    cases phase0 with
    | true =>
      have hmul : v.effective_balance * cfg.BASE_REWARD_FACTOR < 2 ^ 64 := by
        rw [← hfactor]
        exact lt_of_le_of_lt (le_trans (Nat.mul_le_mul_right _ hle) hfit) u64_max_lt
      have hbpe0 : cfg.BASE_REWARDS_PER_EPOCH ≠ 0 := by
        rw [← hbpeq]; exact Nat.pos_iff_ne_zero.mp hbpe1
      rw [if_pos rfl, spec_get_base_reward_phase0_eq cfg ⟨vs, epoch⟩ index.val _ v htotal hT hT64
        hv hmul hbpe0, hrv]
      simp only [tableEntry, if_true, hmulti, sqrtTotal, flooredTotal, hfactor, hbpeq]
    | false =>
      have hbrpi_le : perIncrement inc.val factor.val total.val ≤ inc.val * factor.val :=
        Nat.div_le_self _ _
      have hmul2 : eth * perIncrement inc.val factor.val total.val < 2 ^ 64 := by
        have h1 := Nat.mul_le_mul_left eth hbrpi_le
        have h2 : eth * (inc.val * factor.val) = v.effective_balance * factor.val := by
          rw [← Nat.mul_assoc, hmulti]
        have h3 := Nat.mul_le_mul_right factor.val hle
        simp [u64_max_val] at hfit; omega
      have hinc0 : cfg.EFFECTIVE_BALANCE_INCREMENT ≠ 0 := by
        rw [← hinc]; exact Nat.pos_iff_ne_zero.mp hinc1
      have hincf2 : cfg.EFFECTIVE_BALANCE_INCREMENT * cfg.BASE_REWARD_FACTOR < 2 ^ 64 := by
        rw [← hinc, ← hfactor]; exact lt_of_le_of_lt hincf u64_max_lt
      have hmul2' : v.effective_balance / cfg.EFFECTIVE_BALANCE_INCREMENT *
          (cfg.EFFECTIVE_BALANCE_INCREMENT * cfg.BASE_REWARD_FACTOR /
            Nat.sqrt (max inc.val total.val)) < 2 ^ 64 := by
        rw [← hinc, ← hfactor, ← heth_def]; exact hmul2
      rw [if_neg (by simp), spec_get_base_reward_altair_eq cfg ⟨vs, epoch⟩ index.val _ v htotal hT
        hT64 hv hinc0 hincf2 hmul2', hrv]
      simp only [tableEntry, Bool.false_eq_true, if_false, perIncrement, sqrtTotal, flooredTotal]
      rw [Nat.mul_div_cancel _ (by omega), heth_def, hinc, hfactor]

/-- No false error for `EpochCache::get_effective_balance`: every index below the number of
cached validators returns `ok`. -/
theorem get_effective_balance_no_false_error (ebs : Slice U64) (index : Usize)
    (h : index.val < ebs.val.length) :
    state.base_rewards.get_effective_balance ebs index =
      ok (.Ok (ebs.val[index.val]'h)) := by
  rw [get_effective_balance_spec, List.getElem?_eq_getElem h]

/-! ## Mainnet values -/

/-- The hypotheses of `base_rewards_spec` and `get_base_reward_equiv` hold on mainnet, for the
phase0 limit (32 ETH) and the Electra limit (2048 ETH). -/
theorem mainnet_hypotheses (maxEb : Nat) (h : maxEb = 32000000000 ∨ maxEb = 2048000000000) :
    let inc := 1000000000
    let factor := 64
    1 ≤ inc ∧ inc * factor ≤ 18446744073709551615 ∧ maxEb * factor ≤ 18446744073709551615 ∧
      maxEb / inc < 4294967295 := by
  rcases h with h | h <;> subst h <;> norm_num

/-! ## The floor is required (#9106) -/

/-- Without the floor, a zero total gives `integer_sqrt(0) = 0`, and the base reward per
increment fails with a division by zero. -/
theorem no_floor_division_by_zero (inc factor : U64) (h : inc.val * factor.val ≤ U64.max) :
    state.base_rewards.base_reward_per_increment 0#u64 inc factor =
      ok (.Err .DivisionByZero) := by
  unfold state.base_rewards.base_reward_per_increment
  obtain ⟨a, ha, _⟩ := safe_mul_ok inc factor h
  obtain ⟨sq, hsq, hsqv⟩ := integer_sqrt_spec 0#u64
  have hsq0 : sq.val = 0 := by rw [hsqv]; simp
  simp only [ha, bind_tc_ok, branch_ok, hsq, safe_div_zero a sq hsq0]

/-- The spec does not fail on the same state: with no active validator, the total active
balance is `EFFECTIVE_BALANCE_INCREMENT`, and `get_base_reward_per_increment` returns `ok`. -/
theorem spec_no_active_validators (cfg : Config) (epoch : Nat)
    (hinc : 1 ≤ cfg.EFFECTIVE_BALANCE_INCREMENT) (hinc64 : cfg.EFFECTIVE_BALANCE_INCREMENT < 2 ^ 64)
    (hmul : cfg.EFFECTIVE_BALANCE_INCREMENT * cfg.BASE_REWARD_FACTOR < 2 ^ 64) :
    activeSum (activeAt [] epoch) [] = 0 ∧
    get_total_active_balance cfg ⟨[], epoch⟩ = .ok cfg.EFFECTIVE_BALANCE_INCREMENT ∧
    get_base_reward_per_increment cfg ⟨[], epoch⟩ =
      .ok (cfg.EFFECTIVE_BALANCE_INCREMENT * cfg.BASE_REWARD_FACTOR /
        Nat.sqrt cfg.EFFECTIVE_BALANCE_INCREMENT) := by
  have htotal : get_total_active_balance cfg ⟨[], epoch⟩ = .ok cfg.EFFECTIVE_BALANCE_INCREMENT := by
    have := get_total_active_balance_eq cfg [] epoch (by simp [activeSum, activeSumFrom])
    simpa [activeSum, activeSumFrom] using this
  have hsq0 : Nat.sqrt cfg.EFFECTIVE_BALANCE_INCREMENT ≠ 0 := by
    have := sqrt_pos _ hinc; omega
  refine ⟨by simp [activeSum, activeSumFrom], htotal, ?_⟩
  unfold get_base_reward_per_increment
  rw [htotal]
  simp only [uint64Mul, UINT64_SIZE, uint64Div, Bind.bind, Except.bind, Pure.pure, Except.pure]
  simp only [hmul, if_true, integer_squareroot_eq _ hinc64, hsq0, if_false]

/-- The theorem needs the floor. With the raw total of an empty active set, the unfloored
computation fails where the spec succeeds. With the floor, `base_rewards` succeeds. -/
theorem floor_required (cfg : Config) (epoch : Nat) (inc factor maxEb bpe : U64)
    (hinc : inc.val = cfg.EFFECTIVE_BALANCE_INCREMENT)
    (hfactor : factor.val = cfg.BASE_REWARD_FACTOR) (hinc1 : 1 ≤ inc.val) (hbpe1 : 1 ≤ bpe.val)
    (hincf : inc.val * factor.val ≤ U64.max) (hfit : maxEb.val * factor.val ≤ U64.max)
    (hcap : maxEb.val / inc.val < 4294967295) (phase0 : Bool) :
    (∃ r, get_base_reward_per_increment cfg ⟨[], epoch⟩ = .ok r) ∧
    state.base_rewards.base_reward_per_increment 0#u64 inc factor =
      ok (.Err .DivisionByZero) ∧
    ∃ tbl, state.base_rewards.base_rewards 0#u64 inc maxEb factor bpe phase0 =
      ok (.Ok tbl) := by
  have hinc64 : cfg.EFFECTIVE_BALANCE_INCREMENT < 2 ^ 64 := by
    rw [← hinc]; have := inc.hBounds; simpa using this
  have hmul : cfg.EFFECTIVE_BALANCE_INCREMENT * cfg.BASE_REWARD_FACTOR < 2 ^ 64 := by
    rw [← hinc, ← hfactor]; exact lt_of_le_of_lt hincf u64_max_lt
  obtain ⟨_, _, hspec⟩ := spec_no_active_validators cfg epoch (hinc ▸ hinc1) hinc64 hmul
  obtain ⟨tbl, htbl, _⟩ := base_rewards_spec 0#u64 inc maxEb factor bpe phase0 hinc1 hbpe1 hincf
    hfit hcap
  exact ⟨⟨_, hspec⟩, no_floor_division_by_zero inc factor hincf, tbl, htbl⟩

end CacheProofs.EpochCache
