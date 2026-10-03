import CacheProofs.Generated
import CacheProofs.Spec.ProgressiveBalances

/-!
# `ProgressiveBalancesCache` is coherent with the spec

For each epoch and flag, the cache holds the raw sum of `effective_balance` over the unslashed
validators that are active in that epoch and have the flag (`flagSum`). `Balance::get` applies
the `EFFECTIVE_BALANCE_INCREMENT` floor. `Coherent` states this for both epochs.

- Pure function lemmas (`*_ok`): each function in `../src/state/{balance,participation_totals}.rs` returns `Ok`
  and changes the raw totals as stated.
- `spec_total_eq`: `get_total_balance(get_unslashed_participating_indices(...))` is
  `max EFFECTIVE_BALANCE_INCREMENT flagSum`.
- Build: `build_coherent`. Events: `attestation_coherent`, `slashing_coherent`,
  `effective_balance_change_coherent`, `epoch_transition_coherent`, `registry_change_coherent`,
  `add_validator_coherent`. Reads: `read_current_eq_spec`, `read_previous_eq_spec`.

Each `lh*` definition models Rust glue that Aeneas does not translate. It is written by hand.
-/

namespace CacheProofs.PB

open Aeneas Aeneas.Std Result types
open state.participation_totals state.balance

abbrev Bal := state.balance.Balance
abbrev KErr := state.participation_totals.Error

/-- The `(raw, minimum)` pair of each `Balance`. -/
def ab (l : List Bal) : List (Nat × Nat) := l.map fun b => (b.raw.val, b.minimum.val)

theorem safe_add_assign_ok (b : Bal) (x : U64) (h : b.raw.val + x.val ≤ U64.max) :
    ∃ b', Balance.safe_add_assign b x = ok (.Ok (), b') ∧
      b'.raw.val = b.raw.val + x.val ∧ b'.minimum = b.minimum := by
  unfold Balance.safe_add_assign
  have hc := U64.checked_add_bv_spec b.raw x
  cases hc' : U64.checked_add b.raw x with
  | none => simp only [hc'] at hc; omega
  | some v =>
    simp only [hc'] at hc
    refine ⟨{ b with raw := v }, ?_, hc.2.1, rfl⟩
    simp [U64.Insts.Safe_arithSafeArithU64.safe_add, hc', lift,
      core.option.Option.ok_or, core.result.Result.Insts.CoreOpsTry.branch]


theorem safe_sub_assign_ok (b : Bal) (x : U64) (h : x.val ≤ b.raw.val) :
    ∃ b', Balance.safe_sub_assign b x = ok (.Ok (), b') ∧
      b'.raw.val = b.raw.val - x.val ∧ b'.minimum = b.minimum := by
  unfold Balance.safe_sub_assign
  have hc := U64.checked_sub_bv_spec b.raw x
  cases hc' : U64.checked_sub b.raw x with
  | none => simp only [hc'] at hc; omega
  | some v =>
    simp only [hc'] at hc
    refine ⟨{ b with raw := v }, ?_, hc.2.1, rfl⟩
    simp [U64.Insts.Safe_arithSafeArithU64.safe_sub, hc', lift,
      core.option.Option.ok_or, core.result.Result.Insts.CoreOpsTry.branch]

theorem u64_safe_sub_ok (x y : U64) (h : y.val ≤ x.val) :
    ∃ z : U64, U64.Insts.Safe_arithSafeArithU64.safe_sub x y = ok (.Ok z) ∧
      z.val = x.val - y.val := by
  have hc := U64.checked_sub_bv_spec x y
  cases hc' : U64.checked_sub x y with
  | none => simp only [hc'] at hc; omega
  | some v =>
    simp only [hc'] at hc
    exact ⟨v, by simp [U64.Insts.Safe_arithSafeArithU64.safe_sub, hc', lift,
      core.option.Option.ok_or], hc.2.1⟩

theorem decide_and_eq (flags m : U8) (k : Nat) (hm : m.val = 2 ^ k) :
    decide (flags &&& m = m) = Spec.has_flag flags.val k := by
  have hiff : (flags &&& m = m) ↔ (flags.val &&& m.val = m.val) :=
    ⟨fun h => by rw [← UScalar.val_and, h],
     fun h => UScalar.eq_of_val_eq (by rw [UScalar.val_and]; exact h)⟩
  simp only [Spec.has_flag, ← hm, hiff]
  exact (beq_eq_decide _ _).symm

theorem has_flag_ok (flags : U8) (i : Usize) (hi : i.val < 3) :
    has_flag flags i = ok (.Ok (Spec.has_flag flags.val i.val)) := by
  unfold has_flag
  have hnum : core.consts.altair.NUM_FLAG_INDICES = 3#usize := by
    unfold core.consts.altair.NUM_FLAG_INDICES; rfl
  have hlt : ¬ (i ≥ core.consts.altair.NUM_FLAG_INDICES) := by
    rw [hnum]; scalar_tac
  simp only [hlt, if_false]
  have hcases : i.val = 0 ∨ i.val = 1 ∨ i.val = 2 := by omega
  rcases hcases with h | h | h
  · simp only [h, lift, bind_tc_ok]; rw [decide_and_eq _ _ 0 (by simp)]
  · simp only [h, lift, bind_tc_ok]; rw [decide_and_eq _ _ 1 (by simp)]
  · simp only [h, lift, bind_tc_ok]; rw [decide_and_eq _ _ 2 (by simp)]

theorem set_opt_some {α : Type} (l : List α) (i : Nat) (x : α) :
    l.set_opt i (some x) = l.set i x := by
  induction l generalizing i with
  | nil => simp [List.set_opt]
  | cons h t ih => cases i <;> simp [List.set_opt, ih]

theorem ab_set (l : List Bal) (i : Nat) (b : Bal) :
    ab (l.set i b) = (ab l).set i (b.raw.val, b.minimum.val) := by
  simp [ab, List.map_set]

theorem add_to_flag_ok (s : Slice Bal) (i : Usize) (x : U64) (b : Bal)
    (hb : s.val[i.val]? = some b) (hfit : b.raw.val + x.val ≤ U64.max) :
    ∃ s', add_to_flag s i x = ok (.Ok (), s') ∧
      ab s'.val = (ab s.val).set i.val (b.raw.val + x.val, b.minimum.val) := by
  obtain ⟨b', hb', hraw, hmin⟩ := safe_add_assign_ok b x hfit
  unfold add_to_flag
  refine ⟨s.set_opt i (some b'), ?_, ?_⟩
  · simp [core.slice.Slice.get_mut, Slice.getElem?_Usize_eq, hb, hb',
      core.result.Result.Insts.CoreOpsTry.branch]
  · rw [Slice.set_opt_val_eq, set_opt_some, ab_set, hraw, hmin]

theorem sub_from_flag_ok (s : Slice Bal) (i : Usize) (x : U64) (b : Bal)
    (hb : s.val[i.val]? = some b) (hfit : x.val ≤ b.raw.val) :
    ∃ s', sub_from_flag s i x = ok (.Ok (), s') ∧
      ab s'.val = (ab s.val).set i.val (b.raw.val - x.val, b.minimum.val) := by
  obtain ⟨b', hb', hraw, hmin⟩ := safe_sub_assign_ok b x hfit
  unfold sub_from_flag
  refine ⟨s.set_opt i (some b'), ?_, ?_⟩
  · simp [core.slice.Slice.get_mut, Slice.getElem?_Usize_eq, hb, hb',
      core.result.Result.Insts.CoreOpsTry.branch]
  · rw [Slice.set_opt_val_eq, set_opt_some, ab_set, hraw, hmin]


theorem ab_getElem? (l : List Bal) (i : Nat) (r m : Nat) (h : (ab l)[i]? = some (r, m)) :
    ∃ b, l[i]? = some b ∧ b.raw.val = r ∧ b.minimum.val = m := by
  simp only [ab, List.getElem?_map] at h
  cases hb : l[i]? with
  | none => simp [hb] at h
  | some b =>
    simp only [hb, Option.map_some, Option.some.injEq, Prod.mk.injEq] at h
    exact ⟨b, rfl, h.1, h.2⟩

theorem set_self {α : Type} (l : List α) (i : Nat) (a : α) (h : l[i]? = some a) :
    l.set i a = l := by
  have hi : i < l.length := by
    by_contra hc; rw [List.getElem?_eq_none (by omega)] at h; cases h
  have ha : l[i] = a := by rw [List.getElem?_eq_getElem hi] at h; exact Option.some.inj h
  rw [← ha, List.set_getElem_self]

theorem add_if_flag_ok (s : Slice Bal) (flags : U8) (i : Usize) (x : U64) (r m : Nat)
    (hi : i.val < 3) (hb : (ab s.val)[i.val]? = some (r, m))
    (hfit : Spec.has_flag flags.val i.val → r + x.val ≤ U64.max) :
    ∃ s', add_if_flag s flags i x = ok (.Ok (), s') ∧
      ab s'.val = (ab s.val).set i.val
        (if Spec.has_flag flags.val i.val then r + x.val else r, m) := by
  obtain ⟨b, hb', hr, hm⟩ := ab_getElem? _ _ _ _ hb
  unfold add_if_flag
  rw [has_flag_ok flags i hi]
  by_cases hf : Spec.has_flag flags.val i.val
  · obtain ⟨s', hs, habs⟩ := add_to_flag_ok s i x b hb' (by rw [hr]; exact hfit hf)
    refine ⟨s', ?_, ?_⟩
    · simp [hf, hs, core.result.Result.Insts.CoreOpsTry.branch]
    · rw [habs, hr, hm]; simp [hf]
  · refine ⟨s, ?_, ?_⟩
    · simp [hf, core.result.Result.Insts.CoreOpsTry.branch]
    · simp only [hf, if_false, Bool.false_eq_true]; rw [set_self _ _ _ hb]

theorem sub_if_flag_ok (s : Slice Bal) (flags : U8) (i : Usize) (x : U64) (r m : Nat)
    (hi : i.val < 3) (hb : (ab s.val)[i.val]? = some (r, m))
    (hfit : Spec.has_flag flags.val i.val → x.val ≤ r) :
    ∃ s', sub_if_flag s flags i x = ok (.Ok (), s') ∧
      ab s'.val = (ab s.val).set i.val
        (if Spec.has_flag flags.val i.val then r - x.val else r, m) := by
  obtain ⟨b, hb', hr, hm⟩ := ab_getElem? _ _ _ _ hb
  unfold sub_if_flag
  rw [has_flag_ok flags i hi]
  by_cases hf : Spec.has_flag flags.val i.val
  · obtain ⟨s', hs, habs⟩ := sub_from_flag_ok s i x b hb' (by rw [hr]; exact hfit hf)
    refine ⟨s', ?_, ?_⟩
    · simp [hf, hs, core.result.Result.Insts.CoreOpsTry.branch]
    · rw [habs, hr, hm]; simp [hf]
  · refine ⟨s, ?_, ?_⟩
    · simp [hf, core.result.Result.Insts.CoreOpsTry.branch]
    · simp only [hf, if_false, Bool.false_eq_true]; rw [set_self _ _ _ hb]

/-- The new raw total of one flag after an effective balance change from `o` to `n`. -/
def moved (r o n : Nat) : Nat := if o < n then r + (n - o) else r - (o - n)

theorem change_if_flag_ok (s : Slice Bal) (flags : U8) (i : Usize) (o n : U64) (r m : Nat)
    (hi : i.val < 3) (hb : (ab s.val)[i.val]? = some (r, m))
    (hfit : Spec.has_flag flags.val i.val →
      (o.val < n.val → r + (n.val - o.val) ≤ U64.max) ∧ (n.val ≤ o.val → o.val - n.val ≤ r)) :
    ∃ s', change_if_flag s flags i o n = ok (.Ok (), s') ∧
      ab s'.val = (ab s.val).set i.val
        (if Spec.has_flag flags.val i.val then moved r o.val n.val else r, m) := by
  obtain ⟨b, hb', hr, hm⟩ := ab_getElem? _ _ _ _ hb
  unfold change_if_flag
  rw [has_flag_ok flags i hi]
  by_cases hf : Spec.has_flag flags.val i.val
  · by_cases hgt : n > o
    · have hgt' : o.val < n.val := by scalar_tac
      obtain ⟨d, hd, hdv⟩ := u64_safe_sub_ok n o (by omega)
      obtain ⟨s', hs, habs⟩ := add_to_flag_ok s i d b hb'
        (by rw [hr, hdv]; exact (hfit hf).1 hgt')
      refine ⟨s', ?_, ?_⟩
      · simp [hf, hgt, hd, hs, core.result.Result.Insts.CoreOpsTry.branch]
      · rw [habs, hr, hm, hdv]; simp [hf, moved, hgt']
    · have hle : n.val ≤ o.val := by scalar_tac
      obtain ⟨d, hd, hdv⟩ := u64_safe_sub_ok o n hle
      obtain ⟨s', hs, habs⟩ := sub_from_flag_ok s i d b hb'
        (by rw [hr, hdv]; exact (hfit hf).2 hle)
      refine ⟨s', ?_, ?_⟩
      · simp [hf, hgt, hd, hs, core.result.Result.Insts.CoreOpsTry.branch]
      · rw [habs, hr, hm, hdv]; simp [hf, moved, show ¬ o.val < n.val by omega]
  · refine ⟨s, ?_, ?_⟩
    · simp [hf, core.result.Result.Insts.CoreOpsTry.branch]
    · simp only [hf, if_false, Bool.false_eq_true]; rw [set_self _ _ _ hb]


/-- Three flag totals with the same minimum, as `ab` shows them. -/
def tri (R : Nat → Nat) (m : Nat) : List (Nat × Nat) := [(R 0, m), (R 1, m), (R 2, m)]

theorem ab_length (l : List Bal) : (ab l).length = l.length := by simp [ab]

theorem slice_of_ab (t : Array Bal 3#usize) (s : Slice Bal) (R : Nat → Nat) (m : Nat)
    (h : ab s.val = tri R m) :
    ab (Array.to_slice (Array.from_slice t s)).val = tri R m := by
  have hl : s.val.length = 3 := by rw [← ab_length, h]; rfl
  simp [Array.to_slice, Array.from_slice, hl, h]

/-- The shape that `add_flags`, `on_slashing` and `on_effective_balance_change` share: one
`step` for each flag index 0, 1 and 2, stopping at the first error. -/
def unroll3 (step : Slice Bal → Usize → Result (core.result.Result Unit KErr × Slice Bal))
    (totals : Array Bal 3#usize) : Result (core.result.Result Unit KErr × Array Bal 3#usize) := do
  let (s, to_slice_mut_back) ← lift (Array.to_slice_mut totals)
  let (r, s1) ← step s 0#usize
  let cf ← core.result.Result.Insts.CoreOpsTry.branch r
  match cf with
  | core.ops.control_flow.ControlFlow.Continue _ =>
    let totals1 := to_slice_mut_back s1
    let (s2, to_slice_mut_back1) ← lift (Array.to_slice_mut totals1)
    let (r1, s3) ← step s2 1#usize
    let cf1 ← core.result.Result.Insts.CoreOpsTry.branch r1
    match cf1 with
    | core.ops.control_flow.ControlFlow.Continue _ =>
      let totals2 := to_slice_mut_back1 s3
      let (s4, to_slice_mut_back2) ← lift (Array.to_slice_mut totals2)
      let (r2, s5) ← step s4 2#usize
      let cf2 ← core.result.Result.Insts.CoreOpsTry.branch r2
      match cf2 with
      | core.ops.control_flow.ControlFlow.Continue _ =>
        let totals3 := to_slice_mut_back2 s5
        ok (core.result.Result.Ok (), totals3)
      | core.ops.control_flow.ControlFlow.Break residual =>
        let r3 ←
          core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual
            Unit (core.convert.FromSame KErr) residual
        let totals3 := to_slice_mut_back2 s5
        ok (r3, totals3)
    | core.ops.control_flow.ControlFlow.Break residual =>
      let r2 ←
        core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual
          Unit (core.convert.FromSame KErr) residual
      let totals2 := to_slice_mut_back1 s3
      ok (r2, totals2)
  | core.ops.control_flow.ControlFlow.Break residual =>
    let r1 ←
      core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual
        Unit (core.convert.FromSame KErr) residual
    let totals1 := to_slice_mut_back s1
    ok (r1, totals1)

theorem unroll3_ok (step : Slice Bal → Usize → Result (core.result.Result Unit KErr × Slice Bal))
    (P : Nat → Nat → Prop) (G : Nat → Nat → Nat)
    (hstep : ∀ (s : Slice Bal) (i : Usize) (r m : Nat), i.val < 3 →
      (ab s.val)[i.val]? = some (r, m) → P i.val r →
      ∃ s', step s i = ok (.Ok (), s') ∧ ab s'.val = (ab s.val).set i.val (G i.val r, m))
    (t : Array Bal 3#usize) (R : Nat → Nat) (m : Nat) (ht : ab t.val = tri R m)
    (hP : ∀ f < 3, P f (R f)) :
    ∃ t', unroll3 step t = ok (.Ok (), t') ∧ ab t'.val = tri (fun f => G f (R f)) m := by
  unfold unroll3
  simp only [Array.to_slice_mut, lift, bind_tc_ok]
  have h0 : ab (Array.to_slice t).val = tri R m := by simp [Array.to_slice, ht]
  obtain ⟨s1, e1, a1⟩ := hstep (Array.to_slice t) 0#usize (R 0) m (by simp)
    (by rw [h0]; rfl) (hP 0 (by omega))
  rw [h0] at a1
  have h1 : ab (Array.to_slice (Array.from_slice t s1)).val =
      tri (fun f => if f = 0 then G 0 (R 0) else R f) m := by
    apply slice_of_ab; rw [a1]; simp [tri]
  obtain ⟨s2, e2, a2⟩ := hstep _ 1#usize (R 1) m (by simp)
    (by rw [h1]; rfl) (hP 1 (by omega))
  rw [h1] at a2
  have h2 : ab (Array.to_slice (Array.from_slice (Array.from_slice t s1) s2)).val =
      tri (fun f => if f = 0 then G 0 (R 0) else if f = 1 then G 1 (R 1) else R f) m := by
    apply slice_of_ab; rw [a2]; simp [tri]
  obtain ⟨s3, e3, a3⟩ := hstep _ 2#usize (R 2) m (by simp)
    (by rw [h2]; rfl) (hP 2 (by omega))
  rw [h2] at a3
  simp [e1, e2, e3, core.result.Result.Insts.CoreOpsTry.branch]
  have := slice_of_ab (Array.from_slice (Array.from_slice t s1) s2) s3
    (fun f => G f (R f)) m (by rw [a3]; simp [tri])
  simpa [Array.to_slice] using this

theorem add_flags_ok (t : Array Bal 3#usize) (flags : U8) (x : U64) (R : Nat → Nat) (m : Nat)
    (ht : ab t.val = tri R m)
    (hfit : ∀ f < 3, Spec.has_flag flags.val f → R f + x.val ≤ U64.max) :
    ∃ t', add_flags t flags x = ok (.Ok (), t') ∧
      ab t'.val = tri (fun f => if Spec.has_flag flags.val f then R f + x.val else R f) m := by
  have he : add_flags t flags x = unroll3 (fun s i => add_if_flag s flags i x) t := rfl
  rw [he]
  exact unroll3_ok _ (fun f r => Spec.has_flag flags.val f → r + x.val ≤ U64.max)
    (fun f r => if Spec.has_flag flags.val f then r + x.val else r)
    (fun s i r m hi hb hp => add_if_flag_ok s flags i x r m hi hb hp) t R m ht hfit

theorem on_slashing_ok (t : Array Bal 3#usize) (flags : U8) (x : U64) (R : Nat → Nat) (m : Nat)
    (ht : ab t.val = tri R m)
    (hfit : ∀ f < 3, Spec.has_flag flags.val f → x.val ≤ R f) :
    ∃ t', on_slashing t flags x = ok (.Ok (), t') ∧
      ab t'.val = tri (fun f => if Spec.has_flag flags.val f then R f - x.val else R f) m := by
  have he : on_slashing t flags x = unroll3 (fun s i => sub_if_flag s flags i x) t := rfl
  rw [he]
  exact unroll3_ok _ (fun f r => Spec.has_flag flags.val f → x.val ≤ r)
    (fun f r => if Spec.has_flag flags.val f then r - x.val else r)
    (fun s i r m hi hb hp => sub_if_flag_ok s flags i x r m hi hb hp) t R m ht hfit

theorem on_effective_balance_change_ok (t : Array Bal 3#usize) (is_slashed : Bool) (flags : U8)
    (o n : U64) (R : Nat → Nat) (m : Nat) (ht : ab t.val = tri R m)
    (hfit : is_slashed = false → ∀ f < 3, Spec.has_flag flags.val f →
      (o.val < n.val → R f + (n.val - o.val) ≤ U64.max) ∧ (n.val ≤ o.val → o.val - n.val ≤ R f)) :
    ∃ t', on_effective_balance_change t is_slashed flags o n = ok (.Ok (), t') ∧
      ab t'.val = if is_slashed then tri R m else
        tri (fun f => if Spec.has_flag flags.val f then moved (R f) o.val n.val else R f) m := by
  unfold on_effective_balance_change
  cases is_slashed
  · have he := unroll3_ok (fun s i => change_if_flag s flags i o n)
      (fun f r => Spec.has_flag flags.val f →
        (o.val < n.val → r + (n.val - o.val) ≤ U64.max) ∧ (n.val ≤ o.val → o.val - n.val ≤ r))
      (fun f r => if Spec.has_flag flags.val f then moved r o.val n.val else r)
      (fun s i r m hi hb hp => change_if_flag_ok s flags i o n r m hi hb hp) t R m ht (hfit rfl)
    simp only [Bool.false_eq_true, if_false]
    exact he
  · exact ⟨t, rfl, by simpa using ht⟩

theorem on_new_attestation_ok (t : Array Bal 3#usize) (is_slashed : Bool) (f : Usize) (x : U64)
    (R : Nat → Nat) (m : Nat) (ht : ab t.val = tri R m) (hf : f.val < 3)
    (hfit : is_slashed = false → R f.val + x.val ≤ U64.max) :
    ∃ t', on_new_attestation t is_slashed f x = ok (.Ok (), t') ∧
      ab t'.val = if is_slashed then tri R m else
        tri (fun g => if g = f.val then R g + x.val else R g) m := by
  unfold on_new_attestation
  cases is_slashed
  · have h0 : (ab (Array.to_slice t).val)[f.val]? = some (R f.val, m) := by
      simp only [Array.to_slice, ht, tri]
      have : f.val = 0 ∨ f.val = 1 ∨ f.val = 2 := by omega
      rcases this with h | h | h <;> simp [h]
    obtain ⟨b, hb, hr, hm⟩ := ab_getElem? _ _ _ _ h0
    obtain ⟨s', e, a⟩ := add_to_flag_ok (Array.to_slice t) f x b hb (by rw [hr]; exact hfit rfl)
    simp only [lift, Array.to_slice_mut, bind_tc_ok, Bool.false_eq_true, if_false]
    simp [e]
    have h0' : ab (Array.to_slice t).val = tri R m := by simp [Array.to_slice, ht]
    rw [h0', hr, hm] at a
    have := slice_of_ab t s' (fun g => if g = f.val then R g + x.val else R g) m (by
      rw [a]
      have : f.val = 0 ∨ f.val = 1 ∨ f.val = 2 := by omega
      rcases this with h | h | h <;> simp [h, tri])
    simpa [Array.to_slice] using this
  · exact ⟨t, rfl, by simpa using ht⟩

theorem total_flag_balance_ok (t : Array Bal 3#usize) (f : Usize) (R : Nat → Nat) (m : Nat)
    (ht : ab t.val = tri R m) (hf : f.val < 3) :
    ∃ v : U64, total_flag_balance (Array.to_slice t) f = ok (.Ok v) ∧ v.val = max (R f.val) m := by
  have h0 : (ab (Array.to_slice t).val)[f.val]? = some (R f.val, m) := by
    simp only [Array.to_slice, ht, tri]
    have : f.val = 0 ∨ f.val = 1 ∨ f.val = 2 := by omega
    rcases this with h | h | h <;> simp [h]
  obtain ⟨b, hb, hr, hm⟩ := ab_getElem? _ _ _ _ h0
  have hb2 : t.val[f.val]? = some b := by simpa [Array.to_slice] using hb
  unfold total_flag_balance Balance.get
  by_cases hgt : b.minimum.val < b.raw.val
  · refine ⟨b.raw, ?_, ?_⟩
    · simp [core.slice.Slice.get, Slice.getElem?_Usize_eq, Array.to_slice, hb2, hgt]
    · omega
  · refine ⟨b.minimum, ?_, ?_⟩
    · simp [core.slice.Slice.get, Slice.getElem?_Usize_eq, Array.to_slice, hb2, hgt]
    · omega


/-! ## The reference totals -/

/-- `true` if validator `v` with flags `p` counts in the total of flag `f` in epoch `e`. -/
def contrib (v : Spec.Validator) (p : Nat) (e f : Nat) : Bool :=
  Spec.is_active_validator v e && !v.slashed && Spec.has_flag p f

/-- The sum of `effective_balance` over the unslashed validators that are active in `e` and have
flag `f` in `ps`. -/
def flagSum : List Spec.Validator → List Nat → Nat → Nat → Nat
  | v :: vs, p :: ps, e, f => (if contrib v p e f then v.effective_balance else 0) + flagSum vs ps e f
  | _, _, _, _ => 0

def wOf (v : Spec.Validator) (p : Nat) (e f : Nat) : Nat :=
  if contrib v p e f then v.effective_balance else 0

theorem flagSum_cons (v : Spec.Validator) (vs : List Spec.Validator) (p : Nat) (ps : List Nat)
    (e f : Nat) : flagSum (v :: vs) (p :: ps) e f = wOf v p e f + flagSum vs ps e f := rfl

theorem flagSum_set_ps (vs : List Spec.Validator) (ps : List Nat) (e f j : Nat) (v : Spec.Validator)
    (p p' : Nat) (hv : vs[j]? = some v) (hp : ps[j]? = some p) :
    flagSum vs (ps.set j p') e f + wOf v p e f = flagSum vs ps e f + wOf v p' e f := by
  induction vs generalizing ps j with
  | nil => simp at hv
  | cons v0 vs ih =>
    cases ps with
    | nil => simp at hp
    | cons p0 ps =>
      cases j with
      | zero =>
        simp only [List.getElem?_cons_zero, Option.some.injEq] at hv hp
        subst hv hp
        simp only [List.set_cons_zero, flagSum_cons]; omega
      | succ j =>
        simp only [List.getElem?_cons_succ] at hv hp
        simp only [List.set_cons_succ, flagSum_cons]
        have := ih ps j hv hp; omega

theorem flagSum_set_vs (vs : List Spec.Validator) (ps : List Nat) (e f j : Nat)
    (v v' : Spec.Validator) (p : Nat) (hv : vs[j]? = some v) (hp : ps[j]? = some p) :
    flagSum (vs.set j v') ps e f + wOf v p e f = flagSum vs ps e f + wOf v' p e f := by
  induction vs generalizing ps j with
  | nil => simp at hv
  | cons v0 vs ih =>
    cases ps with
    | nil => simp at hp
    | cons p0 ps =>
      cases j with
      | zero =>
        simp only [List.getElem?_cons_zero, Option.some.injEq] at hv hp
        subst hv hp
        simp only [List.set_cons_zero, flagSum_cons]; omega
      | succ j =>
        simp only [List.getElem?_cons_succ] at hv hp
        simp only [List.set_cons_succ, flagSum_cons]
        have := ih ps j hv hp; omega

theorem has_flag_zero (f : Nat) : Spec.has_flag 0 f = false := by
  simp [Spec.has_flag]

theorem wOf_le_flagSum (vs : List Spec.Validator) (ps : List Nat) (e f j : Nat)
    (v : Spec.Validator) (p : Nat) (hv : vs[j]? = some v) (hp : ps[j]? = some p) :
    wOf v p e f ≤ flagSum vs ps e f := by
  have := flagSum_set_ps vs ps e f j v p 0 hv hp
  have h0 : wOf v 0 e f = 0 := by simp [wOf, contrib, has_flag_zero]
  omega


theorem flagSum_zeros (vs : List Spec.Validator) (n e f : Nat) :
    flagSum vs (List.replicate n 0) e f = 0 := by
  induction vs generalizing n with
  | nil => cases n <;> rfl
  | cons v vs ih =>
    cases n with
    | zero => rfl
    | succ n => simp [List.replicate_succ, flagSum_cons, ih, wOf, contrib, has_flag_zero]

theorem flagSum_append (vs : List Spec.Validator) (ps : List Nat) (v : Spec.Validator) (p e f : Nat)
    (hlen : ps.length = vs.length) :
    flagSum (vs ++ [v]) (ps ++ [p]) e f = flagSum vs ps e f + wOf v p e f := by
  induction vs generalizing ps with
  | nil =>
    cases ps with
    | nil => simp [flagSum_cons, flagSum]
    | cons => simp at hlen
  | cons v0 vs ih =>
    cases ps with
    | nil => simp at hlen
    | cons p0 ps =>
      simp only [List.cons_append, flagSum_cons]
      rw [ih ps (by simpa using hlen)]; omega

theorem has_flag_add_flag (p f g : Nat) :
    Spec.has_flag (Spec.add_flag p f) g = (Spec.has_flag p g || g = f) := by
  have key : ∀ (n i : Nat), (n &&& 2 ^ i == 2 ^ i) = n.testBit i := by
    intro n i
    rw [Nat.and_two_pow]
    cases n.testBit i <;> simp
  simp only [Spec.has_flag, Spec.add_flag, key, Nat.testBit_or, Nat.testBit_two_pow]
  by_cases h : g = f
  · simp [h]
  · simp [h, Ne.symm h]


/-! ## The spec computes `max EFFECTIVE_BALANCE_INCREMENT flagSum` -/

def activeAt (vs : List Spec.Validator) (e i : Nat) : Bool :=
  match vs[i]? with
  | some v => Spec.is_active_validator v e
  | none => false

def slashedAt (vs : List Spec.Validator) (i : Nat) : Bool :=
  match vs[i]? with
  | some v => v.slashed
  | none => false

def ebAt (vs : List Spec.Validator) (i : Nat) : Nat :=
  match vs[i]? with
  | some v => v.effective_balance
  | none => 0

theorem active_indices_eq (vs : List Spec.Validator) (e k : Nat) :
    (vs.zipIdx k).filterMap (fun (x : Spec.Validator × Nat) =>
        if Spec.is_active_validator x.1 e then some x.2 else none) =
      ((List.range vs.length).filter (activeAt vs e)).map (· + k) := by
  induction vs generalizing k with
  | nil => simp
  | cons v vs ih =>
    rw [List.zipIdx_cons, List.length_cons, List.range_succ_eq_map, List.filterMap_cons, ih]
    simp only [List.filter_cons, List.filter_map]
    have hf : (activeAt (v :: vs) e ∘ Nat.succ) = activeAt vs e := by
      funext i; simp [activeAt]
    have hg : ∀ L : List Nat, (L.map Nat.succ).map (· + k) = L.map (· + (k + 1)) := by
      intro L; simp only [List.map_map]; congr 1; funext i; simp; omega
    rw [hf]
    by_cases ha : Spec.is_active_validator v e <;> simp [ha, activeAt, hg]

theorem filterMapM_flags (ps : List Nat) (f : Nat) (L : List Nat)
    (hL : ∀ i ∈ L, i < ps.length) :
    L.filterMapM (fun i => do
        let flags ← Spec.getAt ps i
        pure (if Spec.has_flag flags f then some i else none) : Nat → Spec.SpecM (Option Nat)) =
      .ok (L.filter (fun i => Spec.has_flag (ps.getD i 0) f)) := by
  induction L with
  | nil => rfl
  | cons i L ih =>
    have hi : i < ps.length := hL i (by simp)
    rw [List.filterMapM_cons, ih (fun j hj => hL j (by simp [hj]))]
    have hg : (ps[i]?.getD 0) = ps[i] := by simp [hi]
    simp only [Spec.getAt, List.getElem?_eq_getElem hi]
    by_cases hf : Spec.has_flag ps[i] f
    · simp [hf, hg]
    · simp [hf, hg]

theorem filterMapM_unslashed (vs : List Spec.Validator) (L : List Nat)
    (hL : ∀ i ∈ L, i < vs.length) :
    L.filterMapM (fun index => do
        let v ← Spec.getAt vs index
        pure (if !v.slashed then some index else none) : Nat → Spec.SpecM (Option Nat)) =
      .ok (L.filter (fun i => !slashedAt vs i)) := by
  induction L with
  | nil => rfl
  | cons i L ih =>
    have hi : i < vs.length := hL i (by simp)
    rw [List.filterMapM_cons, ih (fun j hj => hL j (by simp [hj]))]
    have hg : slashedAt vs i = vs[i].slashed := by simp [slashedAt, hi]
    simp only [Spec.getAt, List.getElem?_eq_getElem hi]
    by_cases hs : vs[i].slashed <;> simp [hs, hg]

theorem mapM_balances (vs : List Spec.Validator) (L : List Nat) (hL : ∀ i ∈ L, i < vs.length) :
    L.mapM (fun index => do
        let v ← Spec.getAt vs index
        pure v.effective_balance : Nat → Spec.SpecM Nat) = .ok (L.map (ebAt vs)) := by
  induction L with
  | nil => rfl
  | cons i L ih =>
    have hi : i < vs.length := hL i (by simp)
    rw [List.mapM_cons, ih (fun j hj => hL j (by simp [hj]))]
    simp only [Spec.getAt, List.getElem?_eq_getElem hi]
    simp [ebAt, hi]

def specPred (vs : List Spec.Validator) (ps : List Nat) (e f : Nat) (i : Nat) : Bool :=
  !slashedAt vs i && (Spec.has_flag (ps.getD i 0) f && activeAt vs e i)

theorem sum_filter_eq_flagSum (vs : List Spec.Validator) (ps : List Nat) (e f : Nat)
    (hlen : ps.length = vs.length) :
    (((List.range vs.length).filter (specPred vs ps e f)).map (ebAt vs)).sum =
      flagSum vs ps e f := by
  induction vs generalizing ps with
  | nil => simp [flagSum]
  | cons v vs ih =>
    cases ps with
    | nil => simp at hlen
    | cons p ps =>
      rw [List.length_cons, List.range_succ_eq_map, List.filter_cons, List.filter_map,
        flagSum_cons]
      have hpred : (specPred (v :: vs) (p :: ps) e f ∘ Nat.succ) = specPred vs ps e f := by
        funext i; simp [specPred, slashedAt, activeAt]
      have heb : (ebAt (v :: vs) ∘ Nat.succ) = ebAt vs := by funext i; simp [ebAt]
      rw [hpred]
      have hrest := ih ps (by simpa using hlen)
      by_cases hc : contrib v p e f
      · have h0 : specPred (v :: vs) (p :: ps) e f 0 = true := by
          simp only [contrib, Bool.and_eq_true, Bool.not_eq_eq_eq_not, Bool.not_true] at hc
          simp [specPred, slashedAt, activeAt, hc]
        simp only [h0, if_true, List.map_cons, List.map_map, heb, List.sum_cons, hrest, wOf, hc]
        simp [ebAt]
      · have h0 : specPred (v :: vs) (p :: ps) e f 0 = false := by
          simp only [contrib, Bool.and_eq_true, Bool.not_eq_eq_eq_not, Bool.not_true,
            not_and] at hc
          simp only [specPred, slashedAt, activeAt, List.getElem?_cons_zero, List.getD_cons_zero]
          by_cases h1 : v.slashed <;> by_cases h2 : Spec.is_active_validator v e <;> simp_all
        simp only [h0, wOf, hc]
        simp only [Bool.false_eq_true, if_false, List.map_map, heb, hrest]; omega

/-- The participation list that the spec reads for epoch `e`. -/
def partAt (s : Spec.BeaconState) (e : Nat) : List Nat :=
  if e = Spec.get_current_epoch s then s.current_epoch_participation
  else s.previous_epoch_participation

/-- `get_total_balance(state, get_unslashed_participating_indices(state, f, e))` is
`max EFFECTIVE_BALANCE_INCREMENT flagSum` when the lists have the same length and the total
fits. -/
theorem spec_total_eq (incr : Nat) (s : Spec.BeaconState) (f e : Nat)
    (he : e = Spec.get_previous_epoch s ∨ e = Spec.get_current_epoch s)
    (hlen : (partAt s e).length = s.validators.length)
    (hfit : max incr (flagSum s.validators (partAt s e) e f) < Spec.UINT64_SIZE) :
    (Spec.get_unslashed_participating_indices s f e >>= Spec.get_total_balance incr s) =
      .ok (max incr (flagSum s.validators (partAt s e) e f)) := by
  have hA := active_indices_eq s.validators e 0
  simp only [Nat.add_zero, List.map_id_fun', id_eq] at hA
  have hmem : ∀ i ∈ (List.range s.validators.length).filter (activeAt s.validators e),
      i < s.validators.length := fun i hi => by
    simp only [List.mem_filter, List.mem_range] at hi; exact hi.1
  unfold Spec.get_unslashed_participating_indices Spec.get_active_validator_indices
  have hcond : (!(decide (e = Spec.get_previous_epoch s) || decide (e = Spec.get_current_epoch s)))
      = false := by
    rcases he with h | h <;> simp [h]
  have hF := filterMapM_flags (partAt s e) f _ (fun i hi => hlen ▸ hmem i hi)
  simp only [partAt] at hF hlen hfit ⊢
  simp only [hcond, Bool.false_eq_true, if_false, hA, hF]
  generalize (if e = Spec.get_current_epoch s then s.current_epoch_participation
    else s.previous_epoch_participation) = P at hlen hfit ⊢
  set L := List.filter (fun i => Spec.has_flag (P.getD i 0) f)
    (List.filter (activeAt s.validators e) (List.range s.validators.length)) with hLdef
  have hLmem : ∀ i ∈ L, i < s.validators.length := fun i hi => by
    simp only [hLdef, List.mem_filter, List.mem_range] at hi; exact hi.1.1
  have hU := filterMapM_unslashed s.validators L hLmem
  have hMmem : ∀ i ∈ L.filter (fun i => !slashedAt s.validators i), i < s.validators.length :=
    fun i hi => hLmem i (List.mem_filter.mp hi).1
  have hM := mapM_balances s.validators _ hMmem
  have hsum := sum_filter_eq_flagSum s.validators P e f hlen
  have hfilt : L.filter (fun i => !slashedAt s.validators i) =
      (List.range s.validators.length).filter (specPred s.validators P e f) := by
    simp only [hLdef, List.filter_filter]; rfl
  rw [show (Except.ok L : Spec.SpecM (List Nat)) = pure L from rfl]
  simp only [pure_bind]
  rw [hU, show (Except.ok _ : Spec.SpecM (List Nat)) = pure _ from rfl, pure_bind]
  unfold Spec.get_total_balance
  rw [hM, show (Except.ok _ : Spec.SpecM (List Nat)) = pure _ from rfl, pure_bind, hfilt, hsum]
  simp [hfit]
  rfl


/-! ## Facts about one validator's share -/

theorem tri_congr (R R' : Nat → Nat) (m : Nat) (h : ∀ g < 3, R g = R' g) : tri R m = tri R' m := by
  simp only [tri, h 0 (by omega), h 1 (by omega), h 2 (by omega)]

theorem wOf_add_flag (v : Spec.Validator) (p e f g : Nat) (hnew : Spec.has_flag p f = false)
    (hactive : Spec.is_active_validator v e = true) :
    wOf v (Spec.add_flag p f) e g =
      wOf v p e g + (if g = f ∧ v.slashed = false then v.effective_balance else 0) := by
  unfold wOf contrib
  rw [has_flag_add_flag]
  by_cases hg : g = f
  · subst hg; cases hs : v.slashed <;> simp [hnew, hactive]
  · simp [hg]

theorem wOf_slashed (v : Spec.Validator) (p e f : Nat) :
    wOf { v with slashed := true } p e f = 0 := by
  simp [wOf, contrib]

theorem wOf_unslashed (v : Spec.Validator) (p e f : Nat) (hs : v.slashed = false)
    (hact : Spec.has_flag p f = true → Spec.is_active_validator v e = true) :
    wOf v p e f = if Spec.has_flag p f then v.effective_balance else 0 := by
  by_cases hf : Spec.has_flag p f
  · simp [wOf, contrib, hs, hf, hact hf]
  · simp [wOf, contrib, hf]

theorem wOf_eb (v : Spec.Validator) (p e f b : Nat) :
    wOf { v with effective_balance := b } p e f = if contrib v p e f then b else 0 := by
  simp [wOf, contrib, Spec.is_active_validator]


/-! ## The cache and coherence -/

noncomputable section

/-- `Inner` of `ProgressiveBalancesCache`. The raw totals of each `EpochTotalBalances` are the
three `Balance` values. -/
structure LhCache where
  current_epoch : Nat
  previous_epoch_cache : Array Bal 3#usize
  current_epoch_cache : Array Bal 3#usize

/-- The errors that the glue returns. `totals` wraps the `From` impl into `BeaconStateError`. -/
inductive LhError where
  | totals (e : KErr)
  | arith
  | inconsistent

/-- The current epoch cache holds, for each flag, the reference total of the current epoch. -/
def CurrentCoherent (incr : Nat) (s : Spec.BeaconState) (c : LhCache) : Prop :=
  c.current_epoch = s.current_epoch ∧
  ab c.current_epoch_cache.val =
    tri (flagSum s.validators s.current_epoch_participation s.current_epoch) incr

/-- The previous epoch cache holds, for each flag, the total over `previous_epoch_participation`
and the validators active in `get_previous_epoch(state)`. -/
def PreviousCoherent (incr : Nat) (s : Spec.BeaconState) (c : LhCache) : Prop :=
  ab c.previous_epoch_cache.val =
    tri (flagSum s.validators s.previous_epoch_participation (Spec.get_previous_epoch s)) incr

def Coherent (incr : Nat) (s : Spec.BeaconState) (c : LhCache) : Prop :=
  CurrentCoherent incr s c ∧ PreviousCoherent incr s c

/-! ## Attestation -/

/-- `ProgressiveBalancesCache::on_new_attestation`. -/
def lhOnNewAttestation (c : LhCache) (epoch : Nat) (is_slashed : Bool) (f : Usize) (eb : U64) :
    Result (core.result.Result LhCache LhError) :=
  if epoch = c.current_epoch then do
    let (r, t) ← on_new_attestation c.current_epoch_cache is_slashed f eb
    match r with
    | .Ok _ => ok (.Ok { c with current_epoch_cache := t })
    | .Err e => ok (.Err (.totals e))
  else if U64.max < epoch + 1 then ok (.Err .arith)
  else if epoch + 1 = c.current_epoch then do
    let (r, t) ← on_new_attestation c.previous_epoch_cache is_slashed f eb
    match r with
    | .Ok _ => ok (.Ok { c with previous_epoch_cache := t })
    | .Err e => ok (.Err (.totals e))
  else ok (.Err .inconsistent)

theorem flagSum_add_flag (vs : List Spec.Validator) (ps : List Nat) (e f g j : Nat)
    (v : Spec.Validator) (p : Nat) (hv : vs[j]? = some v) (hp : ps[j]? = some p)
    (hnew : Spec.has_flag p f = false) (hactive : Spec.is_active_validator v e = true) :
    flagSum vs (ps.set j (Spec.add_flag p f)) e g =
      flagSum vs ps e g + (if g = f ∧ v.slashed = false then v.effective_balance else 0) := by
  have h := flagSum_set_ps vs ps e g j v p (Spec.add_flag p f) hv hp
  rw [wOf_add_flag v p e f g hnew hactive] at h
  omega

/-- The attestation hook keeps the cache coherent. The flag was not set, and the validator is
active in the target epoch. -/
theorem attestation_coherent (incr : Nat) (s s' : Spec.BeaconState) (c : LhCache)
    (e j : Nat) (f : Usize) (is_slashed : Bool) (eb : U64) (v : Spec.Validator) (p : Nat)
    (hc : Coherent incr s c) (hspec : Spec.set_participation_flag s e j f.val = .ok s')
    (he : e = Spec.get_previous_epoch s ∨ e = Spec.get_current_epoch s)
    (hepoch : s.current_epoch ≤ U64.max)
    (hv : s.validators[j]? = some v) (hp : (partAt s e)[j]? = some p)
    (hnew : Spec.has_flag p f.val = false) (hactive : Spec.is_active_validator v e = true)
    (hf : f.val < 3) (hslashed : is_slashed = v.slashed) (heb : eb.val = v.effective_balance)
    (hfit : flagSum s'.validators (partAt s' e) e f.val ≤ U64.max) :
    ∃ c', lhOnNewAttestation c e is_slashed f eb = ok (.Ok c') ∧ Coherent incr s' c' := by
  obtain ⟨⟨hce, hcur⟩, hprev⟩ := hc
  by_cases hec : e = s.current_epoch
  · -- The current epoch.
    have hpart : partAt s e = s.current_epoch_participation := by
      simp [partAt, Spec.get_current_epoch, hec]
    rw [hpart] at hp
    have hs' : s' = { s with current_epoch_participation :=
        s.current_epoch_participation.set j (Spec.add_flag p f.val) } := by
      simp [Spec.set_participation_flag, Spec.get_current_epoch, hec, Spec.getAt, hp,
        hnew, pure, Except.pure, Bind.bind, Except.bind] at hspec
      exact hspec.symm
    subst hs'
    have hsum := fun g => flagSum_add_flag s.validators s.current_epoch_participation e f.val g j v p
      hv hp hnew hactive
    simp only [partAt, Spec.get_current_epoch, hec, if_true] at hfit
    rw [hec] at hsum
    obtain ⟨t', ht', habs⟩ := on_new_attestation_ok c.current_epoch_cache is_slashed f eb
      (flagSum s.validators s.current_epoch_participation s.current_epoch) incr hcur hf
      (by
        intro hns
        have := hsum f.val
        rw [hslashed] at hns
        simp only [hns, and_self, if_true] at this
        rw [heb]; omega)
    refine ⟨{ c with current_epoch_cache := t' }, ?_, ⟨⟨hce, ?_⟩, hprev⟩⟩
    · simp [lhOnNewAttestation, hce, hec, ht']
    · simp only [habs]
      cases hs : v.slashed
      · simp only [hslashed, hs, Bool.false_eq_true, if_false]
        apply tri_congr; intro g _
        rw [hsum g, heb]; by_cases hg : g = f.val <;> simp [hg, hs]
      · simp only [hslashed, hs, if_true]
        apply tri_congr; intro g _
        rw [hsum g]; simp [hs]
  · -- The previous epoch, which is not the current epoch.
    have hep : e = Spec.get_previous_epoch s := by
      rcases he with h | h
      · exact h
      · exact absurd h hec
    have hpos : s.current_epoch ≠ 0 := by
      intro h0; apply hec; rw [hep]; simp [Spec.get_previous_epoch, Spec.get_current_epoch, h0,
        Spec.GENESIS_EPOCH]
    have hpe : Spec.get_previous_epoch s = s.current_epoch - 1 := by
      simp [Spec.get_previous_epoch, Spec.get_current_epoch, Spec.GENESIS_EPOCH, hpos]
    have he1 : e + 1 = s.current_epoch := by have h1 := hep.trans hpe; omega
    have hpart : partAt s e = s.previous_epoch_participation := by
      simp [partAt, Spec.get_current_epoch, hec]
    rw [hpart] at hp
    have hs' : s' = { s with previous_epoch_participation :=
        s.previous_epoch_participation.set j (Spec.add_flag p f.val) } := by
      simp [Spec.set_participation_flag, Spec.get_current_epoch, hec, Spec.getAt, hp,
        hnew, pure, Except.pure, Bind.bind, Except.bind] at hspec
      exact hspec.symm
    subst hs'
    have hsum := fun g => flagSum_add_flag s.validators s.previous_epoch_participation e f.val g j v
      p hv hp hnew hactive
    simp only [partAt, Spec.get_current_epoch, hec, if_false] at hfit
    rw [hep] at hsum hfit
    obtain ⟨t', ht', habs⟩ := on_new_attestation_ok c.previous_epoch_cache is_slashed f eb
      (flagSum s.validators s.previous_epoch_participation (Spec.get_previous_epoch s)) incr hprev hf
      (by
        intro hns
        have := hsum f.val
        rw [hslashed] at hns
        simp only [hns, and_self, if_true] at this
        rw [heb]; omega)
    refine ⟨{ c with previous_epoch_cache := t' }, ?_, ⟨⟨hce, hcur⟩, ?_⟩⟩
    · have hne : ¬ e = c.current_epoch := by rw [hce]; exact hec
      have hle : ¬ U64.max < e + 1 := by rw [he1]; exact Nat.not_lt.mpr hepoch
      have heq : e + 1 = c.current_epoch := by rw [hce]; exact he1
      have hlt : ¬ U64.max < s.current_epoch := Nat.not_lt.mpr hepoch
      simp [lhOnNewAttestation, heq, ht', hce, hec, hlt]
    · simp only [PreviousCoherent, habs]
      have hpe : Spec.get_previous_epoch
          { s with previous_epoch_participation :=
            s.previous_epoch_participation.set j (Spec.add_flag p f.val) } =
          Spec.get_previous_epoch s := rfl
      rw [hpe]
      cases hs : v.slashed
      · simp only [hslashed, hs, Bool.false_eq_true, if_false]
        apply tri_congr; intro g _
        rw [hsum g, heb]; by_cases hg : g = f.val <;> simp [hg, hs]
      · simp only [hslashed, hs, if_true]
        apply tri_congr; intro g _
        rw [hsum g]; simp [hs]

/-! ## Slashing -/

/-- `ProgressiveBalancesCache::on_slashing`. -/
def lhOnSlashing (c : LhCache) (pflags cflags : U8) (eb : U64) :
    Result (core.result.Result LhCache LhError) := do
  let (r, t) ← on_slashing c.previous_epoch_cache pflags eb
  match r with
  | .Err e => ok (.Err (.totals e))
  | .Ok _ =>
    let (r2, t2) ← on_slashing c.current_epoch_cache cflags eb
    match r2 with
    | .Err e => ok (.Err (.totals e))
    | .Ok _ => ok (.Ok { c with previous_epoch_cache := t, current_epoch_cache := t2 })

theorem flagSum_slash (vs : List Spec.Validator) (ps : List Nat) (e g j : Nat)
    (v : Spec.Validator) (p : Nat) (hv : vs[j]? = some v) (hp : ps[j]? = some p)
    (hs : v.slashed = false) (hact : ∀ f < 3, Spec.has_flag p f = true →
      Spec.is_active_validator v e = true) (hg : g < 3) :
    flagSum (vs.set j { v with slashed := true }) ps e g =
      flagSum vs ps e g - (if Spec.has_flag p g then v.effective_balance else 0) ∧
    (Spec.has_flag p g = true → v.effective_balance ≤ flagSum vs ps e g) := by
  have h := flagSum_set_vs vs ps e g j v { v with slashed := true } p hv hp
  have hle := wOf_le_flagSum vs ps e g j v p hv hp
  rw [wOf_slashed, wOf_unslashed v p e g hs (hact g hg)] at h
  rw [wOf_unslashed v p e g hs (hact g hg)] at hle
  constructor
  · omega
  · intro hf; simp only [hf, if_true] at hle; exact hle

/-- The slashing hook keeps the cache coherent. It never fails. The validator was not slashed,
and each flag that it holds in an epoch implies that it is active in that epoch. -/
theorem slashing_coherent (incr : Nat) (s s' : Spec.BeaconState) (c : LhCache) (j : Nat)
    (pflags cflags : U8) (eb : U64) (v : Spec.Validator)
    (hc : Coherent incr s c) (hspec : Spec.set_slashed s j = .ok s')
    (hv : s.validators[j]? = some v) (hs : v.slashed = false)
    (hpp : s.previous_epoch_participation[j]? = some pflags.val)
    (hcp : s.current_epoch_participation[j]? = some cflags.val)
    (hpact : ∀ f < 3, Spec.has_flag pflags.val f = true →
      Spec.is_active_validator v (Spec.get_previous_epoch s) = true)
    (hcact : ∀ f < 3, Spec.has_flag cflags.val f = true →
      Spec.is_active_validator v s.current_epoch = true)
    (heb : eb.val = v.effective_balance) :
    ∃ c', lhOnSlashing c pflags cflags eb = ok (.Ok c') ∧ Coherent incr s' c' := by
  obtain ⟨⟨hce, hcur⟩, hprev⟩ := hc
  have hs' : s' = { s with validators := s.validators.set j { v with slashed := true } } := by
    simp [Spec.set_slashed, Spec.getAt, hv, pure, Except.pure, Bind.bind, Except.bind] at hspec
    exact hspec.symm
  subst hs'
  have hP := fun g hg => flagSum_slash s.validators s.previous_epoch_participation
    (Spec.get_previous_epoch s) g j v pflags.val hv hpp hs hpact hg
  have hC := fun g hg => flagSum_slash s.validators s.current_epoch_participation
    s.current_epoch g j v cflags.val hv hcp hs hcact hg
  obtain ⟨t, et, hat⟩ := on_slashing_ok c.previous_epoch_cache pflags eb _ incr hprev
    (fun g hg hf => by rw [heb]; exact (hP g hg).2 hf)
  obtain ⟨t2, et2, hat2⟩ := on_slashing_ok c.current_epoch_cache cflags eb _ incr hcur
    (fun g hg hf => by rw [heb]; exact (hC g hg).2 hf)
  refine ⟨{ c with previous_epoch_cache := t, current_epoch_cache := t2 }, ?_, ⟨⟨hce, ?_⟩, ?_⟩⟩
  · simp [lhOnSlashing, et, et2]
  · simp only [hat2]
    apply tri_congr; intro g hg
    rw [(hC g hg).1, heb]; by_cases hf : Spec.has_flag cflags.val g <;> simp [hf]
  · simp only [PreviousCoherent, hat]
    simp only [Spec.get_previous_epoch, Spec.get_current_epoch] at hP ⊢
    apply tri_congr; intro g hg
    rw [(hP g hg).1, heb]
    by_cases hf : Spec.has_flag pflags.val g <;> simp [hf]

/-! ## Effective balance change -/

/-- `ProgressiveBalancesCache::on_effective_balance_change`. It changes the current epoch cache
only. -/
def lhOnEffectiveBalanceChange (c : LhCache) (is_slashed : Bool) (cflags : U8) (o n : U64) :
    Result (core.result.Result LhCache LhError) := do
  let (r, t) ← on_effective_balance_change c.current_epoch_cache is_slashed cflags o n
  match r with
  | .Err e => ok (.Err (.totals e))
  | .Ok _ => ok (.Ok { c with current_epoch_cache := t })

/-- The effective balance hook keeps the current epoch cache coherent. It does not change the
previous epoch cache, which the epoch transition then drops (see `epoch_transition_coherent`). -/
theorem effective_balance_change_coherent (incr : Nat) (s s' : Spec.BeaconState) (c : LhCache)
    (j : Nat) (is_slashed : Bool) (cflags : U8) (o n : U64) (v : Spec.Validator)
    (hc : CurrentCoherent incr s c) (hspec : Spec.set_effective_balance s j n.val = .ok s')
    (hv : s.validators[j]? = some v) (hslashed : is_slashed = v.slashed)
    (ho : o.val = v.effective_balance)
    (hcp : s.current_epoch_participation[j]? = some cflags.val)
    (hcact : ∀ f < 3, Spec.has_flag cflags.val f = true →
      Spec.is_active_validator v s.current_epoch = true)
    (hfit : ∀ f < 3, flagSum s'.validators s'.current_epoch_participation s'.current_epoch f ≤
      U64.max) :
    ∃ c', lhOnEffectiveBalanceChange c is_slashed cflags o n = ok (.Ok c') ∧
      CurrentCoherent incr s' c' ∧ c'.previous_epoch_cache = c.previous_epoch_cache := by
  obtain ⟨hce, hcur⟩ := hc
  let v' : Spec.Validator := { v with effective_balance := n.val }
  have hs' : s' = { s with validators := s.validators.set j v' } := by
    simp [Spec.set_effective_balance, Spec.getAt, hv, pure, Except.pure, Bind.bind,
      Except.bind] at hspec
    exact hspec.symm
  subst hs'
  have hC : ∀ g, flagSum (s.validators.set j v') s.current_epoch_participation s.current_epoch g
      + wOf v cflags.val s.current_epoch g =
      flagSum s.validators s.current_epoch_participation s.current_epoch g +
        (if contrib v cflags.val s.current_epoch g then n.val else 0) := by
    intro g
    have h := flagSum_set_vs s.validators s.current_epoch_participation s.current_epoch g j v
      v' cflags.val hv hcp
    rw [wOf_eb] at h; exact h
  have hle := fun g => wOf_le_flagSum s.validators s.current_epoch_participation
    s.current_epoch g j v cflags.val hv hcp
  -- For an unslashed validator, `contrib` is `has_flag`.
  have hcontrib : ∀ g < 3, v.slashed = false →
      contrib v cflags.val s.current_epoch g = Spec.has_flag cflags.val g := by
    intro g hg hs
    by_cases hf : Spec.has_flag cflags.val g
    · simp [contrib, hs, hf, hcact g hg hf]
    · simp [contrib, hf]
  obtain ⟨t, et, hat⟩ := on_effective_balance_change_ok c.current_epoch_cache is_slashed cflags
    o n _ incr hcur (fun hns g hg hf => by
      rw [hslashed] at hns
      have hct := hcontrib g hg hns
      rw [hf] at hct
      have hcg := hC g
      have hlg := hle g
      have hfg := hfit g hg
      simp only [wOf, hct, if_true] at hcg hlg
      simp only at hfg
      constructor <;> intro <;> omega)
  refine ⟨{ c with current_epoch_cache := t }, ?_, ⟨hce, ?_⟩, rfl⟩
  · simp [lhOnEffectiveBalanceChange, et]
  · simp only [hat]
    cases hs : v.slashed
    · simp only [hslashed, hs, Bool.false_eq_true, if_false]
      apply tri_congr; intro g hg
      have hct := hcontrib g hg hs
      have hcg := hC g
      have hlg := hle g
      by_cases hf : Spec.has_flag cflags.val g
      · rw [hf] at hct
        simp only [wOf, hct, if_true] at hcg hlg
        simp only [hf, if_true, moved]
        split <;> omega
      · have hf' : Spec.has_flag cflags.val g = false := by simpa using hf
        rw [hf'] at hct
        simp only [wOf, hct, Bool.false_eq_true, if_false] at hcg
        simp only [hf', Bool.false_eq_true, if_false]
        omega
    · simp only [hslashed, hs, if_true]
      apply tri_congr; intro g hg
      have hcg := hC g
      simp only [wOf, contrib, hs, Bool.not_true, Bool.and_false, Bool.false_and,
        Bool.false_eq_true, if_false] at hcg
      omega

/-! ## Epoch transition -/

/-- `EpochTotalBalances::new`: three `Balance::zero(effective_balance_increment)`. -/
def lhNewTotals (incr : U64) : Result (Array Bal 3#usize) := do
  let z ← Balance.zero incr
  ok ⟨[z, z, z], by simp⟩

/-- `ProgressiveBalancesCache::on_epoch_transition`. -/
def lhOnEpochTransition (c : LhCache) (incr : U64) :
    Result (core.result.Result LhCache LhError) :=
  if U64.max < c.current_epoch + 1 then ok (.Err .arith)
  else do
    let fresh ← lhNewTotals incr
    ok (.Ok { current_epoch := c.current_epoch + 1
              previous_epoch_cache := c.current_epoch_cache
              current_epoch_cache := fresh })

/-- The epoch transition gives a coherent cache for the next epoch. It needs only the current
epoch cache. So the previous epoch cache, which the effective balance hook does not update, does
not matter here. -/
theorem epoch_transition_coherent (incr : U64) (s : Spec.BeaconState) (c : LhCache)
    (hc : CurrentCoherent incr.val s c) (hepoch : s.current_epoch + 1 ≤ U64.max) :
    ∃ c', lhOnEpochTransition c incr = ok (.Ok c') ∧
      Coherent incr.val (Spec.next_epoch_participation s) c' := by
  obtain ⟨hce, hcur⟩ := hc
  have hlt : ¬ U64.max < c.current_epoch + 1 := by rw [hce]; exact Nat.not_lt.mpr hepoch
  refine ⟨_, by simp [lhOnEpochTransition, hlt, lhNewTotals, Balance.zero]; rfl, ⟨⟨?_, ?_⟩, ?_⟩⟩
  · simp [Spec.next_epoch_participation, hce]
  · simp only [Spec.next_epoch_participation, Spec.process_participation_flag_updates]
    simp [ab, tri, flagSum_zeros]
  · simp only [PreviousCoherent, Spec.next_epoch_participation,
      Spec.process_participation_flag_updates, Spec.get_previous_epoch, Spec.get_current_epoch,
      Spec.GENESIS_EPOCH]
    simpa using hcur

/-! ## State changes with no hook -/

/-- Two validator lists that agree, index by index, on what the totals of epoch `e` read. -/
def SameView (vs vs' : List Spec.Validator) (e : Nat) : Prop :=
  vs'.length = vs.length ∧ ∀ (i : Nat) (v v' : Spec.Validator), vs[i]? = some v → vs'[i]? = some v' →
    v'.effective_balance = v.effective_balance ∧ v'.slashed = v.slashed ∧
    Spec.is_active_validator v' e = Spec.is_active_validator v e

theorem flagSum_sameView (vs vs' : List Spec.Validator) (ps : List Nat) (e f : Nat)
    (h : SameView vs vs' e) : flagSum vs' ps e f = flagSum vs ps e f := by
  induction vs generalizing vs' ps with
  | nil => cases vs' with
    | nil => rfl
    | cons => simp [SameView] at h
  | cons v vs ih =>
    cases vs' with
    | nil => simp [SameView] at h
    | cons v' vs' =>
      cases ps with
      | nil => rfl
      | cons p ps =>
        obtain ⟨hl, hi⟩ := h
        have h0 := hi 0 v v' rfl rfl
        have hrest : SameView vs vs' e :=
          ⟨by simpa using hl, fun i a b ha hb => hi (i + 1) a b (by simpa using ha) (by simpa using hb)⟩
        rw [flagSum_cons, flagSum_cons, ih vs' ps hrest]
        simp [wOf, contrib, h0.1, h0.2.1, h0.2.2]

/-- A change to the validators that keeps each `effective_balance`, `slashed` flag, and activity
in the previous and current epochs keeps the cache coherent. `initiate_validator_exit` and the
activation in `process_registry_updates` set epochs after the current epoch, so they are such
changes. -/
theorem registry_change_coherent (incr : Nat) (s : Spec.BeaconState) (c : LhCache)
    (vs' : List Spec.Validator) (hc : Coherent incr s c)
    (hprev : SameView s.validators vs' (Spec.get_previous_epoch s))
    (hcur : SameView s.validators vs' s.current_epoch) :
    Coherent incr { s with validators := vs' } c := by
  obtain ⟨⟨hce, hc1⟩, hc2⟩ := hc
  refine ⟨⟨hce, ?_⟩, ?_⟩
  · simp only
    rw [hc1]; apply tri_congr; intro g _
    exact (flagSum_sameView _ _ _ _ _ hcur).symm
  · simp only [PreviousCoherent] at hc2 ⊢
    rw [hc2]; apply tri_congr; intro g _
    exact (flagSum_sameView _ _ _ _ _ hprev).symm

/-- A new validator with empty participation in both epochs keeps the cache coherent. This is
`add_validator_to_registry`. -/
theorem add_validator_coherent (incr : Nat) (s : Spec.BeaconState) (c : LhCache)
    (v : Spec.Validator) (hc : Coherent incr s c)
    (hplen : s.previous_epoch_participation.length = s.validators.length)
    (hclen : s.current_epoch_participation.length = s.validators.length) :
    Coherent incr { s with
      validators := s.validators ++ [v]
      previous_epoch_participation := s.previous_epoch_participation ++ [0]
      current_epoch_participation := s.current_epoch_participation ++ [0] } c := by
  obtain ⟨⟨hce, hc1⟩, hc2⟩ := hc
  have hw : ∀ e f, wOf v 0 e f = 0 := fun e f => by simp [wOf, contrib, has_flag_zero]
  refine ⟨⟨hce, ?_⟩, ?_⟩
  · dsimp only
    rw [hc1]; apply tri_congr; intro g _
    rw [flagSum_append _ _ _ _ _ _ hclen, hw, Nat.add_zero]
  · simp only [PreviousCoherent] at hc2 ⊢
    simp only [Spec.get_previous_epoch, Spec.get_current_epoch] at hc2 ⊢
    rw [hc2]; apply tri_congr; intro g _
    rw [flagSum_append _ _ _ _ _ _ hplen, hw, Nat.add_zero]

/-! ## Reads -/

/-- `ProgressiveBalancesCache::current_epoch_flag_attesting_balance`. -/
def lhCurrentEpochFlagAttestingBalance (c : LhCache) (f : Usize) :=
  total_flag_balance (Array.to_slice c.current_epoch_cache) f

/-- `ProgressiveBalancesCache::previous_epoch_flag_attesting_balance`. -/
def lhPreviousEpochFlagAttestingBalance (c : LhCache) (f : Usize) :=
  total_flag_balance (Array.to_slice c.previous_epoch_cache) f

/-- A read of the current epoch total equals the spec, with the `EFFECTIVE_BALANCE_INCREMENT`
floor. It does not fail. -/
theorem read_current_eq_spec (incr : Nat) (s : Spec.BeaconState) (c : LhCache) (f : Usize)
    (hc : CurrentCoherent incr s c) (hf : f.val < 3)
    (hlen : s.current_epoch_participation.length = s.validators.length)
    (hfit : max incr (flagSum s.validators s.current_epoch_participation s.current_epoch f.val)
      < Spec.UINT64_SIZE) :
    ∃ x : U64, lhCurrentEpochFlagAttestingBalance c f = ok (.Ok x) ∧
      (Spec.get_unslashed_participating_indices s f.val (Spec.get_current_epoch s) >>=
        Spec.get_total_balance incr s) = .ok x.val := by
  obtain ⟨x, hx, hv⟩ := total_flag_balance_ok c.current_epoch_cache f _ incr hc.2 hf
  refine ⟨x, hx, ?_⟩
  have hp : partAt s (Spec.get_current_epoch s) = s.current_epoch_participation := by
    simp [partAt]
  have := spec_total_eq incr s f.val (Spec.get_current_epoch s) (Or.inr rfl)
    (by rw [hp]; exact hlen) (by rw [hp]; exact hfit)
  rw [this, hp, hv, max_comm]; rfl

/-- A read of the previous epoch total equals the spec after the genesis epoch. At the genesis
epoch, `get_previous_epoch` is the current epoch and the spec reads
`current_epoch_participation`, but the cache holds the total over
`previous_epoch_participation`. -/
theorem read_previous_eq_spec (incr : Nat) (s : Spec.BeaconState) (c : LhCache) (f : Usize)
    (hc : PreviousCoherent incr s c) (hf : f.val < 3) (hgen : Spec.GENESIS_EPOCH < s.current_epoch)
    (hlen : s.previous_epoch_participation.length = s.validators.length)
    (hfit : max incr (flagSum s.validators s.previous_epoch_participation
      (Spec.get_previous_epoch s) f.val) < Spec.UINT64_SIZE) :
    ∃ x : U64, lhPreviousEpochFlagAttestingBalance c f = ok (.Ok x) ∧
      (Spec.get_unslashed_participating_indices s f.val (Spec.get_previous_epoch s) >>=
        Spec.get_total_balance incr s) = .ok x.val := by
  obtain ⟨x, hx, hv⟩ := total_flag_balance_ok c.previous_epoch_cache f _ incr hc hf
  refine ⟨x, hx, ?_⟩
  have hne : Spec.get_previous_epoch s ≠ Spec.get_current_epoch s := by
    have h0 : s.current_epoch ≠ 0 := by
      intro h; rw [h] at hgen; exact absurd hgen (by decide)
    simp only [Spec.get_previous_epoch, Spec.get_current_epoch, Spec.GENESIS_EPOCH, h0, if_false]
    exact (Nat.sub_one_lt h0).ne
  have hp : partAt s (Spec.get_previous_epoch s) = s.previous_epoch_participation := by
    simp [partAt, hne]
  have := spec_total_eq incr s f.val (Spec.get_previous_epoch s) (Or.inl rfl)
    (by rw [hp]; exact hlen) (by rw [hp]; exact hfit)
  rw [this, hp, hv, max_comm]

/-! ## Build -/

/-- The fields of a Lighthouse `Validator` that the build reads. -/
structure LhValidator where
  effective_balance : U64
  slashed : Bool
  activation_epoch : U64
  exit_epoch : U64

def LhValidator.toSpec (v : LhValidator) : Spec.Validator :=
  ⟨v.effective_balance.val, v.slashed, v.activation_epoch.val, v.exit_epoch.val⟩

/-- `Validator::is_active_at`. -/
def lhIsActiveAt (v : LhValidator) (e : Nat) : Bool :=
  v.activation_epoch.val ≤ e && e < v.exit_epoch.val

/-- The fields of a Lighthouse `BeaconState` that the build reads. -/
structure LhState where
  current_epoch : Nat
  validators : List LhValidator
  previous_epoch_participation : List U8
  current_epoch_participation : List U8

def LhState.toSpec (s : LhState) : Spec.BeaconState :=
  ⟨s.current_epoch, s.validators.map LhValidator.toSpec,
   s.previous_epoch_participation.map (·.val), s.current_epoch_participation.map (·.val)⟩

/-- `BeaconState::previous_epoch`. -/
def lhPreviousEpoch (e : Nat) : Nat := if e = 0 then 0 else e - 1

/-- The loop of `initialize_progressive_balances_cache` over
`validators.zip(current_epoch_participation).zip(previous_epoch_participation)`.
`update_flag_total_balances` calls `add_flags`. -/
def lhInitLoop (pe ce : Nat) :
    List ((LhValidator × U8) × U8) → Array Bal 3#usize → Array Bal 3#usize →
      Result (core.result.Result (Array Bal 3#usize × Array Bal 3#usize) KErr)
  | [], pt, ct => ok (.Ok (pt, ct))
  | ((v, cf), pf) :: rest, pt, ct =>
    if v.slashed then lhInitLoop pe ce rest pt ct
    else do
      let (r1, ct1) ←
        if lhIsActiveAt v ce then add_flags ct cf v.effective_balance
        else ok (core.result.Result.Ok (), ct)
      match r1 with
      | .Err e => ok (.Err e)
      | .Ok _ =>
        let (r2, pt1) ←
          if lhIsActiveAt v pe then add_flags pt pf v.effective_balance
          else ok (core.result.Result.Ok (), pt)
        match r2 with
        | .Err e => ok (.Err e)
        | .Ok _ => lhInitLoop pe ce rest pt1 ct1

/-- `initialize_progressive_balances_cache`, after the "already initialized" check. -/
def lhInitialize (incr : U64) (s : LhState) : Result (core.result.Result LhCache KErr) := do
  let pt ← lhNewTotals incr
  let ct ← lhNewTotals incr
  let r ← lhInitLoop (lhPreviousEpoch s.current_epoch) s.current_epoch
    ((s.validators.zip s.current_epoch_participation).zip s.previous_epoch_participation) pt ct
  match r with
  | .Ok (p, c) => ok (.Ok ⟨s.current_epoch, p, c⟩)
  | .Err e => ok (.Err e)

theorem init_step_ok (t : Array Bal 3#usize) (v : LhValidator) (fl : U8) (e : Nat)
    (R : Nat → Nat) (m : Nat) (hs : v.slashed = false) (ht : ab t.val = tri R m)
    (hfit : ∀ g < 3, R g + wOf v.toSpec fl.val e g ≤ U64.max) :
    ∃ t', (if lhIsActiveAt v e then add_flags t fl v.effective_balance
        else ok (core.result.Result.Ok (), t)) = ok (core.result.Result.Ok (), t') ∧ ab t'.val = tri (fun g => R g + wOf v.toSpec fl.val e g) m := by
  have hact : lhIsActiveAt v e = Spec.is_active_validator v.toSpec e := rfl
  by_cases ha : lhIsActiveAt v e
  · have hw : ∀ g, wOf v.toSpec fl.val e g =
        if Spec.has_flag fl.val g then v.effective_balance.val else 0 := by
      intro g
      have ha' : Spec.is_active_validator v.toSpec e = true := hact ▸ ha
      have hs2 : v.toSpec.slashed = false := hs
      simp [wOf, contrib, ha', hs2]
      rfl
    obtain ⟨t', et, hat⟩ := add_flags_ok t fl v.effective_balance R m ht (fun g hg hf => by
      have := hfit g hg; rw [hw, hf] at this; simpa using this)
    refine ⟨t', by simp [ha, et], ?_⟩
    rw [hat]; apply tri_congr; intro g _
    rw [hw]; by_cases hf : Spec.has_flag fl.val g <;> simp [hf]
  · have hw : ∀ g, wOf v.toSpec fl.val e g = 0 := by
      intro g; rw [hact] at ha; simp [wOf, contrib, ha]
    refine ⟨t, by simp [ha], ?_⟩
    rw [ht]; apply tri_congr; intro g _; simp [hw]

theorem lhInitLoop_ok (pe ce m : Nat) (vs : List LhValidator) (cs ps : List U8)
    (hcl : cs.length = vs.length) (hpl : ps.length = vs.length)
    (pt ct : Array Bal 3#usize) (Rp Rc : Nat → Nat)
    (hpt : ab pt.val = tri Rp m) (hct : ab ct.val = tri Rc m)
    (hfp : ∀ g < 3, Rp g + flagSum (vs.map LhValidator.toSpec) (ps.map (·.val)) pe g ≤ U64.max)
    (hfc : ∀ g < 3, Rc g + flagSum (vs.map LhValidator.toSpec) (cs.map (·.val)) ce g ≤ U64.max) :
    ∃ pt' ct', lhInitLoop pe ce ((vs.zip cs).zip ps) pt ct = ok (.Ok (pt', ct')) ∧
      ab pt'.val = tri (fun g => Rp g + flagSum (vs.map LhValidator.toSpec) (ps.map (·.val)) pe g) m ∧
      ab ct'.val = tri (fun g => Rc g + flagSum (vs.map LhValidator.toSpec) (cs.map (·.val)) ce g) m := by
  induction vs generalizing cs ps pt ct Rp Rc with
  | nil =>
    cases cs <;> cases ps <;> simp at hcl hpl
    exact ⟨pt, ct, rfl, by simpa [flagSum] using hpt, by simpa [flagSum] using hct⟩
  | cons v vs ih =>
    cases cs with
    | nil => simp at hcl
    | cons cf cs =>
    cases ps with
    | nil => simp at hpl
    | cons pf ps =>
    simp only [List.length_cons, Nat.add_right_cancel_iff] at hcl hpl
    simp only [List.map_cons, flagSum_cons] at hfp hfc ⊢
    simp only [List.zip_cons_cons]
    by_cases hs : v.slashed
    · have hw : ∀ p e g, wOf v.toSpec p e g = 0 := by
        intro p e g; simp [wOf, contrib, LhValidator.toSpec, hs]
      simp only [hw, Nat.zero_add] at hfp hfc ⊢
      simp only [lhInitLoop, hs, if_true]
      exact ih cs ps hcl hpl pt ct Rp Rc hpt hct hfp hfc
    · have hs' : v.slashed = false := by simpa using hs
      obtain ⟨ct1, e1, a1⟩ := init_step_ok ct v cf ce Rc m hs' hct
        (fun g hg => by have := hfc g hg; omega)
      obtain ⟨pt1, e2, a2⟩ := init_step_ok pt v pf pe Rp m hs' hpt
        (fun g hg => by have := hfp g hg; omega)
      obtain ⟨pt', ct', e3, a3, a4⟩ := ih cs ps hcl hpl pt1 ct1 _ _ a2 a1
        (fun g hg => by have := hfp g hg; omega)
        (fun g hg => by have := hfc g hg; omega)
      refine ⟨pt', ct', ?_, ?_, ?_⟩
      · simp only [lhInitLoop, hs', Bool.false_eq_true, if_false]
        rw [e1]; simp only [bind_tc_ok]; rw [e2]; simp only [bind_tc_ok]; exact e3
      · rw [a3]; apply tri_congr; intro g _; omega
      · rw [a4]; apply tri_congr; intro g _; omega

theorem lhNewTotals_ok (incr : U64) :
    ∃ t, lhNewTotals incr = ok t ∧ ab t.val = tri (fun _ => 0) incr.val :=
  ⟨_, rfl, by simp [ab, tri]⟩

/-- The build gives a coherent cache, and it does not fail when each total fits in a `u64`. -/
theorem build_coherent (incr : U64) (s : LhState)
    (hcl : s.current_epoch_participation.length = s.validators.length)
    (hpl : s.previous_epoch_participation.length = s.validators.length)
    (hfit : ∀ g < 3,
      flagSum s.toSpec.validators s.toSpec.previous_epoch_participation
        (Spec.get_previous_epoch s.toSpec) g ≤ U64.max ∧
      flagSum s.toSpec.validators s.toSpec.current_epoch_participation
        s.toSpec.current_epoch g ≤ U64.max) :
    ∃ c, lhInitialize incr s = ok (.Ok c) ∧ Coherent incr.val s.toSpec c := by
  obtain ⟨z, ez, az⟩ := lhNewTotals_ok incr
  have hpe : lhPreviousEpoch s.current_epoch = Spec.get_previous_epoch s.toSpec := by
    simp [lhPreviousEpoch, Spec.get_previous_epoch, Spec.get_current_epoch, LhState.toSpec,
      Spec.GENESIS_EPOCH]
  obtain ⟨pt, ct, e, a1, a2⟩ := lhInitLoop_ok (lhPreviousEpoch s.current_epoch) s.current_epoch
    incr.val s.validators s.current_epoch_participation s.previous_epoch_participation hcl hpl
    z z _ _ az az
    (fun g hg => by have := (hfit g hg).1; rw [hpe]; simpa [LhState.toSpec] using this)
    (fun g hg => by have := (hfit g hg).2; simpa [LhState.toSpec] using this)
  refine ⟨⟨s.current_epoch, pt, ct⟩, ?_, ⟨⟨rfl, ?_⟩, ?_⟩⟩
  · simp [lhInitialize, ez, e]
  · rw [a2]; apply tri_congr; intro g _; simp [LhState.toSpec]
  · simp only [PreviousCoherent]
    rw [a1, ← hpe]; apply tri_congr; intro g _; simp [LhState.toSpec]

end

end CacheProofs.PB
