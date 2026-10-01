import Mathlib.Tactic

/-!
# The schedule walk over natural numbers

Both Rust functions walk the schedule from the genesis entry to the newest entry. The walk
state is the start slot `s`, the start time `t` and the slot duration `d` of the current
segment. `es` lists the start slot and the slot duration of each entry, newest first, as the
Rust slice does. Index `i` means that the entries `es[0]` to `es[i - 1]` are still ahead.

`walkTime` and `walkSlot` follow the two Rust loops step by step, over natural numbers and
without overflow. This file proves the floor and round trip properties for them.
-/

namespace SlotScheduleProofs

/-- The entry at index `i`, as `(start slot, slot duration)`. -/
def entryAt (es : List (Nat × Nat)) (i : Nat) : Nat × Nat := es.getD i (0, 0)

/-- The time of `slot`, walking the entries `i - 1` down to `0` from segment `(s, t, d)`. -/
def walkTime (es : List (Nat × Nat)) (slot : Nat) : Nat → Nat → Nat → Nat → Nat
  | 0, s, t, d => t + (slot - s) * d
  | i + 1, s, t, d =>
    if slot ≤ (entryAt es i).1 then t + (slot - s) * d
    else walkTime es slot i (entryAt es i).1 (t + ((entryAt es i).1 - s) * d) (entryAt es i).2

/-- The slot at `time`, walking the entries `i - 1` down to `0` from segment `(s, t, d)`. -/
def walkSlot (es : List (Nat × Nat)) (time : Nat) : Nat → Nat → Nat → Nat → Nat
  | 0, s, t, d => s + (time - t) / d
  | i + 1, s, t, d =>
    if time < t + ((entryAt es i).1 - s) * d then s + (time - t) / d
    else walkSlot es time i (entryAt es i).1 (t + ((entryAt es i).1 - s) * d) (entryAt es i).2

/-- The entries ahead of the walk start after `s`, in increasing slot order, and every slot
    duration is positive. -/
def Chain (es : List (Nat × Nat)) : Nat → Nat → Nat → Prop
  | 0, _, d => 0 < d
  | i + 1, s, d => 0 < d ∧ i < es.length ∧ s < (entryAt es i).1 ∧
      Chain es i (entryAt es i).1 (entryAt es i).2

theorem Chain.pos {es : List (Nat × Nat)} {i s d : Nat} (h : Chain es i s d) : 0 < d := by
  cases i <;> simp only [Chain] at h
  · exact h
  · exact h.1

theorem walkTime_start {es : List (Nat × Nat)} :
    ∀ {i s t d : Nat}, Chain es i s d → walkTime es s i s t d = t
  | 0, _, _, _, _ => by simp [walkTime]
  | i + 1, s, t, d, h => by
    obtain ⟨-, -, hs, -⟩ := h
    simp [walkTime, hs.le]

/-- Past the next entry, the walk from `(s, t, d)` agrees with the walk from that entry. -/
theorem walkTime_succ_of_le {es : List (Nat × Nat)} {i s t d slot : Nat}
    (h : Chain es (i + 1) s d) (hslot : (entryAt es i).1 ≤ slot) :
    walkTime es slot (i + 1) s t d =
      walkTime es slot i (entryAt es i).1 (t + ((entryAt es i).1 - s) * d) (entryAt es i).2 := by
  obtain ⟨-, -, -, hc⟩ := h
  rcases hslot.lt_or_eq with hlt | heq
  · simp [walkTime, Nat.not_le.mpr hlt]
  · subst heq
    simp [walkTime, walkTime_start hc]

theorem walkTime_lt_succ {es : List (Nat × Nat)} :
    ∀ {i s t d x : Nat}, Chain es i s d → s ≤ x → walkTime es x i s t d < walkTime es (x + 1) i s t d
  | 0, s, t, d, x, h, hx => by
    have hd := h.pos
    simp only [walkTime]
    have : x + 1 - s = (x - s) + 1 := by omega
    rw [this, Nat.add_mul]; omega
  | i + 1, s, t, d, x, h, hx => by
    have hd := h.pos
    by_cases hle : x + 1 ≤ (entryAt es i).1
    · have hle' : x ≤ (entryAt es i).1 := by omega
      simp only [walkTime, hle, hle', if_true]
      have : x + 1 - s = (x - s) + 1 := by omega
      rw [this, Nat.add_mul]; omega
    · have hge : (entryAt es i).1 ≤ x := by omega
      rw [walkTime_succ_of_le h hge, walkTime_succ_of_le h (by omega)]
      exact walkTime_lt_succ h.2.2.2 hge

theorem walkTime_mono {es : List (Nat × Nat)} {i s t d x : Nat} (h : Chain es i s d) (hx : s ≤ x) :
    ∀ {y : Nat}, x ≤ y → walkTime es x i s t d ≤ walkTime es y i s t d := by
  intro y hy
  induction y, hy using Nat.le_induction with
  | base => exact le_rfl
  | succ y hxy ih => exact ih.trans (walkTime_lt_succ h (hx.trans hxy)).le

theorem le_walkTime {es : List (Nat × Nat)} {i s t d x : Nat} (h : Chain es i s d) (hx : s ≤ x) :
    t ≤ walkTime es x i s t d := by
  have := walkTime_mono (t := t) h le_rfl hx
  rwa [walkTime_start h] at this

theorem le_walkSlot {es : List (Nat × Nat)} :
    ∀ {i s t d y : Nat}, Chain es i s d → s ≤ walkSlot es y i s t d
  | 0, _, _, _, _, _ => by simp [walkSlot]
  | i + 1, s, t, d, y, h => by
    simp only [walkSlot]
    split
    · exact Nat.le_add_right _ _
    · exact h.2.2.1.le.trans (le_walkSlot h.2.2.2)

/-- The segment start slot is at most its start time, because every slot lasts at least 1ms. -/
theorem walkSlot_le {es : List (Nat × Nat)} :
    ∀ {i s t d y : Nat}, Chain es i s d → s ≤ t → t ≤ y → walkSlot es y i s t d ≤ y
  | 0, s, t, d, y, h, hst, hty => by
    simp only [walkSlot]
    have := Nat.div_le_self (y - t) d
    generalize (y - t) / d = q at *
    omega
  | i + 1, s, t, d, y, h, hst, hty => by
    have hd := h.pos
    simp only [walkSlot]
    split
    · have := Nat.div_le_self (y - t) d
      generalize (y - t) / d = q at *
      omega
    · rename_i hge
      apply walkSlot_le h.2.2.2
      · have : (entryAt es i).1 - s ≤ ((entryAt es i).1 - s) * d := Nat.le_mul_of_pos_right _ hd
        omega
      · omega

theorem segment_floor {t d y : Nat} (hd : 0 < d) (hty : t ≤ y) :
    t + (y - t) / d * d ≤ y ∧ y < t + ((y - t) / d + 1) * d := by
  have h1 := Nat.div_mul_le_self (y - t) d
  have h2 := Nat.lt_div_mul_add (a := y - t) hd
  rw [Nat.add_mul, one_mul]
  constructor <;> omega

/-- The floor property of the walk: the slot at `y` starts at or before `y`, and the next slot
    starts after `y`. -/
theorem walk_floor {es : List (Nat × Nat)} :
    ∀ {i s t d y : Nat}, Chain es i s d → t ≤ y →
      walkTime es (walkSlot es y i s t d) i s t d ≤ y ∧
      y < walkTime es (walkSlot es y i s t d + 1) i s t d
  | 0, s, t, d, y, h, hty => by
    have hd := h.pos
    have hfl := segment_floor hd hty
    simp only [walkSlot, walkTime]
    generalize (y - t) / d = q at *
    rw [Nat.add_sub_cancel_left, Nat.add_assoc, Nat.add_sub_cancel_left]
    exact hfl
  | i + 1, s, t, d, y, h, hty => by
    have hd := h.pos
    have hs := h.2.2.1
    have hc := h.2.2.2
    simp only [walkSlot]
    split
    · rename_i hlt
      have hq : (y - t) / d < (entryAt es i).1 - s := by
        rw [Nat.div_lt_iff_lt_mul hd]; omega
      have hfl := segment_floor hd hty
      generalize (y - t) / d = q at *
      have hx1 : s + q + 1 ≤ (entryAt es i).1 := by omega
      have hx0 : s + q ≤ (entryAt es i).1 := by omega
      simp only [walkTime, hx1, hx0, if_true]
      rw [Nat.add_sub_cancel_left, Nat.add_assoc, Nat.add_sub_cancel_left]
      exact hfl
    · rename_i hge
      have hx := le_walkSlot (t := t + ((entryAt es i).1 - s) * d) (y := y) hc
      rw [walkTime_succ_of_le h hx, walkTime_succ_of_le h (by omega)]
      exact walk_floor hc (by omega)

/-- The round trip of the walk: the slot at the time of `x` is `x`. -/
theorem walk_round_trip {es : List (Nat × Nat)} {i s t d x : Nat} (h : Chain es i s d)
    (hx : s ≤ x) : walkSlot es (walkTime es x i s t d) i s t d = x := by
  obtain ⟨h1, h2⟩ := walk_floor h (le_walkTime (t := t) h hx)
  have hx' := le_walkSlot (t := t) (y := walkTime es x i s t d) h
  generalize walkSlot es (walkTime es x i s t d) i s t d = x' at *
  rcases lt_trichotomy x' x with hlt | heq | hgt
  · have := walkTime_mono (t := t) h (show s ≤ x' + 1 by omega) (show x' + 1 ≤ x by omega)
    omega
  · exact heq
  · have h3 := walkTime_mono (t := t) h (show s ≤ x + 1 by omega) (show x + 1 ≤ x' by omega)
    have h4 := walkTime_lt_succ (t := t) h hx
    omega

/-! ## The walk against the direct definitions -/

/-- One step of the direct time fold, over `(time, end slot)` and an entry
    `(start slot, slot duration)`. -/
def foldStep (acc : Nat × Nat) (entry : Nat × Nat) : Nat × Nat :=
  if entry.1 < acc.2 then (acc.1 + (acc.2 - entry.1) * entry.2, entry.1) else acc

/-- The slot at `time` in one entry, if the entry starts at or before `time`. `T` gives the start
    time of a slot. -/
def slotIn (T : Nat → Nat) (time : Nat) (entry : Nat × Nat) : Option Nat :=
  if T entry.1 ≤ time then some (entry.1 + (time - T entry.1) / entry.2) else none

theorem walkTime_add {es : List (Nat × Nat)} {x : Nat} :
    ∀ {i s t d c : Nat}, walkTime es x i s (t + c) d = walkTime es x i s t d + c
  | 0, s, t, d, c => by simp only [walkTime]; omega
  | i + 1, s, t, d, c => by
    simp only [walkTime]
    split
    · omega
    · rw [show t + c + ((entryAt es i).1 - s) * d = t + ((entryAt es i).1 - s) * d + c by omega]
      exact walkTime_add

theorem Chain.lt_ahead {es : List (Nat × Nat)} :
    ∀ {i s d : Nat}, Chain es i s d → ∀ j < i, s < (entryAt es j).1
  | 0, _, _, _, j, hj => absurd hj (Nat.not_lt_zero j)
  | i + 1, s, d, h, j, hj => by
    rcases (show j < i ∨ j = i by omega) with hj' | rfl
    · exact h.2.2.1.trans (Chain.lt_ahead h.2.2.2 j hj')
    · exact h.2.2.1

theorem take_succ_entryAt {es : List (Nat × Nat)} {i : Nat} (h : i < es.length) :
    es.take (i + 1) = es.take i ++ [entryAt es i] := by
  rw [List.take_add_one, List.getElem?_eq_getElem h, entryAt, List.getD_eq_getElem?_getD,
    List.getElem?_eq_getElem h]
  rfl

theorem mem_take_entryAt {es : List (Nat × Nat)} {i : Nat} {q : Nat × Nat} (hq : q ∈ es.take i) :
    ∃ j < i, q = entryAt es j := by
  obtain ⟨j, hj, hjq⟩ := List.getElem_of_mem hq
  simp only [List.length_take] at hj
  refine ⟨j, by omega, ?_⟩
  rw [← hjq, List.getElem_take, entryAt, List.getD_eq_getElem?_getD,
    List.getElem?_eq_getElem (by omega)]
  rfl

theorem fold_skip {x : Nat} :
    ∀ {Q : List (Nat × Nat)} {t : Nat}, (∀ q ∈ Q, x ≤ q.1) → Q.foldl foldStep (t, x) = (t, x)
  | [], _, _ => rfl
  | q :: Q, t, h => by
    rw [List.foldl_cons, foldStep, if_neg (Nat.not_lt.mpr (h q (List.mem_cons_self ..)))]
    exact fold_skip fun q' hq' => h q' (List.mem_cons_of_mem _ hq')

/-- The direct time fold over the entries ahead and the current segment is the walk. -/
theorem fold_eq_walkTime {es : List (Nat × Nat)} {x : Nat} :
    ∀ {i s t d : Nat}, Chain es i s d → s ≤ x →
      (es.take i ++ [(s, d)]).foldl foldStep (t, x) = (walkTime es x i s t d, s)
  | 0, s, t, d, _, hsx => by
    simp only [List.take_zero, List.nil_append, List.foldl_cons, List.foldl_nil, foldStep, walkTime]
    split
    · rfl
    · have : s = x := by omega
      subst this; simp
  | i + 1, s, t, d, h, hsx => by
    obtain ⟨hd, hi, hs, hc⟩ := h
    rw [take_succ_entryAt hi, List.foldl_append (l := es.take i ++ [entryAt es i])]
    simp only [walkTime]
    by_cases hxe : x ≤ (entryAt es i).1
    · rw [if_pos hxe, fold_skip (Q := es.take i ++ [entryAt es i])]
      · simp only [List.foldl_cons, List.foldl_nil, foldStep]
        split
        · rfl
        · have : s = x := by omega
          subst this; simp
      · intro q hq
        rcases List.mem_append.mp hq with hq | hq
        · obtain ⟨j, hj, rfl⟩ := mem_take_entryAt hq
          exact hxe.trans (Chain.lt_ahead hc j hj).le
        · simp only [List.mem_singleton] at hq; subst hq; exact hxe
    · rw [if_neg hxe]
      have ih := fold_eq_walkTime (t := t) (x := x) hc (by omega)
      rw [Prod.mk.eta] at ih
      rw [ih]
      simp only [List.foldl_cons, List.foldl_nil, foldStep, if_pos hs]
      rw [walkTime_add]

/-- The direct slot search over the entries ahead and the current segment is the walk. -/
theorem findSome_eq_walkSlot {es : List (Nat × Nat)} {y : Nat} {T : Nat → Nat} :
    ∀ {i s t d : Nat}, Chain es i s d → (∀ x, s ≤ x → T x = walkTime es x i s t d) → t ≤ y →
      (es.take i ++ [(s, d)]).findSome? (slotIn T y) = some (walkSlot es y i s t d)
  | 0, s, t, d, h, hT, hty => by
    have hts : T s = t := by rw [hT s le_rfl, walkTime_start h]
    simp [slotIn, walkSlot, hts, hty]
  | i + 1, s, t, d, h, hT, hty => by
    have hc := h.2.2.2
    have hs := h.2.2.1
    have hts : T s = t := by rw [hT s le_rfl, walkTime_start h]
    rw [take_succ_entryAt h.2.1, List.findSome?_append]
    simp only [walkSlot]
    by_cases hy : y < t + ((entryAt es i).1 - s) * d
    · rw [if_pos hy]
      have hnone : (es.take i ++ [entryAt es i]).findSome? (slotIn T y) = none := by
        rw [List.findSome?_eq_none_iff]
        intro q hq
        have hq1 : (entryAt es i).1 ≤ q.1 := by
          rcases List.mem_append.mp hq with hq | hq
          · obtain ⟨j, hj, rfl⟩ := mem_take_entryAt hq
            exact (Chain.lt_ahead hc j hj).le
          · simp only [List.mem_singleton] at hq; subst hq; exact le_rfl
        have hTE : T (entryAt es i).1 = t + ((entryAt es i).1 - s) * d := by
          rw [hT _ hs.le]; simp [walkTime]
        have hmono : T (entryAt es i).1 ≤ T q.1 := by
          rw [hT _ hs.le, hT _ (hs.le.trans hq1)]
          exact walkTime_mono h hs.le hq1
        simp only [slotIn]
        rw [if_neg (by omega)]
      rw [hnone]
      simp [slotIn, hts, hty]
    · rw [if_neg hy]
      have ih := findSome_eq_walkSlot (T := T) (y := y) hc
        (fun x hx => by rw [hT x (hs.le.trans hx), walkTime_succ_of_le h hx]) (by
          have : T (entryAt es i).1 = t + ((entryAt es i).1 - s) * d := by
            rw [hT _ hs.le]; simp [walkTime]
          omega)
      rw [Prod.mk.eta] at ih
      rw [ih]
      rfl

end SlotScheduleProofs
