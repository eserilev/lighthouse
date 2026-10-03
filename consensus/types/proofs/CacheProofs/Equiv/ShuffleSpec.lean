import Mathlib
import CacheProofs.Spec.CommitteeCache

/-!
# `compute_shuffled_permutation` in closed form

`sigma r` is one round of the swap-or-not shuffle on one index. `csiFold` applies the rounds
`0, 1, ..., R - 1` in order. `compute_shuffled_permutation_eq` proves that the reference
returns `[csiFold i for i in range(n)]` for `0 < n ≤ 2^24` and `R ≤ 256`.

`sha256` is any function that returns 32 bytes.
-/

namespace CacheProofs.Shuffle

open CacheProofs.Spec CacheProofs.Spec.CommitteeCache

/-- `uint_to_bytes(Uint32(x))` for `x < 2^32`. -/
def le4 (x : Nat) : List Nat := [x % 256, x / 256 % 256, x / 65536 % 256, x / 16777216 % 256]

/-- The pivot of round `r`. -/
def pivotOf (sha256 : List Nat → List Nat) (seed : List Nat) (n r : Nat) : Nat :=
  bytes_to_uint64 ((sha256 (seed ++ [r])).take 8) % n

/-- The bit of round `r` at `position`. -/
def bitOf (sha256 : List Nat → List Nat) (seed : List Nat) (r position : Nat) : Nat :=
  (sha256 (seed ++ [r] ++ le4 (position / 256))).getD (position % 256 / 8) 0 / 2 ^ (position % 8) % 2

/-- One round of the shuffle on index `x`. -/
def sigma (sha256 : List Nat → List Nat) (seed : List Nat) (n r x : Nat) : Nat :=
  let flip := (pivotOf sha256 seed n r + n - x) % n
  if bitOf sha256 seed r (max x flip) ≠ 0 then flip else x

/-- Rounds `0, ..., R - 1` on index `x`. -/
def csiFold (sha256 : List Nat → List Nat) (seed : List Nat) (n R x : Nat) : Nat :=
  (List.range R).foldl (fun y r => sigma sha256 seed n r y) x

theorem sigma_lt (sha256 : List Nat → List Nat) (seed : List Nat) (n r x : Nat) (hn : 0 < n)
    (hx : x < n) : sigma sha256 seed n r x < n := by
  unfold sigma
  dsimp only
  split
  · exact Nat.mod_lt _ hn
  · exact hx

theorem foldl_sigma_lt (sha256 : List Nat → List Nat) (seed : List Nat) (n : Nat) (hn : 0 < n)
    (rs : List Nat) (x : Nat) (hx : x < n) :
    rs.foldl (fun y r => sigma sha256 seed n r y) x < n := by
  induction rs generalizing x with
  | nil => exact hx
  | cons r rs ih => exact ih _ (sigma_lt _ _ _ _ _ hn hx)

theorem csiFold_lt (sha256 : List Nat → List Nat) (seed : List Nat) (n R x : Nat) (hn : 0 < n)
    (hx : x < n) : csiFold sha256 seed n R x < n :=
  foldl_sigma_lt _ _ _ hn _ _ hx

/-- The inner loop body sets `indices[i]` to `sigma r indices[i]`. -/
theorem foldl_set_sigma (f : Nat → Nat) (l : List Nat) (k : Nat) (hk : k ≤ l.length) :
    (List.range k).foldl (fun a i => a.set i (f (a.getD i 0))) l =
      (l.take k).map f ++ l.drop k := by
  induction k with
  | zero => simp
  | succ k ih =>
    rw [List.range_succ, List.foldl_append, ih (by omega)]
    simp only [List.foldl_cons, List.foldl_nil]
    have hlt : k < l.length := by omega
    apply List.ext_getElem
    · simp; omega
    · intro i h1 h2
      simp only [List.length_set, List.length_append, List.length_map, List.length_take,
        List.length_drop] at h1
      rw [List.getElem_set]
      split
      · rename_i hik
        subst hik
        simp [List.getD_eq_getElem?_getD, hlt, Nat.min_eq_left (Nat.le_of_lt hlt)]
      · rename_i hik
        rw [List.getElem_append, List.getElem_append]
        by_cases hi : i < k
        · simp [hi, show i < k + 1 by omega, Nat.min_eq_left (Nat.le_of_lt hlt),
            Nat.min_eq_left hk]
        · have hi' : ¬ i < k + 1 := by omega
          simp [hi, hi', Nat.min_eq_left (Nat.le_of_lt hlt), Nat.min_eq_left hk]
          congr 1
          omega

@[simp] theorem except_bind_ok_eq {ε α β : Type} (a : α) (f : α → Except ε β) :
    (Except.ok a >>= f) = f a := rfl

theorem forIn_yield {ε α β : Type} (l : List α) (b : β)
    (f : α → β → Except ε (ForInStep β)) (g : β → α → β) (P : β → Prop) (hb : P b)
    (hf : ∀ x ∈ l, ∀ a, P a → f x a = .ok (.yield (g a x)) ∧ P (g a x)) :
    forIn l b f = .ok (l.foldl g b) := by
  induction l generalizing b with
  | nil => rfl
  | cons x l ih =>
    obtain ⟨h1, h2⟩ := hf x (by simp) b hb
    rw [List.forIn_cons, h1]
    exact ih _ h2 (fun y hy a ha => hf y (by simp [hy]) a ha)

theorem foldl_map_sigma (sha256 : List Nat → List Nat) (seed : List Nat) (n : Nat)
    (rs : List Nat) (l : List Nat) :
    rs.foldl (fun l r => l.map (sigma sha256 seed n r)) l =
      l.map (fun x => rs.foldl (fun y r => sigma sha256 seed n r y) x) := by
  induction rs generalizing l with
  | nil => simp
  | cons r rs ih => simp [ih, List.map_map, Function.comp_def]

/-- Valid shuffle state: `n` indices, each below `n`. -/
def ValidIndices (n : Nat) (l : List Nat) : Prop := l.length = n ∧ ∀ y ∈ l, y < n

theorem compute_shuffled_permutation_eq (p : Preset) (sha256 : List Nat → List Nat)
    (hsha : ∀ l, (sha256 l).length = 32) (seed : List Nat) (n : Nat) (hn : 0 < n)
    (hn24 : n ≤ 16777216) (hR : p.SHUFFLE_ROUND_COUNT ≤ 256) :
    compute_shuffled_permutation p sha256 n seed =
      .ok ((List.range n).map (csiFold sha256 seed n p.SHUFFLE_ROUND_COUNT)) := by
  unfold compute_shuffled_permutation
  dsimp only
  rw [forIn_yield _ _ _ (fun l r => l.map (sigma sha256 seed n r)) (ValidIndices n)]
  · show Except.ok _ = _
    rw [foldl_map_sigma]
    rfl
  · exact ⟨by simp, fun y hy => by simpa using hy⟩
  · intro r hr a ha
    rw [List.mem_range] at hr
    have hr' : r < 256 := by omega
    have hp : pivotOf sha256 seed n r < n := Nat.mod_lt _ hn
    simp only [uint8ToBytes, hr', if_true, uint64Mod, Nat.ne_of_gt hn, if_false, pure_bind]
    rw [forIn_yield _ _ _ (fun a i => a.set i (sigma sha256 seed n r (a.getD i 0)))
      (ValidIndices n) ha]
    · have hlen := ha.1
      rw [foldl_set_sigma _ _ _ (by omega), List.take_of_length_le (by omega),
        List.drop_of_length_le (by omega), List.append_nil]
      refine ⟨rfl, by simp [hlen], ?_⟩
      intro y hy
      obtain ⟨x, hx, rfl⟩ := List.mem_map.mp hy
      exact sigma_lt _ _ _ _ _ hn (ha.2 x hx)
    · intro i hi b hb
      rw [List.mem_range] at hi
      have hib : i < b.length := by rw [hb.1]; exact hi
      have hidx : b.getD i 0 < n := hb.2 _ (by
        rw [List.getD_eq_getElem?_getD, List.getElem?_eq_getElem hib]; exact List.getElem_mem hib)
      have hget : listGet b i = .ok (b.getD i 0) := by
        simp [listGet, List.getElem?_eq_getElem hib, List.getD_eq_getElem?_getD]; rfl
      set x := b.getD i 0 with hx
      set flip := (pivotOf sha256 seed n r + n - x) % n with hflip
      have hflipn : flip < n := Nat.mod_lt _ hn
      have hpos : max x flip < n := max_lt hidx hflipn
      have hbucket : max x flip / 256 < 4294967296 := by omega
      have hsrc : max x flip % 256 / 8 < (sha256 (seed ++ [r] ++ le4 (max x flip / 256))).length := by
        rw [hsha]; omega
      have hbyte : listGet (sha256 (seed ++ [r] ++ le4 (max x flip / 256))) (max x flip % 256 / 8) =
          .ok ((sha256 (seed ++ [r] ++ le4 (max x flip / 256))).getD (max x flip % 256 / 8) 0) := by
        unfold listGet; rw [List.getD_eq_getElem?_getD, List.getElem?_eq_getElem hsrc]; rfl
      simp only [hget, except_bind_ok_eq]
      rw [show bytes_to_uint64 (List.take 8 (sha256 (seed ++ [r]))) % n =
        pivotOf sha256 seed n r from rfl]
      have h1 : pivotOf sha256 seed n r + n < UINT64_SIZE := by unfold UINT64_SIZE; omega
      simp only [uint64Add, h1, if_true, pure_bind, uint64Sub,
        show x ≤ pivotOf sha256 seed n r + n by omega]
      rw [← hflip]
      simp only [uint32ToBytes, hbucket, if_true, pure_bind]
      rw [show [max x flip / 256 % 256, max x flip / 256 / 256 % 256,
        max x flip / 256 / 65536 % 256, max x flip / 256 / 16777216 % 256] =
        le4 (max x flip / 256) from rfl, hbyte]
      refine ⟨rfl, by simp [hb.1], ?_⟩
      intro y hy
      rcases List.mem_or_eq_of_mem_set hy with h | h
      · exact hb.2 y h
      · rw [h]; exact sigma_lt _ _ _ _ _ hn hidx

end CacheProofs.Shuffle
