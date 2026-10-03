import CacheProofs.Generated
import CacheProofs.Equiv.ShuffleSpec
import CacheProofs.Equiv.CommitteeCache

/-!
# `shuffle_list` equals the spec shuffle

The main theorem is `shuffling_spec`. The pure function `shuffling` (backwards `shuffle_list_with`)
returns a list whose entry `i` is `indices[compute_shuffled_index(i, len(indices), seed)]`. This is
`IsShuffling`, the hypothesis of the `CommitteeCache` theorems.

The hash function is the trait instance `inst`. `HashLink inst sha256` says that `inst.hash`
computes `sha256` and does not fail. The theorems hold for every such pair.

Proof outline:

- `bit_at_spec`: the cached hash and byte always belong to the current position `j`.
- `loop1_spec`, `loop2_spec`: the two inner loops swap each pair `(i, j)` with
  `i + j ≡ pivot (mod n)` when the bit of `max(i, j)` is set. So after one round, entry `x`
  holds the old entry `sigma r x`.
- `round_spec`, `rounds_loop_spec`: rounds `R - 1, ..., 0` compose to `csiFold`.
- `compute_shuffled_permutation_eq` (in `ShuffleSpec.lean`): the spec gives the same `csiFold`.
-/

open Aeneas Aeneas.Std Aeneas.Std.WP Result types

namespace CacheProofs.Shuffle

theorem spec_exists {α : Type} {x : Result α} {p : α → Prop} (h : x ⦃ v => p v ⦄) :
    ∃ v, x = ok v ∧ p v := by
  cases x with
  | ok v => exact ⟨v, rfl, (spec_ok v).mp h⟩
  | fail e => exact ((spec_fail e).mp h).elim
  | div => exact (spec_div.mp h).elim

theorem masked_bits (l r : Usize) (b : U8) (hb : b.val ≤ 1) :
    let mask := core.num.Usize.wrapping_sub 0#usize (UScalar.cast .Usize b)
    (l ^^^ ((l ^^^ r) &&& mask) = if b.val = 1 then r else l) ∧
    (r ^^^ ((l ^^^ r) &&& mask) = if b.val = 1 then l else r) := by
  intro mask
  have hb' : b = 0#u8 ∨ b = 1#u8 := by
    rcases Nat.le_one_iff_eq_zero_or_eq_one.mp hb with h | h
    · left; exact UScalar.eq_equiv _ _ |>.mpr (by simpa using h)
    · right; exact UScalar.eq_equiv _ _ |>.mpr (by simpa using h)
  rcases hb' with rfl | rfl
  · simp only [mask]
    constructor <;> (rw [UScalar.eq_equiv_bv_eq]; simp)
  · simp only [mask]
    constructor <;> (rw [UScalar.eq_equiv_bv_eq]; simp [BitVec.neg_one_eq_allOnes])
    · rw [← BitVec.xor_assoc, BitVec.xor_self, BitVec.zero_xor]
    · rw [BitVec.xor_comm l.bv, ← BitVec.xor_assoc, BitVec.xor_self, BitVec.zero_xor]

theorem masked_swap_eq (s : Slice Usize) (i j : Usize) (b : U8) (hb : b.val ≤ 1)
    (hi : i.val < s.val.length) (hj : j.val < s.val.length) :
    ∃ s', swap_or_not_shuffle.shuffle_list.masked_swap s i j b = ok s' ∧
      s'.val = if b.val = 1 then (s.val.set i.val s.val[j.val]).set j.val s.val[i.val]
        else s.val := by
  obtain ⟨h1, h2⟩ := masked_bits s.val[i.val] s.val[j.val] b hb
  have ei : s.index_usize i = ok s.val[i.val] := by
    obtain ⟨v, hv, rfl⟩ := spec_exists (Slice.index_usize_spec s i hi); exact hv
  have ej : s.index_usize j = ok s.val[j.val] := by
    obtain ⟨v, hv, rfl⟩ := spec_exists (Slice.index_usize_spec s j hj); exact hv
  unfold swap_or_not_shuffle.shuffle_list.masked_swap
  have hble : b ≤ 1#u8 := by scalar_tac
  simp only [massert, hble, if_true, bind_tc_ok, lift, ei, ej]
  generalize hm : core.num.Usize.wrapping_sub 0#usize (UScalar.cast UScalarTy.Usize b) = m at h1 h2
  rw [h1, h2]
  have eu1 : s.update i (if b.val = 1 then s.val[j.val] else s.val[i.val]) =
      ok (s.set i (if b.val = 1 then s.val[j.val] else s.val[i.val])) := by
    obtain ⟨v, hv, rfl⟩ := spec_exists (Slice.update_spec s i _ hi); exact hv
  rw [eu1]
  simp only [bind_tc_ok]
  have hj' : j.val < (s.set i (if b.val = 1 then s.val[j.val] else s.val[i.val])).length := by
    simp [Slice.length]; exact hj
  obtain ⟨v, hv, hveq⟩ := spec_exists (Slice.update_spec _ j
    (if b.val = 1 then s.val[i.val] else s.val[j.val]) hj')
  refine ⟨v, hv, ?_⟩
  rw [hveq, Slice.set_val_eq, Slice.set_val_eq]
  split
  · rfl
  · simp

abbrev Buf := swap_or_not_shuffle.shuffle_list.Buf

/-- The bytes of an array, as numbers. -/
def bytes {n : Usize} (a : Array U8 n) : List Nat := a.val.map (·.val)

theorem seed_size_eq : swap_or_not_shuffle.shuffle_list.SEED_SIZE = 32#usize := by
  unfold swap_or_not_shuffle.shuffle_list.SEED_SIZE; rfl

theorem pivot_view_size_eq : swap_or_not_shuffle.shuffle_list.PIVOT_VIEW_SIZE = ok 33#usize := by
  unfold swap_or_not_shuffle.shuffle_list.PIVOT_VIEW_SIZE
  rw [seed_size_eq]
  unfold swap_or_not_shuffle.shuffle_list.ROUND_SIZE
  obtain ⟨z, hz, hzv⟩ := spec_exists (Usize.add_spec (x := 32#usize) (y := 1#usize) (by scalar_tac))
  rw [hz]; congr 1; scalar_tac

theorem buf_new_eq (seed : Slice U8) (h : seed.val.length = 32) :
    ∃ b, swap_or_not_shuffle.shuffle_list.Buf.new seed = ok b ∧
      bytes b = seed.val.map (·.val) ++ [0, 0, 0, 0, 0] := by
  unfold swap_or_not_shuffle.shuffle_list.Buf.new
  rw [seed_size_eq]
  obtain ⟨⟨sl, back⟩, hidx, hsl, hsllen, hback⟩ := spec_exists
    (Array.index_mut_SliceIndexRangeUsizeSlice.step (Array.repeat 37#usize 0#u8)
      { start := 0#usize, «end» := 32#usize } (by scalar_tac) (by scalar_tac))
  dsimp only
  rw [hidx]
  simp only [bind_tc_ok, Std.uncurry_apply_pair]
  obtain ⟨s1, hs1, rfl⟩ := spec_exists (core.slice.Slice.copy_from_slice.step_spec
    core.marker.CopyU8 sl seed (by simp [Slice.length] at hsllen ⊢; omega))
  rw [hs1]
  simp only [bind_tc_ok]
  refine ⟨_, rfl, ?_⟩
  simp only [bytes, hback, Array.repeat_val, List.setSlice!, h]
  simp [h]

theorem set_round_eq (b : Buf) (r : U8) :
    ∃ b', swap_or_not_shuffle.shuffle_list.Buf.set_round b r = ok b' ∧
      bytes b' = (bytes b).set 32 r.val := by
  unfold swap_or_not_shuffle.shuffle_list.Buf.set_round
  rw [seed_size_eq]
  obtain ⟨v, hv, rfl⟩ := spec_exists (Array.update_spec b 32#usize r (by simp))
  rw [hv]
  refine ⟨_, rfl, ?_⟩
  simp [bytes]

theorem set_round_spec' (b : Buf) (r : U8) :
    swap_or_not_shuffle.shuffle_list.Buf.set_round b r ⦃ b' =>
      bytes b' = (bytes b).set 32 r.val ⦄ := by
  obtain ⟨b', h, h'⟩ := set_round_eq b r
  rw [h]; exact (spec_ok _).mpr h'

theorem mix_in_position_spec (b : Buf) (x : Usize) :
    swap_or_not_shuffle.shuffle_list.Buf.mix_in_position b x ⦃ b' =>
      bytes b' = ((((bytes b).set 33 (x.val % 256)).set 34 (x.val / 256 % 256)).set 35
        (x.val / 65536 % 256)).set 36 (x.val / 16777216 % 256) ⦄ := by
  unfold swap_or_not_shuffle.shuffle_list.Buf.mix_in_position
  rw [pivot_view_size_eq]
  simp only [bind_tc_ok]
  step*
  all_goals try (rcases System.Platform.numBits_eq with h | h <;> simp [h])
  simp only [bytes, a3_post, a2_post, a1_post, a_post, i10_post, i7_post, i4_post, i1_post,
    Std.Array.set, List.map_set, UScalar.cast_val_eq, i9_post, i6_post, i3_post, i8_post1,
    i5_post1, i2_post1, Nat.shiftRight_eq_div_pow]
  rfl

abbrev HashInst (H : Type) := swap_or_not_shuffle.shuffle_list.ShuffleHash H

/-- `inst.hash` computes `sha256` on every byte slice and does not fail. -/
def HashLink {H : Type} (inst : HashInst H) (sha256 : List Nat → List Nat) : Prop :=
  ∀ s : Slice U8, ∃ d, inst.hash s = ok d ∧ bytes d = sha256 (s.val.map (·.val))

theorem hash_spec {H : Type} (inst : HashInst H) (sha256 : List Nat → List Nat)
    (hH : HashLink inst sha256) (b : Buf) :
    swap_or_not_shuffle.shuffle_list.Buf.hash inst b ⦃ d => bytes d = sha256 (bytes b) ⦄ := by
  unfold swap_or_not_shuffle.shuffle_list.Buf.hash
  obtain ⟨d, hd, hdb⟩ := hH (Array.to_slice b)
  simp only [lift, bind_tc_ok, hd, spec_ok]
  rw [hdb]; rfl

theorem or_shift_step (acc x k : Nat) (hacc : acc < 2 ^ k) (hx : x < 256) (hk : k + 8 ≤ 64) :
    acc ||| (x <<< k % U64.size) = acc + x * 2 ^ k ∧ acc + x * 2 ^ k < 2 ^ (k + 8) := by
  have hpow : 2 ^ (k + 8) = 2 ^ k * 256 := by rw [pow_add]; norm_num
  have hxk : x * 2 ^ k < 2 ^ (k + 8) := by rw [hpow, Nat.mul_comm]; exact Nat.mul_lt_mul_of_pos_left hx (by positivity)
  have h64 : 2 ^ (k + 8) ≤ 2 ^ 64 := Nat.pow_le_pow_right (by norm_num) hk
  have hsz : U64.size = 2 ^ 64 := by rw [U64.size_def]; simp [U64.numBits]
  rw [hsz, Nat.shiftLeft_eq, Nat.mod_eq_of_lt (by omega), Nat.lor_comm,
    ← Nat.shiftLeft_eq, ← Nat.shiftLeft_add_eq_or_of_lt hacc, Nat.shiftLeft_eq]
  constructor
  · ring
  · have : acc + x * 2 ^ k < 2 ^ k + 255 * 2 ^ k := by
      have : x * 2 ^ k ≤ 255 * 2 ^ k := Nat.mul_le_mul_right _ (by omega)
      omega
    rw [hpow]; omega

theorem bytes_to_uint64_take8 (l : List Nat) (h : 8 ≤ l.length) :
    Spec.CommitteeCache.bytes_to_uint64 (l.take 8) =
      l[0] + l[1] * 2 ^ 8 + l[2] * 2 ^ 16 + l[3] * 2 ^ 24 + l[4] * 2 ^ 32 + l[5] * 2 ^ 40 +
        l[6] * 2 ^ 48 + l[7] * 2 ^ 56 := by
  match l, h with
  | a0 :: a1 :: a2 :: a3 :: a4 :: a5 :: a6 :: a7 :: rest, _ =>
    simp [Spec.CommitteeCache.bytes_to_uint64]; ring

theorem raw_pivot_spec {H : Type} (inst : HashInst H) (sha256 : List Nat → List Nat)
    (hH : HashLink inst sha256) (b : Buf) :
    swap_or_not_shuffle.shuffle_list.Buf.raw_pivot inst b ⦃ v =>
      v.val = Spec.CommitteeCache.bytes_to_uint64 ((sha256 ((bytes b).take 33)).take 8) ⦄ := by
  unfold swap_or_not_shuffle.shuffle_list.Buf.raw_pivot
  rw [pivot_view_size_eq]
  simp only [bind_tc_ok]
  step*
  obtain ⟨d, hd, hdb⟩ := hH s
  rw [hd]
  simp only [bind_tc_ok]
  have hdl : d.val.length = 32 := by simp
  step*
  have hlt : ∀ (k : Nat) (h : k < 32), (d.val[k]'(by omega)).val < 256 := fun k h => by
    have := (d.val[k]'(by omega)).hBounds; simpa using this
  have hc : ∀ (x : U8) (y : U64), y = UScalar.cast UScalarTy.U64 x → y.val = x.val := by
    intro x y hy; rw [hy, UScalar.cast_val_eq]
    exact Nat.mod_eq_of_lt (by have := x.hBounds; simp [UScalarTy.numBits] at *; omega)
  have hb : ∀ (x : U8), x.val < 256 := fun x => by have := x.hBounds; simpa using this
  have h0 : i2.val < 2 ^ 8 := by rw [hc _ _ i2_post]; exact hb i1
  have s1 := or_shift_step _ _ 8 h0 (hb i3) (by norm_num)
  have e1 : i6.val = i2.val + i3.val * 2 ^ 8 := by
    rw [i6_post1, UScalar.val_or, i5_post1, hc _ _ i4_post]; exact s1.1
  have s2 := or_shift_step _ _ 16 (e1 ▸ s1.2) (hb i7) (by norm_num)
  have e2 : i10.val = i6.val + i7.val * 2 ^ 16 := by
    rw [i10_post1, UScalar.val_or, i9_post1, hc _ _ i8_post]; exact s2.1
  have s3 := or_shift_step _ _ 24 (e2 ▸ s2.2) (hb i11) (by norm_num)
  have e3 : i14.val = i10.val + i11.val * 2 ^ 24 := by
    rw [i14_post1, UScalar.val_or, i13_post1, hc _ _ i12_post]; exact s3.1
  have s4 := or_shift_step _ _ 32 (e3 ▸ s3.2) (hb i15) (by norm_num)
  have e4 : i18.val = i14.val + i15.val * 2 ^ 32 := by
    rw [i18_post1, UScalar.val_or, i17_post1, hc _ _ i16_post]; exact s4.1
  have s5 := or_shift_step _ _ 40 (e4 ▸ s4.2) (hb i19) (by norm_num)
  have e5 : i22.val = i18.val + i19.val * 2 ^ 40 := by
    rw [i22_post1, UScalar.val_or, i21_post1, hc _ _ i20_post]; exact s5.1
  have s6 := or_shift_step _ _ 48 (e5 ▸ s5.2) (hb i23) (by norm_num)
  have e6 : i26.val = i22.val + i23.val * 2 ^ 48 := by
    rw [i26_post1, UScalar.val_or, i25_post1, hc _ _ i24_post]; exact s6.1
  have s7 := or_shift_step _ _ 56 (e6 ▸ s6.2) (hb i27) (by norm_num)
  have e7 : (i26 ||| i29).val = i26.val + i27.val * 2 ^ 56 := by
    rw [UScalar.val_or, i29_post1, hc _ _ i28_post]; exact s7.1
  rw [e7, e6, e5, e4, e3, e2, e1, hc _ _ i2_post]
  have hsl : s.val.map (·.val) = List.take 33 (bytes b) := by
    rw [s_post1]; simp [List.slice, bytes]
  rw [← hsl, ← hdb, bytes_to_uint64_take8 _ (by simp [bytes])]
  simp [bytes, i1_post, i3_post, i7_post, i11_post, i15_post, i19_post, i23_post, i27_post]

section Sigma

variable (sha256 : List Nat → List Nat) (seed : List Nat) (n r : Nat)

theorem sigma_low (i : Nat) (hp : pivotOf sha256 seed n r < n)
    (hi : i ≤ pivotOf sha256 seed n r - i) :
    sigma sha256 seed n r i =
        (if bitOf sha256 seed r (pivotOf sha256 seed n r - i) ≠ 0
          then pivotOf sha256 seed n r - i else i) ∧
      sigma sha256 seed n r (pivotOf sha256 seed n r - i) =
        (if bitOf sha256 seed r (pivotOf sha256 seed n r - i) ≠ 0
          then i else pivotOf sha256 seed n r - i) := by
  unfold sigma
  dsimp only
  generalize pivotOf sha256 seed n r = p at *
  have hn : 0 < n := by omega
  have f1 : (p + n - i) % n = p - i := by
    rw [show p + n - i = (p - i) + n * 1 by omega, Nat.add_mul_mod_self_left,
      Nat.mod_eq_of_lt (by omega)]
  have f2 : (p + n - (p - i)) % n = i := by
    rw [show p + n - (p - i) = i + n * 1 by omega, Nat.add_mul_mod_self_left,
      Nat.mod_eq_of_lt (by omega)]
  simp only [f1, f2, show max i (p - i) = p - i from max_eq_right hi,
    show max (p - i) i = p - i from max_eq_left hi]
  exact ⟨trivial, trivial⟩

theorem sigma_high (i : Nat) (hp : pivotOf sha256 seed n r < i) (hin : i < n)
    (hi : i ≤ n + pivotOf sha256 seed n r - i) :
    sigma sha256 seed n r i =
        (if bitOf sha256 seed r (n + pivotOf sha256 seed n r - i) ≠ 0
          then n + pivotOf sha256 seed n r - i else i) ∧
      sigma sha256 seed n r (n + pivotOf sha256 seed n r - i) =
        (if bitOf sha256 seed r (n + pivotOf sha256 seed n r - i) ≠ 0
          then i else n + pivotOf sha256 seed n r - i) := by
  unfold sigma
  dsimp only
  generalize pivotOf sha256 seed n r = p at *
  have f1 : (p + n - i) % n = n + p - i := by
    rw [Nat.mod_eq_of_lt (by omega)]; omega
  have f2 : (p + n - (n + p - i)) % n = i := by
    rw [show p + n - (n + p - i) = i by omega, Nat.mod_eq_of_lt hin]
  simp only [f1, f2, show max i (n + p - i) = n + p - i from max_eq_right hi,
    show max (n + p - i) i = n + p - i from max_eq_left hi]
  exact ⟨trivial, trivial⟩

theorem sigma_fixed (x : Nat) (hx : x < n)
    (h : 2 * x = pivotOf sha256 seed n r ∨ 2 * x = n + pivotOf sha256 seed n r) :
    sigma sha256 seed n r x = x := by
  unfold sigma
  have : (pivotOf sha256 seed n r + n - x) % n = x := by
    rcases h with h | h
    · rw [show pivotOf sha256 seed n r + n - x = x + n * 1 by omega, Nat.add_mul_mod_self_left,
        Nat.mod_eq_of_lt hx]
    · rw [show pivotOf sha256 seed n r + n - x = x by omega, Nat.mod_eq_of_lt hx]
  simp only [this]
  split <;> rfl

theorem sigma_le_iff (x : Nat) (hp : pivotOf sha256 seed n r < n) (hx : x < n) :
    sigma sha256 seed n r x ≤ pivotOf sha256 seed n r ↔ x ≤ pivotOf sha256 seed n r := by
  unfold sigma
  dsimp only
  generalize pivotOf sha256 seed n r = p at *
  have hn : 0 < n := by omega
  split
  · by_cases hxp : x ≤ p
    · rw [show (p + n - x) % n = p - x by
        rw [show p + n - x = (p - x) + n * 1 by omega, Nat.add_mul_mod_self_left,
          Nat.mod_eq_of_lt (by omega)]]
      omega
    · rw [show (p + n - x) % n = n + p - x by rw [Nat.mod_eq_of_lt (by omega)]; omega]
      omega
  · exact Iff.rfl

end Sigma

theorem usize_and_255 (j : Usize) : (j &&& 255#usize).val = j.val % 256 := by
  rw [UScalar.val_and, show (255#usize).val = 2 ^ 8 - 1 by simp, Nat.and_two_pow_sub_one_eq_mod]

theorem usize_and_7 (j : Usize) : (j &&& 7#usize).val = j.val % 8 := by
  rw [UScalar.val_and, show (7#usize).val = 2 ^ 3 - 1 by simp, Nat.and_two_pow_sub_one_eq_mod]

theorem u8_and_1 (x : U8) : (x &&& 1#u8).val = x.val % 2 := by
  rw [UScalar.val_and, show (1#u8).val = 2 ^ 1 - 1 by simp, Nat.and_two_pow_sub_one_eq_mod]

/-- The cache of the inner loops holds the hash and the byte for position `c`. -/
def CacheOK (sha256 : List Nat → List Nat) (pre : List Nat) (buf : Buf) (source : Array U8 32#usize)
    (byte_v : U8) (c : Nat) : Prop :=
  bytes buf = pre ++ le4 (c / 256) ∧ bytes source = sha256 (pre ++ le4 (c / 256)) ∧
    byte_v.val = (sha256 (pre ++ le4 (c / 256))).getD (c % 256 / 8) 0

theorem le4_eq (x : Nat) :
    [x % 256, x / 256 % 256, x / 65536 % 256, x / 16777216 % 256] = le4 x := rfl

theorem mix_bytes (pre : List Nat) (hpre : pre.length = 33) (tail : List Nat)
    (htail : tail.length = 4) (x : Nat) :
    ((((pre ++ tail).set 33 (x % 256)).set 34 (x / 256 % 256)).set 35 (x / 65536 % 256)).set 36
      (x / 16777216 % 256) = pre ++ le4 x := by
  match tail, htail with
  | [a, b, c, d], _ =>
    unfold le4
    apply List.ext_getElem
    · simp
    · intro k h1 h2
      simp only [List.length_set, List.length_append, hpre, List.length_cons,
        List.length_nil] at h1
      simp only [List.getElem_set]
      rw [List.getElem_append, List.getElem_append]
      by_cases hk : k < 33
      · simp [hk, show k ≠ 33 by omega, show k ≠ 34 by omega, show k ≠ 35 by omega,
          show k ≠ 36 by omega, hpre]
      · have : k = 33 ∨ k = 34 ∨ k = 35 ∨ k = 36 := by omega
        rcases this with rfl | rfl | rfl | rfl <;> simp [hpre]

theorem bit_at_spec {H : Type} (inst : HashInst H) (sha256 : List Nat → List Nat)
    (hH : HashLink inst sha256) (pre : List Nat) (hpre : pre.length = 33) (buf : Buf)
    (src : Array U8 32#usize) (byv : U8) (c : Nat) (j : Usize)
    (hcache : CacheOK sha256 pre buf src byv c) (hcj : c = j.val ∨ c = j.val + 1) :
    swap_or_not_shuffle.shuffle_list.bit_at inst buf src byv j ⦃ bit buf' src' byv' =>
      bit.val = (sha256 (pre ++ le4 (j.val / 256))).getD (j.val % 256 / 8) 0 / 2 ^ (j.val % 8) % 2 ∧
      CacheOK sha256 pre buf' src' byv' j.val ⦄ := by
  obtain ⟨hb, hs, hbyte⟩ := hcache
  have hand255 := usize_and_255 j
  have hand7 := usize_and_7 j
  unfold swap_or_not_shuffle.shuffle_list.bit_at
  simp only [lift, bind_tc_ok]
  by_cases h255 : j.val % 256 = 255
  · have e255 : (j &&& 255#usize) = 255#usize := by
      rw [UScalar.eq_equiv]; rw [hand255, h255]; simp
    have e7 : (j &&& 7#usize) = 7#usize := by
      rw [UScalar.eq_equiv]; rw [hand7]; simp; omega
    simp only [e255, if_true, e7]
    step as ⟨i1, hi1, hi1b⟩
    case hy => rcases System.Platform.numBits_eq with h | h <;> simp [h]
    step with mix_in_position_spec as ⟨b2, hb2⟩
    step with hash_spec inst sha256 hH as ⟨s2, hs2⟩
    have hb2' : bytes b2 = pre ++ le4 (j.val / 256) := by
      rw [hb2, hb, mix_bytes pre hpre _ (by simp [le4]), hi1, Nat.shiftRight_eq_div_pow]
    rw [hb2'] at hs2
    step*
    all_goals try (rcases System.Platform.numBits_eq with h | h <;> simp [h] <;> done)
    have hx : x.val = j.val % 256 / 8 := by rw [x_post1, h255]; rfl
    have hbv : byte_v1.val = (sha256 (pre ++ le4 (j.val / 256))).getD (j.val % 256 / 8) 0 := by
      have hx32 : x.val < s2.val.length := by simp; omega
      rw [← hs2, ← hx, byte_v1_post]
      simp [bytes, List.getD_eq_getElem?_getD, List.getElem?_eq_getElem hx32]
    have hj8 : j.val % 8 = 7 := by omega
    refine ⟨?_, hb2', hs2, hbv⟩
    rw [u8_and_1, i3_post1, Nat.shiftRight_eq_div_pow, hbv, hj8]
  · have e255 : ¬ (j &&& 255#usize) = 255#usize := by
      intro h; have := congrArg UScalar.val h; rw [hand255] at this; simp at this; omega
    have hcq : c / 256 = j.val / 256 := by omega
    rw [hcq] at hb hs hbyte
    simp only [e255, if_false, bind_tc_ok, Std.uncurry_apply_pair]
    by_cases h7 : j.val % 8 = 7
    · have e7 : (j &&& 7#usize) = 7#usize := by
        rw [UScalar.eq_equiv]; rw [hand7]; simp; omega
      simp only [e7, if_true]
      step*
      all_goals try (rcases System.Platform.numBits_eq with h | h <;> simp [h] <;> done)
      · rw [x_post1, hand255, Nat.shiftRight_eq_div_pow]; simp; omega
      · have hx : x.val = j.val % 256 / 8 := by
          rw [x_post1, hand255, Nat.shiftRight_eq_div_pow]
        have hx32 : x.val < src.val.length := by simp; omega
        have hbv : byte_v1.val = (sha256 (pre ++ le4 (j.val / 256))).getD (j.val % 256 / 8) 0 := by
          rw [← hs, ← hx, byte_v1_post]
          simp [bytes, List.getD_eq_getElem?_getD, List.getElem?_eq_getElem hx32]
        refine ⟨?_, hb, hs, hbv⟩
        rw [u8_and_1, i3_post1, Nat.shiftRight_eq_div_pow, hbv, h7]
    · have e7 : ¬ (j &&& 7#usize) = 7#usize := by
        intro h; have := congrArg UScalar.val h; rw [hand7] at this; simp at this; omega
      have hcr : c % 256 / 8 = j.val % 256 / 8 := by omega
      rw [hcr] at hbyte
      simp only [e7, if_false, bind_tc_ok]
      step*
      refine ⟨?_, hb, hs, hbyte⟩
      rw [u8_and_1, i3_post1, Nat.shiftRight_eq_div_pow, hbyte, hand7]

theorem inv_step (σ : Nat → Nat) (A0 : List Nat) (P Q : Nat → Prop) [DecidablePred P]
    [DecidablePred Q] (k j n : Nat) (f g : Nat → Nat) (bit : Bool)
    (hPk : ¬ P k) (hPj : ¬ P j) (hQ : ∀ y, Q y ↔ P y ∨ y = k ∨ y = j) (hkj : k ≠ j)
    (hf : ∀ y < n, f y = A0.getD (if P y then σ y else y) 0) (hkn : k < n) (hjn : j < n)
    (hg : ∀ y, g y = if bit ∧ y = k then f j else if bit ∧ y = j then f k else f y)
    (hσk : σ k = if bit then j else k) (hσj : σ j = if bit then k else j) :
    ∀ y < n, g y = A0.getD (if Q y then σ y else y) 0 := by
  intro y hy
  rw [hg]
  by_cases hyk : y = k
  · subst hyk
    have hQ' : Q y := (hQ y).mpr (Or.inr (Or.inl rfl))
    rw [if_pos hQ', hσk]
    cases bit
    · simp [hf y hy, hPk]
    · simp [hf j hjn, hPj]
  · by_cases hyj : y = j
    · subst hyj
      have hQ' : Q y := (hQ y).mpr (Or.inr (Or.inr rfl))
      rw [if_pos hQ', hσj]
      cases bit
      · simp [hyk, hf y hy, hPj]
      · simp [hyk, hf k hkn, hPk]
    · have : Q y ↔ P y := by rw [hQ]; simp [hyk, hyj]
      simp only [hyk, hyj, and_false, if_false, hf y hy]
      by_cases hP : P y
      · rw [if_pos hP, if_pos (this.mpr hP)]
      · rw [if_neg hP, if_neg (fun h => hP (this.mp h))]

theorem nats_swap (l : List Usize) (k j : Nat) (hk : k < l.length) (hj : j < l.length)
    (hkj : k ≠ j) (y : Nat) :
    (((l.set k l[j]).set j l[k]).map (·.val)).getD y 0 =
      if y = k then l[j].val else if y = j then l[k].val else (l.map (·.val)).getD y 0 := by
  simp only [List.getD_eq_getElem?_getD, List.getElem?_map, List.getElem?_set]
  by_cases hyk : y = k
  · subst hyk; simp [Ne.symm hkj, hk]
  · by_cases hyj : y = j
    · subst hyj; simp [hyk, hj]
    · simp [Ne.symm hyk, Ne.symm hyj, hyk, hyj]

theorem masked_swap_spec (s : Slice Usize) (i j : Usize) (b : U8) (hb : b.val ≤ 1)
    (hi : i.val < s.val.length) (hj : j.val < s.val.length) :
    swap_or_not_shuffle.shuffle_list.masked_swap s i j b ⦃ s' =>
      s'.val = if b.val = 1 then (s.val.set i.val s.val[j.val]).set j.val s.val[i.val]
        else s.val ⦄ := by
  obtain ⟨s', h, h'⟩ := masked_swap_eq s i j b hb hi hj
  rw [h]; exact (spec_ok _).mpr h'

/-- Entry `x` of a vector, as a number. -/
def nats (v : alloc.vec.Vec Usize) (x : Nat) : Nat := (v.val.map (·.val)).getD x 0

theorem loop1_spec {H : Type} (inst : HashInst H) (sha256 : List Nat → List Nat)
    (hH : HashLink inst sha256)
    (seed : List Nat) (hseed : seed.length = 32) (r n : Nat) (A0 : List Nat)
    (hn24 : n ≤ 16777216)
    (pivot mirror : Usize) (hp : pivot.val = pivotOf sha256 seed n r) (hpn : pivot.val < n)
    (hm : mirror.val = (pivot.val + 1) / 2)
    (input : alloc.vec.Vec Usize) (buf : Buf) (source : Array U8 32#usize) (byte_v : U8)
    (i : Usize) (hi : i.val ≤ mirror.val) (hlen : input.val.length = n)
    (hA : ∀ x < n, nats input x = A0.getD (if x ≤ pivot.val ∧ (x < i.val ∨ pivot.val - i.val < x)
      then sigma sha256 seed n r x else x) 0)
    (hc : ∃ c, (c = pivot.val - i.val ∨ c = pivot.val - i.val + 1) ∧
      CacheOK sha256 (seed ++ [r]) buf source byte_v c) :
    swap_or_not_shuffle.shuffle_list.shuffle_list_with_loop0_loop0 inst input buf pivot mirror
      source byte_v i ⦃ input' buf' =>
        input'.val.length = n ∧
        (∀ x < n, nats input' x = A0.getD (if x ≤ pivot.val then sigma sha256 seed n r x else x) 0) ∧
        (bytes buf').take 33 = seed ++ [r] ∧ (bytes buf').length = 37 ⦄ := by
  unfold swap_or_not_shuffle.shuffle_list.shuffle_list_with_loop0_loop0
  apply loop.spec_decr_nat
    (measure := fun (x : alloc.vec.Vec Usize × Buf × Array U8 32#usize × U8 × Usize) =>
      mirror.val - x.2.2.2.2.val)
    (inv := fun (x : alloc.vec.Vec Usize × Buf × Array U8 32#usize × U8 × Usize) =>
      x.2.2.2.2.val ≤ mirror.val ∧ x.1.val.length = n ∧
      (∀ y < n, nats x.1 y = A0.getD (if y ≤ pivot.val ∧ (y < x.2.2.2.2.val ∨
        pivot.val - x.2.2.2.2.val < y) then sigma sha256 seed n r y else y) 0) ∧
      ∃ c, (c = pivot.val - x.2.2.2.2.val ∨ c = pivot.val - x.2.2.2.2.val + 1) ∧
        CacheOK sha256 (seed ++ [r]) x.2.1 x.2.2.1 x.2.2.2.1 c)
  · rintro ⟨u, b, src, byv, k⟩ ⟨hk, hu, hAu, c, hck, hcache⟩
    dsimp only at hk hu hAu hck hcache ⊢
    unfold swap_or_not_shuffle.shuffle_list.shuffle_list_with_loop0_loop0.body
    by_cases hlt : k.val < mirror.val
    · have hlt' : k < mirror := by scalar_tac
      simp only [hlt', if_true]
      step as ⟨j, hj1, hj2⟩
      have hkj : k.val < j.val := by omega
      step with bit_at_spec inst sha256 hH (seed ++ [r]) (by simp [hseed]) b src byv c j hcache
        (by omega) as ⟨bit, b1, s1, bv1, hbit, hcache1⟩
      simp only [lift, bind_tc_ok, alloc.vec.Vec.deref_mut, Std.uncurry_apply_pair]
      have hbit1 : bit.val ≤ 1 := by rw [hbit]; omega
      have hkl : k.val < (u.val).length := by omega
      have hjl : j.val < (u.val).length := by omega
      step with masked_swap_spec ⟨u.val, u.property⟩ k j bit hbit1 hkl hjl as ⟨sw, hsw⟩
      step as ⟨k1, hk1⟩
      apply (spec_ok _).mpr
      have hσ := sigma_low sha256 seed n r k.val (hp ▸ hpn) (by rw [← hp]; omega)
      rw [← hp, ← hj1] at hσ
      have hbiteq : bitOf sha256 seed r j.val = bit.val := by rw [hbit]; rfl
      rw [hbiteq] at hσ
      refine ⟨by omega, by simp [hsw]; split <;> simp [hu], ?_, ⟨j.val, Or.inr (by omega), hcache1⟩,
        by omega⟩
      apply inv_step (sigma sha256 seed n r) A0 _ _ k.val j.val n (nats u) _ (bit.val = 1)
        (by omega) (by omega) (by intro y; omega) (by omega) hAu (by omega) (by omega)
      · intro y
        simp only [nats, hsw]
        split
        · rw [nats_swap _ _ _ hkl hjl (by omega)]
          rename_i hb1
          by_cases hyk : y = k.val
          · simp [hyk, hb1, List.getD_eq_getElem?_getD, List.getElem?_eq_getElem hjl]
          · by_cases hyj : y = j.val
            · simp [hyj, hb1, show j.val ≠ k.val by omega, List.getD_eq_getElem?_getD,
                List.getElem?_eq_getElem hkl]
            · simp [hyk, hyj]
        · rename_i hb1; simp [hb1]
      · rcases Nat.lt_or_ge bit.val 1 with h | h
        · simp [show bit.val = 0 by omega] at hσ ⊢; exact hσ.1
        · simp [show bit.val = 1 by omega] at hσ ⊢; exact hσ.1
      · rcases Nat.lt_or_ge bit.val 1 with h | h
        · simp [show bit.val = 0 by omega] at hσ ⊢; exact hσ.2
        · simp [show bit.val = 1 by omega] at hσ ⊢; exact hσ.2
    · have hge : ¬ k < mirror := by scalar_tac
      have hkm : k.val = mirror.val := by omega
      simp only [hge, if_false]
      apply (spec_ok _).mpr
      obtain ⟨hb, -, -⟩ := hcache
      refine ⟨hu, ?_, by rw [hb]; exact List.take_left' (by simp [hseed]), by rw [hb]; simp [hseed, le4]⟩
      intro x hx
      rw [hAu x hx, hkm]
      by_cases hxp : x ≤ pivot.val
      · by_cases hproc : x < mirror.val ∨ pivot.val - mirror.val < x
        · simp [hxp, hproc]
        · have h2 : 2 * x = pivotOf sha256 seed n r := by omega
          simp [hxp, hproc, sigma_fixed sha256 seed n r x hx (Or.inl h2)]
      · simp [hxp]
  · exact ⟨hi, hlen, hA, hc⟩

theorem loop2_spec {H : Type} (inst : HashInst H) (sha256 : List Nat → List Nat)
    (hH : HashLink inst sha256)
    (seed : List Nat) (hseed : seed.length = 32) (r n : Nat) (A0 : List Nat)
    (hn24 : n ≤ 16777216)
    (pivot mirror «end» : Usize) (hp : pivot.val = pivotOf sha256 seed n r) (hpn : pivot.val < n)
    (hm : mirror.val = (pivot.val + n + 1) / 2) (hend : «end».val = n - 1)
    (input : alloc.vec.Vec Usize) (buf : Buf) (source : Array U8 32#usize) (byte_v : U8)
    (i : Usize) (hi : i.val ≤ mirror.val) (hi' : pivot.val + 1 ≤ i.val)
    (hlen : input.val.length = n)
    (hA : ∀ x < n, nats input x = A0.getD (if x ≤ pivot.val ∨ (pivot.val < x ∧
      (x < i.val ∨ n + pivot.val - i.val < x)) then sigma sha256 seed n r x else x) 0)
    (hc : ∃ c, (c = n + pivot.val - i.val ∨ c = n + pivot.val - i.val + 1) ∧
      CacheOK sha256 (seed ++ [r]) buf source byte_v c) :
    swap_or_not_shuffle.shuffle_list.shuffle_list_with_loop0_loop1 inst input buf pivot mirror
      «end» source byte_v i ⦃ input' buf' =>
        input'.val.length = n ∧
        (∀ x < n, nats input' x = A0.getD (sigma sha256 seed n r x) 0) ∧
        (bytes buf').take 33 = seed ++ [r] ∧ (bytes buf').length = 37 ⦄ := by
  unfold swap_or_not_shuffle.shuffle_list.shuffle_list_with_loop0_loop1
  apply loop.spec_decr_nat
    (measure := fun (x : alloc.vec.Vec Usize × Buf × Array U8 32#usize × U8 × Usize) =>
      mirror.val - x.2.2.2.2.val)
    (inv := fun (x : alloc.vec.Vec Usize × Buf × Array U8 32#usize × U8 × Usize) =>
      x.2.2.2.2.val ≤ mirror.val ∧ pivot.val + 1 ≤ x.2.2.2.2.val ∧ x.1.val.length = n ∧
      (∀ y < n, nats x.1 y = A0.getD (if y ≤ pivot.val ∨ (pivot.val < y ∧ (y < x.2.2.2.2.val ∨
        n + pivot.val - x.2.2.2.2.val < y)) then sigma sha256 seed n r y else y) 0) ∧
      ∃ c, (c = n + pivot.val - x.2.2.2.2.val ∨ c = n + pivot.val - x.2.2.2.2.val + 1) ∧
        CacheOK sha256 (seed ++ [r]) x.2.1 x.2.2.1 x.2.2.2.1 c)
  · rintro ⟨u, b, src, byv, k⟩ ⟨hk, hk', hu, hAu, c, hck, hcache⟩
    dsimp only at hk hk' hu hAu hck hcache ⊢
    unfold swap_or_not_shuffle.shuffle_list.shuffle_list_with_loop0_loop1.body
    by_cases hlt : k.val < mirror.val
    · have hlt' : k < mirror := by scalar_tac
      simp only [hlt', if_true]
      step as ⟨i1, hi1⟩
      step as ⟨i2, hi2, hi2'⟩
      step as ⟨j, hj1, hj2⟩
      · omega
      have hjv : j.val = n + pivot.val - k.val := by omega
      have hkj : k.val < j.val := by omega
      have hjn : j.val < n := by omega
      step with bit_at_spec inst sha256 hH (seed ++ [r]) (by simp [hseed]) b src byv c j hcache
        (by omega) as ⟨bit, b1, s1, bv1, hbit, hcache1⟩
      simp only [lift, bind_tc_ok, alloc.vec.Vec.deref_mut, Std.uncurry_apply_pair]
      have hbit1 : bit.val ≤ 1 := by rw [hbit]; omega
      have hkl : k.val < (u.val).length := by omega
      have hjl : j.val < (u.val).length := by omega
      step with masked_swap_spec ⟨u.val, u.property⟩ k j bit hbit1 hkl hjl as ⟨sw, hsw⟩
      step as ⟨k1, hk1⟩
      apply (spec_ok _).mpr
      have hσ := sigma_high sha256 seed n r k.val (by omega) (by omega) (by rw [← hp]; omega)
      rw [← hp, show n + pivot.val - k.val = j.val by omega] at hσ
      have hbiteq : bitOf sha256 seed r j.val = bit.val := by rw [hbit]; rfl
      rw [hbiteq] at hσ
      refine ⟨by omega, by omega, by simp [hsw]; split <;> simp [hu], ?_,
        ⟨j.val, Or.inr (by omega), hcache1⟩, by omega⟩
      apply inv_step (sigma sha256 seed n r) A0 _ _ k.val j.val n (nats u) _ (bit.val = 1)
        (by omega) (by omega) (by intro y; omega) (by omega) hAu (by omega) (by omega)
      · intro y
        simp only [nats, hsw]
        split
        · rw [nats_swap _ _ _ hkl hjl (by omega)]
          rename_i hb1
          by_cases hyk : y = k.val
          · simp [hyk, hb1, List.getD_eq_getElem?_getD, List.getElem?_eq_getElem hjl]
          · by_cases hyj : y = j.val
            · simp [hyj, hb1, show j.val ≠ k.val by omega, List.getD_eq_getElem?_getD,
                List.getElem?_eq_getElem hkl]
            · simp [hyk, hyj]
        · rename_i hb1; simp [hb1]
      · rcases Nat.lt_or_ge bit.val 1 with h | h
        · simp [show bit.val = 0 by omega] at hσ ⊢; exact hσ.1
        · simp [show bit.val = 1 by omega] at hσ ⊢; exact hσ.1
      · rcases Nat.lt_or_ge bit.val 1 with h | h
        · simp [show bit.val = 0 by omega] at hσ ⊢; exact hσ.2
        · simp [show bit.val = 1 by omega] at hσ ⊢; exact hσ.2
    · have hge : ¬ k < mirror := by scalar_tac
      have hkm : k.val = mirror.val := by omega
      simp only [hge, if_false]
      apply (spec_ok _).mpr
      obtain ⟨hb, -, -⟩ := hcache
      refine ⟨hu, ?_, by rw [hb]; exact List.take_left' (by simp [hseed]),
        by rw [hb]; simp [hseed, le4]⟩
      intro x hx
      rw [hAu x hx, hkm]
      by_cases hproc : x ≤ pivot.val ∨ (pivot.val < x ∧ (x < mirror.val ∨
          n + pivot.val - mirror.val < x))
      · simp [hproc]
      · have h2 : 2 * x = n + pivotOf sha256 seed n r := by omega
        simp [hproc, sigma_fixed sha256 seed n r x hx (Or.inr h2)]
  · exact ⟨hi, hi', hlen, hA, hc⟩

theorem buf_split (pre : List Nat) (b : Buf) (h1 : (bytes b).take 33 = pre)
    (h2 : (bytes b).length = 37) : ∃ tail, bytes b = pre ++ tail ∧ tail.length = 4 :=
  ⟨(bytes b).drop 33, by rw [← h1, List.take_append_drop], by simp [h2]⟩

theorem round_spec {H : Type} (inst : HashInst H) (sha256 : List Nat → List Nat)
    (hH : HashLink inst sha256) (seed : List Nat) (hseed : seed.length = 32) (n : Nat)
    (hn : 0 < n) (hn24 : n ≤ 16777216) (rounds : U8) (list_size : Usize)
    (hls : list_size.val = n) (input : alloc.vec.Vec Usize) (hlen : input.val.length = n)
    (buf : Buf) (hb1 : (bytes buf).take 32 = seed) (hb2 : (bytes buf).length = 37) (r : U8) :
    swap_or_not_shuffle.shuffle_list.shuffle_list_with_loop0.body inst rounds list_size input
      false buf r ⦃ res =>
        match res with
        | .done out => r.val = 0 ∧ out.val.length = n ∧
            ∀ x < n, nats out x = nats input (sigma sha256 seed n r.val x)
        | .cont (out, fwd, buf', r') => fwd = false ∧ r.val ≠ 0 ∧ r'.val = r.val - 1 ∧
            out.val.length = n ∧ (bytes buf').take 32 = seed ∧ (bytes buf').length = 37 ∧
            ∀ x < n, nats out x = nats input (sigma sha256 seed n r.val x) ⦄ := by
  unfold swap_or_not_shuffle.shuffle_list.shuffle_list_with_loop0.body
  step with set_round_spec' as ⟨buf1, hbuf1⟩
  have hpre1 : (bytes buf1).take 33 = seed ++ [r.val] := by
    rw [hbuf1]
    apply List.ext_getElem
    · simp [hb2, hseed]
    · intro k h1 h2
      simp only [List.length_take, List.length_set, hb2] at h1
      rw [List.getElem_take, List.getElem_set]
      split
      · rename_i hk; subst hk; simp [hseed]
      · have hk32 : k < 32 := by omega
        rw [List.getElem_append_left (by rw [hseed]; omega)]
        have := congrArg (fun l => l[k]?) hb1
        simp only [List.getElem?_take, if_pos hk32] at this
        rw [List.getElem?_eq_getElem (by omega), List.getElem?_eq_getElem (by omega)] at this
        simpa using this
  have hlen1 : (bytes buf1).length = 37 := by rw [hbuf1]; simp [hb2]
  step with raw_pivot_spec inst sha256 hH as ⟨raw, hraw⟩
  rw [hpre1] at hraw
  step*
  all_goals try (rcases System.Platform.numBits_eq with h | h <;> simp [h] <;> done)
  case hmax =>
    have h1 : (UScalar.cast UScalarTy.Usize i2).val ≤ i2.val := by
      rw [UScalar.cast_val_eq]; exact Nat.mod_le _ _
    have hi1 : i1.val = n := by
      rw [i1_post, UScalar.cast_val_eq, ← hls]; exact Nat.mod_eq_of_lt (by simp; omega)
    have h2 : i2.val < n := by rw [i2_post, hi1]; exact Nat.mod_lt _ hn
    have := Usize.cMax_bound_concrete
    rw [pivot_post]; simp only [UScalar.ofNatCore_val_eq]; omega
  have hi1 : i1.val = n := by
    rw [i1_post, UScalar.cast_val_eq, ← hls]; exact Nat.mod_eq_of_lt (by simp; omega)
  have hpv : pivot.val = pivotOf sha256 seed n r.val := by
    have hlt : raw.val % n < n := Nat.mod_lt _ hn
    rw [pivot_post, UScalar.cast_val_eq, i2_post, hi1, Nat.mod_eq_of_lt, hraw]
    · rfl
    · have := Usize.cMax_bound_concrete
      simp only [UScalarTy.numBits]
      rcases System.Platform.numBits_eq with h | h <;> simp [h] <;> omega
  have hpn : pivot.val < n := by rw [hpv]; exact Nat.mod_lt _ hn
  obtain ⟨tail1, htail1, htl1⟩ := buf_split _ buf1 hpre1 hlen1
  step with mix_in_position_spec as ⟨buf2, hbuf2⟩
  rw [htail1, mix_bytes _ (by simp [hseed]) _ htl1] at hbuf2
  step with hash_spec inst sha256 hH as ⟨src, hsrc⟩
  rw [hbuf2] at hsrc
  step*
  all_goals try (rcases System.Platform.numBits_eq with h | h <;> simp [h] <;> done)
  case hbound =>
    rw [i6_post1, i5_post1, usize_and_255, Nat.shiftRight_eq_div_pow]; simp; omega
  have hi4 : i4.val = pivot.val / 256 := by rw [i4_post1, Nat.shiftRight_eq_div_pow]
  have hi6 : i6.val = pivot.val % 256 / 8 := by
    rw [i6_post1, i5_post1, usize_and_255, Nat.shiftRight_eq_div_pow]
  rw [hi4] at hbuf2 hsrc
  have hcache0 : CacheOK sha256 (seed ++ [r.val]) buf2 src byte_v pivot.val := by
    refine ⟨hbuf2, hsrc, ?_⟩
    have hx32 : i6.val < src.val.length := by simp; omega
    rw [← hsrc, ← hi6, byte_v_post]
    simp [bytes, List.getD_eq_getElem?_getD, List.getElem?_eq_getElem hx32]
  have hmir : mirror.val = (pivot.val + 1) / 2 := by
    rw [mirror_post1, i3_post, Nat.shiftRight_eq_div_pow]
  step with loop1_spec inst sha256 hH seed hseed r.val n (input.val.map (·.val)) hn24 pivot mirror hpv
    hpn hmir input buf2 src byte_v 0#usize (by simp) hlen (by
      intro x hx
      have : ¬ (x ≤ pivot.val ∧ (x < (0#usize).val ∨ pivot.val - (0#usize).val < x)) := by
        simp
      rw [if_neg this]; rfl) ⟨pivot.val, Or.inl (by simp), hcache0⟩ as ⟨input1, buf3, hl1, hA1, hb31, hb32⟩
  step*
  all_goals try (rcases System.Platform.numBits_eq with h | h <;> simp [h] <;> done)
  obtain ⟨tail3, htail3, htl3⟩ := buf_split _ buf3 hb31 hb32
  step with mix_in_position_spec as ⟨buf4, hbuf4⟩
  rw [htail3, mix_bytes _ (by simp [hseed]) _ htl3] at hbuf4
  step with hash_spec inst sha256 hH as ⟨src1, hsrc1⟩
  rw [hbuf4] at hsrc1
  step*
  all_goals try (rcases System.Platform.numBits_eq with h | h <;> simp [h] <;> done)
  case hbound =>
    rw [i11_post1, i10_post1, usize_and_255, Nat.shiftRight_eq_div_pow]; simp; omega
  have hend : «end».val = n - 1 := by rw [end_post1, hls]
  have hi9 : i9.val = «end».val / 256 := by rw [i9_post1, Nat.shiftRight_eq_div_pow]
  have hi11 : i11.val = «end».val % 256 / 8 := by
    rw [i11_post1, i10_post1, usize_and_255, Nat.shiftRight_eq_div_pow]
  rw [hi9] at hbuf4 hsrc1
  have hcache1 : CacheOK sha256 (seed ++ [r.val]) buf4 src1 byte_v1 «end».val := by
    refine ⟨hbuf4, hsrc1, ?_⟩
    have hx32 : i11.val < src1.val.length := by simp; omega
    rw [← hsrc1, ← hi11, byte_v1_post]
    simp [bytes, List.getD_eq_getElem?_getD, List.getElem?_eq_getElem hx32]
  have hmir1 : mirror1.val = (pivot.val + n + 1) / 2 := by
    rw [mirror1_post1, i8_post, i7_post, hls, Nat.shiftRight_eq_div_pow]
  step with loop2_spec inst sha256 hH seed hseed r.val n (input.val.map (·.val)) hn24 pivot mirror1
    «end» hpv hpn hmir1 hend input1 buf4 src1 byte_v1 i12 (by omega) (by omega) hl1 (by
      intro x hx
      rw [hA1 x hx]
      by_cases hxp : x ≤ pivot.val
      · simp [hxp]
      · have : ¬ (pivot.val < x ∧ (x < i12.val ∨ n + pivot.val - i12.val < x)) := by omega
        simp [hxp, this]) ⟨«end».val, Or.inl (by omega), hcache1⟩
    as ⟨input2, buf5, hl2, hA2, hb51, hb52⟩
  have hA2' : ∀ x < n, nats input2 x = nats input (sigma sha256 seed n r.val x) := hA2
  simp only [Bool.false_eq_true, if_false]
  by_cases hr0 : r = 0#u8
  · simp only [hr0, if_true]
    apply (spec_ok _).mpr
    refine ⟨by simp, hl2, fun x hx => ?_⟩
    rw [hA2' x hx, hr0]
  · simp only [hr0, if_false]
    have hr0' : r.val ≠ 0 := by intro h; apply hr0; rw [UScalar.eq_equiv]; simpa using h
    step as ⟨r1, hr1⟩
    apply (spec_ok _).mpr
    refine ⟨hr0', by omega, hl2, ?_, hb52, hA2'⟩
    have := congrArg (List.take 32) hb51
    rw [List.take_take] at this
    simpa [hseed] using this

/-- Rounds `s, ..., R - 1` on index `x`. -/
def gFrom (sha256 : List Nat → List Nat) (seed : List Nat) (n s R x : Nat) : Nat :=
  (List.range' s (R - s)).foldl (fun y k => sigma sha256 seed n k y) x

theorem gFrom_step (sha256 : List Nat → List Nat) (seed : List Nat) (n s R x : Nat) (h : s < R) :
    gFrom sha256 seed n s R x = gFrom sha256 seed n (s + 1) R (sigma sha256 seed n s x) := by
  unfold gFrom
  rw [show R - s = (R - (s + 1)) + 1 by omega, List.range'_succ]
  rfl

theorem gFrom_zero (sha256 : List Nat → List Nat) (seed : List Nat) (n R x : Nat) :
    gFrom sha256 seed n 0 R x = csiFold sha256 seed n R x := by
  unfold gFrom csiFold
  rw [List.range_eq_range', Nat.sub_zero]

theorem gFrom_self (sha256 : List Nat → List Nat) (seed : List Nat) (n R x : Nat) :
    gFrom sha256 seed n R R x = x := by
  simp [gFrom]

theorem rounds_loop_spec {H : Type} (inst : HashInst H) (sha256 : List Nat → List Nat)
    (hH : HashLink inst sha256) (seed : List Nat) (hseed : seed.length = 32) (n : Nat)
    (hn : 0 < n) (hn24 : n ≤ 16777216) (rounds : U8) (list_size : Usize)
    (hls : list_size.val = n) (input0 input : alloc.vec.Vec Usize)
    (hlen : input.val.length = n) (buf : Buf) (hb1 : (bytes buf).take 32 = seed)
    (hb2 : (bytes buf).length = 37) (r : U8) (hr : r.val < rounds.val)
    (hA : ∀ x < n, nats input x = nats input0 (gFrom sha256 seed n (r.val + 1) rounds.val x)) :
    swap_or_not_shuffle.shuffle_list.shuffle_list_with_loop0 inst input rounds false list_size
      buf r ⦃ out => out.val.length = n ∧
        ∀ x < n, nats out x = nats input0 (csiFold sha256 seed n rounds.val x) ⦄ := by
  unfold swap_or_not_shuffle.shuffle_list.shuffle_list_with_loop0
  apply loop.spec_decr_nat
    (measure := fun (x : alloc.vec.Vec Usize × Bool × Buf × U8) => x.2.2.2.val)
    (inv := fun (x : alloc.vec.Vec Usize × Bool × Buf × U8) =>
      x.2.1 = false ∧ x.2.2.2.val < rounds.val ∧ x.1.val.length = n ∧
      (bytes x.2.2.1).take 32 = seed ∧ (bytes x.2.2.1).length = 37 ∧
      ∀ y < n, nats x.1 y = nats input0 (gFrom sha256 seed n (x.2.2.2.val + 1) rounds.val y))
  · rintro ⟨u, fwd, b, k⟩ ⟨hfwd, hk, hu, hb1', hb2', hAu⟩
    dsimp only at hfwd hk hu hb1' hb2' hAu ⊢
    cases fwd
    swap
    · exact absurd hfwd (by simp)
    obtain ⟨res, hres, hpost⟩ := spec_exists
      (round_spec inst sha256 hH seed hseed n hn hn24 rounds list_size hls u hu b hb1' hb2' k)
    rw [hres]
    apply (spec_ok _).mpr
    have hstep : ∀ x < n, nats input0 (gFrom sha256 seed n (k.val + 1) rounds.val
        (sigma sha256 seed n k.val x)) = nats input0 (gFrom sha256 seed n k.val rounds.val x) := by
      intro x hx; rw [gFrom_step _ _ _ _ _ _ hk]
    match res, hpost with
    | .done out, ⟨hk0, hout, hAout⟩ =>
      refine ⟨hout, fun x hx => ?_⟩
      rw [hAout x hx, hAu _ (sigma_lt _ _ _ _ _ hn hx), hstep x hx, hk0, gFrom_zero]
    | .cont (out, fwd', b', k'), ⟨hf, hk0, hk', hout, hb1'', hb2'', hAout⟩ =>
      dsimp only
      refine ⟨⟨hf, by omega, hout, hb1'', hb2'', fun x hx => ?_⟩, by omega⟩
      rw [hAout x hx, hAu _ (sigma_lt _ _ _ _ _ hn hx), hstep x hx, hk', Nat.sub_add_cancel
        (by omega)]
  · exact ⟨rfl, hr, hlen, hb1, hb2, hA⟩

theorem shuffle_list_with_spec {H : Type} (inst : HashInst H) (sha256 : List Nat → List Nat)
    (hH : HashLink inst sha256) (seed : Slice U8) (hseed : seed.val.length = 32)
    (input : alloc.vec.Vec Usize) (rounds : U8) (hn : 0 < input.val.length)
    (hn24 : input.val.length ≤ 16777216) (hr : 0 < rounds.val) :
    swap_or_not_shuffle.shuffle_list.shuffle_list_with inst input rounds seed false ⦃ out =>
      ∃ v, out = some v ∧ v.val.length = input.val.length ∧
        ∀ x < input.val.length, nats v x = nats input
          (csiFold sha256 (seed.val.map (·.val)) input.val.length rounds.val x) ⦄ := by
  unfold swap_or_not_shuffle.shuffle_list.shuffle_list_with
  step*
  all_goals try (rcases System.Platform.numBits_eq with h | h <;> simp [h] <;> done)
  case h1 =>
    exfalso
    rename_i hgt
    have hi1 : i1.val = 16777216 := by
      rw [i1_post1, Usize.size_def]
      rcases System.Platform.numBits_eq with h | h <;> simp [Usize.numBits, h]
    have := (UScalar.lt_equiv _ _).mp hgt
    simp only [alloc.vec.Vec.len_val, alloc.vec.Vec.length] at this
    omega
  obtain ⟨buf, hbuf, hbytes⟩ := buf_new_eq seed hseed
  rw [hbuf]
  simp only [bind_tc_ok]
  step as ⟨r, hr1⟩
  have hb1 : (bytes buf).take 32 = seed.val.map (·.val) := by
    rw [hbytes]; exact List.take_left' (by simp [hseed])
  have hb2 : (bytes buf).length = 37 := by rw [hbytes]; simp [hseed]
  step with rounds_loop_spec inst sha256 hH (seed.val.map (·.val)) (by simp [hseed])
    input.val.length hn hn24 rounds input.len (by simp) input input rfl buf hb1 hb2 r
    (by omega) (by
      intro x hx
      rw [show r.val + 1 = rounds.val by omega, gFrom_self]) as ⟨out, hout, hAout⟩
  exact ⟨out, rfl, hout, hAout⟩

open CacheProofs.Spec.CommitteeCache in
theorem compute_shuffled_index_eq (p : Preset) (sha256 : List Nat → List Nat)
    (hsha : ∀ l, (sha256 l).length = 32) (seed : List Nat) (n : Nat) (hn : 0 < n)
    (hn24 : n ≤ 16777216) (hR : p.SHUFFLE_ROUND_COUNT ≤ 256) (i : Nat) (hi : i < n) :
    compute_shuffled_index p sha256 i n seed = .ok (csiFold sha256 seed n p.SHUFFLE_ROUND_COUNT i) := by
  unfold compute_shuffled_index
  rw [compute_shuffled_permutation_eq p sha256 hsha seed n hn hn24 hR]
  simp only [hi, not_true_eq_false, if_false]
  show listGet _ i = _
  unfold listGet
  rw [List.getElem?_map, List.getElem?_range hi]
  rfl

/-- The committee shuffling of the pure function is the spec shuffling. -/
theorem shuffling_spec {H : Type} (inst : HashInst H) (sha256 : List Nat → List Nat)
    (hH : HashLink inst sha256) (hsha : ∀ l, (sha256 l).length = 32)
    (p : Spec.CommitteeCache.Preset) (seed : Slice U8) (hseed : seed.val.length = 32)
    (indices : alloc.vec.Vec Usize) (rounds : U8) (hR : p.SHUFFLE_ROUND_COUNT = rounds.val)
    (hn : 0 < indices.val.length) (hn24 : indices.val.length ≤ 16777216)
    (hr : 0 < rounds.val) :
    state.committee_assignment.shuffling inst indices rounds seed ⦃ out =>
      ∃ v, out = some v ∧ CommitteeCache.IsShuffling p sha256 (indices.val.map (·.val))
        (seed.val.map (·.val)) (v.val.map (·.val)) ⦄ := by
  unfold state.committee_assignment.shuffling
  obtain ⟨out, hout, v, rfl, hlen, hv⟩ := spec_exists
    (shuffle_list_with_spec inst sha256 hH seed hseed indices rounds hn hn24 hr)
  rw [hout]
  apply (spec_ok _).mpr
  refine ⟨v, rfl, by simp [hlen], fun i hi => ?_⟩
  simp only [List.length_map] at hi
  have hR' : p.SHUFFLE_ROUND_COUNT ≤ 256 := by have := rounds.hBounds; simp at this; omega
  unfold CommitteeCache.shuffledAt
  rw [List.length_map, compute_shuffled_index_eq p sha256 hsha _ _ hn hn24 hR' i hi]
  rw [hR]
  have hc := csiFold_lt sha256 (seed.val.map (·.val)) indices.val.length rounds.val i hn hi
  have hvi := hv i hi
  unfold nats at hvi
  rw [hvi]
  generalize csiFold sha256 (seed.val.map (·.val)) indices.val.length rounds.val i = k at hc ⊢
  unfold Spec.CommitteeCache.listGet
  have hk : k < (indices.val.map (·.val)).length := by simpa using hc
  rw [except_bind_ok_eq]
  rw [List.getElem?_eq_getElem hk, List.getD_eq_getElem?_getD, List.getElem?_eq_getElem hk]
  rfl

end CacheProofs.Shuffle
