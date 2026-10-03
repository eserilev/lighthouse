import CacheProofs.Generated
import CacheProofs.Spec.ActivationQueue

/-!
# `ActivationQueue` is coherent with the spec

Lighthouse keeps a speculative activation queue in the `EpochCache`. Two places build it:

- `initialize_epoch_cache` (`epoch_cache.rs`). It runs during epoch `E` and adds each validator
  with `could_be_eligible_for_activation_at(E + 1)`. The cache is for epoch `E`.
- single-pass epoch processing at the end of epoch `E - 1`
  (`process_single_registry_update_pre_electra`). It adds each validator after its own registry
  update, with `could_be_eligible_for_activation_at(E)`. The cache is for epoch `E`.

`process_registry_updates` at the end of epoch `E` reads the queue with
`get_validators_eligible_for_activation(finalized_epoch, churn_limit)`.

The queue is a `BTreeSet<(Epoch, usize)>`. Here it is a list of keys. `lhInsert` models
`BTreeSet::insert` and the list order models the iteration order.

`vb` is the validator list that the builder sees. `vr` is the validator list at the read, after
the eligibility loop of `process_registry_updates`.
-/

namespace CacheProofs.ActivationQueue

open Aeneas Aeneas.Std Result
open CacheProofs.Spec CacheProofs.Spec.ActivationQueue

/-- `omega` does not see through the `Epoch` and `Gwei` abbrevs. -/
macro "nomega" : tactic =>
  `(tactic| ((try dsimp only [CacheProofs.Spec.Epoch, CacheProofs.Spec.Gwei, CacheProofs.Spec.Uint64] at *) <;> omega))

/-! ## Pure functions -/

/-- The predicate that `could_be_eligible_for_activation_at` computes. -/
def couldBeAt (epoch : Nat) (validator : Validator) : Bool :=
  decide (validator.activation_epoch = FAR_FUTURE_EPOCH)
    && decide (validator.activation_eligibility_epoch < epoch)

theorem could_be_eligible_for_activation_at_equiv (validator : Validator)
    (activation_eligibility_epoch activation_epoch epoch far_future_epoch : U64)
    (helig : activation_eligibility_epoch.val = validator.activation_eligibility_epoch)
    (hact : activation_epoch.val = validator.activation_epoch)
    (hfar : far_future_epoch.val = FAR_FUTURE_EPOCH) :
    types.validator.activation_eligibility.could_be_eligible_for_activation_at
      activation_eligibility_epoch activation_epoch epoch far_future_epoch =
        ok (couldBeAt epoch.val validator) := by
  unfold types.validator.activation_eligibility.could_be_eligible_for_activation_at couldBeAt
  have he : activation_epoch = far_future_epoch ↔
      validator.activation_epoch = FAR_FUTURE_EPOCH := by
    rw [UScalar.eq_equiv, hact, hfar]
  by_cases h : validator.activation_epoch = FAR_FUTURE_EPOCH
  · simp [he.mpr h, h, helig]
  · have h' : ¬ activation_epoch = far_future_epoch := fun e => h (he.mp e)
    simp [h', h]

theorem is_eligible_for_activation_equiv (validator : Validator)
    (activation_eligibility_epoch activation_epoch finalized_epoch far_future_epoch : U64)
    (helig : activation_eligibility_epoch.val = validator.activation_eligibility_epoch)
    (hact : activation_epoch.val = validator.activation_epoch)
    (hfar : far_future_epoch.val = FAR_FUTURE_EPOCH) :
    types.validator.activation_eligibility.is_eligible_for_activation
      activation_eligibility_epoch activation_epoch finalized_epoch far_future_epoch =
        ok (is_eligible_for_activation finalized_epoch.val validator) := by
  unfold types.validator.activation_eligibility.is_eligible_for_activation
    is_eligible_for_activation
  have he : activation_epoch = far_future_epoch ↔
      validator.activation_epoch = FAR_FUTURE_EPOCH := by
    rw [UScalar.eq_equiv, hact, hfar]
  by_cases h : validator.activation_eligibility_epoch ≤ finalized_epoch.val
  · have h' : activation_eligibility_epoch ≤ finalized_epoch := by
      show activation_eligibility_epoch.val ≤ finalized_epoch.val
      nomega
    simp only [h', if_true, h, decide_true, Bool.true_and]
    by_cases hf : validator.activation_epoch = FAR_FUTURE_EPOCH
    · simp [he.mpr hf, hf]
    · have hf' : ¬ activation_epoch = far_future_epoch := fun e => hf (he.mp e)
      simp [hf', hf]
  · have h' : ¬ activation_eligibility_epoch ≤ finalized_epoch := by
      show ¬ activation_eligibility_epoch.val ≤ finalized_epoch.val
      nomega
    simp [h', h]

/-- The cache works: every validator that the spec finds eligible at a read with a finalized
epoch below `epoch` passes `could_be_eligible_for_activation_at(epoch)`. -/
theorem couldBeAt_of_is_eligible_for_activation (finalized_epoch epoch : Nat)
    (validator : Validator) (hfin : finalized_epoch < epoch)
    (h : is_eligible_for_activation finalized_epoch validator = true) :
    couldBeAt epoch validator = true := by
  simp only [is_eligible_for_activation, Bool.and_eq_true, decide_eq_true_eq] at h
  simp only [couldBeAt, Bool.and_eq_true, decide_eq_true_eq]
  nomega

/-! ## Keys and sorted lists -/

abbrev Key := Nat × Nat

/-- Strict tuple order. -/
def keyLt (a b : Key) : Prop := a.1 < b.1 ∨ (a.1 = b.1 ∧ a.2 < b.2)

def key (p : Validator × Nat) : Key := (p.1.activation_eligibility_epoch, p.2)

theorem tupleLe_iff (a b : Key) : tupleLe a b = true ↔ keyLt a b ∨ a = b := by
  obtain ⟨a1, a2⟩ := a
  obtain ⟨b1, b2⟩ := b
  simp only [tupleLe, keyLt, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq, Prod.mk.injEq]
  nomega

theorem keyLt_trans {a b c : Key} (h1 : keyLt a b) (h2 : keyLt b c) : keyLt a c := by
  simp only [keyLt] at *
  nomega

theorem keyLt_irrefl (a : Key) : ¬ keyLt a a := by
  simp only [keyLt]
  nomega

theorem keyLt_asymm {a b : Key} (h : keyLt a b) : ¬ keyLt b a := by
  simp only [keyLt] at *
  nomega

theorem keyLt_trichotomy (a b : Key) : a = b ∨ keyLt a b ∨ keyLt b a := by
  obtain ⟨a1, a2⟩ := a
  obtain ⟨b1, b2⟩ := b
  simp only [keyLt, Prod.mk.injEq]
  nomega

theorem tupleLe_trans (a b c : Key) (h1 : tupleLe a b = true) (h2 : tupleLe b c = true) :
    tupleLe a c = true := by
  simp only [tupleLe, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq] at *
  nomega

theorem tupleLe_total (a b : Key) : (tupleLe a b || tupleLe b a) = true := by
  simp only [tupleLe, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq]
  nomega

/-- Two strictly sorted lists with the same members are equal. -/
theorem eq_of_sorted_of_mem_iff :
    ∀ {l1 l2 : List Key}, l1.Pairwise keyLt → l2.Pairwise keyLt →
      (∀ x, x ∈ l1 ↔ x ∈ l2) → l1 = l2
  | [], [], _, _, _ => rfl
  | [], b :: _, _, _, h => absurd ((h b).mpr (by simp)) (by simp)
  | a :: _, [], _, _, h => absurd ((h a).mp (by simp)) (by simp)
  | a :: t1, b :: t2, h1, h2, h => by
    rw [List.pairwise_cons] at h1 h2
    have hab : a = b := by
      have ha := (h a).mp (by simp)
      have hb := (h b).mpr (by simp)
      rcases List.mem_cons.mp ha with e | ha
      · exact e
      rcases List.mem_cons.mp hb with e | hb
      · exact e.symm
      exact absurd (h1.1 b hb) (keyLt_asymm (h2.1 a ha))
    subst hab
    congr 1
    apply eq_of_sorted_of_mem_iff h1.2 h2.2
    intro x
    constructor
    · intro hx
      have hne : x ≠ a := fun e => keyLt_irrefl a (e ▸ h1.1 x hx)
      rcases List.mem_cons.mp ((h x).mp (List.mem_cons_of_mem _ hx)) with e | hx'
      · exact absurd e hne
      · exact hx'
    · intro hx
      have hne : x ≠ a := fun e => keyLt_irrefl a (e ▸ h2.1 x hx)
      rcases List.mem_cons.mp ((h x).mpr (List.mem_cons_of_mem _ hx)) with e | hx'
      · exact absurd e hne
      · exact hx'

theorem pairwise_keyLt_of (l : List Key) (hle : l.Pairwise fun a b => tupleLe a b = true)
    (hnd : l.Nodup) : l.Pairwise keyLt := by
  refine (hle.and hnd).imp ?_
  intro a b ⟨h1, h2⟩
  rcases (tupleLe_iff a b).mp h1 with h | h
  · exact h
  · exact absurd h h2

theorem zipIdx_pairwise {α : Type} :
    ∀ (l : List α) (n : Nat), (l.zipIdx n).Pairwise fun a b => a.2 < b.2
  | [], _ => List.Pairwise.nil
  | a :: l, n => by
    rw [List.zipIdx_cons, List.pairwise_cons]
    refine ⟨?_, zipIdx_pairwise l (n + 1)⟩
    intro p hp
    have := (List.mem_zipIdx hp).1
    simp only
    nomega

/-- The keys of a filtered `zipIdx` have distinct indices. -/
theorem nodup_keys (l : List (Validator × Nat))
    (h : l.Pairwise fun a b => a.2 < b.2) : (l.map key).Nodup := by
  have : (l.map key).Pairwise fun a b => a.2 < b.2 :=
    List.Pairwise.map key (fun a b hab => by simpa [key] using hab) h
  exact this.imp fun hab e => by rw [e] at hab; nomega

/-! ## The Lighthouse model -/

/-- `BTreeSet::insert` on the sorted list model. -/
def lhInsert (x : Key) : List Key → List Key
  | [] => [x]
  | y :: ys => if x = y then y :: ys else if tupleLe x y then x :: y :: ys else y :: lhInsert x ys

/-- `add_if_could_be_eligible_for_activation`. -/
def lhAdd (epoch : Nat) (q : List Key) (p : Validator × Nat) : List Key :=
  if couldBeAt epoch p.1 then lhInsert (key p) q else q

/-- The queue that `initialize_epoch_cache` and single-pass build, with `epoch = next_epoch`. -/
def lhBuild (epoch : Nat) (validators : List Validator) : List Key :=
  validators.zipIdx.foldl (lhAdd epoch) []

/-- `get_validators_eligible_for_activation`, before `collect`. -/
def lhSelect (q : List Key) (finalized_epoch churn_limit : Nat) : List Nat :=
  ((q.filter fun x => decide (x.1 ≤ finalized_epoch)).map (·.2)).take churn_limit

/-- The summary: the keys of the validators with `could_be_eligible_for_activation_at(epoch)`,
in tuple order. -/
def queueSummary (epoch : Nat) (validators : List Validator) : List Key :=
  ((validators.zipIdx.filter fun p => couldBeAt epoch p.1).map key).mergeSort tupleLe

theorem mem_lhInsert (x y : Key) : ∀ q, y ∈ lhInsert x q ↔ y = x ∨ y ∈ q
  | [] => by simp [lhInsert]
  | z :: zs => by
    unfold lhInsert
    by_cases hxz : x = z
    · subst hxz; simp
    · simp only [hxz, if_false]
      split
      · simp
      · simp only [List.mem_cons, mem_lhInsert x y zs]
        tauto

theorem lhInsert_sorted (x : Key) : ∀ q : List Key, q.Pairwise keyLt →
    (lhInsert x q).Pairwise keyLt
  | [], _ => by simp [lhInsert]
  | z :: zs, h => by
    unfold lhInsert
    rw [List.pairwise_cons] at h
    by_cases hxz : x = z
    · simp only [hxz, if_true]; exact List.pairwise_cons.mpr h
    · simp only [hxz, if_false]
      split
      · rename_i hle
        have hlt : keyLt x z := by
          rcases (tupleLe_iff x z).mp hle with h' | h'
          · exact h'
          · exact absurd h' hxz
        refine List.pairwise_cons.mpr ⟨?_, List.pairwise_cons.mpr h⟩
        intro w hw
        rcases List.mem_cons.mp hw with e | hw
        · exact e ▸ hlt
        · exact keyLt_trans hlt (h.1 w hw)
      · rename_i hle
        have hlt : keyLt z x := by
          rcases keyLt_trichotomy x z with e | e | e
          · exact absurd e hxz
          · exact absurd ((tupleLe_iff x z).mpr (Or.inl e)) hle
          · exact e
        refine List.pairwise_cons.mpr ⟨?_, lhInsert_sorted x zs h.2⟩
        intro w hw
        rcases (mem_lhInsert x w zs).mp hw with e | hw
        · exact e ▸ hlt
        · exact h.1 w hw

theorem foldl_lhAdd (epoch : Nat) :
    ∀ (l : List (Validator × Nat)) (q : List Key), q.Pairwise keyLt →
      (l.foldl (lhAdd epoch) q).Pairwise keyLt ∧
      ∀ x, x ∈ l.foldl (lhAdd epoch) q ↔
        x ∈ q ∨ ∃ p ∈ l, couldBeAt epoch p.1 = true ∧ x = key p
  | [], q, h => ⟨h, fun x => by simp⟩
  | p :: l, q, h => by
    have hq : (lhAdd epoch q p).Pairwise keyLt := by
      unfold lhAdd; split
      · exact lhInsert_sorted _ q h
      · exact h
    obtain ⟨h1, h2⟩ := foldl_lhAdd epoch l (lhAdd epoch q p) hq
    refine ⟨h1, fun x => ?_⟩
    rw [List.foldl_cons, h2 x]
    unfold lhAdd
    split
    · rename_i hc
      rw [mem_lhInsert]
      constructor
      · rintro ((e | hx) | ⟨p', hp', hc', e⟩)
        · exact Or.inr ⟨p, by simp, hc, e⟩
        · exact Or.inl hx
        · exact Or.inr ⟨p', by simp [hp'], hc', e⟩
      · rintro (hx | ⟨p', hp', hc', e⟩)
        · exact Or.inl (Or.inr hx)
        · rcases List.mem_cons.mp hp' with e' | hp'
          · subst e'; exact Or.inl (Or.inl e)
          · exact Or.inr ⟨p', hp', hc', e⟩
    · rename_i hc
      constructor
      · rintro (hx | ⟨p', hp', hc', e⟩)
        · exact Or.inl hx
        · exact Or.inr ⟨p', by simp [hp'], hc', e⟩
      · rintro (hx | ⟨p', hp', hc', e⟩)
        · exact Or.inl hx
        · rcases List.mem_cons.mp hp' with e' | hp'
          · subst e'; exact absurd hc' hc
          · exact Or.inr ⟨p', hp', hc', e⟩

theorem lhBuild_sorted (epoch : Nat) (validators : List Validator) :
    (lhBuild epoch validators).Pairwise keyLt :=
  (foldl_lhAdd epoch validators.zipIdx [] List.Pairwise.nil).1

theorem mem_lhBuild (epoch : Nat) (validators : List Validator) (x : Key) :
    x ∈ lhBuild epoch validators ↔
      ∃ i v, validators[i]? = some v ∧ couldBeAt epoch v = true ∧
        x = (v.activation_eligibility_epoch, i) := by
  rw [lhBuild, (foldl_lhAdd epoch validators.zipIdx [] List.Pairwise.nil).2 x]
  constructor
  · rintro (hx | ⟨p, hp, hc, e⟩)
    · simp at hx
    · exact ⟨p.2, p.1, List.mem_zipIdx_iff_getElem?.mp hp, hc, e⟩
  · rintro ⟨i, v, hv, hc, e⟩
    exact Or.inr ⟨(v, i), List.mem_zipIdx_iff_getElem?.mpr hv, hc, e⟩

theorem queueSummary_sorted (epoch : Nat) (validators : List Validator) :
    (queueSummary epoch validators).Pairwise keyLt := by
  apply pairwise_keyLt_of
  · exact List.pairwise_mergeSort tupleLe_trans tupleLe_total _
  · exact (List.mergeSort_perm _ _).nodup_iff.mpr
      (nodup_keys _ ((zipIdx_pairwise validators 0).filter _))

/-! ## Build -/

/-- Build: both builders give the keys `(activation_eligibility_epoch, index)` of the validators
with `could_be_eligible_for_activation_at(epoch)`, in tuple order. -/
theorem lhBuild_eq_queueSummary (epoch : Nat) (validators : List Validator) :
    lhBuild epoch validators = queueSummary epoch validators := by
  apply eq_of_sorted_of_mem_iff (lhBuild_sorted epoch validators)
    (queueSummary_sorted epoch validators)
  intro x
  rw [mem_lhBuild, queueSummary, List.mem_mergeSort]
  simp only [List.mem_map, List.mem_filter]
  constructor
  · rintro ⟨i, v, hv, hc, e⟩
    exact ⟨(v, i), ⟨List.mem_zipIdx_iff_getElem?.mpr hv, hc⟩, e.symm⟩
  · rintro ⟨p, ⟨hp, hc⟩, e⟩
    exact ⟨p.2, p.1, List.mem_zipIdx_iff_getElem?.mp hp, hc, e.symm⟩

/-! ## Read agreement (phase0 to Deneb) -/

/-- What can change between the build and the read. -/
structure Frame (finalized_epoch : Nat) (vb vr : List Validator) : Prop where
  /-- Validators are only appended. -/
  length_le : vb.length ≤ vr.length
  /-- No activation epoch changes. -/
  activation : ∀ (i : Nat) (b r : Validator), vb[i]? = some b → vr[i]? = some r →
    r.activation_epoch = b.activation_epoch
  /-- An eligibility epoch changes only from `FAR_FUTURE_EPOCH`, to an epoch after
  `finalized_epoch`. -/
  eligibility : ∀ (i : Nat) (b r : Validator), vb[i]? = some b → vr[i]? = some r →
    r.activation_eligibility_epoch = b.activation_eligibility_epoch ∨
      (b.activation_eligibility_epoch = FAR_FUTURE_EPOCH ∧
        finalized_epoch < r.activation_eligibility_epoch)
  /-- A new validator has an eligibility epoch after `finalized_epoch`. -/
  appended : ∀ (i : Nat) (r : Validator), vb.length ≤ i → vr[i]? = some r →
    finalized_epoch < r.activation_eligibility_epoch

/-- The cache works: every validator that the spec finds eligible at the read is in the queue,
with the same key. -/
theorem over_approximation (epoch finalized_epoch : Nat) (vb vr : List Validator)
    (hframe : Frame finalized_epoch vb vr) (hfin : finalized_epoch < epoch) (i : Nat)
    (r : Validator) (hr : vr[i]? = some r)
    (hel : is_eligible_for_activation finalized_epoch r = true) :
    ∃ b, vb[i]? = some b ∧ couldBeAt epoch b = true ∧
      b.activation_eligibility_epoch = r.activation_eligibility_epoch := by
  simp only [is_eligible_for_activation, Bool.and_eq_true, decide_eq_true_eq] at hel
  by_cases hi : i < vb.length
  · obtain ⟨b, hb⟩ : ∃ b, vb[i]? = some b := ⟨vb[i], List.getElem?_eq_getElem hi⟩
    have ha := hframe.activation i b r hb hr
    rcases hframe.eligibility i b r hb hr with he | ⟨_, he⟩
    · refine ⟨b, hb, ?_, he.symm⟩
      simp only [couldBeAt, Bool.and_eq_true, decide_eq_true_eq]
      nomega
    · nomega
  · have := hframe.appended i r (by nomega) hr
    nomega

/-- Read agreement, phase0 to Deneb: the Lighthouse selection equals the spec dequeued list,
in the same order. -/
theorem lhSelect_eq_dequeued (epoch finalized_epoch churn_limit : Nat) (vb vr : List Validator)
    (hframe : Frame finalized_epoch vb vr) (hfin : finalized_epoch < epoch)
    (hepoch : epoch ≤ FAR_FUTURE_EPOCH) :
    lhSelect (lhBuild epoch vb) finalized_epoch churn_limit =
      dequeued finalized_epoch churn_limit vr := by
  let le' : Validator × Nat → Validator × Nat → Bool := fun a b => tupleLe (key a) (key b)
  let eligible := vr.zipIdx.filter fun p => is_eligible_for_activation finalized_epoch p.1
  let sorted := eligible.mergeSort le'
  have hsorted : (sorted.map key).Pairwise keyLt := by
    apply pairwise_keyLt_of
    · exact List.Pairwise.map key (fun a b h => h)
        (List.pairwise_mergeSort (fun a b c => tupleLe_trans (key a) (key b) (key c))
          (fun a b => tupleLe_total (key a) (key b)) eligible)
    · exact ((List.mergeSort_perm eligible le').map key).nodup_iff.mpr
        (nodup_keys _ ((zipIdx_pairwise vr 0).filter _))
  have hfilter : (lhBuild epoch vb).filter (fun x => decide (x.1 ≤ finalized_epoch)) =
      sorted.map key := by
    apply eq_of_sorted_of_mem_iff ((lhBuild_sorted epoch vb).filter _) hsorted
    intro x
    rw [List.mem_filter, mem_lhBuild, List.mem_map]
    simp only [sorted, List.mem_mergeSort, eligible, List.mem_filter, decide_eq_true_eq]
    constructor
    · rintro ⟨⟨i, b, hb, hc, e⟩, hx⟩
      subst e
      simp only [couldBeAt, Bool.and_eq_true, decide_eq_true_eq] at hc
      obtain ⟨r, hr⟩ : ∃ r, vr[i]? = some r := by
        have hi : i < vb.length := (List.getElem?_eq_some_iff.mp hb).1
        exact ⟨vr[i]'(by have := hframe.length_le; nomega),
          List.getElem?_eq_getElem (by have := hframe.length_le; nomega)⟩
      have ha := hframe.activation i b r hb hr
      rcases hframe.eligibility i b r hb hr with he | ⟨he, _⟩
      · refine ⟨(r, i), ⟨List.mem_zipIdx_iff_getElem?.mpr hr, ?_⟩, ?_⟩
        · simp only [is_eligible_for_activation, Bool.and_eq_true, decide_eq_true_eq]
          simp only at hx
          nomega
        · simp [key, he]
      · nomega
    · rintro ⟨p, ⟨hp, hel⟩, e⟩
      subst e
      have hr := List.mem_zipIdx_iff_getElem?.mp hp
      obtain ⟨b, hb, hc, he⟩ := over_approximation epoch finalized_epoch vb vr hframe hfin p.2
        p.1 hr hel
      simp only [is_eligible_for_activation, Bool.and_eq_true, decide_eq_true_eq] at hel
      refine ⟨⟨p.2, b, hb, hc, by simp [key, he]⟩, ?_⟩
      simp only [key]
      nomega
  rw [lhSelect, hfilter, dequeued, activation_queue, List.map_map]
  rfl

/-- Single-pass applies the selection with `BTreeSet::contains`. So a validator activates in
Lighthouse if and only if the spec dequeues it. -/
theorem mem_lhSelect_iff (epoch finalized_epoch churn_limit : Nat) (vb vr : List Validator)
    (hframe : Frame finalized_epoch vb vr) (hfin : finalized_epoch < epoch)
    (hepoch : epoch ≤ FAR_FUTURE_EPOCH) (i : Nat) :
    i ∈ lhSelect (lhBuild epoch vb) finalized_epoch churn_limit ↔
      i ∈ dequeued finalized_epoch churn_limit vr := by
  rw [lhSelect_eq_dequeued epoch finalized_epoch churn_limit vb vr hframe hfin hepoch]

/-! ## The frame at the read -/

theorem mapM_frame (f : Validator → SpecM Validator) (P : Validator → Validator → Prop)
    (hf : ∀ v v', f v = .ok v' → P v v') :
    ∀ (vs vs' : List Validator), vs.mapM f = .ok vs' →
      vs'.length = vs.length ∧
      ∀ (i : Nat) (b r : Validator), vs[i]? = some b → vs'[i]? = some r → P b r
  | [], vs', h => by
    simp only [List.mapM_nil, pure, Except.pure, Except.ok.injEq] at h
    subst h; simp
  | v :: vs, vs', h => by
    rw [List.mapM_cons] at h
    cases h1 : f v with
    | error e => simp [h1, Bind.bind, Except.bind] at h
    | ok v' =>
      cases h2 : vs.mapM f with
      | error e => simp [h1, h2, Bind.bind, Except.bind] at h
      | ok rest =>
        simp only [h1, h2, Bind.bind, Except.bind, pure, Except.pure, Except.ok.injEq] at h
        subst h
        obtain ⟨hl, hi⟩ := mapM_frame f P hf vs rest h2
        refine ⟨by simp [hl], ?_⟩
        intro i b r hb hr
        cases i with
        | zero => simp at hb hr; subst hb hr; exact hf v v' h1
        | succ i => simp at hb hr; exact hi i b r hb hr

/-- The eligibility loop of `process_registry_updates` keeps the frame: it appends nothing,
changes no activation epoch, and sets an eligibility epoch only from `FAR_FUTURE_EPOCH` to
`current_epoch + 1`. -/
theorem process_activation_eligibility_frame (MAX_EFFECTIVE_BALANCE current_epoch : Nat)
    (finalized_epoch : Nat) (hfin : finalized_epoch ≤ current_epoch)
    (vs vs' : List Validator)
    (h : process_activation_eligibility MAX_EFFECTIVE_BALANCE current_epoch vs = .ok vs') :
    vs'.length = vs.length ∧
    ∀ (i : Nat) (b r : Validator), vs[i]? = some b → vs'[i]? = some r →
      r.activation_epoch = b.activation_epoch ∧
      (r.activation_eligibility_epoch = b.activation_eligibility_epoch ∨
        (b.activation_eligibility_epoch = FAR_FUTURE_EPOCH ∧
          finalized_epoch < r.activation_eligibility_epoch)) := by
  refine mapM_frame _ _ ?_ vs vs' h
  intro v v' hv
  split at hv
  · rename_i hq
    simp only [is_eligible_for_activation_queue, Bool.and_eq_true, decide_eq_true_eq] at hq
    by_cases ho : current_epoch + 1 < UINT64_SIZE
    · simp only [uint64Add, ho, if_true, Bind.bind, Except.bind, pure, Except.pure,
        Except.ok.injEq] at hv
      subst hv
      exact ⟨rfl, Or.inr ⟨hq.1, by simp only; nomega⟩⟩
    · simp [uint64Add, ho, Bind.bind, Except.bind, throw, throwThe, MonadExceptOf.throw] at hv
  · simp only [pure, Except.pure, Except.ok.injEq] at hv
    subst hv
    exact ⟨rfl, Or.inl rfl⟩

/-! ## Finality at the read -/

theorem finalization_rule_lt (all_bits : Bool) (source distance current finalized f : Nat)
    (hd : 0 < distance)
    (h : finalization_rule all_bits source distance current finalized = .ok f) :
    f = finalized ∨ f < current := by
  unfold finalization_rule at h
  split at h
  · by_cases ho : source + distance < UINT64_SIZE
    · simp only [uint64Add, ho, if_true, Bind.bind, Except.bind, pure, Except.pure,
        Except.ok.injEq] at h
      subst h
      split
      · right; nomega
      · left; rfl
    · simp [uint64Add, ho, Bind.bind, Except.bind, throw, throwThe, MonadExceptOf.throw] at h
  · simp only [pure, Except.pure, Except.ok.injEq] at h
    left; exact h.symm

/-- The finalization rules finalize only an epoch before `current_epoch`. So if the finalized
epoch is before `current_epoch` before the rules, it stays before `current_epoch`. -/
theorem process_finalizations_lt (bits : List Bool) (old_previous_justified_epoch
    old_current_justified_epoch current_epoch finalized_epoch f : Nat)
    (hfin : finalized_epoch < current_epoch)
    (h : process_finalizations bits old_previous_justified_epoch old_current_justified_epoch
      current_epoch finalized_epoch = .ok f) :
    f < current_epoch := by
  unfold process_finalizations at h
  simp only [Bind.bind, Except.bind] at h
  split at h
  · contradiction
  rename_i f1 h1
  split at h
  · contradiction
  rename_i f2 h2
  split at h
  · contradiction
  rename_i f3 h3
  have := finalization_rule_lt _ _ _ _ _ _ (by nomega) h1
  have := finalization_rule_lt _ _ _ _ _ _ (by nomega) h2
  have := finalization_rule_lt _ _ _ _ _ _ (by nomega) h3
  have := finalization_rule_lt _ _ _ _ _ _ (by nomega) h
  nomega

/-! ## Electra -/

/-- `process_single_registry_update_post_electra` (`single_pass.rs`). It does not read the queue.
`is_eligible_for_activation_queue` and `is_active_at` are the `Validator` methods. The activation
check is the `is_eligible_for_activation` pure function. -/
def lhRegistryUpdatePostElectra (MIN_ACTIVATION_BALANCE EJECTION_BALANCE : Gwei)
    (current_epoch activation_epoch finalized_epoch : Epoch) (validator : Validator) :
    SpecM (Validator × Bool) := do
  let validator ←
    if is_eligible_for_activation_queue_electra MIN_ACTIVATION_BALANCE validator then do
      let epoch ← uint64Add current_epoch 1
      pure { validator with activation_eligibility_epoch := epoch }
    else pure validator
  let eject := is_active_validator validator current_epoch
    && decide (validator.effective_balance ≤ EJECTION_BALANCE)
  let validator := if is_eligible_for_activation finalized_epoch validator then
    { validator with activation_epoch := activation_epoch } else validator
  pure (validator, eject)

/-- Read agreement, Electra: Lighthouse uses three `if`s, the spec uses `if`/`elif`. They give
the same validator and the same ejection decision. -/
theorem lhRegistryUpdatePostElectra_eq (MIN_ACTIVATION_BALANCE EJECTION_BALANCE : Gwei)
    (current_epoch activation_epoch finalized_epoch : Epoch) (validator : Validator)
    (hfin : finalized_epoch ≤ current_epoch) (hcur : current_epoch < FAR_FUTURE_EPOCH)
    (hbal : EJECTION_BALANCE < MIN_ACTIVATION_BALANCE) :
    lhRegistryUpdatePostElectra MIN_ACTIVATION_BALANCE EJECTION_BALANCE current_epoch
        activation_epoch finalized_epoch validator =
      process_registry_update_electra MIN_ACTIVATION_BALANCE EJECTION_BALANCE current_epoch
        activation_epoch finalized_epoch validator := by
  unfold lhRegistryUpdatePostElectra process_registry_update_electra
  have hfar : FAR_FUTURE_EPOCH = 18446744073709551615 := rfl
  by_cases hq : is_eligible_for_activation_queue_electra MIN_ACTIVATION_BALANCE validator = true
  · simp only [hq, if_true]
    simp only [is_eligible_for_activation_queue_electra, Bool.and_eq_true,
      decide_eq_true_eq] at hq
    have ho : current_epoch + 1 < UINT64_SIZE := by
      simp only [UINT64_SIZE]; nomega
    simp only [uint64Add, ho, if_true, Bind.bind, Except.bind, pure, Except.pure,
      is_eligible_for_activation, is_active_validator, Except.ok.injEq, Prod.mk.injEq]
    constructor
    · have : ¬ current_epoch + 1 ≤ finalized_epoch := by nomega
      simp [this]
    · simp only [Bool.and_eq_false_iff, decide_eq_false_iff_not]
      nomega
  · simp only [Bool.not_eq_true] at hq
    simp only [hq, Bool.false_eq_true, if_false, Bind.bind, Except.bind, pure, Except.pure]
    by_cases he : (is_active_validator validator current_epoch
        && decide (validator.effective_balance ≤ EJECTION_BALANCE)) = true
    · simp only [he, if_true]
      have hne : is_eligible_for_activation finalized_epoch validator = false := by
        simp only [is_active_validator, Bool.and_eq_true, decide_eq_true_eq] at he
        simp only [is_eligible_for_activation, Bool.and_eq_false_iff, decide_eq_false_iff_not]
        right; nomega
      simp [hne]
    · simp only [Bool.not_eq_true] at he
      simp only [he, Bool.false_eq_true, if_false]
      split <;> rfl

end CacheProofs.ActivationQueue
