import CacheProofs.Generated
import CacheProofs.Spec.ExitCache

namespace CacheProofs

open Aeneas Aeneas.Std Result types

def recordStep (s : Nat × Nat) (e : Nat) : Option (Nat × Nat) :=
  if e = s.1 then (if s.2 + 1 ≤ 18446744073709551615 then some (s.1, s.2 + 1) else none)
  else if s.1 < e then some (e, 1) else some s

def absRecord : core.result.Result (U64 × U64) safe_arith.ArithError → Option (Nat × Nat)
  | .Ok (m, c) => some (m.val, c.val)
  | .Err _ => none

theorem record_exit_spec (m c e : U64) :
    ∃ r, state.exit_queue.record_exit m c e = ok r ∧
      absRecord r = recordStep (m.val, c.val) e.val := by
  unfold state.exit_queue.record_exit U64.Insts.Safe_arithSafeArithU64.safe_add
  by_cases heq : e = m
  · subst heq
    have h := U64.checked_add_bv_spec c 1#u64
    cases hc : U64.checked_add c 1#u64 with
    | none =>
      simp only [hc] at h
      simp [recordStep, absRecord, core.option.Option.ok_or, lift,
        core.result.Result.Insts.CoreOpsTry.branch,
        core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual]
      simp [U64.max_eq] at h
      omega
    | some v =>
      simp only [hc] at h
      simp [recordStep, absRecord, core.option.Option.ok_or, lift,
        core.result.Result.Insts.CoreOpsTry.branch]
      simp [U64.max_eq] at h
      omega
  · have hne : e.val ≠ m.val := fun h => heq (by scalar_tac)
    by_cases hlt : m.val < e.val
    · have hgt : e > m := by scalar_tac
      simp [heq, hgt, recordStep, absRecord, hne, hlt]
    · have hgt : ¬ e > m := by scalar_tac
      simp [heq, hgt, recordStep, absRecord, hne, hlt]

def maxOr0 (L : List Nat) : Nat := L.foldl max 0

/-- `(max_exit_epoch, max_exit_epoch_churn)` of a list of exit epochs. -/
def closedForm (L : List Nat) : Nat × Nat := (maxOr0 L, L.count (maxOr0 L))

theorem foldl_max_ge_init (L : List Nat) (a : Nat) : a ≤ L.foldl max a := by
  induction L generalizing a with
  | nil => simp
  | cons x t ih => exact le_trans (le_max_left a x) (ih _)

theorem le_foldl_max (L : List Nat) (a : Nat) : ∀ x ∈ L, x ≤ L.foldl max a := by
  induction L generalizing a with
  | nil => simp
  | cons y t ih =>
    intro x hx
    rcases List.mem_cons.mp hx with h | h
    · subst h; exact le_trans (le_max_right a x) (foldl_max_ge_init t _)
    · exact ih _ x h

theorem maxOr0_snoc (L : List Nat) (e : Nat) : maxOr0 (L ++ [e]) = max (maxOr0 L) e := by
  simp [maxOr0]

theorem maxOr0_mem (L : List Nat) (h : L ≠ []) : maxOr0 L ∈ L := by
  induction L using List.reverseRecOn with
  | nil => exact absurd rfl h
  | append_singleton t x ih =>
    rw [maxOr0_snoc]
    by_cases ht : t = []
    · subst ht; simp [maxOr0]
    · rcases max_choice (maxOr0 t) x with hm | hm <;> rw [hm]
      · exact List.mem_append_left _ (ih ht)
      · simp

theorem count_le_length (L : List Nat) (a : Nat) : L.count a ≤ L.length :=
  List.count_le_length

theorem closedForm_snoc (L : List Nat) (e : Nat) (hlen : L.length + 1 ≤ 18446744073709551615) :
    recordStep (closedForm L) e = some (closedForm (L ++ [e])) := by
  have hle := le_foldl_max L 0
  have hc := count_le_length L (maxOr0 L)
  simp only [closedForm, recordStep, maxOr0_snoc]
  by_cases heq : e = maxOr0 L
  · subst heq
    simp [List.count_append]
    omega
  · by_cases hlt : maxOr0 L < e
    · have hmax : max (maxOr0 L) e = e := max_eq_right (le_of_lt hlt)
      have hzero : L.count e = 0 := by
        apply List.count_eq_zero.mpr
        intro hmem
        have := hle e hmem
        simp only [maxOr0] at hlt
        omega
      simp [heq, hlt, hmax, List.count_append, hzero]
    · have hmax : max (maxOr0 L) e = maxOr0 L := max_eq_left (by omega)
      simp [heq, hlt, hmax, List.count_append]

theorem closedForm_perm {L L' : List Nat} (h : L.Perm L') : closedForm L = closedForm L' := by
  have hm : maxOr0 L = maxOr0 L' := by
    unfold maxOr0
    exact h.foldl_eq' (fun x _ y _ z => by rw [max_assoc, max_assoc, max_comm x y]) 0
  simp [closedForm, hm, h.count_eq]

def nonFar (e : Nat) : Bool := e ≠ Spec.FAR_FUTURE_EPOCH

/-- `ExitCache::new` from `exit_cache.rs`: skip `FAR_FUTURE_EPOCH`, then record each exit. -/
def lhBuildFrom (s : U64 × U64) :
    List U64 → Result (core.result.Result (U64 × U64) safe_arith.ArithError)
  | [] => ok (.Ok s)
  | e :: es =>
    if e.val = Spec.FAR_FUTURE_EPOCH then lhBuildFrom s es
    else do
      let r ← state.exit_queue.record_exit s.1 s.2 e
      match r with
      | .Ok s' => lhBuildFrom s' es
      | .Err err => ok (.Err err)

def lhBuild (es : List U64) := lhBuildFrom (0#u64, 0#u64) es

theorem lhBuildFrom_spec (es : List U64) :
    ∀ (L : List Nat) (s : U64 × U64), (s.1.val, s.2.val) = closedForm L →
      L.length + ((es.map (·.val)).filter nonFar).length ≤ 18446744073709551615 →
      ∃ s', lhBuildFrom s es = ok (.Ok s') ∧
        (s'.1.val, s'.2.val) = closedForm (L ++ (es.map (·.val)).filter nonFar) := by
  induction es with
  | nil => intro L s hs _; exact ⟨s, rfl, by simpa using hs⟩
  | cons e t ih =>
    intro L s hs hlen
    by_cases hfar : e.val = Spec.FAR_FUTURE_EPOCH
    · have hf : nonFar e.val = false := by simp [nonFar, hfar]
      simp only [List.map_cons, List.filter_cons, hf] at hlen ⊢
      simp only [lhBuildFrom, hfar, if_true]
      exact ih L s hs (by simpa using hlen)
    · have hf : nonFar e.val = true := by simp [nonFar, hfar]
      simp only [List.map_cons, List.filter_cons, hf, if_true, List.length_cons] at hlen ⊢
      obtain ⟨r, hr, habs⟩ := record_exit_spec s.1 s.2 e
      have hstep := closedForm_snoc L e.val (by omega)
      rw [← hs] at hstep
      rw [hstep] at habs
      cases r with
      | Err err => simp [absRecord] at habs
      | Ok s' =>
        simp only [absRecord, Option.some.injEq] at habs
        obtain ⟨s'', h1, h2⟩ := ih (L ++ [e.val]) s' habs (by simp; omega)
        refine ⟨s'', ?_, by simpa using h2⟩
        simp [lhBuildFrom, hfar, hr, h1]

/-- Build: `ExitCache::new` holds the largest exit epoch below `FAR_FUTURE_EPOCH` (or 0) and
the number of validators that exit at it. -/
theorem build_eq (es : List U64) (hlen : es.length ≤ 18446744073709551615) :
    ∃ s, lhBuild es = ok (.Ok s) ∧
      (s.1.val, s.2.val) = closedForm ((es.map (·.val)).filter nonFar) := by
  have h := lhBuildFrom_spec es [] (0#u64, 0#u64) (by simp [closedForm, maxOr0])
    (by simp; exact le_trans (List.length_filter_le _ _) (by simpa using hlen))
  simpa [lhBuild] using h

theorem churn_at_eq (m c q : U64) :
    state.exit_queue.churn_at m c q =
      ok (if q.val = m.val then some c else if m.val < q.val then some 0#u64 else none) := by
  unfold state.exit_queue.churn_at
  by_cases h1 : q = m
  · subst h1; simp
  · have h1' : q.val ≠ m.val := fun h => h1 (by scalar_tac)
    by_cases h2 : m.val < q.val
    · have : q > m := by scalar_tac
      simp [h1, h1', h2, this]
    · have : ¬ q > m := by scalar_tac
      simp [h1, h1', h2, this]

inductive LhExitError where
  | arith
  | invalidEpoch

def lhFinish (q ch limit : Nat) : Except LhExitError Nat :=
  if limit ≤ ch then (if q + 1 ≤ 18446744073709551615 then .ok (q + 1) else .error .arith)
  else .ok q

/-- The pre-Electra exit queue epoch in `initiate_validator_exit` and in `single_pass.rs`. -/
def lhExitQueueEpoch (m c delayed limit : U64) : Result (Except LhExitError Nat) := do
  let q : U64 := if 0 < c.val then (if m.val ≤ delayed.val then delayed else m) else delayed
  let ch ← state.exit_queue.churn_at m c q
  match ch with
  | none => ok (.error .invalidEpoch)
  | some ch => ok (lhFinish q.val ch.val limit.val)

def absExit : Except LhExitError Nat → Spec.SpecM Nat
  | .ok q => .ok q
  | .error .arith => .error .overflow
  | .error .invalidEpoch => .error .assertionFailed

theorem finish_eq (q ch l : Nat) :
    absExit (lhFinish q ch l) = (if ch ≥ l then Spec.uint64Add q 1 else pure q) := by
  unfold lhFinish Spec.uint64Add Spec.UINT64_SIZE
  by_cases hl : l ≤ ch
  · by_cases ho : q + 1 ≤ 18446744073709551615
    · have : q + 1 < 2 ^ 64 := by omega
      simp only [hl, ho, this, if_true]; rfl
    · have : ¬ q + 1 < 2 ^ 64 := by omega
      simp only [hl, ho, this, if_true, if_false]; rfl
  · simp only [hl, if_false]; rfl

theorem count_filter_nonFar (l : List Nat) (q : Nat) (hq : q ≠ Spec.FAR_FUTURE_EPOCH) :
    (l.filter nonFar).count q = l.count q :=
  List.count_filter (by simp [nonFar, hq])

theorem mem_filter_nonFar_lt {l : List Nat} (hl : ∀ x ∈ l, x < 18446744073709551616) :
    ∀ x ∈ l.filter nonFar, x < Spec.FAR_FUTURE_EPOCH := by
  intro x hx
  simp only [List.mem_filter, nonFar, decide_eq_true_eq] at hx
  have := hl x hx.1
  simp only [Spec.FAR_FUTURE_EPOCH] at hx ⊢
  omega

/-- Read agreement: the exit queue epoch from a built cache equals the spec. -/
theorem exit_queue_epoch_equiv (es : List U64) (delayed limit : U64)
    (hdelayed : delayed.val < Spec.FAR_FUTURE_EPOCH)
    (hlen : es.length ≤ 18446744073709551615) :
    ∃ s, lhBuild es = ok (.Ok s) ∧
      (do let r ← lhExitQueueEpoch s.1 s.2 delayed limit; ok (absExit r)) =
        ok (Spec.exitQueueEpoch (es.map (·.val)) delayed.val limit.val) := by
  obtain ⟨s, hb, hcf⟩ := build_eq es hlen
  refine ⟨s, hb, ?_⟩
  set F := (es.map (·.val)).filter nonFar with hF
  have hm : s.1.val = maxOr0 F := by simp [closedForm] at hcf; exact hcf.1
  have hc : s.2.val = F.count (maxOr0 F) := by simp [closedForm] at hcf; exact hcf.2
  have hall : ∀ x ∈ F, x ≤ maxOr0 F := le_foldl_max F 0
  have hbound : ∀ x ∈ (es.map (·.val)), x < 18446744073709551616 := by
    intro x hx
    obtain ⟨y, _, rfl⟩ := List.mem_map.mp hx
    exact y.hBounds
  have hltF : ∀ x ∈ F, x < Spec.FAR_FUTURE_EPOCH := mem_filter_nonFar_lt hbound
  have hfar : Spec.FAR_FUTURE_EPOCH = 18446744073709551615 := rfl
  set q := max (maxOr0 F) delayed.val with hq
  have hqfar : q ≠ Spec.FAR_FUTURE_EPOCH := by
    by_cases hFe : F = []
    · have : maxOr0 F = 0 := by simp [hFe, maxOr0]
      omega
    · have := hltF _ (maxOr0_mem F hFe)
      omega
  have hlh : ∃ qU chU : U64,
      lhExitQueueEpoch s.1 s.2 delayed limit = ok (lhFinish qU.val chU.val limit.val) ∧
        qU.val = q ∧ chU.val = F.count q := by
    by_cases hFe : F = []
    · have hm0 : s.1.val = 0 := by simp [hm, hFe, maxOr0]
      have hc0 : s.2.val = 0 := by simp [hc, hFe]
      have hq0 : q = delayed.val := by simp [hq, hFe, maxOr0]
      have hcz : ¬ 0 < s.2.val := by omega
      by_cases hd : delayed.val = s.1.val
      · refine ⟨delayed, s.2, ?_, hq0.symm, by simp [hc0, hFe]⟩
        simp [lhExitQueueEpoch, hcz, churn_at_eq, hd]
      · have hlt : s.1.val < delayed.val := by omega
        refine ⟨delayed, 0#u64, ?_, hq0.symm, by simp [hFe]⟩
        simp [lhExitQueueEpoch, hcz, churn_at_eq, hd, hlt]
    · have hpos : 0 < s.2.val := by
        rw [hc]; exact List.count_pos_iff.mpr (maxOr0_mem F hFe)
      have hqU : (if s.1.val ≤ delayed.val then delayed else s.1).val = q := by
        by_cases h : s.1.val ≤ delayed.val
        · rw [if_pos h, hq, ← hm]; omega
        · rw [if_neg h, hq, ← hm]; omega
      by_cases h1 : q = s.1.val
      · refine ⟨_, s.2, ?_, hqU, by rw [hc, h1, hm]⟩
        simp only [lhExitQueueEpoch, hpos, if_true, churn_at_eq, bind_tc_ok, hqU, h1]
      · have hgt : s.1.val < q := by omega
        have hz : F.count q = 0 := by
          apply List.count_eq_zero.mpr
          intro hmem; have := hall q hmem; omega
        refine ⟨_, 0#u64, ?_, hqU, by simp [hz]⟩
        simp only [lhExitQueueEpoch, hpos, if_true, churn_at_eq, bind_tc_ok, hqU, h1, hgt]
        simp
  obtain ⟨qU, chU, hlhEq, hqv, hchv⟩ := hlh
  have hspec_filter : (es.map (·.val)).filter (· ≠ Spec.FAR_FUTURE_EPOCH) = F := by
    rw [hF]; apply List.filter_congr; intro x _; simp [nonFar]
  have hcount : ((es.map (·.val)).filter (· = q)).length = F.count q := by
    rw [count_filter_nonFar _ _ hqfar, List.count_eq_length_filter]
    congr 1
  have hspecq : (F ++ [delayed.val]).foldl max 0 = q := maxOr0_snoc F delayed.val
  rw [hlhEq, bind_tc_ok, hqv, hchv, finish_eq]
  simp only [Spec.exitQueueEpoch, hspec_filter, hspecq, hcount]

theorem filter_set_perm (p : Nat → Bool) (l : List Nat) (i : Nat) (x : Nat)
    (hi : i < l.length) (hp : p l[i] = false) (hx : p x = true) :
    ((l.set i x).filter p).Perm (l.filter p ++ [x]) := by
  induction l generalizing i with
  | nil => simp at hi
  | cons a t ih =>
    cases i with
    | zero =>
      simp only [List.getElem_cons_zero] at hp
      simp only [List.set_cons_zero, List.filter_cons, hx, hp, if_true]
      exact (List.perm_append_singleton x _).symm
    | succ j =>
      simp only [List.length_cons] at hi
      simp only [List.getElem_cons_succ] at hp
      simp only [List.set_cons_succ, List.filter_cons]
      split
      · exact (ih j (by omega) hp).cons a
      · exact ih j (by omega) hp

/-- Preservation: if a validator with `FAR_FUTURE_EPOCH` gets the exit epoch `e`, then
`record_exit` on the old cache equals a rebuild from the new exit epochs. -/
theorem build_set_record (es : List U64) (i : Nat) (e : U64) (hi : i < es.length)
    (hfar : es[i].val = Spec.FAR_FUTURE_EPOCH) (he : e.val ≠ Spec.FAR_FUTURE_EPOCH)
    (hlen : es.length ≤ 18446744073709551615) :
    ∃ s r s', lhBuild es = ok (.Ok s) ∧
      state.exit_queue.record_exit s.1 s.2 e = ok (.Ok r) ∧
      lhBuild (es.set i e) = ok (.Ok s') ∧ r.1.val = s'.1.val ∧ r.2.val = s'.2.val := by
  obtain ⟨s, hb, hcf⟩ := build_eq es hlen
  obtain ⟨s', hb', hcf'⟩ := build_eq (es.set i e) (by simpa using hlen)
  have hperm : (((es.set i e).map (·.val)).filter nonFar).Perm
      ((es.map (·.val)).filter nonFar ++ [e.val]) := by
    rw [List.map_set]
    exact filter_set_perm nonFar _ i e.val (by simpa using hi)
      (by simp [nonFar, hfar]) (by simp [nonFar, he])
  have hlenF : ((es.map (·.val)).filter nonFar).length + 1 ≤ 18446744073709551615 := by
    have h1 := hperm.length_eq
    have h2 := List.length_filter_le nonFar ((es.set i e).map (·.val))
    simp at h1 h2
    omega
  obtain ⟨r, hr, habs⟩ := record_exit_spec s.1 s.2 e
  rw [hcf, closedForm_snoc _ _ hlenF, ← closedForm_perm hperm, ← hcf'] at habs
  cases r with
  | Err err => simp [absRecord] at habs
  | Ok r =>
    simp only [absRecord, Option.some.injEq, Prod.mk.injEq] at habs
    exact ⟨s, r, s', hb, hr, hb', habs.1, habs.2⟩

end CacheProofs
