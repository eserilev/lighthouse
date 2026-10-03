import EpochProofs.Equiv.PendingDeposits
import EpochProofs.Sanity.PendingConsolidations

/-!
# Lighthouse pending consolidations moves equal the reference

`process_pending_consolidations_equiv` relates `process_pending_consolidations` in
`pending_consolidations.rs` to `stepLoop consolidationStep`, over the local table that
`single_pass.rs` builds.

`single_pass.rs` builds the table from the validators that consolidations reference, and
writes the balances back. The theorems take the table as an input.
-/

namespace EpochProofs

open Aeneas Aeneas.Std Aeneas.Std.WP Result state_processing

abbrev LhConsolidationView := per_epoch_processing.pending_consolidations.ConsolidationView
abbrev LhLocalValidator := per_epoch_processing.pending_consolidations.LocalValidator

def absLocal (v : LhLocalValidator) : Spec.LocalValidator :=
  ⟨v.exists, v.slashed, v.withdrawable_epoch.val, v.effective_balance.val⟩

def absView (c : LhConsolidationView) : Nat × Nat := (c.source.val, c.target.val)

theorem get_balance_eq (s : Slice U64) (i : Usize) :
    per_epoch_processing.pending_consolidations.get_balance s i = ok s.val[i.val]? := by
  unfold per_epoch_processing.pending_consolidations.get_balance
  simp only [slice_get_any, bind_tc_ok]
  cases s.val[i.val]? <;> rfl

theorem validator_exists_eq (validators : Slice LhLocalValidator) (i : Usize) :
    per_epoch_processing.pending_consolidations.validator_exists validators i =
      ok (Spec.localExists (validators.val.map absLocal) i.val) := by
  unfold per_epoch_processing.pending_consolidations.validator_exists Spec.localExists
  simp only [slice_get_any, bind_tc_ok, List.getElem?_map]
  cases validators.val[i.val]? <;> rfl

theorem list_set_opt_some {α : Type} (l : List α) (i : Nat) (x : α) :
    l.set_opt i (some x) = l.set i x := by
  induction l generalizing i with
  | nil => simp [List.set_opt]
  | cons hd tl ih =>
    cases i with
    | zero => simp [List.set_opt]
    | succ n => simp [List.set_opt, ih]

theorem list_set_opt_none {α : Type} (l : List α) (i : Nat) : l.set_opt i none = l := by
  induction l generalizing i with
  | nil => simp [List.set_opt]
  | cons hd tl ih =>
    cases i with
    | zero => simp [List.set_opt]
    | succ n => simp [List.set_opt, ih]

theorem set_balance_eq (balances : alloc.vec.Vec U64) (i : Usize) (value : U64) :
    ∃ v, per_epoch_processing.pending_consolidations.set_balance balances i value = ok v
      ∧ v.val = balances.val.set i.val value := by
  unfold per_epoch_processing.pending_consolidations.set_balance
  simp only [lift, alloc.vec.Vec.deref_mut, core.slice.Slice.get_mut,
    core.slice.index.Usize.get_mut, bind_tc_ok,
    uncurry_apply_pair, Slice.getElem?_Usize_eq]
  by_cases h : i.val < balances.val.length
  · rw [List.getElem?_eq_getElem h]
    refine ⟨_, rfl, ?_⟩
    simp [Slice.set_opt, list_set_opt_some]
  · rw [List.getElem?_eq_none (Nat.le_of_not_lt h)]
    refine ⟨_, rfl, ?_⟩
    simp [Slice.set_opt, list_set_opt_none, List.set_eq_of_length_le (Nat.le_of_not_lt h)]

def absConsolidationError :
    per_epoch_processing.pending_consolidations.ConsolidationError → Spec.SpecError
  | .UnknownValidator _ => .indexOutOfRange
  | .Overflow => .overflow

theorem consolidations_loop_spec (consolidations : Slice LhConsolidationView)
    (validators : Slice LhLocalValidator) (next_epoch : U64)
    (init : List Spec.Gwei) (balances : alloc.vec.Vec U64) (next i : Usize)
    (hi : i.val ≤ consolidations.val.length) (hnext : next.val ≤ i.val)
    (hlen : balances.val.length = validators.val.length)
    (hst : Spec.stepLoop (Spec.consolidationStep (validators.val.map absLocal) next_epoch.val)
      (init, 0) ((consolidations.val.map absView).take i.val) =
      .ok ((balances.val.map (·.val), next.val), false)) :
    per_epoch_processing.pending_consolidations.process_pending_consolidations_loop
      consolidations validators next_epoch balances next i ⦃ r =>
        (∀ e, r.2.2 = some e → Spec.stepLoop
            (Spec.consolidationStep (validators.val.map absLocal) next_epoch.val) (init, 0)
            (consolidations.val.map absView) = .error (absConsolidationError e))
        ∧ (r.2.2 = none → ∃ stopped, Spec.stepLoop
            (Spec.consolidationStep (validators.val.map absLocal) next_epoch.val) (init, 0)
            (consolidations.val.map absView) =
              .ok ((r.1.val.map (·.val), r.2.1.val), stopped)) ⦄ := by
  unfold per_epoch_processing.pending_consolidations.process_pending_consolidations_loop
  apply loop.spec_decr_nat
    (measure := fun (x : alloc.vec.Vec U64 × Usize × Usize) =>
      consolidations.val.length - x.2.2.val)
    (inv := fun (x : alloc.vec.Vec U64 × Usize × Usize) =>
      x.2.2.val ≤ consolidations.val.length ∧ x.2.1.val ≤ x.2.2.val
      ∧ x.1.val.length = validators.val.length
      ∧ Spec.stepLoop (Spec.consolidationStep (validators.val.map absLocal) next_epoch.val)
          (init, 0) ((consolidations.val.map absView).take x.2.2.val) =
          .ok ((x.1.val.map (·.val), x.2.1.val), false))
  · rintro ⟨bal, nx, j⟩ ⟨hj, hnx, hbl, hstj⟩
    dsimp only at hj hnx hbl hstj
    unfold per_epoch_processing.pending_consolidations.process_pending_consolidations_loop.body
    simp only [slice_get_any, lift, bind_tc_ok]
    split
    · rename_i hlt
      have hjl : j.val < consolidations.val.length := by scalar_tac
      have hclen : consolidations.val.length ≤ Usize.max := by scalar_tac
      simp only [List.getElem?_eq_getElem hjl]
      have hget : (consolidations.val.map absView)[j.val]? =
          some (absView consolidations.val[j.val]) := by
        simp [hjl]
      have hjmax : j.val < Usize.max := by omega
      have hsat := usize_saturating_add_one j hjmax
      have hnxmax : nx.val < Usize.max := by omega
      have hsatn := usize_saturating_add_one nx hnxmax
      rw [validator_exists_eq, bind_tc_ok]
      by_cases hex : Spec.localExists (validators.val.map absLocal)
          consolidations.val[j.val].source.val = true
      swap
      · simp only [Bool.not_eq_true] at hex
        simp only [hex, Bool.false_eq_true, if_false, spec_ok]
        refine ⟨fun e he => ?_, fun he => by simp at he⟩
        simp only [Option.some.injEq] at he
        subst he
        exact Spec.stepLoop_error_at _ _ _ _ j.val _ hget hstj _
          (by simp [Spec.consolidationStep, absView, hex, throw, throwThe, MonadExceptOf.throw,
            Bind.bind, Except.bind, absConsolidationError])
      simp only [hex, if_true]
      have hexv := hex
      unfold Spec.localExists at hexv
      simp only [List.getElem?_map] at hexv
      cases hv : validators.val[consolidations.val[j.val].source.val]? with
      | none => simp [hv] at hexv
      | some v =>
      simp only [hv, Option.map_some] at hexv
      have hsrc : consolidations.val[j.val].source.val < validators.val.length :=
        (List.getElem?_eq_some_iff.mp hv).1
      have hlv : (validators.val.map absLocal)[consolidations.val[j.val].source.val]? =
          some (absLocal v) := by simp [hv]
      by_cases hsl : v.slashed = true
      · simp only [hsl, if_true, spec_ok, hsat, hsatn]
        refine ⟨⟨by omega, by omega, hbl, ?_⟩, by omega⟩
        exact Spec.stepLoop_next_at _ _ _ _ j.val _ hget hstj _
          (by simp [Spec.consolidationStep, absView, hex, Spec.listGet, hlv, absLocal, hsl,
            Pure.pure, Except.pure, Bind.bind, Except.bind])
      simp only [hsl, Bool.false_eq_true, if_false]
      by_cases hwe : v.withdrawable_epoch > next_epoch
      · have hwe' : next_epoch.val < v.withdrawable_epoch.val := by scalar_tac
        simp only [hwe, if_true, spec_ok]
        refine ⟨fun e he => by simp at he, fun _ => ⟨true, ?_⟩⟩
        exact Spec.stepLoop_stop_at _ _ _ _ j.val _ hget hstj _
          (by simp [Spec.consolidationStep, absView, hex, Spec.listGet, hlv, absLocal, hsl, hwe',
            Pure.pure, Except.pure, Bind.bind, Except.bind])
      have hwe' : ¬ next_epoch.val < v.withdrawable_epoch.val := by scalar_tac
      simp only [hwe, if_false]
      have hsrcb : consolidations.val[j.val].source.val < bal.val.length := by omega
      simp only [get_balance_eq, alloc.vec.Vec.deref, bind_tc_ok, List.getElem?_eq_getElem hsrcb,
        core.cmp.min, liftFun2]
      have hmin := core.cmp.impls.OrdU64.min_val bal.val[consolidations.val[j.val].source.val]
        v.effective_balance
      obtain ⟨b1, hb1, hb1v⟩ := set_balance_eq bal consolidations.val[j.val].source
        (core.num.U64.saturating_sub bal.val[consolidations.val[j.val].source.val]
          (core.cmp.impls.OrdU64.min bal.val[consolidations.val[j.val].source.val]
            v.effective_balance))
      rw [hb1, bind_tc_ok, validator_exists_eq, bind_tc_ok]
      have hb1len : b1.val.length = validators.val.length := by simp [hb1v, hbl]
      have hL1 : b1.val.map (·.val) = (bal.val.map (·.val)).set
          consolidations.val[j.val].source.val
          (bal.val[consolidations.val[j.val].source.val].val -
            min bal.val[consolidations.val[j.val].source.val].val v.effective_balance.val) := by
        simp [hb1v, List.map_set, u64_saturating_sub_val, hmin]
      have hdec : Spec.decrease_balance (bal.val.map (·.val)) consolidations.val[j.val].source.val
          (min bal.val[consolidations.val[j.val].source.val].val v.effective_balance.val) =
          .ok (b1.val.map (·.val)) := by
        simp [Spec.decrease_balance, Spec.listGet, Spec.listSet, hsrcb, hL1, saturating_sub_eq,
          Pure.pure, Except.pure, Bind.bind, Except.bind]
      by_cases hext : Spec.localExists (validators.val.map absLocal)
          consolidations.val[j.val].target.val = true
      swap
      · simp only [Bool.not_eq_true] at hext
        simp only [hext, Bool.false_eq_true, if_false, spec_ok]
        refine ⟨fun e he => ?_, fun he => by simp at he⟩
        simp only [Option.some.injEq] at he
        subst he
        exact Spec.stepLoop_error_at _ _ _ _ j.val _ hget hstj _
          (by simp [Spec.consolidationStep, absView, hex, Spec.listGet, hlv, absLocal, hsl, hwe',
            List.getElem?_map, List.getElem?_eq_getElem hsrcb, hdec, hext, throw, throwThe,
            MonadExceptOf.throw, Pure.pure, Except.pure, Bind.bind, Except.bind,
            absConsolidationError])
      simp only [hext, if_true]
      have hextv := hext
      unfold Spec.localExists at hextv
      simp only [List.getElem?_map] at hextv
      have htgt : consolidations.val[j.val].target.val < b1.val.length := by
        rw [hb1len]
        cases hv2 : validators.val[consolidations.val[j.val].target.val]? with
        | none => simp [hv2] at hextv
        | some _ => exact (List.getElem?_eq_some_iff.mp hv2).1
      simp only [List.getElem?_eq_getElem htgt]
      have hca := U64.checked_add_bv_spec b1.val[consolidations.val[j.val].target.val]
        (core.cmp.impls.OrdU64.min bal.val[consolidations.val[j.val].source.val] v.effective_balance)
      simp only [U64.max_eq] at hca
      have hstep_pre : ∀ r : Spec.SpecM ((List Spec.Gwei × Nat) × Bool),
          (do
            let bs ← Spec.increase_balance (b1.val.map (·.val)) consolidations.val[j.val].target.val
              (min bal.val[consolidations.val[j.val].source.val].val v.effective_balance.val)
            pure ((bs, nx.val + 1), false)) = r →
          Spec.consolidationStep (validators.val.map absLocal) next_epoch.val
            (bal.val.map (·.val), nx.val) (absView consolidations.val[j.val]) = r := by
        intro r hr
        rw [← hr]
        simp [Spec.consolidationStep, absView, hex, Spec.listGet, hlv, absLocal, hsl, hwe',
          List.getElem?_map, List.getElem?_eq_getElem hsrcb, hdec, hext,
          Pure.pure, Except.pure, Bind.bind, Except.bind]
      cases hc : b1.val[consolidations.val[j.val].target.val].checked_add
          (core.cmp.impls.OrdU64.min bal.val[consolidations.val[j.val].source.val]
            v.effective_balance) <;> simp only [hc] at hca
      · simp only [spec_ok]
        refine ⟨fun e he => ?_, fun he => by simp at he⟩
        simp only [Option.some.injEq] at he
        subst he
        have hov : ¬ b1.val[consolidations.val[j.val].target.val].val +
            min bal.val[consolidations.val[j.val].source.val].val v.effective_balance.val <
            18446744073709551616 := by rw [← hmin]; omega
        refine Spec.stepLoop_error_at _ _ _ _ j.val _ hget hstj _ (hstep_pre _ ?_)
        simp [Spec.increase_balance, Spec.listGet, Spec.uint64Add, Spec.UINT64_SIZE,
          List.getElem?_map, List.getElem?_eq_getElem htgt, hov, throw, throwThe,
          MonadExceptOf.throw, Pure.pure, Except.pure, Bind.bind, Except.bind,
          absConsolidationError]
      · rename_i nb
        obtain ⟨b2, hb2, hb2v⟩ := set_balance_eq b1 consolidations.val[j.val].target nb
        dsimp only
        rw [hb2, bind_tc_ok]
        simp only [spec_ok, hsat, hsatn]
        refine ⟨⟨by omega, by omega, by simp [hb2v, hb1len], ?_⟩, by omega⟩
        have hfit : b1.val[consolidations.val[j.val].target.val].val +
            min bal.val[consolidations.val[j.val].source.val].val v.effective_balance.val <
            18446744073709551616 := by rw [← hmin]; omega
        refine Spec.stepLoop_next_at _ _ _ _ j.val _ hget hstj _ (hstep_pre _ ?_)
        simp [Spec.increase_balance, Spec.listGet, Spec.listSet, Spec.uint64Add, Spec.UINT64_SIZE,
          hfit, htgt, hb2v, List.map_set,
          hca.2.1, hmin, Pure.pure, Except.pure, Bind.bind, Except.bind]
    · rename_i hge
      have hjl : consolidations.val.length ≤ j.val := by scalar_tac
      simp only [spec_ok]
      refine ⟨fun e he => by simp at he, fun _ => ⟨false, ?_⟩⟩
      exact Spec.stepLoop_end _ _ _ j.val (by simp; omega) _ hstj
  · exact ⟨hi, hnext, hlen, hst⟩

def absConsolidations :
    core.result.Result (Usize × alloc.vec.Vec U64)
      per_epoch_processing.pending_consolidations.ConsolidationError →
    Spec.SpecM (List Spec.Gwei × Nat)
  | .Ok (n, b) => .ok (b.val.map (·.val), n.val)
  | .Err e => .error (absConsolidationError e)

theorem process_pending_consolidations_equiv (consolidations : Slice LhConsolidationView)
    (validators : Slice LhLocalValidator) (balances : alloc.vec.Vec U64) (next_epoch : U64)
    (hlen : balances.val.length = validators.val.length) :
    per_epoch_processing.pending_consolidations.process_pending_consolidations consolidations
      validators balances next_epoch ⦃ r =>
        absConsolidations r = (·.1) <$> Spec.stepLoop
          (Spec.consolidationStep (validators.val.map absLocal) next_epoch.val)
          (balances.val.map (·.val), 0) (consolidations.val.map absView) ⦄ := by
  unfold per_epoch_processing.pending_consolidations.process_pending_consolidations
  apply exists_imp_spec
  obtain ⟨res, hres, hpost⟩ := spec_imp_exists (consolidations_loop_spec consolidations
    validators next_epoch (balances.val.map (·.val)) balances 0#usize 0#usize
    (by simp) (by simp) hlen (by simp [Spec.stepLoop, Pure.pure, Except.pure]))
  rw [hres]
  obtain ⟨b, n, err⟩ := res
  simp only [uncurry_apply_pair, bind_tc_ok]
  cases err with
  | none =>
    obtain ⟨stopped, hloop⟩ := hpost.2 rfl
    refine ⟨_, rfl, ?_⟩
    simp [absConsolidations, hloop, Functor.map, Except.map]
  | some e =>
    refine ⟨_, rfl, ?_⟩
    simp [absConsolidations, hpost.1 e rfl, Functor.map, Except.map]

end EpochProofs
