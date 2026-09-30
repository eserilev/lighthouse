import SlotScheduleProofs.Primitives

/-!
# What `validate_schedule` checks

`validate_schedule` runs three loops over the slice and then checks the genesis entry.
`validate_facts` collects what an `Ok(())` result shows about the slice.
`validate_slot_duration_changes` runs one loop, and `validate_changes_facts` states what its
`Ok(())` result shows.
-/

namespace SlotScheduleProofs

open Aeneas Aeneas.Std Aeneas.Std.WP Result types

theorem duplicate_loop_spec (schedule : Slice Entry) (index : Usize)
    (previous : Option core.slot_epoch.Epoch) (hidx : index.val ≤ schedule.length)
    (hprev : previous = if index.val = 0 then none else (schedule.val[index.val - 1]?).map (·.epoch))
    (hseen : ∀ j, j + 1 < index.val →
      (schedule.val[j]?).map (·.epoch) ≠ (schedule.val[j + 1]?).map (·.epoch)) :
    features.eip8198.validation.validate_schedule_loop0 index schedule previous ⦃ r =>
      r = none → ∀ j, j + 1 < schedule.length →
        (schedule.val[j]?).map (·.epoch) ≠ (schedule.val[j + 1]?).map (·.epoch) ⦄ := by
  unfold features.eip8198.validation.validate_schedule_loop0
  apply loop.spec_decr_nat (fun a => schedule.length - a.1.val)
    (fun a => a.1.val ≤ schedule.length ∧
      a.2 = (if a.1.val = 0 then none else (schedule.val[a.1.val - 1]?).map (·.epoch)) ∧
      ∀ j, j + 1 < a.1.val →
        (schedule.val[j]?).map (·.epoch) ≠ (schedule.val[j + 1]?).map (·.epoch))
  · rintro ⟨index1, previous1⟩ ⟨hidx1, hprev1, hseen1⟩
    simp only at hidx1 hprev1 hseen1
    simp only [features.eip8198.validation.validate_schedule_loop0.body, core.slice.Slice.get,
      core.slice.index.Usize.get, bind_tc_ok, Slice.getElem?_Usize_eq]
    rcases hget : schedule.val[index1.val]? with _ | entry
    · have hge : schedule.length ≤ index1.val := by
        simpa [Slice.length] using hget
      simp only [spec_ok]
      intro _ j hj
      exact hseen1 j (by omega)
    have hlt : index1.val < schedule.length := by
      have := (List.getElem?_eq_some_iff.mp hget).1
      simpa [Slice.length] using this
    have hsucc := index_succ_val index1 hlt
    simp only [lift, bind_tc_ok]
    rcases previous1 with _ | epoch
    · have h0 : index1.val = 0 := by
        by_contra hne
        rw [if_neg hne, List.getElem?_eq_getElem (by simp [Slice.length] at hlt; omega)] at hprev1
        simp at hprev1
      simp only [spec_ok]
      refine ⟨⟨by omega, ?_, by omega⟩, by omega⟩
      rw [hsucc, if_neg (by omega), h0]
      simp [h0] at hget
      simp [hget]
    · have hne : index1.val ≠ 0 := by
        intro h0; rw [if_pos h0] at hprev1; simp at hprev1
      rw [if_neg hne] at hprev1
      simp only [core.slot_epoch.Epoch.Insts.CoreCmpPartialEqEpoch.eq, bind_tc_ok]
      split
      · simp
      · rename_i hneq
        simp only [spec_ok]
        refine ⟨⟨by omega, ?_, ?_⟩, by omega⟩
        · rw [hsucc, if_neg (by omega)]
          simp [hget]
        · intro j hj
          rw [hsucc] at hj
          rcases (show j + 1 < index1.val ∨ j + 1 = index1.val by omega) with hj' | hj'
          · exact hseen1 j hj'
          · have : j = index1.val - 1 := by omega
            subst this
            rw [show index1.val - 1 + 1 = index1.val by omega, hget, ← hprev1]
            simpa using hneq
  · exact ⟨hidx, hprev, hseen⟩

theorem duration_loop_spec (schedule : Slice Entry) (index : Usize)
    (hidx : index.val ≤ schedule.length)
    (hseen : ∀ j e, j < index.val → schedule.val[j]? = some e →
      e.slot_duration_ms.val ≠ 0 ∧ e.slot_duration_ms.val % 1000 = 0) :
    features.eip8198.validation.validate_schedule_loop1 index schedule ⦃ r =>
      r = none → ∀ j e, j < schedule.length → schedule.val[j]? = some e →
        e.slot_duration_ms.val ≠ 0 ∧ e.slot_duration_ms.val % 1000 = 0 ⦄ := by
  unfold features.eip8198.validation.validate_schedule_loop1
  apply loop.spec_decr_nat (fun a => schedule.length - a.val)
    (fun a => a.val ≤ schedule.length ∧ ∀ j e, j < a.val → schedule.val[j]? = some e →
      e.slot_duration_ms.val ≠ 0 ∧ e.slot_duration_ms.val % 1000 = 0)
  · rintro index1 ⟨hidx1, hseen1⟩
    simp only [features.eip8198.validation.validate_schedule_loop1.body, core.slice.Slice.get,
      core.slice.index.Usize.get, bind_tc_ok, Slice.getElem?_Usize_eq]
    rcases hget : schedule.val[index1.val]? with _ | entry
    · have hge : schedule.length ≤ index1.val := by
        simpa [Slice.length] using hget
      simp only [spec_ok]
      intro _ j e hj he
      exact hseen1 j e (by omega) he
    have hlt : index1.val < schedule.length := by
      have := (List.getElem?_eq_some_iff.mp hget).1
      simpa [Slice.length] using this
    have hsucc := index_succ_val index1 hlt
    simp only
    split
    · simp
    · rename_i hz
      step as ⟨m, hm⟩
      split
      · simp
      · rename_i hm0
        simp only [lift, bind_tc_ok, spec_ok]
        refine ⟨by omega, ?_, by omega⟩
        intro j e hj he
        rw [hsucc] at hj
        rcases (show j < index1.val ∨ j = index1.val by omega) with hj' | hj'
        · exact hseen1 j e hj' he
        · subst hj'
          rw [hget] at he
          cases he
          refine ⟨by scalar_tac, ?_⟩
          simp only [bne_iff_ne, ne_eq, Decidable.not_not] at hm0
          rw [← hm, hm0]; rfl
  · exact ⟨hidx, hseen⟩

theorem start_slot_loop_spec (schedule : Slice Entry) (spe : U64) (index : Usize)
    (hidx : index.val ≤ schedule.length)
    (hseen : ∀ j e, j < index.val → schedule.val[j]? = some e →
      e.epoch.val * spe.val ≤ U64.max) :
    features.eip8198.validation.validate_schedule_loop2 index schedule spe ⦃ r =>
      r = none → ∀ j e, j < schedule.length → schedule.val[j]? = some e →
        e.epoch.val * spe.val ≤ U64.max ⦄ := by
  unfold features.eip8198.validation.validate_schedule_loop2
  apply loop.spec_decr_nat (fun a => schedule.length - a.val)
    (fun a => a.val ≤ schedule.length ∧ ∀ j e, j < a.val → schedule.val[j]? = some e →
      e.epoch.val * spe.val ≤ U64.max)
  · rintro index1 ⟨hidx1, hseen1⟩
    simp only [features.eip8198.validation.validate_schedule_loop2.body, core.slice.Slice.get,
      core.slice.index.Usize.get, bind_tc_ok, Slice.getElem?_Usize_eq]
    rcases hget : schedule.val[index1.val]? with _ | entry
    · have hge : schedule.length ≤ index1.val := by
        simpa [Slice.length] using hget
      simp only [spec_ok]
      intro _ j e hj he
      exact hseen1 j e (by omega) he
    have hlt : index1.val < schedule.length := by
      have := (List.getElem?_eq_some_iff.mp hget).1
      simpa [Slice.length] using this
    have hsucc := index_succ_val index1 hlt
    have hmul := U64.checked_mul_bv_spec entry.epoch spe
    simp only [core.slot_epoch.Epoch.as_u64, lift, bind_tc_ok, core.option.Option.is_none]
    rcases hc : U64.checked_mul entry.epoch spe with _ | w
    · simp
    · simp only [hc] at hmul
      simp only [Option.isNone_some, Bool.false_eq_true, if_false, spec_ok]
      refine ⟨⟨by omega, ?_⟩, by omega⟩
      intro j e hj he
      rw [hsucc] at hj
      rcases (show j < index1.val ∨ j = index1.val by omega) with hj' | hj'
      · exact hseen1 j e hj' he
      · subst hj'
        rw [hget] at he
        cases he
        exact hmul.1
  · exact ⟨hidx, hseen⟩

theorem fork_epoch_loop_spec (schedule : Slice Entry) (fork : Option core.slot_epoch.Epoch)
    (index : Usize) (hidx : index.val ≤ schedule.length)
    (hseen : ∀ j e, j < index.val → schedule.val[j]? = some e →
      e.epoch.val = 0 ∨ fork = some e.epoch) :
    features.eip8198.validation.validate_slot_duration_changes_loop index schedule fork ⦃ r =>
      r = none → ∀ j e, j < schedule.length → schedule.val[j]? = some e →
        e.epoch.val = 0 ∨ fork = some e.epoch ⦄ := by
  unfold features.eip8198.validation.validate_slot_duration_changes_loop
  apply loop.spec_decr_nat (fun a => schedule.length - a.val)
    (fun a => a.val ≤ schedule.length ∧ ∀ j e, j < a.val → schedule.val[j]? = some e →
      e.epoch.val = 0 ∨ fork = some e.epoch)
  · rintro index1 ⟨hidx1, hseen1⟩
    simp only [features.eip8198.validation.validate_slot_duration_changes_loop.body,
      core.slice.Slice.get, core.slice.index.Usize.get, bind_tc_ok, Slice.getElem?_Usize_eq]
    rcases hget : schedule.val[index1.val]? with _ | entry
    · have hge : schedule.length ≤ index1.val := by
        simpa [Slice.length] using hget
      simp only [spec_ok]
      intro _ j e hj he
      exact hseen1 j e (by omega) he
    have hlt : index1.val < schedule.length := by
      have := (List.getElem?_eq_some_iff.mp hget).1
      simpa [Slice.length] using this
    have hsucc := index_succ_val index1 hlt
    have hextend : (entry.epoch.val = 0 ∨ fork = some entry.epoch) →
        ∀ j e, j < (core.num.Usize.saturating_add index1 1#usize).val →
          schedule.val[j]? = some e → e.epoch.val = 0 ∨ fork = some e.epoch := by
      intro hentry j e hj he
      rw [hsucc] at hj
      rcases (show j < index1.val ∨ j = index1.val by omega) with hj' | hj'
      · exact hseen1 j e hj' he
      · subst hj'
        rw [hget] at he
        cases he
        exact hentry
    simp only [core.slot_epoch.Epoch.new, bind_tc_ok]
    step as ⟨b, hb⟩
    · simp [core.slot_epoch.Epoch.Insts.CoreCmpPartialEqEpoch.eq]
    split
    · rename_i hb1
      simp only [core.option.Option.is_some_and,
        features.eip8198.validation.validate_slot_duration_changes.closure.Insts.CoreOpsFunctionFnOnceTupleEpochBool.call_once,
        core.slot_epoch.Epoch.Insts.CoreCmpPartialEqEpoch.eq]
      rcases fork with _ | fork_epoch
      · simp
      · simp only [bind_tc_ok]
        by_cases heq : fork_epoch = entry.epoch
        · have hx := hextend (Or.inr (by rw [heq]))
          subst heq
          simp only [lift, bind_tc_ok]
          exact ⟨by omega, hx, by omega⟩
        · simp [heq]
    · rename_i hb1
      simp only [lift, bind_tc_ok, spec_ok]
      refine ⟨by omega, hextend (Or.inl ?_), by omega⟩
      simp only [Bool.not_eq_true] at hb1
      subst hb1
      simp only [Bool.false_eq_true, false_iff, ne_eq, Decidable.not_not] at hb
      rw [hb]; rfl
  · exact ⟨hidx, hseen⟩

theorem validate_facts (schedule : Slice Entry) (genesis_slot_duration_ms spe : U64)
    (hv : features.eip8198.validation.validate_schedule schedule genesis_slot_duration_ms spe =
      ok (.Ok ()))
    (hne : 0 < schedule.length) :
    (∀ j, j + 1 < schedule.length →
      (schedule.val[j]?).map (·.epoch) ≠ (schedule.val[j + 1]?).map (·.epoch)) ∧
    (∀ j e, j < schedule.length → schedule.val[j]? = some e →
      e.slot_duration_ms.val ≠ 0 ∧ e.slot_duration_ms.val % 1000 = 0) ∧
    (∀ j e, j < schedule.length → schedule.val[j]? = some e → e.epoch.val * spe.val ≤ U64.max) ∧
    (schedule.val[schedule.length - 1]?).map (fun e => (e.epoch.val, e.slot_duration_ms)) =
      some (0, genesis_slot_duration_ms) := by
  unfold features.eip8198.validation.validate_schedule at hv
  have hempty : core.slice.Slice.is_empty schedule = ok false := by
    rw [core.slice.Slice.is_empty]
    simp only [ok.injEq, decide_eq_false_iff_not]
    omega
  rw [hempty] at hv
  simp only [bind_tc_ok, Bool.false_eq_true, if_false] at hv
  obtain ⟨r0, h0eq, h0⟩ := spec_imp_exists (duplicate_loop_spec schedule 0#usize none
    (by simp) (by simp) (by simp))
  rw [h0eq] at hv
  rcases r0 with _ | epoch
  swap
  · simp only [bind_tc_ok] at hv
    exact absurd hv (by repeat (apply bind_ne_ok; intro _)
                        intro h; cases h)
  simp only [bind_tc_ok] at hv
  have hnodup := h0 rfl
  obtain ⟨r1, h1eq, h1⟩ := spec_imp_exists (duration_loop_spec schedule 0#usize
    (by simp) (by simp))
  rw [h1eq] at hv
  rcases r1 with _ | p1
  swap
  · simp only [bind_tc_ok] at hv
    exact absurd hv (by repeat (apply bind_ne_ok; intro _)
                        intro h; cases h)
  simp only [bind_tc_ok] at hv
  have hdur := h1 rfl
  obtain ⟨r2, h2eq, h2⟩ := spec_imp_exists (start_slot_loop_spec schedule spe 0#usize
    (by simp) (by simp))
  rw [h2eq] at hv
  rcases r2 with _ | p2
  swap
  · simp only [bind_tc_ok] at hv
    exact absurd hv (by repeat (apply bind_ne_ok; intro _)
                        intro h; cases h)
  simp only [bind_tc_ok] at hv
  have hfit := h2 rfl
  have hlast : schedule.length - 1 < schedule.length := by omega
  have hsub : (core.num.Usize.saturating_sub (Slice.len schedule) 1#usize).val = schedule.length - 1 := by
    rw [usize_saturating_sub_val, Slice.len_val]; rfl
  have hget : (schedule[core.num.Usize.saturating_sub (Slice.len schedule) 1#usize]? : Option Entry) =
      some schedule.val[schedule.length - 1] := by
    rw [Slice.getElem?_Usize_eq, hsub, List.getElem?_eq_getElem]
  simp only [lift, bind_tc_ok, core.slice.Slice.get, core.slice.index.Usize.get, hget,
    core.slot_epoch.Epoch.new, core.slot_epoch.Epoch.Insts.CoreCmpPartialEqEpoch.eq] at hv
  by_cases hg0 : schedule.val[schedule.length - 1].epoch = 0#u64
  swap
  · rw [if_neg (by simpa using hg0)] at hv
    exact absurd hv (by repeat (apply bind_ne_ok; intro _)
                        intro h; cases h)
  rw [if_pos (by simpa using hg0)] at hv
  by_cases hgd : schedule.val[schedule.length - 1].slot_duration_ms = genesis_slot_duration_ms
  swap
  · rw [if_pos (by simpa [UScalar.eq_equiv] using hgd)] at hv
    exact absurd hv (by repeat (apply bind_ne_ok; intro _)
                        intro h; cases h)
  refine ⟨hnodup, hdur, hfit, ?_⟩
  rw [List.getElem?_eq_getElem hlast, Option.map_some, hg0, hgd]
  rfl

theorem validate_changes_facts (schedule : Slice Entry) (fork : Option core.slot_epoch.Epoch)
    (hv : features.eip8198.validation.validate_slot_duration_changes schedule fork = ok (.Ok ())) :
    ∀ j e, j < schedule.length → schedule.val[j]? = some e →
      e.epoch.val = 0 ∨ fork = some e.epoch := by
  unfold features.eip8198.validation.validate_slot_duration_changes at hv
  obtain ⟨r, heq, h⟩ := spec_imp_exists (fork_epoch_loop_spec schedule fork 0#usize
    (by simp) (by simp))
  rw [heq] at hv
  rcases r with _ | epoch
  swap
  · simp only [bind_tc_ok] at hv
    exact absurd hv (by repeat (apply bind_ne_ok; intro _)
                        intro h; cases h)
  exact h rfl

end SlotScheduleProofs
