import SlotScheduleProofs.Primitives

/-!
# The Rust walks refine the walk over natural numbers

`compute_time_at_slot_ms` computes `timeAtSlot`, or returns an error when that value does not fit
in a `u64`. `compute_slot_at_time_ms` computes `slotAtTime` at or after genesis, and returns an
error before genesis. Each loop is proved with an invariant that ties the loop state to the walk.
-/

namespace SlotScheduleProofs

open Aeneas Aeneas.Std Aeneas.Std.WP Result types

theorem time_loop_spec (schedule : Slice Entry) (spe : U64) (x : core.slot_epoch.Slot)
    (hfit : StartSlotsFit schedule spe) (index : Usize) (s : core.slot_epoch.Slot) (t d : U64)
    (hlen : index.val < schedule.length)
    (hc : Chain (entries schedule spe) index.val s.val d.val) (hsx : s.val ≤ x.val) :
    core.slot_duration_schedule.compute_time_at_slot_ms_loop schedule spe x index s t d ⦃ r =>
      (r.2.2.2 = true → U64.max < walkTime (entries schedule spe) x.val index.val s.val t.val d.val) ∧
      (r.2.2.2 = false → r.1.val ≤ x.val ∧
        walkTime (entries schedule spe) x.val index.val s.val t.val d.val =
          r.2.1.val + (x.val - r.1.val) * r.2.2.1.val) ⦄ := by
  unfold core.slot_duration_schedule.compute_time_at_slot_ms_loop
  apply loop.spec_decr_nat (fun a => a.1.val)
    (fun a => a.1.val < schedule.length ∧
      Chain (entries schedule spe) a.1.val a.2.1.val a.2.2.2.val ∧ a.2.1.val ≤ x.val ∧
      walkTime (entries schedule spe) x.val a.1.val a.2.1.val a.2.2.1.val a.2.2.2.val =
        walkTime (entries schedule spe) x.val index.val s.val t.val d.val)
  · rintro ⟨index1, s1, t1, d1⟩ ⟨hlen1, hc1, hsx1, hw1⟩
    simp only at hlen1 hc1 hsx1 hw1
    simp only [core.slot_duration_schedule.compute_time_at_slot_ms_loop.body]
    split
    · rename_i hpos
      obtain ⟨n, hn1⟩ : ∃ n, index1.val = n + 1 := ⟨index1.val - 1, by scalar_tac⟩
      have hsub : (core.num.Usize.saturating_sub index1 1#usize).val = n := by
        rw [usize_saturating_sub_val]; simp [hn1]
      rw [hn1] at hc1 hw1 hlen1
      obtain ⟨hdpos, hnlen, hse, hcn⟩ := hc1
      have hnlen' : n < schedule.length := by omega
      have hentry := entryAt_entries (spe := spe) hnlen'
      have hget : (schedule[core.num.Usize.saturating_sub index1 1#usize]? : Option Entry) =
          some schedule.val[n] := by
        rw [Slice.getElem?_Usize_eq, hsub, List.getElem?_eq_getElem]
      simp only [lift, bind_tc_ok, core.slice.Slice.get, core.slice.index.Usize.get, hget]
      have hf := hfit _ (List.getElem_mem hnlen')
      rw [hentry] at hse hcn
      simp only at hse hcn
      step with start_slot_spec as ⟨entry_slot, hes⟩
      simp only [core.slot_epoch.Slot.as_u64, bind_tc_ok]
      split
      · rename_i hxe
        simp only [spec_ok, Bool.false_eq_true, false_implies, true_and, forall_const]
        refine ⟨hsx1, ?_⟩
        rw [← hw1, walkTime, hentry, if_pos (by scalar_tac)]
      · rename_i hxe
        step with time_after_slots_ms_spec as ⟨r, hr⟩
        have hwalk := walkTime_succ_of_le (t := t1) (slot := x.val)
          (show Chain (entries schedule spe) (n + 1) s1.val d1.val from ⟨hdpos, hnlen, by rw [hentry]; exact hse, by rw [hentry]; exact hcn⟩)
          (by rw [hentry]; scalar_tac)
        rw [hentry] at hwalk
        simp only at hwalk
        rcases r with v | e
        · simp only [spec_ok]
          simp only at hr
          refine ⟨by rw [hsub]; exact hnlen', by rw [hsub, hes]; exact hcn, by scalar_tac, ?_,
            by scalar_tac⟩
          rw [hsub, hes, hr, ← hw1, hwalk, hes]
        · simp only [spec_ok, forall_const]
          rw [← hw1, hwalk]
          have := le_walkTime (t := t1.val + (entry_slot.val - s1.val) * d1.val) (x := x.val)
            (hes ▸ hcn) (by scalar_tac)
          simp only at hr
          rw [hes] at this hr
          exact ⟨by omega, by simp⟩
    · rename_i hzero
      have h0 : index1.val = 0 := by scalar_tac
      rw [h0] at hw1
      simp only [spec_ok, Bool.false_eq_true, false_implies, true_and, forall_const]
      exact ⟨hsx1, by rw [← hw1]; rfl⟩
  · exact ⟨hlen, hc, hsx, rfl⟩

theorem compute_time_at_slot_ms_walk (schedule : Slice Entry) (spe genesis_time_ms : U64)
    (slot : core.slot_epoch.Slot) (h : ScheduleOk schedule spe) :
    core.slot_duration_schedule.compute_time_at_slot_ms schedule spe genesis_time_ms slot ⦃ r => match r with
      | .Ok t => t.val = timeAtSlot schedule spe genesis_time_ms slot.val
      | .Err _ => U64.max < timeAtSlot schedule spe genesis_time_ms slot.val ⦄ := by
  have hpos := h.length_pos
  have hlast : schedule.length - 1 < schedule.length := by omega
  have hsub : (core.num.Usize.saturating_sub (Slice.len schedule) 1#usize).val = schedule.length - 1 := by
    rw [usize_saturating_sub_val, Slice.len_val]; rfl
  have hget : (schedule[core.num.Usize.saturating_sub (Slice.len schedule) 1#usize]? : Option Entry) =
      some schedule.val[schedule.length - 1] := by
    rw [Slice.getElem?_Usize_eq, hsub, List.getElem?_eq_getElem]
  have hentry := entryAt_entries (spe := spe) hlast
  have hgen := h.genesis_slot
  rw [hentry] at hgen
  simp only at hgen
  unfold core.slot_duration_schedule.compute_time_at_slot_ms
  simp only [lift, bind_tc_ok, core.slice.Slice.get, core.slice.index.Usize.get, hget,
    core.option.Option.ok_or, core.result.Result.Insts.CoreOpsTry.branch]
  step with start_slot_spec as ⟨start_slot, hss⟩
  rw [hgen] at hss
  have hc := h.chain _ hlast
  rw [hentry, hgen] at hc
  simp only at hc
  have hloop := time_loop_spec schedule spe slot h.fits
    (core.num.Usize.saturating_sub (Slice.len schedule) 1#usize) start_slot genesis_time_ms
    (schedule.val[schedule.length - 1].slot_duration_ms) (by rw [hsub]; exact hlast)
    (by rw [hsub, hss]; exact hc) (by rw [hss]; exact Nat.zero_le _)
  step with hloop as ⟨s', t', d', ov, hov, hnov⟩
  rw [hsub, hss] at hov hnov
  have htime : timeAtSlot schedule spe genesis_time_ms slot.val =
      walkTime (entries schedule spe) slot.val (schedule.length - 1) 0 genesis_time_ms.val
        (schedule.val[schedule.length - 1].slot_duration_ms).val := by
    rw [timeAtSlot, hentry]
  rw [htime]
  cases ov
  · obtain ⟨hsx, hw⟩ := hnov rfl
    simp only [Bool.false_eq_true, if_false]
    step with time_after_slots_ms_spec as ⟨r, hr⟩
    rw [hw]
    rcases r with v | e <;> exact hr
  · simp only [if_true, spec_ok]
    exact hov rfl

theorem slot_loop_spec (schedule : Slice Entry) (spe : U64) (y : U64)
    (hfit : StartSlotsFit schedule spe) (index : Usize) (s : core.slot_epoch.Slot) (t d : U64)
    (hlen : index.val < schedule.length)
    (hc : Chain (entries schedule spe) index.val s.val d.val) (hst : s.val ≤ t.val)
    (hty : t.val ≤ y.val) :
    core.slot_duration_schedule.compute_slot_at_time_ms_loop schedule spe y index s t d ⦃ r =>
      r.1.val ≤ r.2.1.val ∧ r.2.1.val ≤ y.val ∧ 0 < r.2.2.val ∧
      walkSlot (entries schedule spe) y.val index.val s.val t.val d.val =
        r.1.val + (y.val - r.2.1.val) / r.2.2.val ⦄ := by
  unfold core.slot_duration_schedule.compute_slot_at_time_ms_loop
  apply loop.spec_decr_nat (fun a => a.1.val)
    (fun a => a.1.val < schedule.length ∧
      Chain (entries schedule spe) a.1.val a.2.1.val a.2.2.2.val ∧ a.2.1.val ≤ a.2.2.1.val ∧
      a.2.2.1.val ≤ y.val ∧
      walkSlot (entries schedule spe) y.val a.1.val a.2.1.val a.2.2.1.val a.2.2.2.val =
        walkSlot (entries schedule spe) y.val index.val s.val t.val d.val)
  · rintro ⟨index1, s1, t1, d1⟩ ⟨hlen1, hc1, hst1, hty1, hw1⟩
    simp only at hlen1 hc1 hst1 hty1 hw1
    simp only [core.slot_duration_schedule.compute_slot_at_time_ms_loop.body]
    split
    · rename_i hpos
      obtain ⟨n, hn1⟩ : ∃ n, index1.val = n + 1 := ⟨index1.val - 1, by scalar_tac⟩
      have hsub : (core.num.Usize.saturating_sub index1 1#usize).val = n := by
        rw [usize_saturating_sub_val]; simp [hn1]
      rw [hn1] at hc1 hw1 hlen1
      obtain ⟨hdpos, hnlen, hse, hcn⟩ := hc1
      have hnlen' : n < schedule.length := by omega
      have hentry := entryAt_entries (spe := spe) hnlen'
      have hget : (schedule[core.num.Usize.saturating_sub index1 1#usize]? : Option Entry) =
          some schedule.val[n] := by
        rw [Slice.getElem?_Usize_eq, hsub, List.getElem?_eq_getElem]
      simp only [lift, bind_tc_ok, core.slice.Slice.get, core.slice.index.Usize.get, hget]
      have hf := hfit _ (List.getElem_mem hnlen')
      rw [hentry] at hse hcn
      simp only at hse hcn
      step with start_slot_spec as ⟨entry_slot, hes⟩
      step with time_after_slots_ms_spec as ⟨r, hr⟩
      have hstop : walkSlot (entries schedule spe) y.val (n + 1) s1.val t1.val d1.val =
          if y.val < t1.val + (entry_slot.val - s1.val) * d1.val then s1.val + (y.val - t1.val) / d1.val
          else walkSlot (entries schedule spe) y.val n entry_slot.val
            (t1.val + (entry_slot.val - s1.val) * d1.val)
            (schedule.val[n].slot_duration_ms).val := by
        rw [walkSlot, hentry, hes]
      rcases r with v | e
      · simp only
        split
        · rename_i hyv
          simp only [spec_ok]
          refine ⟨hst1, hty1, hdpos, ?_⟩
          rw [← hw1, hstop, if_pos (by scalar_tac)]
        · rename_i hyv
          simp only [spec_ok]
          have hmul : entry_slot.val - s1.val ≤ (entry_slot.val - s1.val) * d1.val :=
            Nat.le_mul_of_pos_right _ hdpos
          refine ⟨by rw [hsub]; exact hnlen', by rw [hsub, hes]; exact hcn, by scalar_tac,
            by scalar_tac, ?_, by scalar_tac⟩
          rw [hsub, hr, ← hw1, hstop, if_neg (by scalar_tac)]
      · simp only [spec_ok]
        refine ⟨hst1, hty1, hdpos, ?_⟩
        rw [← hw1, hstop, if_pos (by scalar_tac)]
    · rename_i hzero
      have h0 : index1.val = 0 := by scalar_tac
      rw [h0] at hw1 hc1
      simp only [spec_ok]
      exact ⟨hst1, hty1, hc1.pos, by rw [← hw1]; rfl⟩
  · exact ⟨hlen, hc, hst, hty, rfl⟩

theorem compute_slot_at_time_ms_walk (schedule : Slice Entry) (spe genesis_time_ms time_ms : U64)
    (h : ScheduleOk schedule spe) (htime : genesis_time_ms.val ≤ time_ms.val) :
    core.slot_duration_schedule.compute_slot_at_time_ms schedule spe genesis_time_ms time_ms ⦃ r =>
      ∃ slot, r = .Ok slot ∧ slot.val = slotAtTime schedule spe genesis_time_ms time_ms.val ⦄ := by
  have hpos := h.length_pos
  have hlast : schedule.length - 1 < schedule.length := by omega
  have hsub : (core.num.Usize.saturating_sub (Slice.len schedule) 1#usize).val = schedule.length - 1 := by
    rw [usize_saturating_sub_val, Slice.len_val]; rfl
  have hget : (schedule[core.num.Usize.saturating_sub (Slice.len schedule) 1#usize]? : Option Entry) =
      some schedule.val[schedule.length - 1] := by
    rw [Slice.getElem?_Usize_eq, hsub, List.getElem?_eq_getElem]
  have hentry := entryAt_entries (spe := spe) hlast
  have hgen := h.genesis_slot
  rw [hentry] at hgen
  simp only at hgen
  unfold core.slot_duration_schedule.compute_slot_at_time_ms
  simp only [lift, bind_tc_ok, core.slice.Slice.get, core.slice.index.Usize.get, hget,
    core.option.Option.ok_or, core.result.Result.Insts.CoreOpsTry.branch]
  step with start_slot_spec as ⟨start_slot, hss⟩
  rw [hgen] at hss
  have hc := h.chain _ hlast
  rw [hentry, hgen] at hc
  simp only at hc
  have hloop := slot_loop_spec schedule spe time_ms h.fits
    (core.num.Usize.saturating_sub (Slice.len schedule) 1#usize) start_slot genesis_time_ms
    (schedule.val[schedule.length - 1].slot_duration_ms) (by rw [hsub]; exact hlast)
    (by rw [hsub, hss]; exact hc) (by rw [hss]; exact Nat.zero_le _) htime
  step with hloop as ⟨s', t', d', hst, hty, hd, hw⟩
  rw [hsub, hss] at hw
  step with safe_sub_ok as ⟨r1, hr1⟩
  rcases r1 with q1 | e1
  swap
  · exact hr1.elim
  simp only at hr1
  simp only [bind_tc_ok]
  step with safe_div_ok as ⟨r2, hr2⟩
  rcases r2 with q2 | e2
  swap
  · exact hr2.elim
  simp only at hr2
  simp only [bind_tc_ok]
  have hq : q2.val ≤ time_ms.val - t'.val := by rw [hr2, hr1]; exact Nat.div_le_self _ _
  step with slot_safe_add_ok as ⟨r3, hr3⟩
  rcases r3 with z | e3
  swap
  · exact hr3.elim
  simp only at hr3
  refine ⟨z, rfl, ?_⟩
  rw [hr3, hr2, hr1, slotAtTime, hentry]
  exact hw.symm

theorem slot_loop_before_genesis (schedule : Slice Entry) (spe : U64) (y : U64)
    (hfit : StartSlotsFit schedule spe) (index : Usize) (s : core.slot_epoch.Slot) (t d : U64)
    (hlen : index.val < schedule.length)
    (hc : Chain (entries schedule spe) index.val s.val d.val) (hyt : y.val < t.val) :
    core.slot_duration_schedule.compute_slot_at_time_ms_loop schedule spe y index s t d ⦃ r => r.2.1 = t ⦄ := by
  unfold core.slot_duration_schedule.compute_slot_at_time_ms_loop
  apply loop.spec_decr_nat (fun a => a.1.val) (fun a => a = (index, s, t, d))
  · rintro ⟨index1, s1, t1, d1⟩ heq
    simp only [Prod.mk.injEq] at heq
    obtain ⟨rfl, rfl, rfl, rfl⟩ := heq
    simp only [core.slot_duration_schedule.compute_slot_at_time_ms_loop.body]
    split
    · obtain ⟨n, hn1⟩ : ∃ n, index1.val = n + 1 := ⟨index1.val - 1, by scalar_tac⟩
      have hsub : (core.num.Usize.saturating_sub index1 1#usize).val = n := by
        rw [usize_saturating_sub_val]; simp [hn1]
      rw [hn1] at hc hlen
      obtain ⟨hdpos, hnlen, hse, hcn⟩ := hc
      have hnlen' : n < schedule.length := by omega
      have hentry := entryAt_entries (spe := spe) hnlen'
      have hget : (schedule[core.num.Usize.saturating_sub index1 1#usize]? : Option Entry) =
          some schedule.val[n] := by
        rw [Slice.getElem?_Usize_eq, hsub, List.getElem?_eq_getElem]
      simp only [lift, bind_tc_ok, core.slice.Slice.get, core.slice.index.Usize.get, hget]
      have hf := hfit _ (List.getElem_mem hnlen')
      rw [hentry] at hse
      simp only at hse
      step with start_slot_spec as ⟨entry_slot, hes⟩
      step with time_after_slots_ms_spec as ⟨r, hr⟩
      rcases r with v | e
      · simp only at hr
        have : y < v := by scalar_tac
        simp [this]
      · simp
    · simp
  · rfl

theorem compute_slot_at_time_ms_before_genesis (schedule : Slice Entry)
    (spe genesis_time_ms time_ms : U64) (h : ScheduleOk schedule spe)
    (htime : time_ms.val < genesis_time_ms.val) :
    core.slot_duration_schedule.compute_slot_at_time_ms schedule spe genesis_time_ms time_ms ⦃ r => match r with
      | .Ok _ => False
      | .Err _ => True ⦄ := by
  have hpos := h.length_pos
  have hlast : schedule.length - 1 < schedule.length := by omega
  have hsub : (core.num.Usize.saturating_sub (Slice.len schedule) 1#usize).val = schedule.length - 1 := by
    rw [usize_saturating_sub_val, Slice.len_val]; rfl
  have hget : (schedule[core.num.Usize.saturating_sub (Slice.len schedule) 1#usize]? : Option Entry) =
      some schedule.val[schedule.length - 1] := by
    rw [Slice.getElem?_Usize_eq, hsub, List.getElem?_eq_getElem]
  have hentry := entryAt_entries (spe := spe) hlast
  have hgen := h.genesis_slot
  rw [hentry] at hgen
  simp only at hgen
  unfold core.slot_duration_schedule.compute_slot_at_time_ms
  simp only [lift, bind_tc_ok, core.slice.Slice.get, core.slice.index.Usize.get, hget,
    core.option.Option.ok_or, core.result.Result.Insts.CoreOpsTry.branch]
  step with start_slot_spec as ⟨start_slot, hss⟩
  rw [hgen] at hss
  have hc := h.chain _ hlast
  rw [hentry, hgen] at hc
  simp only at hc
  have hloop := slot_loop_before_genesis schedule spe time_ms h.fits
    (core.num.Usize.saturating_sub (Slice.len schedule) 1#usize) start_slot genesis_time_ms
    (schedule.val[schedule.length - 1].slot_duration_ms) (by rw [hsub]; exact hlast)
    (by rw [hsub, hss]; exact hc) htime
  step with hloop as ⟨s', t', d', ht'⟩
  subst ht'
  step with safe_sub_err as ⟨r1, hr1⟩
  rcases r1 with q1 | e1
  · exact hr1.elim
  simp [
    core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual]

theorem compute_time_at_slot_ms_eq {schedule : Slice Entry} {spe genesis_time_ms : U64}
    {slot : core.slot_epoch.Slot} {t : U64} (h : ScheduleOk schedule spe)
    (ht : core.slot_duration_schedule.compute_time_at_slot_ms schedule spe genesis_time_ms slot = ok (.Ok t)) :
    t.val = timeAtSlot schedule spe genesis_time_ms slot.val := by
  obtain ⟨r, hr, hp⟩ := spec_imp_exists (compute_time_at_slot_ms_walk schedule spe genesis_time_ms slot h)
  rw [ht] at hr
  cases hr
  exact hp

theorem compute_time_at_slot_ms_of_le {schedule : Slice Entry} {spe genesis_time_ms : U64}
    {slot : core.slot_epoch.Slot} (h : ScheduleOk schedule spe)
    (hfit : timeAtSlot schedule spe genesis_time_ms slot.val ≤ U64.max) :
    ∃ t : U64, core.slot_duration_schedule.compute_time_at_slot_ms schedule spe genesis_time_ms slot = ok (.Ok t) ∧
      t.val = timeAtSlot schedule spe genesis_time_ms slot.val := by
  obtain ⟨r, hr, hp⟩ := spec_imp_exists (compute_time_at_slot_ms_walk schedule spe genesis_time_ms slot h)
  rcases r with t | e
  · exact ⟨t, hr, hp⟩
  · exact absurd hp (by omega)

theorem compute_slot_at_time_ms_eq {schedule : Slice Entry} {spe genesis_time_ms time_ms : U64}
    (h : ScheduleOk schedule spe) (htime : genesis_time_ms.val ≤ time_ms.val) :
    ∃ slot : core.slot_epoch.Slot,
      core.slot_duration_schedule.compute_slot_at_time_ms schedule spe genesis_time_ms time_ms = ok (.Ok slot) ∧
      slot.val = slotAtTime schedule spe genesis_time_ms time_ms.val := by
  obtain ⟨r, hr, slot, rfl, hp⟩ :=
    spec_imp_exists (compute_slot_at_time_ms_walk schedule spe genesis_time_ms time_ms h htime)
  exact ⟨slot, hr, hp⟩

end SlotScheduleProofs
