import EpochProofs.Sanity.Invariants.LighthouseTailDeposits
import EpochProofs.Sanity.Invariants.LighthouseTailEB

/-!
# Deposit top-ups and new validator deposits

The spec applies the handled deposits one by one. A top-up of a known validator touches only
that validator's balance, and a deposit for an unknown pubkey touches only new validators. So the
spec equals all top-ups first, then the new validator deposits in order. The top-ups of one
validator add up to one sum, as Lighthouse computes them.
-/

namespace EpochProofs.Spec

/-- The top-ups of known validators among the handled deposits that are not postponed. -/
def knownTopups (pubkeys : List BLSPubkey) (handled : List (PendingDeposit × Bool)) :
    List (ValidatorIndex × Gwei) :=
  (handled.filter fun x => !x.2 && decide (x.1.pubkey ∈ pubkeys)).map
    fun x => (pubkeys.idxOf x.1.pubkey, x.1.amount)

/-- The handled deposits for unknown pubkeys that are not postponed, in order. -/
def newDeposits (pubkeys : List BLSPubkey) (handled : List (PendingDeposit × Bool)) :
    List PendingDeposit :=
  (handled.filter fun x => !x.2 && !decide (x.1.pubkey ∈ pubkeys)).map (·.1)

/-- Add every top-up sum to its validator's balance, in index order. -/
def addSums (s : BeaconState) (sums : List Gwei) : SpecM BeaconState :=
  (List.range s.validators.length).foldlM (fun t i => increaseAt i (sums.getD i 0) t) s

/-! ## The plan fold of Lighthouse -/

/-- The plan fold from any accumulator: the sums come from the top-up fold, and the postponed
and new deposits are appended. -/
theorem lhPlan_fold_gen (P : List BLSPubkey) :
    ∀ (handled : List (PendingDeposit × Bool)) (sums : List Gwei)
      (post news : List PendingDeposit),
      handled.foldlM (lhPlanStep P) (sums, post, news) =
        (fun sums' => (sums', post ++ (handled.filter (·.2)).map (·.1),
          news ++ newDeposits P handled)) <$>
          (knownTopups P handled).foldlM (fun acc x => increase_balance acc x.1 x.2) sums
  | [], sums, post, news => by
    simp [knownTopups, newDeposits, pure, Except.pure, Functor.map, Except.map]
  | (d, pp) :: handled, sums, post, news => by
    have ih := lhPlan_fold_gen P handled
    rw [List.foldlM_cons]
    by_cases hpp : pp = true
    · subst hpp
      simp only [lhPlanStep, if_true, pure, Except.pure, bind, Except.bind]
      rw [ih]
      simp [knownTopups, newDeposits]
    · have hpp' : pp = false := by simpa using hpp
      subst hpp'
      by_cases hm : d.pubkey ∈ P
      · simp only [lhPlanStep, Bool.false_eq_true, if_false, hm, if_true, bind, Except.bind]
        simp only [knownTopups, newDeposits, List.filter_cons, hm, Bool.not_false,
          decide_true, Bool.and_self, if_true, Bool.not_true, Bool.and_false,
          Bool.false_eq_true, if_false, List.map_cons, List.foldlM_cons]
        cases increase_balance sums (P.idxOf d.pubkey) d.amount with
        | error e => rfl
        | ok sums1 =>
          simp only [pure, Except.pure]
          rw [ih]
          rfl
      · simp only [lhPlanStep, Bool.false_eq_true, if_false, hm, pure, Except.pure, bind,
          Except.bind]
        rw [ih]
        simp [knownTopups, newDeposits, hm]

/-- The plan fold of `lhDepositPlan`: the sums are the top-up fold from zero, the postponed
deposits and the new validator deposits are filters of the handled deposits. -/
theorem lhPlan_fold_eq (P : List BLSPubkey) (n : Nat)
    (handled : List (PendingDeposit × Bool)) :
    handled.foldlM (lhPlanStep P) (List.replicate n 0, [], []) =
      (fun sums => (sums, (handled.filter (·.2)).map (·.1), newDeposits P handled)) <$>
        (knownTopups P handled).foldlM (fun acc x => increase_balance acc x.1 x.2)
          (List.replicate n 0) := by
  rw [lhPlan_fold_gen]
  simp

/-! ## Top-up sums -/

/-- The total of the top-ups for index `i`. -/
def topupTotal (pairs : List (ValidatorIndex × Gwei)) (i : Nat) : Nat :=
  ((pairs.filter fun x => x.1 == i).map (·.2)).sum

/-- One more top-up adds to the total of its index only. -/
theorem topupTotal_cons (j : Nat) (a : Gwei) (pairs : List (ValidatorIndex × Gwei)) (i : Nat) :
    topupTotal ((j, a) :: pairs) i = (if j = i then a else 0) + topupTotal pairs i := by
  unfold topupTotal
  by_cases h : j = i
  · subst h; simp
  · simp [h]

/-- One checked balance increase at an index in range. -/
theorem increase_balance_lt {bs : List Gwei} {j : Nat} (a : Gwei) (hj : j < bs.length) :
    increase_balance bs j a =
      if bs[j] + a < UINT64_SIZE then .ok (bs.set j (bs[j] + a)) else .error .overflow := by
  unfold increase_balance listGet uint64Add listSet
  by_cases hfit : bs[j] + a < UINT64_SIZE
  · simp [hfit, hj, bind, Except.bind, pure, Except.pure]
  · simp [List.getElem?_eq_getElem hj, hfit, bind, Except.bind, pure, Except.pure, throw,
      throwThe, MonadExceptOf.throw]

/-- A fold of checked balance increases succeeds exactly when every balance plus its total fits,
and then adds each total. -/
theorem increase_fold_ok :
    ∀ (pairs : List (ValidatorIndex × Gwei)) (bs : List Gwei),
      (∀ x ∈ pairs, x.1 < bs.length) → (∀ i (h : i < bs.length), bs[i] < UINT64_SIZE) → ∀ v,
        pairs.foldlM (fun acc x => increase_balance acc x.1 x.2) bs = .ok v ↔
          (∀ i (h : i < bs.length), bs[i] + topupTotal pairs i < UINT64_SIZE) ∧
            v = bs.mapIdx fun i b => b + topupTotal pairs i
  | [], bs, _, hbs, v => by
    simp only [List.foldlM_nil, pure, Except.pure, Except.ok.injEq]
    constructor
    · rintro rfl
      refine ⟨fun i h => ?_, ?_⟩
      · simpa [topupTotal] using hbs i h
      · apply List.ext_getElem <;> simp [topupTotal]
    · rintro ⟨-, rfl⟩
      apply List.ext_getElem <;> simp [topupTotal]
  | (j, a) :: pairs, bs, hin, hbs, v => by
    have hj : j < bs.length := hin (j, a) (by simp)
    rw [List.foldlM_cons, increase_balance_lt a hj]
    split
    · rename_i hfit
      have hlen : (bs.set j (bs[j] + a)).length = bs.length := List.length_set
      have hpt : ∀ i (h : i < bs.length),
          (bs.set j (bs[j] + a))[i]'(by rw [hlen]; exact h) + topupTotal pairs i =
            bs[i] + topupTotal ((j, a) :: pairs) i := by
        intro i h
        rw [topupTotal_cons, List.getElem_set]
        by_cases hji : j = i
        · subst hji
          rw [if_pos rfl, if_pos rfl]
          exact Nat.add_assoc _ _ _
        · simp [hji]
      have hbs' : ∀ i (h : i < (bs.set j (bs[j] + a)).length),
          (bs.set j (bs[j] + a))[i] < UINT64_SIZE := by
        intro i h
        rw [List.getElem_set]
        split
        · exact hfit
        · exact hbs i (by rw [← hlen]; exact h)
      have ih := increase_fold_ok pairs (bs.set j (bs[j] + a))
        (fun x hx => by rw [hlen]; exact hin x (by simp [hx])) hbs' v
      simp only [ok_bind']
      rw [ih]
      constructor
      · rintro ⟨hc, rfl⟩
        refine ⟨fun i h => ?_, ?_⟩
        · rw [← hpt i h]; exact hc i (by rw [hlen]; exact h)
        · apply List.ext_getElem
          · simp
          · intro i h1 h2
            simp only [List.getElem_mapIdx]
            exact hpt i (by simpa using h2)
      · rintro ⟨hc, rfl⟩
        refine ⟨fun i h => ?_, ?_⟩
        · have h' : i < bs.length := by rw [← hlen]; exact h
          rw [hpt i h']; exact hc i h'
        · apply List.ext_getElem
          · simp
          · intro i h1 h2
            simp only [List.getElem_mapIdx]
            exact (hpt i (by simpa using h2)).symm
    · rename_i hfit
      simp only [bind, Except.bind]
      constructor
      · intro h; cases h
      · rintro ⟨hc, -⟩
        have hc' := hc j hj
        rw [topupTotal_cons, if_pos rfl] at hc'
        rw [← Nat.add_assoc] at hc'
        exact absurd (Nat.lt_of_le_of_lt (Nat.le_add_right _ _) hc') hfit

/-- A mapped computation succeeds exactly when the inner one does. -/
theorem map_ok_iff' {α β : Type} (f : α → β) (x : SpecM α) (v : β) :
    (f <$> x) = .ok v ↔ ∃ a, x = .ok a ∧ f a = v := by
  cases x with
  | error e => simp [Functor.map, Except.map]
  | ok a => simp [Functor.map, Except.map, eq_comm]

/-- A bind succeeds exactly when both parts do. -/
theorem bind_ok_iff' {α β : Type} (x : SpecM α) (k : α → SpecM β) (v : β) :
    (x >>= k) = .ok v ↔ ∃ a, x = .ok a ∧ k a = .ok v := by
  cases x with
  | error e => simp [bind, Except.bind]
  | ok a => simp [bind, Except.bind]

/-- `increaseAt` is a list increase on the balances. -/
theorem increaseAt_eq (j : Nat) (a : Gwei) (S : BeaconState) :
    increaseAt j a S = (fun bs => { S with balances := bs }) <$>
      increase_balance S.balances j a := by
  unfold increaseAt
  cases increase_balance S.balances j a <;> rfl

/-- A fold of state balance increases is the fold of list increases on the balances. -/
theorem increaseAt_fold_eq :
    ∀ (pairs : List (ValidatorIndex × Gwei)) (S : BeaconState),
      pairs.foldlM (fun t x => increaseAt x.1 x.2 t) S =
        (fun bs => { S with balances := bs }) <$>
          pairs.foldlM (fun acc x => increase_balance acc x.1 x.2) S.balances
  | [], S => by simp [pure, Except.pure, Functor.map, Except.map]
  | (j, a) :: pairs, S => by
    rw [List.foldlM_cons, List.foldlM_cons, increaseAt_eq]
    cases increase_balance S.balances j a with
    | error e => rfl
    | ok bs =>
      simp only [Functor.map, Except.map, bind, Except.bind]
      exact increaseAt_fold_eq pairs { S with balances := bs }

/-- The total of the pairs `(k, g k)` for `k < n` at index `i`. -/
theorem topupTotal_range (g : Nat → Gwei) :
    ∀ (n i : Nat), topupTotal ((List.range n).map fun k => (k, g k)) i =
      if i < n then g i else 0
  | 0, i => by simp [topupTotal]
  | n + 1, i => by
    have ih := topupTotal_range g n i
    rw [List.range_succ, List.map_append]
    unfold topupTotal at ih ⊢
    rw [List.filter_append, List.map_append, List.sum_append, ih]
    by_cases h : n = i
    · subst h; simp
    · by_cases hi : i < n
      · simp [h, hi, Nat.lt_succ_of_lt hi]
      · have : ¬ i < n + 1 := by omega
        simp [h, hi, this]

/-- `addSums` is a fold of list increases on the balances. -/
theorem addSums_eq (S : BeaconState) (sums : List Gwei) :
    addSums S sums = (fun bs => { S with balances := bs }) <$>
      ((List.range S.validators.length).map fun k => (k, sums.getD k 0)).foldlM
        (fun acc x => increase_balance acc x.1 x.2) S.balances := by
  unfold addSums
  rw [← increaseAt_fold_eq, List.foldlM_map]

/-- The sums from zero hold the totals. -/
theorem replicate_mapIdx_getD (n : Nat) (t : Nat → Nat) (k : Nat) (hk : k < n) :
    ((List.replicate n 0).mapIdx fun i b => b + t i).getD k 0 = t k := by
  rw [List.getD_eq_getElem?_getD, List.getElem?_mapIdx, List.getElem?_replicate]
  simp [hk]

/-- The top-ups one by one equal Lighthouse's per-validator sums added once. A balance below
`2^64` fails exactly when it plus its total does not fit, in both orders. -/
theorem topups_sum (S : BeaconState) (pairs : List (ValidatorIndex × Gwei))
    (hin : ∀ x ∈ pairs, x.1 < S.validators.length)
    (hlen : S.balances.length = S.validators.length)
    (hbs : ∀ i (h : i < S.balances.length), S.balances[i] < UINT64_SIZE) :
    SameOk (pairs.foldlM (fun t x => increaseAt x.1 x.2 t) S)
      (do
        let sums ← pairs.foldlM (fun acc x => increase_balance acc x.1 x.2)
          (List.replicate S.validators.length 0)
        addSums S sums) := by
  intro v
  have hin' : ∀ x ∈ pairs, x.1 < S.balances.length := fun x hx => hlen ▸ hin x hx
  rw [increaseAt_fold_eq, map_ok_iff', bind_ok_iff']
  constructor
  · rintro ⟨bs, hbs1, rfl⟩
    obtain ⟨hc, rfl⟩ := (increase_fold_ok pairs S.balances hin' hbs bs).mp hbs1
    have hz := (increase_fold_ok pairs (List.replicate S.validators.length 0)
      (by simpa using hin) (by simp [UINT64_SIZE]) _).mpr ⟨fun i h => ?_, rfl⟩
    · refine ⟨_, hz, ?_⟩
      rw [addSums_eq, map_ok_iff']
      refine ⟨_, (increase_fold_ok _ S.balances (by simp [hlen]) hbs _).mpr
        ⟨fun i h => ?_, rfl⟩, ?_⟩
      · rw [topupTotal_range, if_pos (hlen ▸ h), replicate_mapIdx_getD _ _ _ (hlen ▸ h)]
        exact hc i h
      · congr 1
        apply List.ext_getElem
        · simp
        · intro i h1 h2
          have hi : i < S.validators.length := by simpa [hlen] using h2
          simp only [List.getElem_mapIdx]
          rw [topupTotal_range, if_pos hi, replicate_mapIdx_getD _ _ _ hi]
    · have := hc i (by simpa [hlen] using h)
      simp only [List.getElem_replicate, Nat.zero_add]
      exact Nat.lt_of_le_of_lt (Nat.le_add_left _ _) this
  · rintro ⟨sums, hsums, hadd⟩
    obtain ⟨hcz, rfl⟩ := (increase_fold_ok pairs (List.replicate S.validators.length 0)
      (by simpa using hin) (by simp [UINT64_SIZE]) sums).mp hsums
    rw [addSums_eq, map_ok_iff'] at hadd
    obtain ⟨bs, hbs1, rfl⟩ := hadd
    obtain ⟨hc, rfl⟩ := (increase_fold_ok _ S.balances (by simp [hlen]) hbs bs).mp hbs1
    have htot : ∀ i (h : i < S.balances.length),
        topupTotal ((List.range S.validators.length).map fun k =>
          (k, ((List.replicate S.validators.length 0).mapIdx fun i b =>
            b + topupTotal pairs i).getD k 0)) i = topupTotal pairs i := by
      intro i h
      rw [topupTotal_range, if_pos (hlen ▸ h), replicate_mapIdx_getD _ _ _ (hlen ▸ h)]
    refine ⟨_, (increase_fold_ok pairs S.balances hin' hbs _).mpr
      ⟨fun i h => ?_, rfl⟩, ?_⟩
    · rw [← htot i h]; exact hc i h
    · congr 1
      apply List.ext_getElem
      · simp
      · intro i h1 h2
        simp only [List.getElem_mapIdx]
        rw [htot i (by simpa using h1)]

/-! ## Top-ups move before new validator deposits -/

/-- `s` with balance `x` for validator `j`. -/
def setBal (s : BeaconState) (j : Nat) (x : Gwei) : BeaconState :=
  { s with balances := s.balances.set j x }

/-- `set_or_append_list` past an index commutes with a write at that index. -/
theorem set_or_append_set (bs : List Gwei) (j L : Nat) (x a : Gwei) (hj : j < bs.length)
    (hjL : j < L) :
    set_or_append_list (bs.set j x) L a = (fun l => l.set j x) <$> set_or_append_list bs L a := by
  unfold set_or_append_list listSet
  simp only [List.length_set]
  split
  · simp [Functor.map, Except.map, pure, Except.pure, List.set_append_left _ _ hj]
  · split
    · simp [Functor.map, Except.map, pure, Except.pure, List.set_comm _ _ (Nat.ne_of_gt hjL)]
    · rfl

/-- A balance increase at another index commutes with a write at `j`. -/
theorem increase_balance_set (bs : List Gwei) (j k : Nat) (x a : Gwei) (hkj : k ≠ j) :
    increase_balance (bs.set j x) k a = (fun l => l.set j x) <$> increase_balance bs k a := by
  unfold increase_balance listGet listSet
  simp only [List.getElem?_set_ne (Ne.symm hkj), List.length_set]
  cases bs[k]? with
  | none => rfl
  | some b =>
    simp only [bind, Except.bind, pure, Except.pure]
    cases uint64Add b a with
    | error e => rfl
    | ok c =>
      simp only
      split
      · simp [Functor.map, Except.map, List.set_comm _ _ hkj]
      · rfl

/-- `add_validator_to_registry` commutes with a balance write at an existing validator. -/
theorem add_validator_setBal (T : BeaconState) (j : Nat) (x : Gwei)
    (hj : j < T.balances.length) (hjL : j < T.validators.length) (pubkey : BLSPubkey)
    (withdrawal_credentials : Bytes32) (amount : Gwei) :
    add_validator_to_registry Preset.mainnet (setBal T j x) pubkey withdrawal_credentials amount =
      (fun T' => setBal T' j x) <$>
        add_validator_to_registry Preset.mainnet T pubkey withdrawal_credentials amount := by
  unfold add_validator_to_registry get_index_for_new_validator setBal
  simp only [set_or_append_list_len]
  cases get_validator_from_deposit Preset.mainnet pubkey withdrawal_credentials amount with
  | error e => rfl
  | ok v =>
    simp only [bind, Except.bind, pure, Except.pure]
    rw [set_or_append_set _ _ _ _ _ hj hjL]
    cases set_or_append_list T.balances T.validators.length amount with
    | error e => rfl
    | ok bs =>
      simp only [Functor.map, Except.map]
      cases set_or_append_list T.previous_epoch_participation T.validators.length 0 with
      | error e => rfl
      | ok pp =>
        simp only
        cases set_or_append_list T.current_epoch_participation T.validators.length 0 with
        | error e => rfl
        | ok cp =>
          simp only
          cases set_or_append_list T.inactivity_scores T.validators.length 0 <;> rfl

/-- `apply_pending_deposit` commutes with a balance write at a validator with another pubkey. -/
theorem apply_setBal (o : Oracle) (d : PendingDeposit) (T : BeaconState) (j : Nat) (x : Gwei)
    (hj : j < T.balances.length) (hP : ApplyOther j d T) :
    apply_pending_deposit Preset.mainnet o (setBal T j x) d =
      (fun T' => setBal T' j x) <$> apply_pending_deposit Preset.mainnet o T d := by
  unfold apply_pending_deposit
  have hv : (setBal T j x).validators = T.validators := rfl
  simp only [hv]
  split
  · split
    · exact add_validator_setBal T j x hj hP.1 _ _ _
    · rfl
  · rename_i hm
    have hm' : d.pubkey ∈ T.validators.map (·.pubkey) := by simpa using hm
    have hkj : (T.validators.map (·.pubkey)).idxOf d.pubkey ≠ j := by
      intro heq
      have hg := getElem?_idxOf hm'
      rw [heq, List.getElem?_map] at hg
      obtain ⟨v, hv⟩ : ∃ v, T.validators[j]? = some v := ⟨_, List.getElem?_eq_getElem hP.1⟩
      rw [hv] at hg
      exact hP.2 v hv (Option.some.inj hg)
    simp only [setBal]
    rw [increase_balance_set _ _ _ _ _ hkj]
    cases increase_balance T.balances ((T.validators.map (·.pubkey)).idxOf d.pubkey) d.amount <;>
      rfl

/-- `increaseAt` reads only the balance at its index. -/
theorem increaseAt_char (j : Nat) (a : Gwei) (T : BeaconState) :
    increaseAt j a T = match T.balances[j]? with
      | none => .error .indexOutOfRange
      | some b => if b + a < UINT64_SIZE then .ok (setBal T j (b + a)) else .error .overflow := by
  rw [increaseAt_eq]
  cases hb : T.balances[j]? with
  | none =>
    unfold increase_balance listGet
    simp [hb, bind, Except.bind, throw, throwThe, MonadExceptOf.throw, Functor.map, Except.map]
  | some b =>
    have hj : j < T.balances.length := (List.getElem?_eq_some_iff.mp hb).1
    have hbj : T.balances[j] = b := (List.getElem?_eq_some_iff.mp hb).2
    rw [increase_balance_lt a hj, hbj]
    by_cases hfit : b + a < UINT64_SIZE <;> simp [hfit, Functor.map, Except.map, setBal]

/-- `increaseAt` keeps the validators. -/
theorem increaseAt_validators {j : Nat} {a : Gwei} {T T' : BeaconState}
    (h : increaseAt j a T = .ok T') : T'.validators = T.validators := by
  rw [increaseAt_char] at h
  split at h
  · cases h
  · split at h
    · cases h; rfl
    · cases h

/-- A balance increase at a validator with another pubkey commutes with a deposit. -/
theorem increaseAt_apply_comm (o : Oracle) (d : PendingDeposit) (T : BeaconState) (j : Nat)
    (a : Gwei) (hP : ApplyOther j d T) :
    SameOk (increaseAt j a T >>= fun t => apply_pending_deposit Preset.mainnet o t d)
      (apply_pending_deposit Preset.mainnet o T d >>= increaseAt j a) := by
  intro v
  have hkeep : ∀ T', apply_pending_deposit Preset.mainnet o T d = .ok T' →
      T'.balances[j]? = T.balances[j]? := fun T' hA => (apply_hF2 o d j T T' hP hA).2
  rw [increaseAt_char]
  cases hb : T.balances[j]? with
  | none =>
    simp only [bind, Except.bind]
    constructor
    · intro h; cases h
    · intro h
      cases hA : apply_pending_deposit Preset.mainnet o T d with
      | error e => rw [hA] at h; cases h
      | ok T' =>
        rw [hA] at h
        simp only [increaseAt_char, hkeep T' hA, hb] at h
        cases h
  | some b =>
    have hj : j < T.balances.length := (List.getElem?_eq_some_iff.mp hb).1
    by_cases hfit : b + a < UINT64_SIZE
    · simp only [hfit, if_true, ok_bind']
      rw [apply_setBal o d T j (b + a) hj hP]
      cases hA : apply_pending_deposit Preset.mainnet o T d with
      | error e => exact Iff.rfl.trans (by simp [Functor.map, Except.map, bind, Except.bind])
      | ok T' =>
        simp only [Functor.map, Except.map, bind, Except.bind]
        rw [increaseAt_char, hkeep T' hA, hb]
        simp [hfit]
    · simp only [hfit, if_false, bind, Except.bind]
      constructor
      · intro h; cases h
      · intro h
        cases hA : apply_pending_deposit Preset.mainnet o T d with
        | error e => rw [hA] at h; cases h
        | ok T' =>
          rw [hA] at h
          simp only [increaseAt_char, hkeep T' hA, hb, hfit, if_false] at h
          cases h

/-- `SameOk` is symmetric. -/
theorem SameOk.flip {α : Type} {x y : SpecM α} (h : SameOk x y) : SameOk y x :=
  fun v => (h v).symm

/-- Run a list of balance increases. -/
def topupsF (L : List (ValidatorIndex × Gwei)) (T : BeaconState) : SpecM BeaconState :=
  L.foldlM (fun t x => increaseAt x.1 x.2 t) T

/-- Balance increases at validators with other pubkeys commute with a deposit. -/
theorem topups_apply_comm (o : Oracle) (d : PendingDeposit) :
    ∀ (L : List (ValidatorIndex × Gwei)) (T : BeaconState), (∀ x ∈ L, ApplyOther x.1 d T) →
      SameOk (apply_pending_deposit Preset.mainnet o T d >>= topupsF L)
        (topupsF L T >>= fun t => apply_pending_deposit Preset.mainnet o t d)
  | [], T, _ => by
    apply SameOk.of_eq
    exact bind_pure _
  | (j, a) :: L, T, hL => by
    have hcons : ∀ t, topupsF ((j, a) :: L) t = increaseAt j a t >>= topupsF L := by
      intro t; rfl
    have hc : topupsF ((j, a) :: L) = fun t => increaseAt j a t >>= topupsF L := funext hcons
    rw [hc, bind_assoc (increaseAt j a T)]
    rw [← bind_assoc (apply_pending_deposit Preset.mainnet o T d) (increaseAt j a)]
    refine SameOk.trans (SameOk.bind
      (increaseAt_apply_comm o d T j a (hL (j, a) (by simp))).flip
      (fun _ _ => SameOk.refl _)) ?_
    rw [bind_assoc]
    refine SameOk.bind (SameOk.refl _) (fun t ht => ?_)
    have hv := increaseAt_validators ht
    refine topups_apply_comm o d L t (fun x hx => ?_)
    have h := hL x (by simp [hx])
    exact ⟨by rw [hv]; exact h.1, by rw [hv]; exact h.2⟩

/-- A deposit for a pubkey of `S` is a balance increase, in a state that extends `S`. -/
theorem apply_known (o : Oracle) (d : PendingDeposit) (S T : BeaconState) (hT : ValExt S T)
    (hm : d.pubkey ∈ S.validators.map (·.pubkey)) :
    apply_pending_deposit Preset.mainnet o T d =
      increaseAt ((S.validators.map (·.pubkey)).idxOf d.pubkey) d.amount T := by
  obtain ⟨extra, hv, -, -⟩ := hT
  have hm' : d.pubkey ∈ T.validators.map (·.pubkey) := by
    rw [hv, List.map_append]; exact List.mem_append_left _ hm
  have hidx : (T.validators.map (·.pubkey)).idxOf d.pubkey =
      (S.validators.map (·.pubkey)).idxOf d.pubkey := by
    rw [hv, List.map_append, List.idxOf_append, if_pos hm]
  unfold apply_pending_deposit
  simp only [hm', not_true_eq_false, if_false, hidx]
  rfl

/-- In a state that extends `S`, a deposit for a pubkey outside `S` has another pubkey than
every validator of `S`. -/
theorem applyOther_of_new (d : PendingDeposit) (S T : BeaconState) (hT : ValExt S T)
    (hm : d.pubkey ∉ S.validators.map (·.pubkey)) (j : Nat) (hj : j < S.validators.length) :
    ApplyOther j d T := by
  obtain ⟨extra, hv, -, -⟩ := hT
  refine ⟨by rw [hv, List.length_append]; omega, fun v hvj => ?_⟩
  rw [hv, List.getElem?_append_left hj] at hvj
  intro heq
  apply hm
  rw [← heq]
  exact List.mem_map_of_mem (List.mem_of_getElem? hvj)

/-- `increaseAt` keeps `ValExt`. -/
theorem increaseAt_valExt {S T T' : BeaconState} {j : Nat} {a : Gwei} (hT : ValExt S T)
    (h : increaseAt j a T = .ok T') : ValExt S T' := by
  obtain ⟨extra, hv, hfar, hpd⟩ := hT
  refine ⟨extra, by rw [increaseAt_validators h]; exact hv, hfar, ?_⟩
  rw [increaseAt_char] at h
  split at h
  · cases h
  · split at h
    · cases h; exact hpd
    · cases h

/-- The general split: from any state that extends `S`. -/
theorem applyHandled_split_gen (o : Oracle) (S : BeaconState) :
    ∀ (handled : List (PendingDeposit × Bool)) (T : BeaconState), ValExt S T →
      SameOk (applyHandled o T handled)
        (topupsF (knownTopups (S.validators.map (·.pubkey)) handled) T >>= fun t =>
          (newDeposits (S.validators.map (·.pubkey)) handled).foldlM
            (apply_pending_deposit Preset.mainnet o) t)
  | [], T, _ => by
    apply SameOk.of_eq
    simp [applyHandled, topupsF, knownTopups, newDeposits]
  | (d, pp) :: handled, T, hT => by
    have ih := applyHandled_split_gen o S handled
    have hstep : applyHandled o T ((d, pp) :: handled) =
        (if pp then pure T else apply_pending_deposit Preset.mainnet o T d) >>= fun T1 =>
          applyHandled o T1 handled := rfl
    rw [hstep]
    cases pp with
    | true =>
      simp only [if_true]
      have hk : knownTopups (S.validators.map (·.pubkey)) ((d, true) :: handled) =
          knownTopups (S.validators.map (·.pubkey)) handled := by simp [knownTopups]
      have hn : newDeposits (S.validators.map (·.pubkey)) ((d, true) :: handled) =
          newDeposits (S.validators.map (·.pubkey)) handled := by simp [newDeposits]
      rw [hk, hn]
      exact ih T hT
    | false =>
      simp only [Bool.false_eq_true, if_false]
      by_cases hm : d.pubkey ∈ S.validators.map (·.pubkey)
      · have hk : knownTopups (S.validators.map (·.pubkey)) ((d, false) :: handled) =
            ((S.validators.map (·.pubkey)).idxOf d.pubkey, d.amount) ::
              knownTopups (S.validators.map (·.pubkey)) handled := by
          simp only [knownTopups, List.filter_cons, Bool.not_false, decide_eq_true hm,
            Bool.and_self, if_true, List.map_cons]
        have hn : newDeposits (S.validators.map (·.pubkey)) ((d, false) :: handled) =
            newDeposits (S.validators.map (·.pubkey)) handled := by
          simp only [newDeposits, List.filter_cons, Bool.not_false, decide_eq_true hm,
            Bool.not_true, Bool.and_false, Bool.false_eq_true, if_false]
        rw [hk, hn, apply_known o d S T hT hm]
        have hc : topupsF (((S.validators.map (·.pubkey)).idxOf d.pubkey, d.amount) ::
            knownTopups (S.validators.map (·.pubkey)) handled) T =
            increaseAt ((S.validators.map (·.pubkey)).idxOf d.pubkey) d.amount T >>=
              topupsF (knownTopups (S.validators.map (·.pubkey)) handled) := rfl
        rw [hc, bind_assoc]
        exact SameOk.bind (SameOk.refl _) (fun T1 h1 => ih T1 (increaseAt_valExt hT h1))
      · have hk : knownTopups (S.validators.map (·.pubkey)) ((d, false) :: handled) =
            knownTopups (S.validators.map (·.pubkey)) handled := by
          simp only [knownTopups, List.filter_cons, Bool.not_false, decide_eq_false hm,
            Bool.and_false, Bool.false_eq_true, if_false]
        have hn : newDeposits (S.validators.map (·.pubkey)) ((d, false) :: handled) =
            d :: newDeposits (S.validators.map (·.pubkey)) handled := by
          simp only [newDeposits, List.filter_cons, Bool.not_false, decide_eq_false hm,
            Bool.and_self, if_true, List.map_cons]
        rw [hk, hn]
        refine SameOk.trans (SameOk.bind (SameOk.refl _)
          (fun T1 h1 => ih T1 (apply_pending_deposit_valExt o S T T1 d hT h1))) ?_
        rw [← bind_assoc]
        have hother : ∀ x ∈ knownTopups (S.validators.map (·.pubkey)) handled,
            ApplyOther x.1 d T := by
          intro x hx
          simp only [knownTopups, List.mem_map, List.mem_filter] at hx
          obtain ⟨y, ⟨-, hy⟩, rfl⟩ := hx
          have hy' : y.1.pubkey ∈ S.validators.map (·.pubkey) := by
            simp only [Bool.and_eq_true, Bool.not_eq_true', decide_eq_true_eq] at hy
            exact List.mem_map.mpr hy.2
          have hlt := List.idxOf_lt_length_of_mem hy'
          rw [List.length_map] at hlt
          exact applyOther_of_new d S T hT hm _ hlt
        refine SameOk.trans (SameOk.bind (topups_apply_comm o d _ T hother)
          (fun _ _ => SameOk.refl _)) ?_
        rw [bind_assoc]
        exact SameOk.refl _

/-- The spec's deposits split into the top-ups of known validators, then the deposits for new
pubkeys in order. -/
theorem applyHandled_split (o : Oracle) (S : BeaconState)
    (handled : List (PendingDeposit × Bool)) :
    SameOk (applyHandled o S handled)
      (do
        let t ← (knownTopups (S.validators.map (·.pubkey)) handled).foldlM
          (fun t x => increaseAt x.1 x.2 t) S
        (newDeposits (S.validators.map (·.pubkey)) handled).foldlM
          (apply_pending_deposit Preset.mainnet o) t) :=
  applyHandled_split_gen o S handled S (ValExt.refl S)

end EpochProofs.Spec
