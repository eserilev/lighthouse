import EpochProofs.Spec.Block.SyncAggregate
import EpochProofs.Sanity.Effects
import EpochProofs.Sanity.RewardsBound

/-!
# `process_sync_aggregate`: frame and balance bound

`process_sync_aggregate` writes only `balances`. One call lowers a balance by at most
`participant_reward` for each committee seat of that validator with a zero bit. A validator can
hold many seats, so one call can lower its balance up to `SYNC_COMMITTEE_SIZE` times.
On mainnet, `participant_reward * 8192 ≤ integer_squareroot total_active_balance + 2`.
-/

namespace EpochProofs.Spec

/-! ## Balance helpers -/

/-- A successful `listSet` is `List.set` at an index in range. -/
private theorem listSet_ok {α : Type} {l l' : List α} {i : Nat} {a : α}
    (h : listSet l i a = .ok l') :
    i < l.length ∧ l' = l.set i a := by
  unfold listSet at h
  split at h
  · cases h
    exact ⟨‹_›, rfl⟩
  · cases h

/-- `increase_balance` keeps the length and lowers no balance. -/
private theorem increase_balance_up {bs bs' : List Gwei} {j : ValidatorIndex} {d : Gwei}
    (h : increase_balance bs j d = .ok bs') :
    bs'.length = bs.length ∧
      ∀ (i : Nat) (b : Gwei), bs[i]? = some b → ∃ b', bs'[i]? = some b' ∧ b ≤ b' := by
  unfold increase_balance at h
  obtain ⟨x, hx, h⟩ := specM_bind_ok h
  obtain ⟨y, hy, h⟩ := specM_bind_ok h
  obtain ⟨-, rfl⟩ := listSet_ok h
  have hx' := (listGet_ok hx).2
  unfold uint64Add at hy
  split at hy
  · cases hy
    refine ⟨List.length_set, fun i b hb => ?_⟩
    rw [List.getElem?_set]
    by_cases hji : j = i
    · subst hji
      rw [hx'] at hb
      cases hb
      exact ⟨x + d, by simp [List.getElem?_eq_some_iff.mp hx' |>.1], Nat.le_add_right _ _⟩
    · exact ⟨b, by simp [hji, hb], Nat.le_refl _⟩
  · cases hy

/-- `decrease_balance bs j d` keeps the length. It lowers the balance at `j` by at most `d` and
no other balance. -/
private theorem decrease_balance_down {bs bs' : List Gwei} {j : ValidatorIndex} {d : Gwei}
    (h : decrease_balance bs j d = .ok bs') :
    bs'.length = bs.length ∧
      ∀ (i : Nat) (b : Gwei), bs[i]? = some b →
        ∃ b', bs'[i]? = some b' ∧ b ≤ b' + (if j = i then d else 0) := by
  unfold decrease_balance at h
  obtain ⟨x, hx, h⟩ := specM_bind_ok h
  obtain ⟨-, rfl⟩ := listSet_ok h
  have hx' := (listGet_ok hx).2
  refine ⟨List.length_set, fun i b hb => ?_⟩
  rw [List.getElem?_set]
  by_cases hji : j = i
  · subst hji
    rw [hx'] at hb
    cases hb
    refine ⟨saturating_sub x d, by simp [List.getElem?_eq_some_iff.mp hx' |>.1], ?_⟩
    rw [saturating_sub_eq_sub, if_pos rfl]
    simp only [Gwei] at *
    omega
  · exact ⟨b, by simp [hji, hb], by simp [hji]⟩

/-! ## The reward loop -/

/-- The number of zero bits in `pairs` at validator `i`. -/
def zeroBitCount (pairs : List (ValidatorIndex × Bool)) (i : ValidatorIndex) : Nat :=
  (pairs.filter fun x => x.1 == i && !x.2).length

/-- The reward loop keeps the length of `balances`. It lowers the balance of `i` by at most
`participant_reward` for each zero bit at `i`. -/
theorem process_sync_aggregate_loop_bound (p : Preset) (state : BeaconState)
    (participant_reward proposer_reward : Gwei) :
    ∀ (pairs : List (ValidatorIndex × Bool)) (bs bs' : List Gwei),
      process_sync_aggregate_loop p state participant_reward proposer_reward pairs bs = .ok bs' →
      bs'.length = bs.length ∧
        ∀ i b, bs[i]? = some b →
          ∃ b', bs'[i]? = some b' ∧ b ≤ b' + zeroBitCount pairs i * participant_reward := by
  intro pairs
  induction pairs with
  | nil =>
    intro bs bs' h
    cases h
    exact ⟨rfl, fun i b hb => ⟨b, hb, Nat.le_add_right _ _⟩⟩
  | cons x rest ih =>
    obtain ⟨k, bit⟩ := x
    intro bs bs' h
    simp only [process_sync_aggregate_loop] at h
    cases bit with
    | true =>
      simp only [ite_true] at h
      obtain ⟨bs0, h0, h⟩ := specM_bind_ok h
      obtain ⟨_, -, h⟩ := specM_bind_ok h
      obtain ⟨bs1, h1, h⟩ := specM_bind_ok h
      obtain ⟨hl, hb⟩ := ih bs1 bs' h
      obtain ⟨hl0, hb0⟩ := increase_balance_up h0
      obtain ⟨hl1, hb1⟩ := increase_balance_up h1
      refine ⟨by rw [hl, hl1, hl0], fun i b hbi => ?_⟩
      obtain ⟨b0, hb0', hle0⟩ := hb0 i b hbi
      obtain ⟨b1, hb1', hle1⟩ := hb1 i b0 hb0'
      obtain ⟨b', hb', hle'⟩ := hb i b1 hb1'
      refine ⟨b', hb', ?_⟩
      have hc : zeroBitCount ((k, true) :: rest) i = zeroBitCount rest i := by
        simp [zeroBitCount]
      rw [hc]
      simp only [Gwei] at *
      omega
    | false =>
      simp only [Bool.false_eq_true, ite_false] at h
      obtain ⟨bs1, h1, h⟩ := specM_bind_ok h
      obtain ⟨hl, hb⟩ := ih bs1 bs' h
      obtain ⟨hl1, hb1⟩ := decrease_balance_down h1
      refine ⟨by rw [hl, hl1], fun i b hbi => ?_⟩
      obtain ⟨b1, hb1', hle1⟩ := hb1 i b hbi
      obtain ⟨b', hb', hle'⟩ := hb i b1 hb1'
      refine ⟨b', hb', ?_⟩
      by_cases hki : k = i
      · subst hki
        have hc : zeroBitCount ((k, false) :: rest) k = zeroBitCount rest k + 1 := by
          simp [zeroBitCount]
        rw [hc, Nat.add_mul, Nat.one_mul]
        rw [if_pos rfl] at hle1
        simp only [Gwei] at *
        omega
      · have hc : zeroBitCount ((k, false) :: rest) i = zeroBitCount rest i := by
          simp [zeroBitCount, hki]
        rw [hc]
        rw [if_neg hki] at hle1
        simp only [Gwei] at *
        omega

/-! ## Committee seats -/

/-- The number of seats in the current sync committee that belong to validator `i` and have a
zero bit in `bits`. A seat belongs to the first validator with its pubkey, as in
`all_pubkeys.index(pubkey)`. -/
def syncPenaltyCount (state : BeaconState) (bits : List Bool) (i : ValidatorIndex) : Nat :=
  ((state.current_sync_committee.pubkeys.zip bits).filter fun x =>
    (state.validators.map (·.pubkey)).findIdx? (· == x.1) == some i && !x.2).length

/-- A successful `listIndexOf` is `List.findIdx?`. -/
private theorem listIndexOf_ok {α : Type} [BEq α] {l : List α} {a : α} {i : Nat}
    (h : listIndexOf l a = .ok i) : l.findIdx? (· == a) = some i := by
  unfold listIndexOf at h
  split at h
  · cases h
    assumption
  · cases h

/-- The seat count over the committee indices equals the count over the pubkeys. -/
theorem zeroBitCount_mapM (all : List BLSPubkey) (i : ValidatorIndex) :
    ∀ (pubkeys : List BLSPubkey) (indices : List ValidatorIndex) (bits : List Bool),
      pubkeys.mapM (fun pubkey => listIndexOf all pubkey) = .ok indices →
      zeroBitCount (indices.zip bits) i =
        ((pubkeys.zip bits).filter fun x => all.findIdx? (· == x.1) == some i && !x.2).length := by
  intro pubkeys
  induction pubkeys with
  | nil =>
    intro indices bits h
    cases h
    rfl
  | cons pk rest ih =>
    intro indices bits h
    simp only [List.mapM_cons, bind, Except.bind, pure, Except.pure] at h
    split at h
    · cases h
    · rename_i k hk
      split at h
      · cases h
      · rename_i ks hks
        cases h
        have hk' := listIndexOf_ok hk
        cases bits with
        | nil => rfl
        | cons bit bits =>
          have := ih ks bits hks
          simp only [zeroBitCount] at this ⊢
          simp only [List.zip_cons_cons, List.filter_cons, hk']
          by_cases hki : k = i
          · subst hki
            simp only [beq_self_eq_true, Bool.true_and]
            split <;> simp [this]
          · have h1 : (k == i) = false := by simpa using hki
            have h2 : (some k == some i) = false := by simpa using hki
            simp only [h1, h2, Bool.false_and, Bool.false_eq_true, ite_false]
            exact this

/-- The seat count of one validator is at most the committee size. -/
theorem syncPenaltyCount_le (state : BeaconState) (bits : List Bool) (i : ValidatorIndex) :
    syncPenaltyCount state bits i ≤ state.current_sync_committee.pubkeys.length := by
  unfold syncPenaltyCount
  refine Nat.le_trans (List.length_filter_le _ _) ?_
  rw [List.length_zip]
  exact Nat.min_le_left _ _

/-! ## The participant reward -/

/-- `participant_reward` of `process_sync_aggregate` as a `Nat` formula of the total active
balance. -/
def sync_participant_reward (p : Preset) (total_active_balance : Gwei) : Gwei :=
  p.EFFECTIVE_BALANCE_INCREMENT * p.BASE_REWARD_FACTOR / integer_squareroot total_active_balance *
    (total_active_balance / p.EFFECTIVE_BALANCE_INCREMENT) * SYNC_REWARD_WEIGHT /
    WEIGHT_DENOMINATOR / p.SLOTS_PER_EPOCH / p.SYNC_COMMITTEE_SIZE

/-- The general bound: `participant_reward * 64 * SLOTS_PER_EPOCH * SYNC_COMMITTEE_SIZE` times
`integer_squareroot total_active_balance` is at most
`SYNC_REWARD_WEIGHT * BASE_REWARD_FACTOR * total_active_balance`. -/
theorem sync_participant_reward_mul_le (p : Preset) (total_active_balance : Gwei) :
    sync_participant_reward p total_active_balance *
        (WEIGHT_DENOMINATOR * p.SLOTS_PER_EPOCH * p.SYNC_COMMITTEE_SIZE) *
        integer_squareroot total_active_balance ≤
      SYNC_REWARD_WEIGHT * p.BASE_REWARD_FACTOR * total_active_balance := by
  generalize hr : integer_squareroot total_active_balance = r
  generalize hI : p.EFFECTIVE_BALANCE_INCREMENT = I
  generalize hF : p.BASE_REWARD_FACTOR = F
  generalize hD : WEIGHT_DENOMINATOR * p.SLOTS_PER_EPOCH * p.SYNC_COMMITTEE_SIZE = D
  have hpr : sync_participant_reward p total_active_balance =
      I * F / r * (total_active_balance / I) * SYNC_REWARD_WEIGHT / D := by
    rw [← hD, ← Nat.div_div_eq_div_mul, ← Nat.div_div_eq_div_mul]
    simp only [sync_participant_reward, hr, hI, hF]
  rw [hpr]
  -- x / D * D ≤ x, then each division only lowers the product.
  have h1 := Nat.div_mul_le_self (I * F / r * (total_active_balance / I) * SYNC_REWARD_WEIGHT) D
  have h2 : I * F / r * r ≤ I * F := Nat.div_mul_le_self _ _
  have h3 : total_active_balance / I * I ≤ total_active_balance := Nat.div_mul_le_self _ _
  calc I * F / r * (total_active_balance / I) * SYNC_REWARD_WEIGHT / D * D * r
      ≤ I * F / r * (total_active_balance / I) * SYNC_REWARD_WEIGHT * r :=
        Nat.mul_le_mul_right _ h1
    _ = SYNC_REWARD_WEIGHT * (total_active_balance / I) * (I * F / r * r) := by
        simp only [Nat.mul_comm, Nat.mul_left_comm]
    _ ≤ SYNC_REWARD_WEIGHT * (total_active_balance / I) * (I * F) :=
        Nat.mul_le_mul_left _ h2
    _ = SYNC_REWARD_WEIGHT * F * (total_active_balance / I * I) := by
        simp only [Nat.mul_comm, Nat.mul_left_comm]
    _ ≤ SYNC_REWARD_WEIGHT * F * total_active_balance := Nat.mul_le_mul_left _ h3

/-- On mainnet, `participant_reward * 8192 ≤ integer_squareroot(total_active_balance) + 2`.
The bound holds for every total active balance. -/
theorem sync_participant_reward_mainnet (total_active_balance : Gwei) :
    sync_participant_reward Preset.mainnet total_active_balance * 8192 ≤
      integer_squareroot total_active_balance + 2 := by
  by_cases hr : integer_squareroot total_active_balance = 0
  · simp only [sync_participant_reward, hr, Nat.div_zero, Nat.zero_mul, Nat.zero_div]
    exact Nat.zero_le _
  have hmul := sync_participant_reward_mul_le Preset.mainnet total_active_balance
  have hs := integer_squareroot_spec total_active_balance
  generalize sync_participant_reward Preset.mainnet total_active_balance = x at hmul ⊢
  generalize integer_squareroot total_active_balance = r at hmul hs hr ⊢
  simp only [Preset.mainnet, WEIGHT_DENOMINATOR, SYNC_REWARD_WEIGHT] at hmul
  rw [Nat.mul_right_comm] at hmul
  have hsq : (r + 1) * (r + 1) = r * r + 2 * r + 1 := by
    simp only [Uint64] at *
    rw [Nat.add_mul, Nat.mul_add, Nat.mul_add]
    omega
  have hy : x * 8192 * r ≤ (r + 2) * r := by
    rw [Nat.mul_right_comm, Nat.add_mul]
    rw [hsq] at hs
    simp only [Nat.reduceMul] at hmul
    generalize x * r = y at hmul ⊢
    generalize r * r = q at hs ⊢
    have h128 : 128 * (y * 8192) ≤ 128 * total_active_balance := by
      rw [Nat.mul_left_comm]
      exact hmul
    exact Nat.le_of_lt_succ (Nat.lt_of_le_of_lt (Nat.le_of_mul_le_mul_left h128 (by decide)) hs)
  exact Nat.le_of_mul_le_mul_right hy (Nat.pos_of_ne_zero hr)

/-! ## One run of `process_sync_aggregate` -/

/-- A successful `get_base_reward_per_increment` is the `Nat` formula. -/
private theorem get_base_reward_per_increment_ok {p : Preset} {t b : Gwei}
    (h : get_base_reward_per_increment p t = .ok b) :
    b = p.EFFECTIVE_BALANCE_INCREMENT * p.BASE_REWARD_FACTOR / integer_squareroot t := by
  unfold get_base_reward_per_increment at h
  obtain ⟨m, hm, h⟩ := specM_bind_ok h
  rw [(uint64Div_ok_eq h).1, (uint64Mul_ok_eq hm).1]

/-- The shape of one successful `process_sync_aggregate` run. Only `balances` changes, and the
reward loop computes the new balances with `participant_reward` from
`sync_participant_reward`. -/
theorem process_sync_aggregate_shape (p : Preset) (o : Oracle) (state state' : BeaconState)
    (sync_aggregate : SyncAggregate)
    (h : process_sync_aggregate p o state sync_aggregate = .ok state') :
    ∃ (total_active_balance proposer_reward : Gwei) (committee_indices : List ValidatorIndex)
      (balances : List Gwei),
      get_total_active_balance p state = .ok total_active_balance ∧
      state.current_sync_committee.pubkeys.mapM
          (fun pubkey => listIndexOf (state.validators.map (·.pubkey)) pubkey) =
        .ok committee_indices ∧
      process_sync_aggregate_loop p state (sync_participant_reward p total_active_balance)
          proposer_reward (committee_indices.zip sync_aggregate.sync_committee_bits)
          state.balances = .ok balances ∧
      state' = { state with balances } := by
  unfold process_sync_aggregate at h
  dsimp only at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  simp only [throw, throwThe, MonadExceptOf.throw] at h
  split at h
  · cases h
  obtain ⟨tab, htab, h⟩ := specM_bind_ok h
  obtain ⟨tai, htai, h⟩ := specM_bind_ok h
  obtain ⟨tab2, htab2, h⟩ := specM_bind_ok h
  rw [htab] at htab2
  cases htab2
  obtain ⟨brpi, hbrpi, h⟩ := specM_bind_ok h
  obtain ⟨tbr, htbr, h⟩ := specM_bind_ok h
  obtain ⟨m1, hm1, h⟩ := specM_bind_ok h
  obtain ⟨m2, hm2, h⟩ := specM_bind_ok h
  obtain ⟨mpr, hmpr, h⟩ := specM_bind_ok h
  obtain ⟨pr, hpr, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨prop, -, h⟩ := specM_bind_ok h
  obtain ⟨ci, hci, h⟩ := specM_bind_ok h
  split at h
  · cases h
  obtain ⟨bal, hbal, h⟩ := specM_bind_ok h
  cases h
  have hpr' : pr = sync_participant_reward p tab := by
    rw [(uint64Div_ok_eq hpr).1, (uint64Div_ok_eq hmpr).1, (uint64Div_ok_eq hm2).1,
      (uint64Mul_ok_eq hm1).1, (uint64Mul_ok_eq htbr).1, (uint64Div_ok_eq htai).1,
      get_base_reward_per_increment_ok hbrpi]
    rfl
  subst hpr'
  exact ⟨tab, prop, ci, bal, htab, hci, hbal, rfl⟩

/-- `process_sync_aggregate` writes only `balances`, and keeps their number. -/
theorem process_sync_aggregate_frame (p : Preset) (o : Oracle) (state state' : BeaconState)
    (sync_aggregate : SyncAggregate)
    (h : process_sync_aggregate p o state sync_aggregate = .ok state') :
    state' = { state with balances := state'.balances } ∧
      state'.balances.length = state.balances.length := by
  obtain ⟨_, _, _, _, -, -, hloop, rfl⟩ := process_sync_aggregate_shape p o state state' _ h
  exact ⟨rfl, (process_sync_aggregate_loop_bound p state _ _ _ _ _ hloop).1⟩

/-- `process_sync_aggregate` keeps `validators`, so it keeps effective balances. -/
theorem process_sync_aggregate_ebStable (p : Preset) (o : Oracle) (state state' : BeaconState)
    (sync_aggregate : SyncAggregate)
    (h : process_sync_aggregate p o state sync_aggregate = .ok state') :
    EBStable state state' := by
  obtain ⟨hs, -⟩ := process_sync_aggregate_frame p o state state' _ h
  rw [hs]
  exact EBStable.refl state

/-- `process_sync_aggregate` keeps the slot and the checkpoints. -/
theorem process_sync_aggregate_checkpointsStable (p : Preset) (o : Oracle)
    (state state' : BeaconState) (sync_aggregate : SyncAggregate)
    (h : process_sync_aggregate p o state sync_aggregate = .ok state') :
    CheckpointsStable state state' := by
  obtain ⟨hs, -⟩ := process_sync_aggregate_frame p o state state' _ h
  rw [hs]
  exact CheckpointsStable.refl state

/-- `process_sync_aggregate` keeps `ExitOrder`. -/
theorem process_sync_aggregate_exitOrder (p : Preset) (o : Oracle) (state state' : BeaconState)
    (sync_aggregate : SyncAggregate)
    (h : process_sync_aggregate p o state sync_aggregate = .ok state') (hx : ExitOrder state) :
    ExitOrder state' := by
  obtain ⟨hs, -⟩ := process_sync_aggregate_frame p o state state' _ h
  rw [hs]
  exact hx

/-- The balance bound of one `process_sync_aggregate` call. Each balance goes down by at most
`syncPenaltyCount × participant_reward`. `syncPenaltyCount` counts the committee seats of the
validator with a zero bit. A validator with many seats is penalized once for each seat. -/
theorem process_sync_aggregate_balance_bound (p : Preset) (o : Oracle) (state state' : BeaconState)
    (sync_aggregate : SyncAggregate)
    (h : process_sync_aggregate p o state sync_aggregate = .ok state') :
    ∃ total_active_balance : Gwei,
      get_total_active_balance p state = .ok total_active_balance ∧
      ∀ (i : Nat) (b : Gwei), state.balances[i]? = some b →
        ∃ b' : Gwei, state'.balances[i]? = some b' ∧
          b ≤ b' + syncPenaltyCount state sync_aggregate.sync_committee_bits i *
            sync_participant_reward p total_active_balance := by
  obtain ⟨tab, _, ci, bal, htab, hci, hloop, rfl⟩ :=
    process_sync_aggregate_shape p o state state' _ h
  refine ⟨tab, htab, fun i b hb => ?_⟩
  obtain ⟨b', hb', hle⟩ := (process_sync_aggregate_loop_bound p state _ _ _ _ _ hloop).2 i b hb
  refine ⟨b', hb', ?_⟩
  rw [zeroBitCount_mapM _ i _ ci _ hci] at hle
  exact hle

/-- The bound with the seat count replaced by the committee size. -/
theorem process_sync_aggregate_balance_bound_size (p : Preset) (o : Oracle)
    (state state' : BeaconState) (sync_aggregate : SyncAggregate)
    (hlen : state.current_sync_committee.pubkeys.length = p.SYNC_COMMITTEE_SIZE)
    (h : process_sync_aggregate p o state sync_aggregate = .ok state') :
    ∃ total_active_balance : Gwei,
      get_total_active_balance p state = .ok total_active_balance ∧
      ∀ (i : Nat) (b : Gwei), state.balances[i]? = some b →
        ∃ b' : Gwei, state'.balances[i]? = some b' ∧
          b ≤ b' + p.SYNC_COMMITTEE_SIZE * sync_participant_reward p total_active_balance := by
  obtain ⟨tab, htab, hb⟩ := process_sync_aggregate_balance_bound p o state state' _ h
  refine ⟨tab, htab, fun i b hbi => ?_⟩
  obtain ⟨b', hb', hle⟩ := hb i b hbi
  have hc := syncPenaltyCount_le state sync_aggregate.sync_committee_bits i
  rw [hlen] at hc
  exact ⟨b', hb', Nat.le_trans hle (Nat.add_le_add_left (Nat.mul_le_mul_right _ hc) _)⟩

/-- On mainnet, one `process_sync_aggregate` call lowers a balance by at most
`syncPenaltyCount × (integer_squareroot(total_active_balance) + 2) / 8192`. With 34M ETH
staked, that is about 22,500 Gwei for each seat. -/
theorem process_sync_aggregate_balance_bound_mainnet (o : Oracle) (state state' : BeaconState)
    (sync_aggregate : SyncAggregate)
    (h : process_sync_aggregate Preset.mainnet o state sync_aggregate = .ok state') :
    ∃ total_active_balance : Gwei,
      get_total_active_balance Preset.mainnet state = .ok total_active_balance ∧
      ∀ (i : Nat) (b : Gwei), state.balances[i]? = some b →
        ∃ b' : Gwei, state'.balances[i]? = some b' ∧
          b * 8192 ≤ b' * 8192 + syncPenaltyCount state sync_aggregate.sync_committee_bits i *
            (integer_squareroot total_active_balance + 2) := by
  obtain ⟨tab, htab, hb⟩ := process_sync_aggregate_balance_bound _ o state state' _ h
  refine ⟨tab, htab, fun i b hbi => ?_⟩
  obtain ⟨b', hb', hle⟩ := hb i b hbi
  refine ⟨b', hb', ?_⟩
  have hr := Nat.mul_le_mul_left (syncPenaltyCount state sync_aggregate.sync_committee_bits i)
    (sync_participant_reward_mainnet tab)
  have h8 := Nat.mul_le_mul_right 8192 hle
  rw [Nat.add_mul, Nat.mul_assoc] at h8
  exact Nat.le_trans h8 (Nat.add_le_add_left hr _)

end EpochProofs.Spec
