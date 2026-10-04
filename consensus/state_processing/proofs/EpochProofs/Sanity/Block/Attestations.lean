import EpochProofs.Sanity.Effects
import EpochProofs.Spec.Block.Attestations

/-!
# Effects of `process_attestation` and `process_payload_attestation`

`process_attestation` writes one participation list, one builder pending payment and the
proposer balance. It does not write `validators`, the slot or the checkpoints.
`process_payload_attestation` only checks, so it returns its input state.
-/

namespace EpochProofs.Spec

/-- A `for` loop in `SpecM` keeps every property that each successful pass keeps. -/
private theorem forIn_ok_invariant {α β : Type} (P : β → Prop)
    (f : α → β → SpecM (ForInStep β)) (hf : ∀ a b r, P b → f a b = .ok r → P r.value) :
    ∀ (l : List α) (b b' : β), P b → forIn l b f = .ok b' → P b' := by
  intro l
  induction l with
  | nil => intro b b' hb h; cases h; exact hb
  | cons a as ih =>
    intro b b' hb h
    rw [List.forIn_cons] at h
    obtain ⟨r, hr, h⟩ := specM_bind_ok h
    have := hf a b r hb hr
    cases r with
    | done c => cases h; exact this
    | yield c => exact ih c b' this h

/-- A successful `listSet` keeps the length. -/
private theorem listSet_ok_length {α : Type} {l l' : List α} {i : Nat} {a : α}
    (h : listSet l i a = .ok l') : l'.length = l.length := by
  unfold listSet at h
  split at h
  · cases h; exact List.length_set
  · cases h

/-- A successful `uint64Add` returns the sum. -/
private theorem uint64Add_ok_eq {a b c : Uint64} (h : uint64Add a b = .ok c) : c = a + b := by
  unfold uint64Add at h
  split at h
  · cases h; rfl
  · cases h

/-- `increase_balance` keeps the length and does not lower any balance. -/
theorem increase_balance_up {balances balances' : List Gwei} {index : ValidatorIndex}
    {delta : Gwei} (h : increase_balance balances index delta = .ok balances') :
    balances'.length = balances.length ∧
      ∀ (i : Nat) (b : Gwei), balances[i]? = some b →
        ∃ b' : Gwei, balances'[i]? = some b' ∧ b ≤ b' := by
  unfold increase_balance at h
  obtain ⟨old, hold, h⟩ := specM_bind_ok h
  obtain ⟨sum, hsum, h⟩ := specM_bind_ok h
  unfold listGet at hold
  unfold uint64Add at hsum
  unfold listSet at h
  split at hold
  case h_2 => cases hold
  case h_1 hget =>
    cases hold
    split at hsum
    case isFalse => cases hsum
    case isTrue =>
      cases hsum
      split at h
      case isFalse => cases h
      case isTrue =>
        cases h
        refine ⟨List.length_set, fun i b hb => ?_⟩
        rw [List.getElem?_set]
        by_cases hi : index = i
        case pos =>
          subst hi
          rw [hget] at hb
          cases hb
          exact ⟨old + delta, by simp [‹index < balances.length›], Nat.le_add_right _ _⟩
        case neg => exact ⟨b, by simp [hi, hb], Nat.le_refl _⟩

/-- On success, `process_attestation` changes only one participation list, the balances and
the builder pending payments. The balances come from one `increase_balance` call. The payments
come from one `listSet`. -/
theorem process_attestation_shape (p : Preset) (o : Oracle) (s s' : BeaconState)
    (attestation : Attestation) (parent_slot : Slot)
    (h : process_attestation p o s attestation parent_slot = .ok s') :
    ∃ (epoch_participation : List ParticipationFlags) (balances : List Gwei)
      (builder_pending_payments : List BuilderPendingPayment),
      (∃ index reward, increase_balance s.balances index reward = .ok balances) ∧
      (∃ k payment, listSet s.builder_pending_payments k payment = .ok builder_pending_payments) ∧
      (s' = { s with
          current_epoch_participation := epoch_participation
          balances := balances
          builder_pending_payments := builder_pending_payments } ∨
        s' = { s with
          previous_epoch_participation := epoch_participation
          balances := balances
          builder_pending_payments := builder_pending_payments }) := by
  unfold process_attestation at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact ⟨_, _, _, ⟨_, _, ‹_›⟩, ⟨_, _, ‹_›⟩, .inl rfl⟩)
    | (cases h; exact ⟨_, _, _, ⟨_, _, ‹_›⟩, ⟨_, _, ‹_›⟩, .inr rfl⟩)

/-- The attester loop keeps the length of the participation list. It changes only the weight
of the payment, and the weight does not go down. -/
theorem process_attestation_loop_shape (p : Preset) (state : BeaconState)
    (data : AttestationData) (participation_flag_indices : List Nat)
    (attesting_indices : List ValidatorIndex) (epoch_participation : List ParticipationFlags)
    (payment : BuilderPendingPayment) (epoch_participation' : List ParticipationFlags)
    (proposer_reward_numerator : Gwei) (payment' : BuilderPendingPayment)
    (h : process_attestation_loop p state data participation_flag_indices attesting_indices
      epoch_participation payment =
        .ok (epoch_participation', proposer_reward_numerator, payment')) :
    epoch_participation'.length = epoch_participation.length ∧
      payment'.withdrawal = payment.withdrawal ∧
      payment'.proposer_index = payment.proposer_index ∧ payment.weight ≤ payment'.weight := by
  unfold process_attestation_loop at h
  obtain ⟨⟨e, pa, n⟩, hr, h⟩ := specM_bind_ok h
  cases h
  refine forIn_ok_invariant
    (fun r : MProd (List ParticipationFlags) (MProd BuilderPendingPayment Uint64) =>
      r.fst.length = epoch_participation.length ∧ r.snd.fst.withdrawal = payment.withdrawal ∧
        r.snd.fst.proposer_index = payment.proposer_index ∧ payment.weight ≤ r.snd.fst.weight)
    _ ?_ attesting_indices ⟨epoch_participation, payment, 0⟩ _ ⟨rfl, rfl, rfl, Nat.le_refl _⟩ hr
  intro index ⟨b1, b2, b3⟩ r hb hr
  obtain ⟨_, -, hr⟩ := specM_bind_ok hr
  obtain ⟨⟨e2, n2, w2⟩, hin, hr⟩ := specM_bind_ok hr
  have hlen := forIn_ok_invariant
    (fun x : MProd (List ParticipationFlags) (MProd Uint64 Bool) => x.fst.length = b1.length)
    _ ?_ _ _ _ rfl hin
  · obtain ⟨hb1, hb2, hb3, hb4⟩ := hb
    simp only [bind, Except.bind, pure, Except.pure] at hr
    repeat' split at hr
    all_goals first
      | contradiction
      | (cases hr; exact ⟨hlen.trans hb1, hb2, hb3, hb4⟩)
      | (cases hr
         have hw := uint64Add_ok_eq ‹uint64Add b2.weight _ = _›
         exact ⟨hlen.trans hb1, hb2, hb3, Nat.le_trans hb4 (hw ▸ Nat.le_add_right _ _)⟩)
  · intro ⟨_, flag_index⟩ ⟨c1, c2, c3⟩ r hc hr
    simp only [bind, Except.bind, pure, Except.pure] at hr
    repeat' split at hr
    all_goals first
      | contradiction
      | (cases hr; exact hc)
      | (cases hr; exact (listSet_ok_length ‹listSet _ _ _ = _›).trans hc)

/-- `process_attestation` keeps the lengths of both participation lists. -/
theorem process_attestation_participation_length (p : Preset) (o : Oracle)
    (s s' : BeaconState) (attestation : Attestation) (parent_slot : Slot)
    (h : process_attestation p o s attestation parent_slot = .ok s') :
    s'.current_epoch_participation.length = s.current_epoch_participation.length ∧
      s'.previous_epoch_participation.length = s.previous_epoch_participation.length := by
  unfold process_attestation at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h
       have hl := process_attestation_loop_shape _ _ _ _ _ _ _ _ _ _
         ‹process_attestation_loop _ _ _ _ _ _ _ = _›
       first | exact ⟨hl.1, rfl⟩ | exact ⟨rfl, hl.1⟩)

/-- `process_attestation` rewrites one builder pending payment. The new payment keeps the
withdrawal and the proposer index of the old one, and its weight does not go down. -/
theorem process_attestation_payment (p : Preset) (o : Oracle) (s s' : BeaconState)
    (attestation : Attestation) (parent_slot : Slot)
    (h : process_attestation p o s attestation parent_slot = .ok s') :
    ∃ (k : Nat) (old new : BuilderPendingPayment),
      listGet s.builder_pending_payments k = .ok old ∧
      listSet s.builder_pending_payments k new = .ok s'.builder_pending_payments ∧
      new.withdrawal = old.withdrawal ∧ new.proposer_index = old.proposer_index ∧
      old.weight ≤ new.weight := by
  unfold process_attestation at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h
       have hl := process_attestation_loop_shape _ _ _ _ _ _ _ _ _ _
         ‹process_attestation_loop _ _ _ _ _ _ _ = _›
       exact ⟨_, _, _, ‹listGet _ _ = _›, ‹listSet _ _ _ = _›, hl.2.1, hl.2.2.1, hl.2.2.2⟩)

/-- `process_attestation` does not write `validators`. -/
theorem process_attestation_validators (p : Preset) (o : Oracle) (s s' : BeaconState)
    (attestation : Attestation) (parent_slot : Slot)
    (h : process_attestation p o s attestation parent_slot = .ok s') :
    s'.validators = s.validators := by
  obtain ⟨_, _, _, -, -, h | h⟩ := process_attestation_shape p o s s' attestation parent_slot h
  all_goals (subst h; rfl)

/-- `process_attestation` keeps the registry and each effective balance. -/
theorem process_attestation_ebStable (p : Preset) (o : Oracle) (s s' : BeaconState)
    (attestation : Attestation) (parent_slot : Slot)
    (h : process_attestation p o s attestation parent_slot = .ok s') : EBStable s s' := by
  have hv := process_attestation_validators p o s s' attestation parent_slot h
  exact ⟨by rw [hv]; exact Nat.le_refl _, fun i v hi => ⟨v, by rw [hv]; exact hi, rfl⟩⟩

/-- `process_attestation` keeps the slot and the checkpoints. -/
theorem process_attestation_checkpointsStable (p : Preset) (o : Oracle) (s s' : BeaconState)
    (attestation : Attestation) (parent_slot : Slot)
    (h : process_attestation p o s attestation parent_slot = .ok s') :
    CheckpointsStable s s' := by
  obtain ⟨_, _, _, -, -, h | h⟩ := process_attestation_shape p o s s' attestation parent_slot h
  all_goals (subst h; exact ⟨rfl, rfl, rfl, rfl⟩)

/-- `process_attestation` keeps `ExitOrder`. -/
theorem process_attestation_exitOrder (p : Preset) (o : Oracle) (s s' : BeaconState)
    (attestation : Attestation) (parent_slot : Slot)
    (h : process_attestation p o s attestation parent_slot = .ok s') (hs : ExitOrder s) :
    ExitOrder s' := by
  intro v hv
  rw [process_attestation_validators p o s s' attestation parent_slot h] at hv
  exact hs v hv

/-- `process_attestation` only adds the proposer reward, so no balance goes down. -/
theorem process_attestation_balancesUp (p : Preset) (o : Oracle) (s s' : BeaconState)
    (attestation : Attestation) (parent_slot : Slot)
    (h : process_attestation p o s attestation parent_slot = .ok s') : BalancesUp s s' := by
  obtain ⟨_, bal, _, ⟨_, _, hb⟩, -, h | h⟩ :=
    process_attestation_shape p o s s' attestation parent_slot h
  all_goals
    subst h
    obtain ⟨hlen, hup⟩ := increase_balance_up hb
    exact ⟨Nat.le_of_eq hlen.symm, hup⟩

/-- `process_attestation` keeps the length of `builder_pending_payments`. -/
theorem process_attestation_builder_pending_payments_length (p : Preset) (o : Oracle)
    (s s' : BeaconState) (attestation : Attestation) (parent_slot : Slot)
    (h : process_attestation p o s attestation parent_slot = .ok s') :
    s'.builder_pending_payments.length = s.builder_pending_payments.length := by
  obtain ⟨_, _, bpp, -, ⟨k, pay, hset⟩, h | h⟩ :=
    process_attestation_shape p o s s' attestation parent_slot h
  all_goals
    subst h
    unfold listSet at hset
    split at hset
    · cases hset; exact List.length_set
    · cases hset

/-- `process_payload_attestation` only checks. On success it returns its input state. -/
theorem process_payload_attestation_eq (p : Preset) (o : Oracle) (GLOAS_FORK_EPOCH : Epoch)
    (s s' : BeaconState) (payload_attestation : PayloadAttestation)
    (h : process_payload_attestation p o GLOAS_FORK_EPOCH s payload_attestation = .ok s') :
    s' = s := by
  unfold process_payload_attestation at h
  simp only [throw, throwThe, MonadExceptOf.throw, bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first | contradiction | (cases h; rfl)

/-- `process_payload_attestation` keeps the registry and each effective balance. -/
theorem process_payload_attestation_ebStable (p : Preset) (o : Oracle)
    (GLOAS_FORK_EPOCH : Epoch) (s s' : BeaconState) (payload_attestation : PayloadAttestation)
    (h : process_payload_attestation p o GLOAS_FORK_EPOCH s payload_attestation = .ok s') :
    EBStable s s' := by
  rw [process_payload_attestation_eq p o GLOAS_FORK_EPOCH s s' payload_attestation h]
  exact EBStable.refl s

/-- `process_payload_attestation` keeps the balances. -/
theorem process_payload_attestation_balancesUp (p : Preset) (o : Oracle)
    (GLOAS_FORK_EPOCH : Epoch) (s s' : BeaconState) (payload_attestation : PayloadAttestation)
    (h : process_payload_attestation p o GLOAS_FORK_EPOCH s payload_attestation = .ok s') :
    BalancesUp s s' := by
  rw [process_payload_attestation_eq p o GLOAS_FORK_EPOCH s s' payload_attestation h]
  exact BalancesUp.refl s

/-- `process_payload_attestation` keeps the slot and the checkpoints. -/
theorem process_payload_attestation_checkpointsStable (p : Preset) (o : Oracle)
    (GLOAS_FORK_EPOCH : Epoch) (s s' : BeaconState) (payload_attestation : PayloadAttestation)
    (h : process_payload_attestation p o GLOAS_FORK_EPOCH s payload_attestation = .ok s') :
    CheckpointsStable s s' := by
  rw [process_payload_attestation_eq p o GLOAS_FORK_EPOCH s s' payload_attestation h]
  exact CheckpointsStable.refl s

/-- `process_payload_attestation` keeps `ExitOrder`. -/
theorem process_payload_attestation_exitOrder (p : Preset) (o : Oracle)
    (GLOAS_FORK_EPOCH : Epoch) (s s' : BeaconState) (payload_attestation : PayloadAttestation)
    (h : process_payload_attestation p o GLOAS_FORK_EPOCH s payload_attestation = .ok s')
    (hs : ExitOrder s) : ExitOrder s' := by
  rw [process_payload_attestation_eq p o GLOAS_FORK_EPOCH s s' payload_attestation h]
  exact hs

end EpochProofs.Spec
