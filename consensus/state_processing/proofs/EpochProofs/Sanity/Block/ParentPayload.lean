import EpochProofs.Spec.Block.ParentPayload
import EpochProofs.Sanity.Block.ExecutionRequests

/-!
# Effects of the parent execution payload and the payload bid

`process_parent_execution_payload` has a `RequestEffect`: it runs the execution requests, and
the builder payment and the payload fields do not touch validators or balances.
`process_execution_payload_bid` keeps validators, balances and checkpoints.
-/

namespace EpochProofs.Spec

/-- A successful `if` ran one of its branches. -/
private theorem ite_ok {α : Type} {c : Prop} [Decidable c] {t e : SpecM α} {x : α}
    (h : (if c then t else e) = .ok x) : (c ∧ t = .ok x) ∨ (¬ c ∧ e = .ok x) := by
  by_cases hc : c
  · exact .inl ⟨hc, by rw [if_pos hc] at h; exact h⟩
  · exact .inr ⟨hc, by rw [if_neg hc] at h; exact h⟩

/-- Splits the binds and `if`s of a successful run without `simp`. -/
local macro "peel_ok" h:ident : tactic => `(tactic| repeat' first
  | (guard_hyp $h:ident : ((_ : SpecM _) >>= _) = _
     obtain ⟨_, hx, $h:ident⟩ := specM_bind_ok $h
     try (guard_hyp hx : (throw _ : SpecM _) = _; cases hx))
  | (guard_hyp $h:ident : (@ite (SpecM _) _ (_) _ _) = _;
     rcases ite_ok $h with ⟨_, $h:ident⟩ | ⟨_, $h:ident⟩))

/-- A `for` loop keeps every reflexive and transitive relation that each body step keeps. -/
theorem forIn_rel {α : Type} (R : BeaconState → BeaconState → Prop) (hrefl : ∀ s, R s s)
    (htrans : ∀ s1 s2 s3, R s1 s2 → R s2 s3 → R s1 s3) (l : List α)
    (body : α → BeaconState → SpecM (ForInStep BeaconState)) (s s' : BeaconState)
    (hloop : forIn l s body = .ok s')
    (hb : ∀ a st r, body a st = .ok r → R st r.value) : R s s' := by
  induction l generalizing s with
  | nil =>
    cases hloop
    exact hrefl _
  | cons a l ih =>
    rw [List.forIn_cons] at hloop
    obtain ⟨r, hr, hloop⟩ := specM_bind_ok hloop
    cases r with
    | done x =>
      cases hloop
      exact hb a s _ hr
    | yield x => exact htrans _ _ _ (hb a s _ hr) (ih x hloop)

/-- A request loop of `apply_parent_execution_payload` has a request effect. -/
private theorem request_loop_effect {α : Type} {p : Preset} {l : List α}
    {body : α → BeaconState → SpecM (ForInStep BeaconState)} {s s' : BeaconState}
    (hloop : forIn l s body = .ok s')
    (hb : ∀ a st r, body a st = .ok r → RequestEffect p st r.value) : RequestEffect p s s' :=
  forIn_rel (RequestEffect p) (RequestEffect.refl p) (fun _ _ _ => RequestEffect.trans) l body
    s s' hloop hb

/-- Proves the body step of a request loop from the effect of one request. -/
local macro "loop_body" e:term : tactic => `(tactic| (
  intro _ _ _ hbody
  obtain ⟨_, h1, hbody⟩ := specM_bind_ok hbody
  obtain ⟨_, -, hbody⟩ := specM_bind_ok hbody
  cases hbody
  exact $e h1))

/-- `settle_builder_payment` only writes the builder payment queues. -/
theorem settle_builder_payment_frame (s s' : BeaconState) (payment_index : Uint64)
    (h : settle_builder_payment s payment_index = .ok s') : BuilderFrame s s' := by
  unfold settle_builder_payment at h
  peel_ok h
  all_goals first
    | (cases h; split <;> exact ⟨⟨rfl, rfl, rfl⟩, CheckpointsStable.refl _⟩)

/-- `apply_parent_execution_payload` has a request effect. -/
theorem apply_parent_execution_payload_effect (p : Preset) (o : Oracle) (s s' : BeaconState)
    (requests : ExecutionRequests)
    (h : apply_parent_execution_payload p o s requests = .ok s') : RequestEffect p s s' := by
  unfold apply_parent_execution_payload at h
  peel_ok h
  all_goals first
    | (cases h
       refine (request_loop_effect (by assumption : forIn requests.deposits _ _ = Except.ok _)
         (by loop_body fun h => (process_deposit_request_effect _ _ _ _ h).2)).trans
         ((request_loop_effect (by assumption : forIn requests.withdrawals _ _ = Except.ok _)
         (by loop_body fun h => (process_withdrawal_request_effect _ _ _ _ h).2)).trans
         ((request_loop_effect (by assumption : forIn requests.consolidations _ _ = Except.ok _)
         (by loop_body process_consolidation_request_effect _ _ _ _)).trans
         ((request_loop_effect (by assumption : forIn requests.builder_deposits _ _ = Except.ok _)
         (by loop_body fun h =>
           (process_builder_deposit_request_frame _ _ _ _ _ h).requestEffect)).trans
         ((request_loop_effect (by assumption : forIn requests.builder_exits _ _ = Except.ok _)
         (by loop_body fun h => (process_builder_exit_request_frame _ _ _ _ h).requestEffect)).trans
         ?_))))
       first
         | (have hs := settle_builder_payment_frame _ _ _
             ‹settle_builder_payment _ _ = Except.ok _›
            exact (hs.requestEffect (p := p)).trans (.of_eq rfl rfl ⟨rfl, rfl, rfl, rfl⟩))
         | exact .of_eq rfl rfl ⟨rfl, rfl, rfl, rfl⟩)

/-- `process_parent_execution_payload` has a request effect. -/
theorem process_parent_execution_payload_effect (p : Preset) (o : Oracle) (s s' : BeaconState)
    (block : BeaconBlock) (h : process_parent_execution_payload p o s block = .ok s') :
    RequestEffect p s s' := by
  unfold process_parent_execution_payload at h
  peel_ok h
  all_goals first
    | (cases h; exact RequestEffect.refl p _)
    | exact apply_parent_execution_payload_effect p o _ _ _ h

/-- `process_parent_execution_payload` keeps every effective balance, the slot and the
checkpoints. -/
theorem process_parent_execution_payload_stable (p : Preset) (o : Oracle) (s s' : BeaconState)
    (block : BeaconBlock) (h : process_parent_execution_payload p o s block = .ok s') :
    EBStable s s' ∧ CheckpointsStable s s' :=
  have he := process_parent_execution_payload_effect p o s s' block h
  ⟨he.ebStable, he.2.2.2.2⟩

/-- `process_parent_execution_payload` keeps every exit epoch at most its withdrawable epoch. -/
theorem process_parent_execution_payload_exitOrder (p : Preset) (o : Oracle)
    (s s' : BeaconState) (block : BeaconBlock)
    (h : process_parent_execution_payload p o s block = .ok s') (hs : ExitOrder s) :
    ExitOrder s' :=
  (process_parent_execution_payload_effect p o s s' block h).exitOrder hs

/-- After `process_parent_execution_payload`, each balance is at most the old balance and at
least `min old MIN_ACTIVATION_BALANCE`. Only a switch to compounding lowers a balance. -/
theorem process_parent_execution_payload_balance (p : Preset) (o : Oracle) (s s' : BeaconState)
    (block : BeaconBlock) (h : process_parent_execution_payload p o s block = .ok s') (i : Nat)
    (b : Gwei) (hb : s.balances[i]? = some b) :
    ∃ b', s'.balances[i]? = some b' ∧ min b p.MIN_ACTIVATION_BALANCE ≤ b' ∧ b' ≤ b :=
  (process_parent_execution_payload_effect p o s s' block h).balance_bounds i b hb

/-- `process_execution_payload_bid` does not touch validators or balances. -/
theorem process_execution_payload_bid_frame (p : Preset) (o : Oracle)
    (max_blobs_per_block : Epoch → Uint64) (s s' : BeaconState)
    (signed_bid : SignedExecutionPayloadBid)
    (h : process_execution_payload_bid p o max_blobs_per_block s signed_bid = .ok s') :
    BuilderFrame s s' := by
  unfold process_execution_payload_bid at h
  peel_ok h
  all_goals first
    | (cases h; exact ⟨⟨rfl, rfl, rfl⟩, ⟨rfl, rfl, rfl, rfl⟩⟩)

/-- `process_execution_payload_bid` keeps effective balances, balances, the slot, the
checkpoints and the exit order. -/
theorem process_execution_payload_bid_stable (p : Preset) (o : Oracle)
    (max_blobs_per_block : Epoch → Uint64) (s s' : BeaconState)
    (signed_bid : SignedExecutionPayloadBid)
    (h : process_execution_payload_bid p o max_blobs_per_block s signed_bid = .ok s') :
    EBStable s s' ∧ BalancesUp s s' ∧ CheckpointsStable s s' ∧ (ExitOrder s → ExitOrder s') :=
  have hf := process_execution_payload_bid_frame p o max_blobs_per_block s s' signed_bid h
  ⟨hf.1.ebStable, hf.1.balancesUp, hf.2, fun hs => (hf.requestEffect (p := p)).exitOrder hs⟩

end EpochProofs.Spec
