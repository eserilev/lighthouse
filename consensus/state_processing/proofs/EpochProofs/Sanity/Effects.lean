import EpochProofs.Sanity.Frame
import EpochProofs.Spec.TotalActiveBalance

/-!
# Effects of block operations

Each block operation proves these facts about one successful run, from state `s` to `s'`.
The state invariants compose them over a whole epoch.
-/

namespace EpochProofs.Spec

/-- The registry does not shrink, and existing validators keep their effective balance. -/
def EBStable (s s' : BeaconState) : Prop :=
  s.validators.length ≤ s'.validators.length ∧
    ∀ (i : Nat) (v : Validator), s.validators[i]? = some v →
      ∃ v' : Validator, s'.validators[i]? = some v' ∧ v'.effective_balance = v.effective_balance

/-- No balance goes down. -/
def BalancesUp (s s' : BeaconState) : Prop :=
  s.balances.length ≤ s'.balances.length ∧
    ∀ (i : Nat) (b : Gwei), s.balances[i]? = some b → ∃ b' : Gwei, s'.balances[i]? = some b' ∧ b ≤ b'

/-- The slot and the checkpoints do not change. -/
def CheckpointsStable (s s' : BeaconState) : Prop :=
  s'.slot = s.slot ∧ s'.finalized_checkpoint = s.finalized_checkpoint ∧
    s'.current_justified_checkpoint = s.current_justified_checkpoint ∧
    s'.previous_justified_checkpoint = s.previous_justified_checkpoint

/-- Every validator exits no later than it becomes withdrawable. -/
def ExitOrder (s : BeaconState) : Prop :=
  ∀ v ∈ s.validators, v.exit_epoch ≤ v.withdrawable_epoch

theorem EBStable.refl (s : BeaconState) : EBStable s s :=
  ⟨Nat.le_refl _, fun _ v h => ⟨v, h, rfl⟩⟩

theorem EBStable.trans {s1 s2 s3 : BeaconState} (h12 : EBStable s1 s2) (h23 : EBStable s2 s3) :
    EBStable s1 s3 := by
  refine ⟨Nat.le_trans h12.1 h23.1, fun i v h => ?_⟩
  obtain ⟨v2, h2, he2⟩ := h12.2 i v h
  obtain ⟨v3, h3, he3⟩ := h23.2 i v2 h2
  exact ⟨v3, h3, he3.trans he2⟩

theorem BalancesUp.refl (s : BeaconState) : BalancesUp s s :=
  ⟨Nat.le_refl _, fun _ b h => ⟨b, h, Nat.le_refl _⟩⟩

theorem BalancesUp.trans {s1 s2 s3 : BeaconState} (h12 : BalancesUp s1 s2)
    (h23 : BalancesUp s2 s3) : BalancesUp s1 s3 := by
  refine ⟨Nat.le_trans h12.1 h23.1, fun i b h => ?_⟩
  obtain ⟨b2, h2, hb2⟩ := h12.2 i b h
  obtain ⟨b3, h3, hb3⟩ := h23.2 i b2 h2
  exact ⟨b3, h3, Nat.le_trans hb2 hb3⟩

theorem CheckpointsStable.refl (s : BeaconState) : CheckpointsStable s s := ⟨rfl, rfl, rfl, rfl⟩

theorem CheckpointsStable.trans {s1 s2 s3 : BeaconState} (h12 : CheckpointsStable s1 s2)
    (h23 : CheckpointsStable s2 s3) : CheckpointsStable s1 s3 :=
  ⟨h23.1.trans h12.1, h23.2.1.trans h12.2.1, h23.2.2.1.trans h12.2.2.1,
    h23.2.2.2.trans h12.2.2.2⟩

/-- A registry frame keeps effective balances and balances. -/
theorem RegistryFrame.ebStable {s s' : BeaconState} (h : RegistryFrame s s') : EBStable s s' := by
  obtain ⟨hv, -, -⟩ := h
  exact ⟨by rw [hv]; exact Nat.le_refl _, fun i v hi => ⟨v, by rw [hv]; exact hi, rfl⟩⟩

theorem RegistryFrame.balancesUp {s s' : BeaconState} (h : RegistryFrame s s') :
    BalancesUp s s' := by
  obtain ⟨-, hb, -⟩ := h
  exact ⟨by rw [hb]; exact Nat.le_refl _, fun i b hi => ⟨b, by rw [hb]; exact hi, Nat.le_refl _⟩⟩

end EpochProofs.Spec
