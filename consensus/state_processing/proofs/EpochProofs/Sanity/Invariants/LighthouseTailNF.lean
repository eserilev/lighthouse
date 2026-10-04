import EpochProofs.Sanity.Invariants.LighthouseTailRows
import EpochProofs.Sanity.Invariants.LighthouseTailViews

/-!
# The common form of the two epoch tails

`tailNF` is the order of steps that both tails reach. `s1` is the state after justification and
finalization, and `S4` is the state after the inactivity, rewards, registry and slashings
steps. The deposit decisions read `s1`; the other steps act on `S4`.
-/

namespace EpochProofs.Spec

/-- The epoch tail after the four steps, in the Lighthouse order with every deposit and
effective balance step written out. -/
def tailNF (o : Oracle) (s1 S4 : BeaconState) (total_active_balance : Gwei)
    (current_epoch : Epoch) : SpecM BeaconState := do
  let finalized_slot ←
    compute_start_slot_at_epoch Preset.mainnet s1.finalized_checkpoint.epoch
  let (next, dbtc, postponed) ← depositDecisions Preset.mainnet finalized_slot
    s1.deposit_balance_to_consume
    (← get_activation_churn_limit Preset.mainnet total_active_balance)
    ((s1.pending_deposits.take Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH).map
      (lhDepositView s1 current_epoch))
  let handled := (s1.pending_deposits.take next).zip postponed
  let pubkeys := s1.validators.map (·.pubkey)
  let n := s1.validators.length
  let named := consolidationIndices s1
  let sums ← (knownTopups pubkeys handled).foldlM (fun acc x => increase_balance acc x.1 x.2)
    (List.replicate n 0)
  let t1 ← addSums S4 sums
  let t2 ← ((List.range n).filter (fun i => !decide (i ∈ named))).foldlM ebUpdateAt t1
  let t3 ← process_eth1_data_reset Preset.mainnet t2
  let t4 ← writeQueue (s1.pending_deposits.drop next ++ (handled.filter (·.2)).map (·.1)) dbtc t3
  let t5 ← (newDeposits pubkeys handled).foldlM (apply_pending_deposit Preset.mainnet o) t4
  let t6 ← (List.range' n (t5.validators.length - n)).foldlM ebUpdateAt t5
  let t7 ← process_pending_consolidations Preset.mainnet t6
  let t8 ← named.foldlM ebUpdateAt t7
  let t9 ← process_builder_pending_payments Preset.mainnet total_active_balance t8
  epochRest2 o t9

end EpochProofs.Spec
