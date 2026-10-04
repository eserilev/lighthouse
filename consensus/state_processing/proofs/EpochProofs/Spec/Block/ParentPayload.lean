import EpochProofs.Spec.Block.ExecutionRequests

/-!
# Reference: parent execution payload and execution payload bid

Transcribed from `specs/gloas/beacon-chain.md` (v1.7.0-beta.2). Each definition quotes its
pyspec. The blob schedule is a parameter: `max_blobs_per_block epoch` is
`get_blob_parameters(epoch).max_blobs_per_block`. Gloas `process_operations` asserts
`len(body.deposits) == 0`, so the legacy `process_deposit` is not here.
-/

namespace EpochProofs.Spec

/-- ```python
def settle_builder_payment(state: BeaconState, payment_index: Uint64) -> None:
    assert payment_index < len(state.builder_pending_payments)
    payment = state.builder_pending_payments[payment_index]
    if payment.withdrawal.amount > 0:
        state.builder_pending_withdrawals.append(payment.withdrawal)
    state.builder_pending_payments[payment_index] = BuilderPendingPayment.empty()
``` -/
def settle_builder_payment (state : BeaconState) (payment_index : Uint64) :
    SpecM BeaconState := do
  if ¬ payment_index < state.builder_pending_payments.length then throw .assertionFailed
  let payment ← listGet state.builder_pending_payments payment_index
  let state :=
    if payment.withdrawal.amount > 0 then
      { state with
        builder_pending_withdrawals := state.builder_pending_withdrawals ++ [payment.withdrawal] }
    else state
  pure { state with
    builder_pending_payments :=
      ← listSet state.builder_pending_payments payment_index BuilderPendingPayment.empty }

/-- ```python
def apply_parent_execution_payload(
    state: BeaconState,
    requests: ExecutionRequests,
) -> None:
    parent_bid = state.latest_execution_payload_bid
    parent_slot = state.latest_block_header.slot
    parent_epoch = compute_epoch_at_slot(parent_slot)

    assert len(requests.withdrawals) <= MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD
    assert len(requests.consolidations) <= MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD
    assert len(requests.builder_deposits) <= MAX_BUILDER_DEPOSIT_REQUESTS_PER_PAYLOAD
    assert len(requests.builder_exits) <= MAX_BUILDER_EXIT_REQUESTS_PER_PAYLOAD

    # Process execution requests from parent's payload. The execution
    # requests are processed at state.slot (child's slot), not the parent's slot.
    def for_ops(operations: Sequence[Any], fn: Callable[[BeaconState, Any], None]) -> None:
        for operation in operations:
            fn(state, operation)

    for_ops(requests.deposits, process_deposit_request)
    for_ops(requests.withdrawals, process_withdrawal_request)
    for_ops(requests.consolidations, process_consolidation_request)
    for_ops(requests.builder_deposits, process_builder_deposit_request)
    for_ops(requests.builder_exits, process_builder_exit_request)

    # Settle the builder payment
    if parent_epoch == get_current_epoch(state):
        payment_index = SLOTS_PER_EPOCH + parent_slot % SLOTS_PER_EPOCH
        settle_builder_payment(state, payment_index)
    elif parent_epoch == get_previous_epoch(state):
        payment_index = parent_slot % SLOTS_PER_EPOCH
        settle_builder_payment(state, payment_index)
    elif parent_bid.value > 0:
        # Parent is older than the previous epoch, its payment entry has been
        # evicted from builder_pending_payments. Append the withdrawal directly.
        state.builder_pending_withdrawals.append(
            BuilderPendingWithdrawal(
                fee_recipient=parent_bid.fee_recipient,
                amount=parent_bid.value,
                builder_index=parent_bid.builder_index,
            )
        )

    # Update parent payload availability and latest block hash
    state.execution_payload_availability[parent_slot % SLOTS_PER_HISTORICAL_ROOT] = Boolean(True)
    state.latest_block_hash = parent_bid.block_hash
``` -/
def apply_parent_execution_payload (p : Preset) (o : Oracle) (state : BeaconState)
    (requests : ExecutionRequests) : SpecM BeaconState := do
  let parent_bid := state.latest_execution_payload_bid
  let parent_slot := state.latest_block_header.slot
  let parent_epoch ← compute_epoch_at_slot p parent_slot

  if ¬ requests.withdrawals.length ≤ MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD then
    throw .assertionFailed
  if ¬ requests.consolidations.length ≤ MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD then
    throw .assertionFailed
  if ¬ requests.builder_deposits.length ≤ MAX_BUILDER_DEPOSIT_REQUESTS_PER_PAYLOAD then
    throw .assertionFailed
  if ¬ requests.builder_exits.length ≤ MAX_BUILDER_EXIT_REQUESTS_PER_PAYLOAD then
    throw .assertionFailed

  let mut state := state
  for operation in requests.deposits do
    state ← process_deposit_request state operation
  for operation in requests.withdrawals do
    state ← process_withdrawal_request p state operation
  for operation in requests.consolidations do
    state ← process_consolidation_request p state operation
  for operation in requests.builder_deposits do
    state ← process_builder_deposit_request p o state operation
  for operation in requests.builder_exits do
    state ← process_builder_exit_request p state operation

  if parent_epoch == (← get_current_epoch p state) then
    let payment_index ← uint64Add p.SLOTS_PER_EPOCH (← uint64Mod parent_slot p.SLOTS_PER_EPOCH)
    state ← settle_builder_payment state payment_index
  else if parent_epoch == (← get_previous_epoch p state) then
    let payment_index ← uint64Mod parent_slot p.SLOTS_PER_EPOCH
    state ← settle_builder_payment state payment_index
  else if parent_bid.value > 0 then
    state := { state with
      builder_pending_withdrawals := state.builder_pending_withdrawals ++ [{
        fee_recipient := parent_bid.fee_recipient
        amount := parent_bid.value
        builder_index := parent_bid.builder_index }] }

  state := { state with
    execution_payload_availability := ← listSet state.execution_payload_availability
      (← uint64Mod parent_slot p.SLOTS_PER_HISTORICAL_ROOT) true }
  pure { state with latest_block_hash := parent_bid.block_hash }

/-- ```python
def process_parent_execution_payload(state: BeaconState, block: BeaconBlock) -> None:
    bid = block.body.signed_execution_payload_bid.message
    parent_bid = state.latest_execution_payload_bid
    requests = block.body.parent_execution_requests

    if bid.parent_block_hash != parent_bid.block_hash:
        # Parent was EMPTY -- no execution requests expected
        assert requests == ExecutionRequests.empty()
        return

    # Parent was FULL -- verify the bid commitment and apply the payload
    assert hash_tree_root(requests) == parent_bid.execution_requests_root
    apply_parent_execution_payload(state, requests)
``` -/
def process_parent_execution_payload (p : Preset) (o : Oracle) (state : BeaconState)
    (block : BeaconBlock) : SpecM BeaconState := do
  let bid := block.body.signed_execution_payload_bid.message
  let parent_bid := state.latest_execution_payload_bid
  let requests := block.body.parent_execution_requests

  if bid.parent_block_hash != parent_bid.block_hash then
    if ¬ requests = {} then throw .assertionFailed
    return state

  if ¬ o.hash_tree_root_ExecutionRequests requests = parent_bid.execution_requests_root then
    throw .assertionFailed
  apply_parent_execution_payload p o state requests

/-- ```python
def can_builder_cover_bid(
    state: BeaconState, builder_index: BuilderIndex, bid_amount: Gwei
) -> bool:
    builder_balance = state.builders[builder_index].balance
    pending_withdrawals_amount = get_pending_balance_to_withdraw_for_builder(state, builder_index)
    min_balance = MIN_DEPOSIT_AMOUNT + pending_withdrawals_amount
    if builder_balance < min_balance:
        return False
    return builder_balance - min_balance >= bid_amount
``` -/
def can_builder_cover_bid (state : BeaconState) (builder_index : BuilderIndex)
    (bid_amount : Gwei) : SpecM Bool := do
  let builder_balance := (← listGet state.builders builder_index).balance
  let pending_withdrawals_amount ← get_pending_balance_to_withdraw_for_builder state builder_index
  let min_balance ← uint64Add MIN_DEPOSIT_AMOUNT pending_withdrawals_amount
  if builder_balance < min_balance then
    return false
  return (← uint64Sub builder_balance min_balance) ≥ bid_amount

/-- ```python
def verify_execution_payload_bid_signature(
    state: BeaconState, signed_bid: SignedExecutionPayloadBid
) -> bool:
    builder = state.builders[signed_bid.message.builder_index]
    signing_root = compute_signing_root(
        signed_bid.message, get_domain(state, DOMAIN_BEACON_BUILDER)
    )
    return bls.Verify(builder.pubkey, signing_root, signed_bid.signature)
``` -/
def verify_execution_payload_bid_signature (p : Preset) (o : Oracle) (state : BeaconState)
    (signed_bid : SignedExecutionPayloadBid) : SpecM Bool := do
  let builder ← listGet state.builders signed_bid.message.builder_index
  let signing_root := compute_signing_root o
    (o.hash_tree_root_ExecutionPayloadBid signed_bid.message)
    (← get_domain p o state DOMAIN_BEACON_BUILDER)
  pure (o.bls_Verify builder.pubkey signing_root signed_bid.signature)

/-- ```python
def process_execution_payload_bid(
    state: BeaconState, signed_bid: SignedExecutionPayloadBid
) -> None:
    bid = signed_bid.message
    builder_index = bid.builder_index
    amount = bid.value

    # For self-builds, amount must be zero regardless of withdrawal credential prefix
    if builder_index == BUILDER_INDEX_SELF_BUILD:
        assert amount == 0
        assert signed_bid.signature == bls.G2_POINT_AT_INFINITY
    else:
        # Verify that the builder is active
        assert is_active_builder(state, builder_index)
        # Verify that the builder is a payload builder
        assert state.builders[builder_index].version == PAYLOAD_BUILDER_VERSION
        # Verify that the builder has funds to cover the bid
        assert can_builder_cover_bid(state, builder_index, amount)
        # Verify that the bid signature is valid
        assert verify_execution_payload_bid_signature(state, signed_bid)

    # Verify commitments are under limit
    assert (
        len(bid.blob_kzg_commitments)
        <= get_blob_parameters(get_current_epoch(state)).max_blobs_per_block
    )

    # Verify that the bid is for the current slot
    assert bid.slot == state.slot
    assert state.slot > GENESIS_SLOT
    # Verify that the bid is for the right parent block
    assert bid.parent_block_hash == state.latest_block_hash
    # Verify that the bid's block hash differs from its parent block hash
    assert bid.block_hash != bid.parent_block_hash
    assert bid.parent_block_root == get_block_root_at_slot(state, state.slot - 1)
    assert bid.prev_randao == get_randao_mix(state, get_current_epoch(state))

    # Record the pending payment if there is some payment
    if amount > 0:
        pending_payment = BuilderPendingPayment(
            weight=Gwei(0),
            withdrawal=BuilderPendingWithdrawal(
                fee_recipient=bid.fee_recipient,
                amount=amount,
                builder_index=builder_index,
            ),
            proposer_index=get_beacon_proposer_index(state),
        )
        state.builder_pending_payments[SLOTS_PER_EPOCH + bid.slot % SLOTS_PER_EPOCH] = (
            pending_payment
        )

    # Cache the signed execution payload bid
    state.latest_execution_payload_bid = bid
``` -/
def process_execution_payload_bid (p : Preset) (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (state : BeaconState) (signed_bid : SignedExecutionPayloadBid) : SpecM BeaconState := do
  let bid := signed_bid.message
  let builder_index := bid.builder_index
  let amount := bid.value

  if builder_index == BUILDER_INDEX_SELF_BUILD then
    if ¬ amount = 0 then throw .assertionFailed
    if ¬ signed_bid.signature = G2_POINT_AT_INFINITY then throw .assertionFailed
  else
    if ¬ (← is_active_builder state builder_index) then throw .assertionFailed
    if ¬ (← listGet state.builders builder_index).version = PAYLOAD_BUILDER_VERSION then
      throw .assertionFailed
    if ¬ (← can_builder_cover_bid state builder_index amount) then throw .assertionFailed
    if ¬ (← verify_execution_payload_bid_signature p o state signed_bid) then
      throw .assertionFailed

  if ¬ bid.blob_kzg_commitments.length ≤ max_blobs_per_block (← get_current_epoch p state) then
    throw .assertionFailed

  if ¬ bid.slot = state.slot then throw .assertionFailed
  if ¬ state.slot > GENESIS_SLOT then throw .assertionFailed
  if ¬ bid.parent_block_hash = state.latest_block_hash then throw .assertionFailed
  if ¬ bid.block_hash ≠ bid.parent_block_hash then throw .assertionFailed
  if ¬ bid.parent_block_root = (← get_block_root_at_slot p state (← uint64Sub state.slot 1)) then
    throw .assertionFailed
  if ¬ bid.prev_randao = (← get_randao_mix p state (← get_current_epoch p state)) then
    throw .assertionFailed

  let mut state := state
  if amount > 0 then
    let pending_payment : BuilderPendingPayment := {
      weight := 0
      withdrawal := {
        fee_recipient := bid.fee_recipient
        amount
        builder_index }
      proposer_index := ← get_beacon_proposer_index p state }
    state := { state with
      builder_pending_payments := ← listSet state.builder_pending_payments
        (← uint64Add p.SLOTS_PER_EPOCH (← uint64Mod bid.slot p.SLOTS_PER_EPOCH)) pending_payment }

  pure { state with latest_execution_payload_bid := bid }

end EpochProofs.Spec
