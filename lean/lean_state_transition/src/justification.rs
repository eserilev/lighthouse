use crate::Error;
use lean_types::{BitList, JustifiedSlots, Slot};
use safe_arith::SafeArith;
use ssz_types::typenum::Unsigned;
use std::iter;

pub fn is_slot_justified(
    justified_slots: &JustifiedSlots,
    finalized_slot: Slot,
    slot: Slot,
) -> Result<bool, Error> {
    let Some(index) = slot.justified_index_after(finalized_slot) else {
        return Ok(true);
    };
    justified_slots
        .get(index)
        .map_err(|_| Error::JustifiedSlotOutOfRange {
            slot,
            finalized_slot,
        })
}

pub fn extend_justified_slots(
    justified_slots: &mut JustifiedSlots,
    finalized_slot: Slot,
    slot: Slot,
) -> Result<(), Error> {
    let Some(index) = slot.justified_index_after(finalized_slot) else {
        return Ok(());
    };
    let required_len = index.safe_add(1)?;
    let current_len = justified_slots.len();
    if required_len <= current_len {
        return Ok(());
    }

    *justified_slots = bitlist_from_bools(
        justified_slots
            .iter()
            .chain(iter::repeat_n(false, required_len.safe_sub(current_len)?)),
    )?;
    Ok(())
}

pub(crate) fn bitlist_from_bools<N: Unsigned + Clone>(
    bits: impl IntoIterator<Item = bool>,
) -> Result<BitList<N>, Error> {
    let bits: Vec<bool> = bits.into_iter().collect();
    let mut bitlist = BitList::with_capacity(bits.len())?;
    for (index, bit) in bits.into_iter().enumerate() {
        if bit {
            bitlist.set(index, true)?;
        }
    }
    Ok(bitlist)
}
