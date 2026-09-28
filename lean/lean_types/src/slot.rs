use crate::config::IMMEDIATE_JUSTIFICATION_WINDOW;
use fixed_bytes::Hash256;
use ssz::{Decode, DecodeError, Encode};
use std::fmt;
use tree_hash::{PackedEncoding, TreeHash, TreeHashType};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Slot(u64);

impl Slot {
    pub const fn new(slot: u64) -> Self {
        Self(slot)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }

    pub fn justified_index_after(self, finalized_slot: Slot) -> Option<usize> {
        justified_index_after(self.0, finalized_slot.0)
    }

    pub fn is_justifiable_after(self, finalized_slot: Slot) -> bool {
        is_justifiable_after(self.0, finalized_slot.0)
    }
}

fn justified_index_after(slot: u64, finalized_slot: u64) -> Option<usize> {
    let Some(delta) = slot.checked_sub(finalized_slot) else {
        return None;
    };
    let Some(index) = delta.checked_sub(1) else {
        return None;
    };
    if index > usize::MAX as u64 {
        return None;
    }
    Some(index as usize)
}

fn is_justifiable_after(slot: u64, finalized_slot: u64) -> bool {
    let Some(delta) = slot.checked_sub(finalized_slot) else {
        return false;
    };

    if delta <= IMMEDIATE_JUSTIFICATION_WINDOW {
        return true;
    }

    let root = delta.isqrt();
    if root * root == delta {
        return true;
    }

    let discriminant = 4 * u128::from(delta) + 1;
    let root = discriminant.isqrt();
    root * root == discriminant
}

impl From<u64> for Slot {
    fn from(slot: u64) -> Self {
        Self(slot)
    }
}

impl From<Slot> for u64 {
    fn from(slot: Slot) -> Self {
        slot.0
    }
}

impl fmt::Display for Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Encode for Slot {
    fn is_ssz_fixed_len() -> bool {
        <u64 as Encode>::is_ssz_fixed_len()
    }

    fn ssz_fixed_len() -> usize {
        <u64 as Encode>::ssz_fixed_len()
    }

    fn ssz_bytes_len(&self) -> usize {
        self.0.ssz_bytes_len()
    }

    fn ssz_append(&self, buf: &mut Vec<u8>) {
        self.0.ssz_append(buf)
    }
}

impl Decode for Slot {
    fn is_ssz_fixed_len() -> bool {
        <u64 as Decode>::is_ssz_fixed_len()
    }

    fn ssz_fixed_len() -> usize {
        <u64 as Decode>::ssz_fixed_len()
    }

    fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        u64::from_ssz_bytes(bytes).map(Self)
    }
}

impl TreeHash for Slot {
    fn tree_hash_type() -> TreeHashType {
        u64::tree_hash_type()
    }

    fn tree_hash_packed_encoding(&self) -> PackedEncoding {
        self.0.tree_hash_packed_encoding()
    }

    fn tree_hash_packing_factor() -> usize {
        u64::tree_hash_packing_factor()
    }

    fn tree_hash_root(&self) -> Hash256 {
        self.0.tree_hash_root()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn justifiable_deltas() {
        let justifiable: Vec<u64> = (0..=110)
            .filter(|delta| Slot::new(*delta).is_justifiable_after(Slot::new(0)))
            .collect();
        assert_eq!(
            justifiable,
            vec![
                0, 1, 2, 3, 4, 5, 6, 9, 12, 16, 20, 25, 30, 36, 42, 49, 56, 64, 72, 81, 90, 100,
                110
            ]
        );
    }

    #[test]
    fn justifiable_at_u64_extremes() {
        assert!(!Slot::new(0).is_justifiable_after(Slot::new(1)));
        assert!(!Slot::new(u64::MAX).is_justifiable_after(Slot::new(0)));
        let root = u64::from(u32::MAX);
        assert!(Slot::new(root * root).is_justifiable_after(Slot::new(0)));
        assert!(Slot::new(root * (root + 1)).is_justifiable_after(Slot::new(0)));
    }

    #[test]
    fn justified_index() {
        assert_eq!(Slot::new(10).justified_index_after(Slot::new(10)), None);
        assert_eq!(Slot::new(9).justified_index_after(Slot::new(10)), None);
        assert_eq!(Slot::new(11).justified_index_after(Slot::new(10)), Some(0));
    }
}
