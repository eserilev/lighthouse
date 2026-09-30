use safe_arith::{ArithError, SafeArith};
use serde::{Deserialize, Serialize};

use crate::core::{
    Epoch, Slot,
    chain_spec::{EpochSchedule, ScheduleEntry},
};

#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "UPPERCASE")]
pub struct SlotDurationScheduleEntry {
    pub epoch: Epoch,
    #[serde(with = "serde_utils::quoted_u64")]
    pub slot_duration_ms: u64,
}

impl ScheduleEntry for SlotDurationScheduleEntry {
    fn epoch(&self) -> Epoch {
        self.epoch
    }
}

pub type SlotDurationSchedule = EpochSchedule<SlotDurationScheduleEntry>;

impl SlotDurationSchedule {
    pub fn slot_duration_ms_for_epoch(&self, epoch: Epoch) -> Option<u64> {
        self.entry_for_epoch(epoch)
            .map(|entry| entry.slot_duration_ms)
    }

    /// Returns `ArithError::Overflow` for an empty schedule. Callers use
    /// `ChainSpec::slot_duration_schedule()`, which is never empty.
    pub fn compute_time_at_slot_ms(
        &self,
        slots_per_epoch: u64,
        genesis_time_ms: u64,
        slot: Slot,
    ) -> Result<u64, ArithError> {
        compute_time_at_slot_ms(self.as_vec(), slots_per_epoch, genesis_time_ms, slot)
    }

    /// Returns `ArithError::Overflow` for an empty schedule. Callers use
    /// `ChainSpec::slot_duration_schedule()`, which is never empty.
    pub fn compute_slot_at_time_ms(
        &self,
        slots_per_epoch: u64,
        genesis_time_ms: u64,
        time_ms: u64,
    ) -> Result<Slot, ArithError> {
        compute_slot_at_time_ms(self.as_vec(), slots_per_epoch, genesis_time_ms, time_ms)
    }
}

fn compute_time_at_slot_ms(
    schedule: &[SlotDurationScheduleEntry],
    slots_per_epoch: u64,
    genesis_time_ms: u64,
    slot: Slot,
) -> Result<u64, ArithError> {
    let mut index = schedule.len().saturating_sub(1);
    let genesis = schedule.get(index).ok_or(ArithError::Overflow)?;
    let mut start_slot = genesis.epoch.start_slot(slots_per_epoch);
    let mut start_time_ms = genesis_time_ms;
    let mut slot_duration_ms = genesis.slot_duration_ms;
    let mut overflow = false;
    while index > 0 {
        index = index.saturating_sub(1);
        let Some(entry) = schedule.get(index) else {
            break;
        };
        let entry_slot = entry.epoch.start_slot(slots_per_epoch);
        if slot.as_u64() <= entry_slot.as_u64() {
            break;
        }
        let Ok(entry_time_ms) =
            time_after_slots_ms(start_time_ms, start_slot, entry_slot, slot_duration_ms)
        else {
            overflow = true;
            break;
        };
        start_slot = entry_slot;
        start_time_ms = entry_time_ms;
        slot_duration_ms = entry.slot_duration_ms;
    }
    if overflow {
        return Err(ArithError::Overflow);
    }
    time_after_slots_ms(start_time_ms, start_slot, slot, slot_duration_ms)
}

fn compute_slot_at_time_ms(
    schedule: &[SlotDurationScheduleEntry],
    slots_per_epoch: u64,
    genesis_time_ms: u64,
    time_ms: u64,
) -> Result<Slot, ArithError> {
    let mut index = schedule.len().saturating_sub(1);
    let genesis = schedule.get(index).ok_or(ArithError::Overflow)?;
    let mut start_slot = genesis.epoch.start_slot(slots_per_epoch);
    let mut start_time_ms = genesis_time_ms;
    let mut slot_duration_ms = genesis.slot_duration_ms;
    while index > 0 {
        index = index.saturating_sub(1);
        let Some(entry) = schedule.get(index) else {
            break;
        };
        let entry_slot = entry.epoch.start_slot(slots_per_epoch);
        let Ok(entry_time_ms) =
            time_after_slots_ms(start_time_ms, start_slot, entry_slot, slot_duration_ms)
        else {
            break;
        };
        if time_ms < entry_time_ms {
            break;
        }
        start_slot = entry_slot;
        start_time_ms = entry_time_ms;
        slot_duration_ms = entry.slot_duration_ms;
    }
    let slots = time_ms
        .safe_sub(start_time_ms)?
        .safe_div(slot_duration_ms)?;
    start_slot.safe_add(slots)
}

fn time_after_slots_ms(
    start_time_ms: u64,
    start_slot: Slot,
    end_slot: Slot,
    slot_duration_ms: u64,
) -> Result<u64, ArithError> {
    start_time_ms.safe_add(
        end_slot
            .safe_sub(start_slot)?
            .as_u64()
            .safe_mul(slot_duration_ms)?,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{EthSpec, MainnetEthSpec};

    fn slot_duration_schedule(entries: &[(u64, u64)]) -> SlotDurationSchedule {
        SlotDurationSchedule::new(
            entries
                .iter()
                .map(|&(epoch, slot_duration_ms)| SlotDurationScheduleEntry {
                    epoch: Epoch::new(epoch),
                    slot_duration_ms,
                })
                .collect(),
        )
    }

    #[test]
    fn slot_duration_schedule_yaml_keys() {
        let schedule: SlotDurationSchedule =
            yaml_serde::from_str("- EPOCH: 0\n  SLOT_DURATION_MS: 12000\n")
                .expect("error while deserializing");
        assert_eq!(schedule, slot_duration_schedule(&[(0, 12000)]));
    }

    #[test]
    fn slot_time_mapping_ignores_unreachable_entries() {
        let schedule = slot_duration_schedule(&[(0, 12000), (u64::MAX, 6000)]);
        let slots_per_epoch = MainnetEthSpec::slots_per_epoch();
        assert_eq!(
            schedule.compute_slot_at_time_ms(slots_per_epoch, 0, 120_000),
            Ok(Slot::new(10))
        );
        assert_eq!(
            schedule.compute_time_at_slot_ms(slots_per_epoch, 0, Slot::new(10)),
            Ok(120_000)
        );
    }
}

#[cfg(test)]
mod slot_duration_schedule_properties {
    use super::*;
    use proptest::prelude::*;

    const SLOTS_PER_EPOCH: u64 = 32;

    fn spec_time_at_slot(schedule: &[(u64, u64)], genesis_time_ms: u128, slot: u128) -> u128 {
        let mut end_slot = slot;
        let mut time_ms = genesis_time_ms;
        for &(epoch, slot_duration_ms) in schedule.iter().rev() {
            let entry_slot = u128::from(epoch) * u128::from(SLOTS_PER_EPOCH);
            if entry_slot < end_slot {
                time_ms += (end_slot - entry_slot) * u128::from(slot_duration_ms);
                end_slot = entry_slot;
            }
        }
        time_ms
    }

    fn spec_slot_at_time(
        schedule: &[(u64, u64)],
        genesis_time_ms: u128,
        time_ms: u128,
    ) -> Option<u128> {
        let (first_epoch, first_slot_duration_ms) = schedule[0];
        let mut entry_slot = u128::from(first_epoch) * u128::from(SLOTS_PER_EPOCH);
        let mut entry_time_ms = genesis_time_ms;
        let mut slot_duration_ms = u128::from(first_slot_duration_ms);
        for &(epoch, next_slot_duration_ms) in &schedule[1..] {
            let next_slot = u128::from(epoch) * u128::from(SLOTS_PER_EPOCH);
            let next_time_ms = entry_time_ms + (next_slot - entry_slot) * slot_duration_ms;
            if time_ms < next_time_ms {
                break;
            }
            entry_slot = next_slot;
            entry_time_ms = next_time_ms;
            slot_duration_ms = u128::from(next_slot_duration_ms);
        }
        let slots = time_ms.checked_sub(entry_time_ms)? / slot_duration_ms;
        Some(entry_slot + slots)
    }

    fn to_schedule(entries: &[(u64, u64)]) -> SlotDurationSchedule {
        SlotDurationSchedule::new(
            entries
                .iter()
                .map(|&(epoch, slot_duration_ms)| SlotDurationScheduleEntry {
                    epoch: Epoch::new(epoch),
                    slot_duration_ms,
                })
                .collect(),
        )
    }

    fn epoch() -> impl Strategy<Value = u64> {
        prop_oneof![1u64..200, 1u64..u64::MAX / SLOTS_PER_EPOCH, Just(u64::MAX)]
    }

    fn slot_duration_ms() -> impl Strategy<Value = u64> {
        (1u64..=24).prop_map(|seconds| seconds * 1000)
    }

    fn schedule() -> impl Strategy<Value = Vec<(u64, u64)>> {
        (
            slot_duration_ms(),
            proptest::collection::vec((epoch(), slot_duration_ms()), 0..4),
        )
            .prop_map(|(genesis_slot_duration_ms, later)| {
                let mut entries = vec![(0, genesis_slot_duration_ms)];
                entries.extend(later);
                entries.sort_by_key(|&(epoch, _)| epoch);
                entries.dedup_by_key(|&mut (epoch, _)| epoch);
                entries
            })
    }

    proptest! {
        #[test]
        fn time_at_slot_matches_spec(
            entries in schedule(),
            genesis_time_ms in 0u64..1 << 44,
            slot in prop_oneof![0u64..20_000, any::<u64>()],
        ) {
            let expected = spec_time_at_slot(&entries, u128::from(genesis_time_ms), u128::from(slot));
            match to_schedule(&entries).compute_time_at_slot_ms(SLOTS_PER_EPOCH, genesis_time_ms, Slot::new(slot)) {
                Ok(time_ms) => prop_assert_eq!(u128::from(time_ms), expected),
                Err(_) => prop_assert!(expected > u128::from(u64::MAX)),
            }
        }

        #[test]
        fn slot_at_time_matches_spec(
            entries in schedule(),
            genesis_time_ms in 0u64..1 << 44,
            offset_ms in prop_oneof![0u64..1 << 30, 0u64..1 << 50],
        ) {
            let time_ms = genesis_time_ms + offset_ms;
            let expected =
                spec_slot_at_time(&entries, u128::from(genesis_time_ms), u128::from(time_ms));
            let slot = to_schedule(&entries)
                .compute_slot_at_time_ms(SLOTS_PER_EPOCH, genesis_time_ms, time_ms);
            prop_assert_eq!(slot.map(|slot| u128::from(slot.as_u64())).ok(), expected);
        }

        #[test]
        fn slot_at_time_inverts_time_at_slot(
            entries in schedule(),
            genesis_time_ms in 0u64..1 << 44,
            slot in 0u64..20_000,
        ) {
            let schedule = to_schedule(&entries);
            let time_at = |slot: u64| {
                schedule
                    .compute_time_at_slot_ms(SLOTS_PER_EPOCH, genesis_time_ms, Slot::new(slot))
                    .unwrap()
            };
            let slot_at = |time_ms: u64| {
                schedule
                    .compute_slot_at_time_ms(SLOTS_PER_EPOCH, genesis_time_ms, time_ms)
                    .unwrap()
                    .as_u64()
            };
            prop_assert!(time_at(slot) < time_at(slot + 1));
            prop_assert_eq!(slot_at(time_at(slot)), slot);
            prop_assert_eq!(slot_at(time_at(slot + 1) - 1), slot);
        }

        #[test]
        fn slot_at_time_is_total_after_genesis(
            entries in schedule(),
            genesis_time_ms in 1u64..1 << 44,
            offset_ms in any::<u64>(),
        ) {
            let schedule = to_schedule(&entries);
            let time_ms = genesis_time_ms.saturating_add(offset_ms);
            prop_assert!(
                schedule
                    .compute_slot_at_time_ms(SLOTS_PER_EPOCH, genesis_time_ms, time_ms)
                    .is_ok()
            );
            prop_assert!(
                schedule
                    .compute_slot_at_time_ms(SLOTS_PER_EPOCH, genesis_time_ms, genesis_time_ms - 1)
                    .is_err()
            );
        }
    }
}
