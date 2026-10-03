//! Pure functions for the flag totals of `ProgressiveBalancesCache`.
//!
//! Aeneas translates this module for the proofs in `consensus/types/proofs`, so it uses plain
//! integers, indexed `while` loops and no iterator adapters.

use safe_arith::{ArithError, SafeArith};

use super::Balance;
use crate::core::consts::altair::NUM_FLAG_INDICES;

/// An error from a flag total update.
#[derive(PartialEq, Debug, Clone, Copy)]
pub enum Error {
    InvalidFlagIndex(usize),
    Arith(ArithError),
}

impl From<ArithError> for Error {
    fn from(e: ArithError) -> Self {
        Error::Arith(e)
    }
}

/// Returns `true` if bit `flag_index` of `flags` is set, as `ParticipationFlags::has_flag`.
pub fn has_flag(flags: u8, flag_index: usize) -> Result<bool, ArithError> {
    if flag_index >= NUM_FLAG_INDICES {
        return Err(ArithError::Overflow);
    }
    let mask: u8 = match flag_index {
        0 => 1,
        1 => 2,
        _ => 4,
    };
    Ok(flags & mask == mask)
}

/// Returns the total of `flag_index` with the minimum applied.
pub fn total_flag_balance(totals: &[Balance], flag_index: usize) -> Result<u64, Error> {
    match totals.get(flag_index) {
        Some(balance) => Ok(balance.get()),
        None => Err(Error::InvalidFlagIndex(flag_index)),
    }
}

/// Adds `amount` to the total of `flag_index`.
pub fn add_to_flag(totals: &mut [Balance], flag_index: usize, amount: u64) -> Result<(), Error> {
    match totals.get_mut(flag_index) {
        Some(balance) => {
            balance.safe_add_assign(amount)?;
            Ok(())
        }
        None => Err(Error::InvalidFlagIndex(flag_index)),
    }
}

/// Subtracts `amount` from the total of `flag_index`.
pub fn sub_from_flag(totals: &mut [Balance], flag_index: usize, amount: u64) -> Result<(), Error> {
    match totals.get_mut(flag_index) {
        Some(balance) => {
            balance.safe_sub_assign(amount)?;
            Ok(())
        }
        None => Err(Error::InvalidFlagIndex(flag_index)),
    }
}

/// Adds `effective_balance` to the total of `flag_index` if `flags` has that flag.
pub fn add_if_flag(
    totals: &mut [Balance],
    flags: u8,
    flag_index: usize,
    effective_balance: u64,
) -> Result<(), Error> {
    if has_flag(flags, flag_index)? {
        add_to_flag(totals, flag_index, effective_balance)?;
    }
    Ok(())
}

/// Subtracts `effective_balance` from the total of `flag_index` if `flags` has that flag.
pub fn sub_if_flag(
    totals: &mut [Balance],
    flags: u8,
    flag_index: usize,
    effective_balance: u64,
) -> Result<(), Error> {
    if has_flag(flags, flag_index)? {
        sub_from_flag(totals, flag_index, effective_balance)?;
    }
    Ok(())
}

/// Moves the total of `flag_index` from `old_effective_balance` to `new_effective_balance` if
/// `flags` has that flag.
pub fn change_if_flag(
    totals: &mut [Balance],
    flags: u8,
    flag_index: usize,
    old_effective_balance: u64,
    new_effective_balance: u64,
) -> Result<(), Error> {
    if has_flag(flags, flag_index)? {
        if new_effective_balance > old_effective_balance {
            add_to_flag(
                totals,
                flag_index,
                new_effective_balance.safe_sub(old_effective_balance)?,
            )?;
        } else {
            sub_from_flag(
                totals,
                flag_index,
                old_effective_balance.safe_sub(new_effective_balance)?,
            )?;
        }
    }
    Ok(())
}

/// Adds `effective_balance` to the total of each flag in `flags`.
pub fn add_flags(
    totals: &mut [Balance; NUM_FLAG_INDICES],
    flags: u8,
    effective_balance: u64,
) -> Result<(), Error> {
    add_if_flag(totals, flags, 0, effective_balance)?;
    add_if_flag(totals, flags, 1, effective_balance)?;
    add_if_flag(totals, flags, 2, effective_balance)?;
    Ok(())
}

/// Updates the totals when `flag_index` is newly set for a validator.
pub fn on_new_attestation(
    totals: &mut [Balance; NUM_FLAG_INDICES],
    is_slashed: bool,
    flag_index: usize,
    effective_balance: u64,
) -> Result<(), Error> {
    if is_slashed {
        return Ok(());
    }
    add_to_flag(totals, flag_index, effective_balance)
}

/// Removes `effective_balance` from the total of each flag in `flags`.
pub fn on_slashing(
    totals: &mut [Balance; NUM_FLAG_INDICES],
    flags: u8,
    effective_balance: u64,
) -> Result<(), Error> {
    sub_if_flag(totals, flags, 0, effective_balance)?;
    sub_if_flag(totals, flags, 1, effective_balance)?;
    sub_if_flag(totals, flags, 2, effective_balance)?;
    Ok(())
}

/// Moves the totals of each flag in `flags` to the new effective balance, if not slashed.
pub fn on_effective_balance_change(
    totals: &mut [Balance; NUM_FLAG_INDICES],
    is_slashed: bool,
    flags: u8,
    old_effective_balance: u64,
    new_effective_balance: u64,
) -> Result<(), Error> {
    if is_slashed {
        return Ok(());
    }
    change_if_flag(
        totals,
        flags,
        0,
        old_effective_balance,
        new_effective_balance,
    )?;
    change_if_flag(
        totals,
        flags,
        1,
        old_effective_balance,
        new_effective_balance,
    )?;
    change_if_flag(
        totals,
        flags,
        2,
        old_effective_balance,
        new_effective_balance,
    )?;
    Ok(())
}
