//! Garbage collection process that runs at start-up to clean up the database.
use crate::database::interface::BeaconNodeBackend;
use crate::hot_cold_store::HotColdDB;
use crate::{DBColumn, Error, StoreOp};
use slog::debug;
use types::EthSpec;

impl<E> HotColdDB<E, BeaconNodeBackend<E>, BeaconNodeBackend<E>>
where
    E: EthSpec,
{
    /// Clean up the database by performing one-off maintenance at start-up.
    pub fn remove_garbage(&self) -> Result<(), Error> {
        self.delete_temp_states()?;
        Ok(())
    }

    /// Delete the temporary states that were leftover by failed block imports.

    pub fn delete_temp_states(&self) -> Result<(), Error> {
        let delete_state_ops =
            self.iter_temporary_state_roots()?
                .try_fold(vec![], |mut ops, state_root| {
                    let state_root = state_root?;
                    ops.push(StoreOp::DeleteState(state_root, None));
                    Result::<_, Error>::Ok(ops)
                })?;
        let delete_temp_state_ops =
            self.iter_temporary_state_roots()?
                .try_fold(vec![], |mut ops, state_root| {
                    let state_root = state_root?;
                    ops.push(StoreOp::DeleteStateTemporaryFlag(state_root));
                    Result::<_, Error>::Ok(ops)
                })?;
        if !delete_state_ops.is_empty() || !delete_temp_state_ops.is_empty() {
            debug!(
                self.log,
                "Garbage collecting {} temporary states",
                (delete_state_ops.len() / 2) + (delete_temp_state_ops.len() / 2)
            );
            let state_col: &str = DBColumn::BeaconStateSummary.into();
            let temp_state_col: &str = DBColumn::BeaconStateTemporary.into();
            self.do_atomically_for_garbage_collection(state_col, delete_state_ops)?;
            self.do_atomically_for_garbage_collection(temp_state_col, delete_temp_state_ops)?;
        }

        Ok(())
    }
}
