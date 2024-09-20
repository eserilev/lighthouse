//! Garbage collection process that runs at start-up to clean up the database.
use crate::database::interface::BeaconNodeBackend;
use crate::hot_cold_store::HotColdDB;
use crate::{DBColumn, Error};
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
        let mut ops = vec![];
        // let mut delete_state_ops = vec![];
        // let mut delete_summary_ops = vec![];
        // let mut delete_temporary_state_ops = vec![];
        let mut delete_states = false;
        self.iter_temporary_state_roots()?.for_each(|state_root| {
            if let Ok(state_root) = state_root {
                ops.push(state_root);
                delete_states = true
            }
        });
        if delete_states {
            debug!(
                self.log,
                "Garbage collecting {} temporary states",
                ops.len()
            );
            let state_col: &str = DBColumn::BeaconState.into();
            let summary_col: &str = DBColumn::BeaconStateSummary.into();
            let temp_state_col: &str = DBColumn::BeaconStateTemporary.into();
            // self.do_atomically_for_garbage_collection(state_col, delete_state_ops)?;
            // self.do_atomically_for_garbage_collection(summary_col, delete_summary_ops)?;
            // self.do_atomically_for_garbage_collection(temp_state_col, delete_temporary_state_ops)?;

            self.extract_if(state_col, ops.clone())?;
            self.extract_if(summary_col, ops.clone())?;
            self.extract_if(temp_state_col, ops)?;
        }

        Ok(())
    }
}
