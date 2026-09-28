use crate::{Case, Outcome};
use p3_field::PrimeField32;
use p3_koala_bear::{KoalaBear, default_koalabear_poseidon1_16, default_koalabear_poseidon1_24};
use p3_symmetric::Permutation;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoseidonPermutationCase {
    width: usize,
    input_state: Vec<String>,
    output_state: Vec<String>,
}

impl Case for PoseidonPermutationCase {
    const CATEGORY: &'static str = "poseidon_permutation";

    fn run(&self) -> Result<Outcome, String> {
        let input = parse_state(&self.input_state)?;
        let expected = parse_state(&self.output_state)?;
        let output = match self.width {
            16 => permute(default_koalabear_poseidon1_16(), &input)?,
            24 => permute(default_koalabear_poseidon1_24(), &input)?,
            width => return Err(format!("unsupported width {width}")),
        };
        if output != expected {
            return Err(format!("expected {expected:?}, got {output:?}"));
        }
        Ok(Outcome::Passed)
    }
}

fn parse_state(state: &[String]) -> Result<Vec<u32>, String> {
    state
        .iter()
        .map(|element| {
            element
                .parse::<u32>()
                .map_err(|e| format!("{element}: {e}"))
        })
        .collect()
}

fn permute<const WIDTH: usize>(
    permutation: impl Permutation<[KoalaBear; WIDTH]>,
    input: &[u32],
) -> Result<Vec<u32>, String> {
    let state: [KoalaBear; WIDTH] = input
        .iter()
        .map(|element| KoalaBear::new(*element))
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|_| format!("expected {WIDTH} elements, got {}", input.len()))?;
    Ok(permutation
        .permute(state)
        .iter()
        .map(PrimeField32::as_canonical_u32)
        .collect())
}
