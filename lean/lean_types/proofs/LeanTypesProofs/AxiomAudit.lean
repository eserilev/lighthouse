import LeanTypesProofs.Correctness
import Lean.Util.CollectAxioms

/-!
# Axiom audit

Walks every theorem under `LeanTypesProofs` and fails if one depends on an axiom outside the
three standard ones and the `isqrt` axioms below, or if the library declares any other axiom.
A `sorry` anywhere beneath a theorem shows up as `sorryAx`, so this also rejects incomplete
proofs.

The Aeneas library has no model of `isqrt`. `Generated.lean` declares `u64::isqrt` and
`u128::isqrt` as opaque functions, and `Isqrt.lean` states their standard specification. These
four declarations are the whole of the trusted `isqrt` model. The audit also fails if one of
them disappears, so the list below stays exact.

This runs at elaboration time, so `lake build` fails on a violation. CI also runs it directly
with `lake env lean LeanTypesProofs/AxiomAudit.lean`, since Lake can replay a cached log.
-/

open Lean Elab Command

namespace LeanTypesProofs

def allowedAxioms : List Name := [``propext, ``Classical.choice, ``Quot.sound]

def isqrtAxioms : List Name :=
  [``lean_types.core.num.U64.isqrt, ``lean_types.core.num.U128.isqrt,
    ``u64_isqrt_spec, ``u128_isqrt_spec]

run_cmd do
  let env ← getEnv
  let mut theorems := 0
  let mut declared : Array Name := #[]
  let mut violations : Array MessageData := #[]
  for (declName, info) in env.constants.toList do
    let some idx := env.getModuleIdxFor? declName | continue
    let moduleName := env.header.moduleNames[idx]!
    unless Name.isPrefixOf `LeanTypesProofs moduleName do continue
    match info with
    | .thmInfo _ =>
      let axioms ← collectAxioms declName
      theorems := theorems + 1
      for ax in axioms do
        unless allowedAxioms.contains ax || isqrtAxioms.contains ax do
          violations := violations.push m!"{declName} depends on {ax}"
    | .axiomInfo _ =>
      declared := declared.push declName
      unless isqrtAxioms.contains declName do
        violations := violations.push m!"{declName} is declared as an axiom"
    | _ => pure ()
  for ax in isqrtAxioms do
    unless declared.contains ax do
      violations := violations.push m!"{ax} is listed in isqrtAxioms but not declared"
  if theorems == 0 then
    throwError "no theorems found under LeanTypesProofs"
  unless violations.isEmpty do
    throwError m!"axiom audit failed:\n{MessageData.joinSep violations.toList "\n"}"
  logInfo m!"audited {theorems} theorems, all within {allowedAxioms} and {isqrtAxioms}"

end LeanTypesProofs
