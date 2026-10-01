import SlotScheduleProofs.Correctness
import Lean.Util.CollectAxioms

/-!
# Axiom audit

Walks every theorem under `SlotScheduleProofs` and fails if one depends on an axiom outside the
three standard ones and the lists below, or if the library declares any other axiom. A
`sorry` anywhere beneath a theorem shows up as `sorryAx`, so this also rejects incomplete
proofs.

The Aeneas library has no model of `u64::saturating_mul`. `Generated.lean` declares it as an
opaque function, and `SaturatingMul.lean` states its standard specification. These two
declarations are the whole of the trusted `saturating_mul` model.

`validate_schedule` builds error strings, and three more kinds of axioms come from that code only:

- `Generated.lean` declares four formatting functions as opaque: `alloc::fmt::format`,
  `ToString::to_string`, `Display for str` and `core::hint::must_use`. No axiom states anything
  about them, so a theorem cannot learn anything from them.
- The Aeneas library declares the type `core::fmt::Formatter` as opaque.
- Aeneas's `toStr` checks the length of a string literal with `decide +native`. Lean records
  each check as an auxiliary axiom with `_native` in its name. The audit accepts such axioms
  only when `Generated.lean` declares them.

The theorems about `validate_schedule` only use its `Ok(())` result, never an error string.

The audit also fails if an axiom in the lists disappears, so the lists stay exact.

This runs at elaboration time, so `lake build` fails on a violation. CI also runs it directly
with `lake env lean SlotScheduleProofs/Axioms.lean`, since Lake can replay a cached log.

The `#print axioms` lines at the end are for `.github/workflows/proofs.yml`. It requires each of
them to report exactly `propext`, `Classical.choice` and `Quot.sound`. The theorems about the Rust
functions also depend on the model axioms above, so those lines list only the walk theorems and
the direct definitions. The `run_cmd` audit covers every theorem.
-/

open Lean Elab Command

namespace SlotScheduleProofs

def allowedAxioms : List Name := [``propext, ``Classical.choice, ``Quot.sound]

def saturatingMulAxioms : List Name :=
  [``types.core.num.U64.saturating_mul, ``u64_saturating_mul_spec]

def formattingAxioms : List Name :=
  [``types.alloc.fmt.format, ``types.alloc.string.ToString.Blanket.to_string,
    ``types.Str.Insts.CoreFmtDisplay.fmt, ``types.core.hint.must_use]

def modelAxioms : List Name := saturatingMulAxioms ++ formattingAxioms

def libraryAxioms : List Name := [``Aeneas.Std.core.fmt.Formatter]

/-- An auxiliary axiom that `decide +native` adds for a string literal in `Generated.lean`. -/
def isNativeDecide (env : Environment) (n : Name) : Bool :=
  n.components.contains `_native &&
    match env.getModuleIdxFor? n with
    | some idx => env.header.moduleNames[idx]! == `SlotScheduleProofs.Generated
    | none => false

run_cmd do
  let env ← getEnv
  let mut theorems := 0
  let mut declared : Array Name := #[]
  let mut violations : Array MessageData := #[]
  for (declName, info) in env.constants.toList do
    let some idx := env.getModuleIdxFor? declName | continue
    let moduleName := env.header.moduleNames[idx]!
    unless Name.isPrefixOf `SlotScheduleProofs moduleName do continue
    match info with
    | .thmInfo _ =>
      let axioms ← collectAxioms declName
      theorems := theorems + 1
      for ax in axioms do
        unless allowedAxioms.contains ax || modelAxioms.contains ax || libraryAxioms.contains ax ||
            isNativeDecide env ax do
          violations := violations.push m!"{declName} depends on {ax}"
    | .axiomInfo _ =>
      declared := declared.push declName
      unless modelAxioms.contains declName || isNativeDecide env declName do
        violations := violations.push m!"{declName} is declared as an axiom"
    | _ => pure ()
  for ax in modelAxioms do
    unless declared.contains ax do
      violations := violations.push m!"{ax} is listed in modelAxioms but not declared"
  if theorems == 0 then
    throwError "no theorems found under SlotScheduleProofs"
  unless violations.isEmpty do
    throwError m!"axiom audit failed:\n{MessageData.joinSep violations.toList "\n"}"
  logInfo m!"audited {theorems} theorems, all within {allowedAxioms}, {modelAxioms}, \
    {libraryAxioms} and the string literal checks"

end SlotScheduleProofs

#print axioms SlotScheduleProofs.walk_floor
#print axioms SlotScheduleProofs.walk_round_trip
#print axioms SlotScheduleProofs.timeAtSlot_eq_spec
#print axioms SlotScheduleProofs.slotAtTime_eq_spec
