#!/usr/bin/env bash
# Regenerate LeanTypesProofs/Generated.lean from ../src/slot.rs.
#
# Needs Charon and Aeneas built at the pinned revisions. See README.md for the build steps
# and .github/workflows/proofs.yml for the pins. Point CHARON and AENEAS at the binaries if
# they are not on PATH. The Aeneas binary is src/_build/default/main.exe in its checkout.
set -euo pipefail

CHARON=${CHARON:-charon}
AENEAS=${AENEAS:-aeneas}

proofs_dir=$(cd "$(dirname "$0")" && pwd)
crate_dir=$(dirname "$proofs_dir")
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT

# `--start-from` does not accept methods of inherent impls, so the translation starts from
# the free functions that the `Slot` methods call.
(cd "$crate_dir" && "$CHARON" cargo --preset=aeneas \
  --start-from lean_types::slot::is_justifiable_after \
  --start-from lean_types::slot::justified_index_after \
  --dest-file "$out/pure.llbc" -- --lib)

# Aeneas names the output after the llbc file.
"$AENEAS" -backend lean "$out/pure.llbc" -dest "$out"
mv "$out/Pure.lean" "$proofs_dir/LeanTypesProofs/Generated.lean"
echo "wrote $proofs_dir/LeanTypesProofs/Generated.lean"
