#!/usr/bin/env bash
# Regenerate CacheProofs/Generated.lean from the pure functions of the BeaconState caches.
#
# Needs Charon and Aeneas built at the pinned revisions. See README.md for the build steps
# and .github/workflows/lean-proofs.yml for the pins. Point CHARON and AENEAS at the binaries if
# they are not on PATH. The Aeneas binary is src/_build/default/main.exe in its checkout.
set -euo pipefail

CHARON=${CHARON:-charon}
AENEAS=${AENEAS:-aeneas}

proofs_dir=$(cd "$(dirname "$0")" && pwd)
crate_dir=$(dirname "$proofs_dir")
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT

# `--include`: translate the bodies of these dependencies. Without it they become axioms.
# `-- --lib`: without it Charon translates the crate's binaries into opaque bodies.
(cd "$crate_dir" && "$CHARON" cargo --preset=aeneas \
  --start-from types::state::exit_queue \
  --start-from types::validator::activation_eligibility \
  --start-from types::state::total_active_balance \
  --start-from types::state::base_rewards \
  --start-from types::state::balance \
  --start-from types::state::participation_totals \
  --include safe_arith \
  --dest-file "$out/pure.llbc" -- --lib)

# Aeneas names the output after the llbc file.
"$AENEAS" -backend lean "$out/pure.llbc" -dest "$out"
mv "$out/Pure.lean" "$proofs_dir/CacheProofs/Generated.lean"
echo "wrote $proofs_dir/CacheProofs/Generated.lean"
