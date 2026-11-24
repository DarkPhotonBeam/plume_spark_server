#!/usr/bin/env bash
# Downloads the Spark Connect protocol definitions into proto/spark/connect.
set -euo pipefail
SPARK_TAG="${SPARK_TAG:-v4.3.0}"
BASE="https://raw.githubusercontent.com/apache/spark/${SPARK_TAG}/sql/connect/common/src/main/protobuf/spark/connect"
DEST="$(cd "$(dirname "$0")/.." && pwd)/proto/spark/connect"
mkdir -p "$DEST"

# Start from base.proto and follow spark/connect imports transitively.
declare -A seen
queue=(base.proto)
while ((${#queue[@]})); do
  f="${queue[0]}"; queue=("${queue[@]:1}")
  [[ -n "${seen[$f]:-}" ]] && continue
  seen[$f]=1
  echo "fetching $f"
  curl -fsSL "$BASE/$f" -o "$DEST/$f"
  while read -r dep; do queue+=("$dep"); done < <(grep -oP '^import\s+"spark/connect/\K[^"]+' "$DEST/$f" || true)
done
echo "done: ${!seen[*]}"
