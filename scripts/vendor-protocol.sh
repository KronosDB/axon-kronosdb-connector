#!/usr/bin/env bash
# Copies the native protocol's generated Java codec, its schema and its golden
# fixtures out of a KronosDB checkout into this repository.
#
#   scripts/vendor-protocol.sh ~/Development/kronosdb
#
# The codec is generated from protocol/kronosdb.schema in the server repository
# and is never edited here. After copying, update PINNED_SCHEMA_HASH in
# src/test/java/io/kronosdb/protocol/SchemaHashTest.java to the hash this script
# prints: that one-line change is the review point for "the wire changed", and
# the conformance suite proves the new codec matches the new fixtures byte for
# byte.
#
# Where things land:
#   src/main/java/io/kronosdb/protocol/*.java          the codec (shipped in the jar)
#   src/test/java/io/kronosdb/protocol/{ConformanceTest,Json,ProtocolJson}.java
#   protocol/kronosdb.schema                           the schema the codec was generated from
#   protocol/fixtures/*.{bin,json}                     the golden frames

set -euo pipefail

if [ $# -ne 1 ]; then
  echo "usage: scripts/vendor-protocol.sh <path to a kronosdb checkout>" >&2
  exit 2
fi

source_root="$(cd "$1" && pwd)/protocol"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

codec="$source_root/java/src/main/java/io/kronosdb/protocol"
tests="$source_root/java/src/test/java/io/kronosdb/protocol"
for needed in "$source_root/kronosdb.schema" "$codec/Frames.java" "$tests/ConformanceTest.java" "$source_root/fixtures"; do
  if [ ! -e "$needed" ]; then
    echo "$needed does not exist; is $1 a KronosDB checkout with the native protocol?" >&2
    exit 1
  fi
done

main_target="$here/src/main/java/io/kronosdb/protocol"
test_target="$here/src/test/java/io/kronosdb/protocol"
fixtures_target="$here/protocol/fixtures"

# The codec: replaced wholesale, so a file the generator dropped does not linger.
rm -rf "$main_target"
mkdir -p "$main_target"
cp "$codec"/*.java "$main_target"/

# The conformance suite and its JSON bridge. SchemaHashTest is ours and stays.
mkdir -p "$test_target"
cp "$tests/ConformanceTest.java" "$tests/Json.java" "$tests/ProtocolJson.java" "$test_target"/

mkdir -p "$here/protocol"
cp "$source_root/kronosdb.schema" "$here/protocol/kronosdb.schema"

# Fixtures: the golden frames and their canonical JSON, not the hand-written sources.
rm -rf "$fixtures_target"
mkdir -p "$fixtures_target"
copied=0
for file in "$source_root"/fixtures/*.bin "$source_root"/fixtures/*.json; do
  cp "$file" "$fixtures_target"/
  copied=$((copied + 1))
done

hash="$(sed -n 's/.*SCHEMA_HASH = "\([0-9a-f]*\)".*/\1/p' "$main_target/Frames.java")"
echo "vendored the codec, the schema and $copied fixture files from $source_root"
echo "SCHEMA_HASH is $hash; pin it in src/test/java/io/kronosdb/protocol/SchemaHashTest.java"
