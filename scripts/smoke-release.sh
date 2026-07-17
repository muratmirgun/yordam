#!/usr/bin/env bash
set -euo pipefail

archive=${1:?archive path required}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

tar -xzf "$archive" -C "$work"
output=$("$work/yordam" --version)
case "$output" in
  "yordam "*) ;;
  *)
    echo "unexpected version output: $output" >&2
    exit 1
    ;;
esac
