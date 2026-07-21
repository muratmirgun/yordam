#!/usr/bin/env bash
set -euo pipefail

archive=${1:?archive path required}
expected_commit=${2:?expected commit required}
archive_name=${archive##*/}

if [[ ! "$archive_name" =~ ^yordam_([^_]+)_(darwin|linux)_(amd64|arm64)\.tar\.gz$ ]]; then
  echo "unexpected archive name: $archive_name" >&2
  exit 1
fi
expected_version=${BASH_REMATCH[1]}
if [[ ! "$expected_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ && ! "$expected_version" =~ ^0\.0\.0-SNAPSHOT-[0-9a-f]+$ ]]; then
  echo "unexpected packaged version: $expected_version" >&2
  exit 1
fi
if [[ ! "$expected_commit" =~ ^[0-9a-f]{40}$ ]]; then
  echo "expected commit must be a full lowercase Git SHA" >&2
  exit 1
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

tar -xzf "$archive" -C "$work"
output=$("$work/yordam" --version)
if [[ ! "$output" =~ ^yordam\ ([^[:space:]]+)\ \(([0-9a-f]{40}),\ ([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z)\)$ ]]; then
  echo "unexpected version output: $output" >&2
  exit 1
fi
packaged_date=${BASH_REMATCH[3]}
expected_output="yordam $expected_version ($expected_commit, $packaged_date)"
if ! [[ "$output" == "$expected_output" ]]; then
  echo "packaged version/commit mismatch: got $output, want $expected_output" >&2
  exit 1
fi
