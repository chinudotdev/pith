#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <version>" >&2
  echo "Example: $0 v0.1.4" >&2
  exit 1
fi

version="$1"
if [[ ! "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-.+)?$ ]]; then
  echo "Version must look like v0.1.4 (got: $version)" >&2
  exit 1
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
modules=(protocol gateway loop agent)

for mod in "${modules[@]}"; do
  if [[ ! -f "${root}/${mod}/LICENSE" ]]; then
    echo "Missing ${mod}/LICENSE — pkg.go.dev will hide API docs without it." >&2
    exit 1
  fi
done

for mod in "${modules[@]}"; do
  tag="${mod}/${version}"
  echo "Tagging ${tag}..."
  git tag -a "$tag" -m "Release ${mod} ${version}"
done

echo "Created tags:"
for mod in "${modules[@]}"; do
  echo "  ${mod}/${version}"
done
echo "Push with: git push origin ${modules[0]}/${version} ..."
