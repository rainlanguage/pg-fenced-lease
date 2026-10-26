#!/usr/bin/env bash
# Check that every version of this crate on crates.io was built from the
# matching vX.Y.Z tag in this repository: the commit the package names is
# the tag's commit, and every file in the package is that commit's file.
# Anyone can run it; it needs git, curl, jq and tar.
set -euo pipefail

crate=pg-fenced-lease
agent="github.com/rainlanguage/${crate} verify-published"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

git fetch -q --tags origin 2>/dev/null || true
versions=$(curl -sf -A "$agent" "https://crates.io/api/v1/crates/${crate}/versions" |
  jq -r '.versions[] | select(.yanked | not) | .num')
[ -n "$versions" ] || { echo "no published versions"; exit 0; }

status=0
for version in $versions; do
  dir="$work/$version"
  mkdir -p "$dir/pkg" "$dir/src"
  curl -sfL -A "$agent" -o "$dir/crate.tar.gz" \
    "https://static.crates.io/crates/${crate}/${crate}-${version}.crate"
  tar -xzf "$dir/crate.tar.gz" -C "$dir/pkg"
  pkg="$dir/pkg/${crate}-${version}"
  commit=$(jq -r '.git.sha1' "$pkg/.cargo_vcs_info.json")
  tagged=$(git rev-parse -q --verify "refs/tags/v${version}^{commit}" || true)
  if [ "$commit" != "$tagged" ]; then
    echo "FAIL ${version}: package names ${commit}, tag v${version} is ${tagged:-missing}"
    status=1
    continue
  fi
  git archive "$commit" | tar -x -C "$dir/src"
  bad=0
  while IFS= read -r file; do
    case "$file" in
      .cargo_vcs_info.json | Cargo.toml) continue ;; # written by cargo
      Cargo.toml.orig) other=Cargo.toml ;;
      *) other=$file ;;
    esac
    if ! cmp -s "$pkg/$file" "$dir/src/$other"; then
      echo "FAIL ${version}: ${file} differs from ${commit}"
      bad=1
    fi
  done < <(cd "$pkg" && find . -type f | sed 's|^\./||' | sort)
  if [ "$bad" = 0 ]; then
    echo "ok   ${version}: matches tag v${version} (${commit})"
  else
    status=1
  fi
done
exit "$status"
