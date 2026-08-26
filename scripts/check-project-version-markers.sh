#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

# Reject standalone/path markers plus Java-style numbered design-line prefixes
# and suffixes. External protocol numbers such as Kafka v13, Pulsar v22,
# UUIDv7 and SigV4 do not match these forms.
pattern='(?<![A-Za-z0-9])[vV][12](?![A-Za-z0-9])|[-_/][vV][12](?=[-_/\.\x00]|$)|\b[vV][12][A-Z_]|[A-Za-z0-9_]V[12]\b'
matches="$({
  rg --pcre2 --line-number --hidden "${pattern}" \
    --glob '!.git/**' \
    --glob '!build/**' \
    --glob '!scripts/check-project-version-markers.sh' \
    . || true
})"

# These are externally fixed spellings, not Nereus Delay project versions.
violations="$(printf '%s\n' "${matches}" \
  | grep -Ev 'transaction-v2|/admin/v2|stateDiagram-v2' \
  || true)"

if [[ -n "${violations}" ]]; then
  echo "project-wide numbered version markers are forbidden:" >&2
  printf '%s\n' "${violations}" >&2
  exit 1
fi

filename_violations="$(find . -path './.git' -prune -o -path './build' -prune -o -print \
  | grep -E '(^|[-_/])[vV][12]($|[-_/.])|[vV][12][A-Z_]|[A-Za-z0-9_]V[12]($|[-_/.])' \
  || true)"
if [[ -n "${filename_violations}" ]]; then
  echo "project-wide numbered version markers are forbidden in paths:" >&2
  printf '%s\n' "${filename_violations}" >&2
  exit 1
fi
