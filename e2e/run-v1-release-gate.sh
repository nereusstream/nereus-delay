#!/usr/bin/env bash
set -euo pipefail

# Stable runbook entry point for the fail-closed ten-gate full-V1 validator.
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "${script_dir}/run-v1-full-release-gate.sh" "$@"
