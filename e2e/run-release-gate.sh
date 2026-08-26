#!/usr/bin/env bash
set -euo pipefail

# Stable runbook entry point for the fail-closed ten-gate full- validator.
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "${script_dir}/run-full-release-gate.sh" "$@"
