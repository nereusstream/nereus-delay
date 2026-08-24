#!/usr/bin/env bash
set -euo pipefail

e2e_root=$(cd "$(dirname "$0")" && pwd)
delay_root=$(cd "$e2e_root/.." && pwd)
artifact_dir=${NEREUS_DELAY_CAPACITY_ARTIFACT_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/nereus-delay-capacity.XXXXXX")}
gradle_user_home=${NEREUS_DELAY_E2E_GRADLE_USER_HOME:-$(mktemp -d "${TMPDIR:-/tmp}/nereus-delay-capacity-gradle.XXXXXX")}

if [[ ! -d "$artifact_dir" ]]; then
    mkdir -p "$artifact_dir"
fi
if [[ -n "$(git -C "$delay_root" status --porcelain)" ]]; then
    echo "capacity probe requires a clean Delay worktree" >&2
    exit 1
fi

delay_source=$(git -C "$delay_root" rev-parse HEAD)
delay_ref=$(git -C "$delay_root" symbolic-ref --short HEAD 2>/dev/null || echo detached)
export NEREUS_DELAY_CAPACITY_ARTIFACT_DIR="$artifact_dir"
export NEREUS_DELAY_CAPACITY_SOURCE_LOCK="$delay_source"

echo "Delay source: $delay_ref@$delay_source"
echo "Artifact directory: $artifact_dir"
echo "Gradle user home: $gradle_user_home"
echo "Docker: not used by this bounded local probe"

NEREUS_DELAY_CAPACITY_ARTIFACT_DIR="$artifact_dir" \
NEREUS_DELAY_CAPACITY_SOURCE_LOCK="$delay_source" \
GRADLE_USER_HOME="$gradle_user_home" \
    "$delay_root/gradlew" test \
        --tests com.nereusstream.delay.store.BoundedCapacitySloProbeTest \
        --rerun-tasks \
        --no-daemon --console=plain

artifact="$artifact_dir/bounded-capacity-slo-probe.json"
if [[ ! -s "$artifact" ]]; then
    echo "bounded capacity probe did not produce an artifact: $artifact" >&2
    exit 1
fi

echo "Bounded capacity/SLO probe artifact: $artifact"
sed -n '1,80p' "$artifact"
