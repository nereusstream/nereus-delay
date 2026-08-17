#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
artifact_dir="${NEREUS_DELAY_CAPACITY_CONTAINER_ARTIFACT_DIR:-$(mktemp -d -t nereus-delay-capacity-container.XXXXXX)}"
gradle_dir="${NEREUS_DELAY_CAPACITY_CONTAINER_GRADLE_USER_HOME:-${artifact_dir}/gradle-user-home}"
image="${NEREUS_DELAY_CAPACITY_CONTAINER_IMAGE:-eclipse-temurin@sha256:57865c22b954cf920cb05a610af81d577e89783282514ba071e99c7357f6c769}"
project="${NEREUS_DELAY_CAPACITY_CONTAINER_PROJECT:-nereus-delay-capacity-container-$(date +%s)-$$}"

mkdir -p "${artifact_dir}" "${gradle_dir}"
if [[ -n "$(git -C "${delay_dir}" status --porcelain)" ]]; then
    echo "container capacity probe requires a clean Delay worktree" >&2
    exit 1
fi
if ! docker image inspect "${image}" >/dev/null 2>&1; then
    echo "required pinned container image is not present: ${image}" >&2
    echo "pull it explicitly before running this source-locked probe" >&2
    exit 1
fi

delay_source=$(git -C "${delay_dir}" rev-parse HEAD)
delay_ref=$(git -C "${delay_dir}" symbolic-ref --short HEAD 2>/dev/null || echo detached)
echo "Delay source: ${delay_ref}@${delay_source}"
echo "Container image: ${image}"
echo "Artifact directory: ${artifact_dir}"
echo "Gradle user home: ${gradle_dir}"
echo "Container limits: memory=2g, cpus=2, nofile=65536, tmpfs=/tmp:4g exec"

docker run --rm --name "${project}" \
    --memory=2g --memory-swap=2g --cpus=2 \
    --ulimit nofile=65536:65536 \
    --tmpfs /tmp:rw,exec,size=4g \
    -e GRADLE_USER_HOME=/gradle \
    -e JAVA_TOOL_OPTIONS=-XX:MaxDirectMemorySize=256m \
    -e NEREUS_DELAY_CAPACITY_ARTIFACT_DIR=/artifacts \
    -e NEREUS_DELAY_CAPACITY_SOURCE_LOCK="${delay_source}" \
    -v "${delay_dir}:/workspace" \
    -v "${gradle_dir}:/gradle" \
    -v "${artifact_dir}:/artifacts" \
    "${image}" \
    bash -lc 'cd /workspace && ./gradlew test --tests io.nereusstream.delay.store.BoundedCapacitySloProbeTest --rerun-tasks --no-daemon --console=plain'

artifact="${artifact_dir}/bounded-capacity-slo-probe.json"
if [[ ! -s "${artifact}" ]]; then
    echo "bounded capacity container probe did not produce an artifact: ${artifact}" >&2
    exit 1
fi
if command -v jq >/dev/null 2>&1; then
    jq empty "${artifact}"
fi
echo "Bounded capacity/SLO container artifact: ${artifact}"
