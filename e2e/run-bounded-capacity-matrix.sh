#!/usr/bin/env bash
set -eo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
delay_dir=$(cd "$script_dir/.." && pwd)
artifact_dir=$NEREUS_DELAY_CAPACITY_MATRIX_ARTIFACT_DIR
gradle_dir=$NEREUS_DELAY_CAPACITY_MATRIX_GRADLE_USER_HOME
image=$NEREUS_DELAY_CAPACITY_MATRIX_IMAGE
project_prefix=$NEREUS_DELAY_CAPACITY_MATRIX_PROJECT
pull_image=$NEREUS_DELAY_CAPACITY_MATRIX_PULL_IMAGE
if [[ -z "$artifact_dir" ]]; then artifact_dir=$(mktemp -d -t nereus-delay-capacity-matrix.XXXXXX); fi
if [[ -z "$gradle_dir" ]]; then gradle_dir="$artifact_dir/gradle-user-home"; fi
if [[ -z "$image" ]]; then image=eclipse-temurin@sha256:57865c22b954cf920cb05a610af81d577e89783282514ba071e99c7357f6c769; fi
if [[ -z "$project_prefix" ]]; then project_prefix=nereus-delay-capacity-matrix-$(date +%s)-$$; fi
if [[ -z "$pull_image" ]]; then pull_image=0; fi
set -u

if [[ -n "$(git -C "$delay_dir" status --porcelain)" ]]; then
    echo "capacity matrix requires a clean Delay worktree" >&2
    exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
    echo "capacity matrix requires jq to build the canonical JSON index" >&2
    exit 1
fi
if [[ ! "$project_prefix" =~ ^[a-z0-9][a-z0-9_.-]*$ ]]; then
    echo "capacity matrix project prefix contains unsupported characters: $project_prefix" >&2
    exit 1
fi

mkdir -p "$artifact_dir" "$gradle_dir"
if [[ -n "$(find "$artifact_dir" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "capacity matrix artifact directory must be empty: $artifact_dir" >&2
    exit 1
fi

image_id=""
remove_image_after=0
active_project=""
cleanup() {
    set +e
    if [[ -n "$active_project" ]]; then
        docker rm -f "$active_project" >/dev/null 2>&1 || true
    fi
    if [[ "$remove_image_after" == 1 && -n "$image_id" ]]; then
        docker image rm "$image_id" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT INT TERM

if ! docker image inspect "$image" >/dev/null 2>&1; then
    if [[ "$pull_image" != 1 ]]; then
        echo "required pinned container image is not present: $image" >&2
        echo "set NEREUS_DELAY_CAPACITY_MATRIX_PULL_IMAGE=1 to pull this exact digest" >&2
        exit 1
    fi
    docker pull "$image"
    remove_image_after=1
fi
image_id=$(docker image inspect "$image" --format '{{.Id}}')
delay_source=$(git -C "$delay_dir" rev-parse HEAD)
delay_ref=$(git -C "$delay_dir" symbolic-ref --short HEAD 2>/dev/null || echo detached)
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)

echo "Delay source: $delay_ref@$delay_source"
echo "Container image: $image ($image_id)"
echo "Artifact directory: $artifact_dir"
echo "Gradle user home: $gradle_dir"
echo "Cases: smoke=16/24, burst=256/128, sustained=1024/512"

for case_name in smoke burst sustained; do
    if [[ "$case_name" == smoke ]]; then
        payload_records=16
        slo_samples=24
    elif [[ "$case_name" == burst ]]; then
        payload_records=256
        slo_samples=128
    else
        payload_records=1024
        slo_samples=512
    fi
    case_dir="$artifact_dir/$case_name"
    active_project="$project_prefix-$case_name"
    mkdir -p "$case_dir"
    echo "===== CASE $case_name payload_records=$payload_records slo_samples=$slo_samples ====="
    docker run --rm --name "$active_project" \
        --memory=2g --memory-swap=2g --cpus=2 \
        --ulimit nofile=65536:65536 \
        --tmpfs /tmp:rw,exec,size=4g \
        -e GRADLE_USER_HOME=/gradle \
        -e JAVA_TOOL_OPTIONS=-XX:MaxDirectMemorySize=256m \
        -e NEREUS_DELAY_CAPACITY_ARTIFACT_DIR=/artifacts \
        -e NEREUS_DELAY_CAPACITY_SOURCE_LOCK="$delay_source" \
        -e NEREUS_DELAY_CAPACITY_PAYLOAD_RECORDS="$payload_records" \
        -e NEREUS_DELAY_CAPACITY_SLO_SAMPLES="$slo_samples" \
        -v "$delay_dir:/workspace" \
        -v "$gradle_dir:/gradle" \
        -v "$case_dir:/artifacts" \
        "$image" \
        bash -lc '
            cd /workspace
            if ./gradlew test --tests com.nereusstream.delay.store.BoundedCapacitySloProbeTest --rerun-tasks --no-daemon --console=plain > /tmp/nereus-delay-capacity-gradle.log 2>&1; then
                cat /tmp/nereus-delay-capacity-gradle.log
            else
                status=$?
                cat /tmp/nereus-delay-capacity-gradle.log
                if ! grep -q "Permission denied" /tmp/nereus-delay-capacity-gradle.log; then
                    exit "$status"
                fi
                find /gradle/caches/modules-2/files-2.1 -type f \( -name "*.exe" -o -name "*linux*" \) -exec chmod u+x {} +
                ./gradlew test --tests com.nereusstream.delay.store.BoundedCapacitySloProbeTest --rerun-tasks --no-daemon --console=plain
            fi
        '
    active_project=""
    artifact="$case_dir/bounded-capacity-slo-probe.json"
    if [[ ! -s "$artifact" ]]; then
        echo "case did not produce an artifact: $artifact" >&2
        exit 1
    fi
    jq -e --arg source "$delay_source" --argjson records "$payload_records" --argjson samples "$slo_samples" \
        '.status == "PARTIAL" and .source_lock == $source and .configuration.payload_records_per_size == $records and .configuration.slo_samples == $samples and .store.reopen_verified == true and .slo.collector_reopen_verified == true' \
        "$artifact" >/dev/null
done

matrix_artifact="$artifact_dir/capacity-benchmark-matrix.json"
matrix_tmp="$artifact_dir/.capacity-benchmark-matrix.json.tmp"
jq -n \
    --arg schema "nereus-delay-bounded-capacity-benchmark-matrix-v1" \
    --arg status "PARTIAL" \
    --arg source "$delay_source" \
    --arg image "$image" \
    --arg image_id "$image_id" \
    --arg started_at "$started_at" \
    --slurpfile smoke "$artifact_dir/smoke/bounded-capacity-slo-probe.json" \
    --slurpfile burst "$artifact_dir/burst/bounded-capacity-slo-probe.json" \
    --slurpfile sustained "$artifact_dir/sustained/bounded-capacity-slo-probe.json" \
    '{
      schema: $schema,
      status: $status,
      matrix_status: "PASS_BOUNDED",
      source_lock: $source,
      container_image: $image,
      container_image_id: $image_id,
      started_at: $started_at,
      cases: [
        {name: "smoke", artifact: $smoke[0]},
        {name: "burst", artifact: $burst[0]},
        {name: "sustained", artifact: $sustained[0]}
      ],
      boundaries: [
        "This is a bounded Linux local Store/SLO campaign artifact, not a V1 release certification.",
        "It covers three source-locked payload-record/SLO-sample configurations, synchronous RocksDB writes, payload readback, durable SLO merge, and persistent reopen.",
        "It does not certify Broker throughput, Lane distributions, multi-shard placement, compaction/restore throughput, inline-object flow, long-cycle soak, or external credential/provider authority."
      ]
    }' > "$matrix_tmp"
mv "$matrix_tmp" "$matrix_artifact"
jq -e '(.matrix_status == "PASS_BOUNDED") and (.cases | length == 3)' "$matrix_artifact" >/dev/null
echo "Bounded capacity/benchmark matrix artifact: $matrix_artifact"
echo "matrix_status=PASS_BOUNDED"
if [[ "$remove_image_after" == 1 ]]; then
    echo "related temporary image cleanup: image_id=$image_id removed_after_run=true"
else
    echo "related temporary image cleanup: image_id=$image_id removed_after_run=false"
fi
