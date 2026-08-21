#!/usr/bin/env bash
set -euo pipefail

e2e_root=$(cd "$(dirname "$0")" && pwd)
delay_root=$(cd "$e2e_root/.." && pwd)
oxia_checkout=${NEREUS_DELAY_OXIA_CHECKOUT:-"$delay_root/../../oxia"}
oxia_port=${NEREUS_DELAY_CREDENTIAL_CHAOS_OXIA_PORT:-$((16660 + ($$ % 100)))}
artifact_dir=${NEREUS_DELAY_CREDENTIAL_CHAOS_ARTIFACT_DIR:-$(mktemp -d -t nereus-delay-credential-binding-chaos.XXXXXX)}
gradle_user_home=${NEREUS_DELAY_CREDENTIAL_CHAOS_GRADLE_USER_HOME:-$artifact_dir/gradle-user-home}
prefix=${NEREUS_DELAY_CREDENTIAL_CHAOS_PREFIX:-nereus-delay-v1-credential-drift/$(date +%s)-$$}
compose_file=$e2e_root/docker-compose.oxia.yml
compose_project="nereus-delay-credential-binding-e2e-$(date +%s)-$$"
compose=(docker compose --project-name "$compose_project" --file "$compose_file")
test_class=io.nereusstream.delay.runtime.CredentialBindingDurableChaosTest
before_log=$artifact_dir/before-process.log
after_log=$artifact_dir/after-process.log
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)

fail() {
    echo "credential binding drift E2E: $*" >&2
    exit 1
}

command -v docker >/dev/null 2>&1 || fail "docker is required"
docker compose version >/dev/null 2>&1 || fail "docker compose is required"
git -C "$oxia_checkout" rev-parse --verify HEAD >/dev/null 2>&1 \
    || fail "Oxia checkout is not a Git repository: $oxia_checkout"
[[ "$artifact_dir" != "/" && "$artifact_dir" != "/private/tmp" ]] \
    || fail "artifact directory is too broad"
if [[ -e "$artifact_dir" && -n "$(find "$artifact_dir" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    fail "artifact directory must be empty: $artifact_dir"
fi
mkdir -p "$artifact_dir" "$gradle_user_home"

cleanup() {
    local exit_code=$?
    if [[ "$exit_code" != 0 ]]; then
        "${compose[@]}" logs oxia >&2 || true
    fi
    "${compose[@]}" down --volumes --remove-orphans --rmi local >/dev/null 2>&1 || true
    exit "$exit_code"
}
trap cleanup EXIT INT TERM

wait_for_oxia() {
    for attempt in $(seq 1 90); do
        if "${compose[@]}" exec --no-TTY oxia oxia health --host 127.0.0.1 --port 6648 --timeout 2s \
                >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    "${compose[@]}" ps >&2 || true
    return 1
}

run_phase() {
    local phase=$1
    local log=$2
    NEREUS_DELAY_OXIA_ENDPOINT="127.0.0.1:$oxia_port" \
    NEREUS_DELAY_CREDENTIAL_CHAOS_ARTIFACT_DIR="$artifact_dir" \
    NEREUS_DELAY_CREDENTIAL_CHAOS_PHASE="$phase" \
    NEREUS_DELAY_CREDENTIAL_CHAOS_PREFIX="$prefix" \
    GRADLE_USER_HOME="$gradle_user_home" \
        "$delay_root/gradlew" test --tests "$test_class" --rerun-tasks --no-daemon --console=plain \
        >"$log" 2>&1
}

export NEREUS_DELAY_OXIA_CHECKOUT="$oxia_checkout"
export NEREUS_DELAY_OXIA_E2E_PORT="$oxia_port"
"${compose[@]}" up --build --detach
wait_for_oxia
run_phase before "$before_log"
run_phase after "$after_log"

before_dump=$artifact_dir/before.json
after_dump=$artifact_dir/after.json
[[ -s "$before_dump" && -s "$after_dump" ]] || fail "durable before/after dumps are missing"
jq -e --slurpfile before "$before_dump" --slurpfile after "$after_dump" \
    --arg prefix "$prefix" '
    ($before | length) == 1 and ($after | length) == 1 and
    $before[0].schema == "nereus-delay-chaos-durable-state-dump-v1" and
    $after[0].schema == "nereus-delay-chaos-durable-state-dump-v1" and
    $before[0].cell == "credential-binding-drift" and $after[0].cell == "credential-binding-drift" and
    $before[0].phase == "BEFORE_FRESH_PROCESS_RECOVERY" and
    $after[0].phase == "RECOVERED_AFTER_FRESH_PROCESS" and
    $before[0].fault == "CREDENTIAL_BINDING_ROTATION" and
    $after[0].fault == "CREDENTIAL_BINDING_ROTATION" and
    $before[0].dump_forced == true and $after[0].dump_forced == true and
    $before[0].durable_oxia_read == true and $after[0].durable_oxia_read == true and
    $before[0].key_prefix == $prefix and $after[0].key_prefix == $prefix and
    ($before[0].process_pid != $after[0].process_pid) and
    $before[0].head_generation == 2 and $after[0].head_generation == 2 and
    $before[0].head_revision == 2 and $after[0].head_revision == 2 and
    $before[0].old_lease_generation == 1 and $after[0].old_lease_generation == 1 and
    $after[0].stale_fingerprint_rejected == true and
    $after[0].fresh_lease_generation == 2 and
    $after[0].fresh_protection_until >= $after[0].old_lease_valid_until' \
    "$before_dump" >/dev/null

delay_sha=$(git -C "$delay_root" rev-parse HEAD)
oxia_sha=$(git -C "$oxia_checkout" rev-parse HEAD)
jq -n \
    --arg started "$started_at" \
    --arg finished "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg artifact "$artifact_dir" \
    --arg prefix "$prefix" \
    --arg delay "$delay_sha" \
    --arg oxia "$oxia_sha" \
    --arg before "$before_dump" \
    --arg after "$after_dump" \
    --arg before_log "$before_log" \
    --arg after_log "$after_log" \
    '{schema:"nereus-delay-v1-credential-binding-drift-e2e-v1",status:"PASS",cell:"credential-binding-drift",
      artifact_dir:$artifact,started_at:$started,finished_at:$finished,key_prefix:$prefix,
      source_locks:{delay:$delay,oxia:$oxia},fresh_process_recovery:true,
      before_dump:$before,after_dump:$after,before_log:$before_log,after_log:$after_log}' \
    >"$artifact_dir/credential-binding-drift-e2e.json"
echo "Credential binding drift E2E passed: real Oxia CAS rotated the Head across a protected old lease, a fresh JVM rejected stale fingerprint material before provider ownership, and activated generation 2."
