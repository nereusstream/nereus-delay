#!/usr/bin/env bash
set -euo pipefail

e2e_root=$(cd "$(dirname "$0")" && pwd)
delay_root=$(cd "$e2e_root/.." && pwd)
kafka_checkout=${NEREUS_DELAY_KAFKA_CHECKOUT:-"$delay_root/../../kafka-worktrees/nereus-delay-k1"}
pulsar_checkout=${NEREUS_DELAY_PULSAR_CHECKOUT:-"$delay_root/../../pulsar-worktrees/nereus-delay-p1"}
oxia_checkout=${NEREUS_DELAY_OXIA_CHECKOUT:-"$delay_root/../../oxia"}

kafka_branch="nereus/delay-guarded-producer-v1"
kafka_base="c300006a7705c240642db6950b5a95fec982bfc5"
kafka_head="05849884ca81fad767fda058444d1e17c7f9cbf9"
pulsar_branch="nereus/delay-resource-guard-v1"
pulsar_base="8dae0236c0a0d405ed7f8303081080520fe91551"
pulsar_head="0a2536484cd3932801a98dc88ff112b2df88a1c7"
oxia_head="37a17bef17202d5fd6e23282da5fd26d94865484"
delay_worker_head="7a839678"

fail() {
    echo "cross-repo contract audit failed: $*" >&2
    exit 1
}

require_git_checkout() {
    local repo=$1
    git -C "$repo" rev-parse --verify HEAD >/dev/null 2>&1 \
        || fail "not a Git checkout: $repo"
}

require_clean() {
    local repo=$1
    [[ -z "$(git -C "$repo" status --porcelain)" ]] \
        || fail "worktree is not clean: $repo"
}

require_branch() {
    local repo=$1
    local expected=$2
    local actual
    actual=$(git -C "$repo" symbolic-ref --quiet --short HEAD) \
        || fail "detached HEAD in $repo"
    [[ "$actual" == "$expected" ]] \
        || fail "$repo is on $actual, expected $expected"
}

require_head() {
    local repo=$1
    local expected=$2
    local actual
    actual=$(git -C "$repo" rev-parse HEAD)
    [[ "$actual" == "$expected" ]] \
        || fail "$repo is at $actual, expected $expected"
}

require_ancestor() {
    local repo=$1
    local base=$2
    git -C "$repo" merge-base --is-ancestor "$base" HEAD \
        || fail "$repo HEAD is not descended from $base"
}

require_file_text() {
    local file=$1
    local text=$2
    [[ -f "$file" ]] || fail "missing file: $file"
    rg -F --quiet -- "$text" "$file" \
        || fail "missing '$text' in $file"
}

for repo in "$delay_root" "$kafka_checkout" "$pulsar_checkout" "$oxia_checkout"; do
    require_git_checkout "$repo"
    require_clean "$repo"
done

require_branch "$delay_root" "nereus/delay-full-implementation-v1"
require_ancestor "$delay_root" "$delay_worker_head"
require_branch "$kafka_checkout" "$kafka_branch"
require_head "$kafka_checkout" "$kafka_head"
require_ancestor "$kafka_checkout" "$kafka_base"
require_branch "$pulsar_checkout" "$pulsar_branch"
require_head "$pulsar_checkout" "$pulsar_head"
require_ancestor "$pulsar_checkout" "$pulsar_base"
require_head "$oxia_checkout" "$oxia_head"

require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "trunk@c300006a7705c240642db6950b5a95fec982bfc5"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "5.0.0-M1@8dae0236c0a0d405ed7f8303081080520fe91551"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "412441c47cce4e61d3cc015b95c7d3cffcab2f7f"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "72d4accf"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "c72cac90"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "a7fd5fa7dd35d5d8535d3c63e577208d29fc2c5"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "5dbd0874"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "4865ba4f"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "e7495086"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "d413869b"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "54c58557"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "10e21cbf0e6f741f10b353c56a316a0b57b71b9d"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "nereus/delay-full-implementation-v1@2dd2cfff83f4d029972cf7fbeb569fbf4538c026"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "nereus/delay-full-implementation-v1@a73faf3e836ada67931f709d46214dde7caf3ad0"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "nereus/delay-full-implementation-v1@bf858b089b927fcf65129214d8ed5a7fc5300deb"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "7e94d0f8a3e374832a111dbd2f741be5f20795d5"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "7e94d0f8a3e374832a111dbd2f741be5f20795d5"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "nereus-delay-kafka-e2e-1786787846-2966"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "nereus-delay-kafka-e2e-1786788428-10652"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "nereus-delay-kafka-e2e-1786788428-10652"
require_file_text "$delay_root/e2e/README.md" \
    "NEREUS_DELAY_KAFKA_ROUTE_FAILOVER_ONLY=1"
require_file_text "$delay_root/e2e/README.md" \
    "nereus-delay-kafka-e2e-1786788428-10652"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "202368d46fedfe12ae414edaa9c3db32cc8e5073"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "358ce4a103"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "PulsarClientArtifactRecoverySourcePositioner"

require_file_text "$delay_root/src/main/java/io/nereusstream/delay/protocol/KafkaBrokerResourceIdentityV1.java" \
    "nativeTopicUuid"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/protocol/PulsarBrokerResourceIdentityV1.java" \
    "resourceIncarnation"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/transport/ProductionKafkaProduceTransport.java" \
    "acks != -1 || !idempotenceEnabled || !autoTopicCreationDisabled"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/transport/ProductionPulsarSendTransport.java" \
    "!batchingDisabled || !chunkingDisabled || !autoTopicCreationDisabled"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/RsaSha256GatewayJwtVerifier.java" \
    "SHA256withRSA"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/RsaSha256GatewayJwtVerifier.java" \
    "x5t#S256"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/OxiaGatewayAdmissionController.java" \
    "IfVersionIdEquals"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/OxiaGatewayAdmissionController.java" \
    "containsExact"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaRealGatewayGrpcSmokeTest.java" \
    "GatewayGrpcServer.mutualTls"
require_file_text "$delay_root/e2e/run-gateway-real-e2e.sh" \
    "OxiaRealGatewayGrpcSmokeTest"
require_file_text "$delay_root/e2e/run-gateway-real-e2e.sh" \
    "NEREUS_DELAY_GATEWAY_CA_CERT"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaRealGatewayGrpcSmokeTest.java" \
    "afterRestart"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "232ce29d"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaRealGatewayGrpcSmokeTest.java" \
    "concurrentDuplicateRequestsAcrossTwoGatewayServersUseOneDurableAttempt"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "1213650b"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaRealGatewayGrpcSmokeTest.java" \
    "rotatedGatewayCertificatesRejectOldClientAndReuseDurableOutcome"
require_file_text "$delay_root/e2e/run-gateway-real-e2e.sh" \
    "NEREUS_DELAY_GATEWAY_ROTATED_SERVER_CERT"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "cbe895e1"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Gateway certificate replacement audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Gateway certificate replacement and channel revalidation"
require_file_text "$delay_root/e2e/README.md" \
    "Gateway certificate replacement and channel revalidation"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/SessionBoundOxiaGatewayRecordClient.java" \
    "before and after every operation"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/OxiaGatewaySessionUnavailableException.java" \
    "durable operation is fenced"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/OxiaSyncOwnerLeaseBackend.java" \
    "assertConnectedSession"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaRealGatewayGrpcSmokeTest.java" \
    "gatewayDurableRecordsRecoverAfterOxiaSessionChurn"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaRealGatewayGrpcSmokeTest.java" \
    "assertThrows(OxiaGatewaySessionUnavailableException.class"
require_file_text "$delay_root/e2e/run-gateway-real-e2e.sh" \
    "NEREUS_DELAY_GATEWAY_OXIA_SESSION_CHURN"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "241068fd"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Gateway durable admission/idempotency session-churn audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Gateway durable admission/idempotency recovery after Oxia session churn"
require_file_text "$delay_root/e2e/README.md" \
    "NEREUS_DELAY_GATEWAY_OXIA_SESSION_CHURN=1"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaRealGatewayGrpcSmokeTest.java" \
    "gatewayRecoversAcrossRealOxiaDataServerFailover"
require_file_text "$delay_root/e2e/docker-compose.oxia-cluster.yml" \
    "raft-bootstrap-nodes"
require_file_text "$delay_root/e2e/run-oxia-multi-node-gateway-e2e.sh" \
    "wait_for_namespace_leader"
require_file_text "$delay_root/e2e/run-oxia-multi-node-gateway-e2e.sh" \
    "NEREUS_DELAY_GATEWAY_MULTI_NODE_FAILOVER_GATE"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "43493a709e4041e94c7f4f270a25b2725534ab59"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Gateway multi-node Oxia DataServer failover audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Gateway recovery across a real multi-node Oxia DataServer leader stop"
require_file_text "$delay_root/e2e/README.md" \
    "run-oxia-multi-node-gateway-e2e.sh"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaGatewayIdempotencyStoreTest.java" \
    "attemptCasResponseLossConvergesToUncertainAfterDeadlineWithoutPermit"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/OxiaGatewayIdempotencyStore.java" \
    "recoverExpiredStartedAttempt"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Gateway STARTED CAS response-loss recovery"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "7adb95f0"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Gateway STARTED CAS response-loss recovery audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Gateway STARTED CAS response-loss recovery"
require_file_text "$delay_root/e2e/README.md" \
    "Gateway STARTED CAS response-loss recovery"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaRealGatewayGrpcSmokeTest.java" \
    "gatewayRecoversAfterCommittedOxiaAttemptResponseLoss"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaRealGatewayGrpcSmokeTest.java" \
    "Gateway Oxia STARTED response-loss E2E passed"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "real Oxia Gateway STARTED CAS response-loss receipt"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "1ce8b7e604ca969adabd7372e80ce04f96e5b45a"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "nereus-delay-gateway-e2e-1786827281-47103"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "real Oxia Gateway STARTED CAS response-loss receipt audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "real Oxia Gateway STARTED CAS response-loss receipt"
require_file_text "$delay_root/e2e/README.md" \
    "Gateway STARTED CAS response-loss recovery against real Oxia"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaRealGatewayGrpcSmokeTest.java" \
    "gatewayRecoversAfterCommittedOxiaRetryAttemptResponseLoss"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaRealGatewayGrpcSmokeTest.java" \
    "Gateway Oxia RETRY_UNCERTAIN response-loss E2E passed"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/GatewayIdempotencyRecordV1.java" \
    "GatewayIdempotencyPhaseV1.ACTIVE, next, null"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "real Oxia Gateway RETRY_UNCERTAIN response-loss receipt"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "bcac733ae7e48776ce7d427d66643d21a6dd2a7d"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "nereus-delay-gateway-e2e-1786828250-57299"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "real Oxia Gateway RETRY_UNCERTAIN response-loss receipt audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "real Oxia Gateway RETRY_UNCERTAIN response-loss receipt"
require_file_text "$delay_root/e2e/README.md" \
    "Gateway RETRY_UNCERTAIN response-loss recovery against real Oxia"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/GatewayAdmissionRecordV1.java" \
    "nereus-delay-gateway-admission-record-v1"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaGatewayAdmissionControllerTest.java" \
    "responseLossIsAcceptedOnlyAfterExactRereadForReserveAndRelease"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaRealGatewayAdmissionSmokeTest.java" \
    "admissionPoolsAndExpiryWorkAgainstRealService"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/route/OxiaRouteAuthoritySession.java" \
    "reconnectSession"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/route/OxiaSignedRouteSnapshotProviderTest.java" \
    "explicitSessionReconnectRotatesMarkerAfterFenceAndRestoresReads"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/semantic/VerifiedNativePreparationSnapshotCache.java" \
    "NativePreparationEligibilityV1"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/semantic/NativeCapabilitySnapshotIssuer.java" \
    "protectNativeCapability"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/semantic/NativeCapabilityIssuanceAuthority.java" \
    "resolveGuard"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/client/DelayClient.java" \
    "prepareScheduleSubmissionV1"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/protocol/ActivationBarrierV1.java" \
    "toSourceBarrier"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/runtime/DelayShard.java" \
    "resolveClaimMaterializationV1"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/protocol/CanonicalLaneTupleV1.java" \
    "public static Projection project"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/WorkerCommandRuntime.java" \
    "materialization derived from the accepted"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/PublishAdmissionWorkClassExecutor.java" \
    "deriveDescriptor"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/WorkerShardRuntime.java" \
    "pollAndSubmitClaim"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/RouteSourceAssignmentFactory.java" \
    "fromRoute"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/RouteSourceAssignmentResolver.java" \
    "active"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSourceFactory.java" \
    "ACTIVE_FOR_COMMANDS"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSourceFactory.java" \
    "consumer.seek"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactRecoverySourceCursor.java" \
    "never commits a group offset"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactRecoverySourceCursor.java" \
    "OwnerRecoveryCoordinator"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSourceFactory.java" \
    "resourceGuardAttestation"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSourceFactory.java" \
    "WorkerShardRuntime"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactRecoverySourceCursor.java" \
    "PulsarClientArtifactRecoverySourceCursor"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactRecoverySourceCursor.java" \
    "requireProof"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactRecoverySourcePositioner.java" \
    "seekAfter"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactRecoverySourcePositioner.java" \
    "awaitStableProof"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactSourceRecordConsumer.java" \
    "GuardedConsumer<byte[]>"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactSourceRecordConsumer.java" \
    "connectionGeneration()"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactSourceRecordDecoder.java" \
    "SourceReplayMutation"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactShardLogMutationAppender.java" \
    "sendAsync"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactShardLogMutationAppender.java" \
    "AppendOutcome.unknown"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactMutationSmoke.java" \
    "Pulsar Shard Log mutation append/replay/ACK smoke passed"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactSourceRecordConsumer.java" \
    "KafkaClientArtifactSourceRecordConsumer"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactSourceRecordConsumer.java" \
    "consumer.commitSync"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactSourceConsumerFactory.java" \
    "bindResourceGuard"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/protocol/SystemMutationIdentityV1.java" \
    "RESOURCE_DELETE_CONFIRMED"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/protocol/SystemMutation.java" \
    "public static SystemMutation decodeFrame"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactSourceRecordDecoder.java" \
    "SourceReplayMutation"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactShardLogMutationAppender.java" \
    "sendGuarded"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactShardLogMutationAppender.java" \
    "AppendOutcome.unknown"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactMutationSmoke.java" \
    "Kafka Shard Log mutation append/replay/ACK smoke passed"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactFetchEvidence.java" \
    "requireBatch"
require_file_text "$kafka_checkout/clients/src/main/java/org/apache/kafka/clients/consumer/GuardedConsumer.java" \
    "pollGuarded"
require_file_text "$kafka_checkout/clients/src/main/java/org/apache/kafka/clients/consumer/GuardedFetchEvidence.java" \
    "fetchResponseBodySha256"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactSourceSmoke.java" \
    "KafkaClientArtifactRecoverySourceCursor"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactFetchResponseLossSmoke.java" \
    "Kafka source Fetch response-loss smoke passed"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactRetentionFloorSmoke.java" \
    "Kafka source retention-floor smoke passed"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactProcessCrashRecoverySmoke.java" \
    "Kafka source process-crash recovery smoke passed"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Kafka source Fetch response-loss receipt"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "nereus-delay-kafka-e2e-1786879840-36136"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Kafka source retention-floor receipt"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "nereus-delay-kafka-e2e-1786880647-45643"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Kafka source process-crash recovery receipt"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Kafka source Fetch response-loss audit"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Kafka source retention-floor audit"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Kafka source process-crash recovery audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Kafka source Fetch response-loss receipt"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Kafka source retention-floor receipt"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Kafka source process-crash recovery receipt"
require_file_text "$delay_root/e2e/README.md" \
    "Kafka source Fetch response-loss receipt"
require_file_text "$delay_root/e2e/README.md" \
    "Kafka source retention-floor receipt"
require_file_text "$delay_root/e2e/README.md" \
    "Kafka source process-crash recovery receipt"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSmoke.java" \
    "Kafka Worker vertical smoke passed"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSmoke.java" \
    "Kafka Worker authority smoke passed"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSmoke.java" \
    "CheckpointFileInventory.collect"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSmoke.java" \
    "and final checkpoint"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSmoke.java" \
    "Kafka Worker restart preparation passed"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSmoke.java" \
    "runSourceAppliedPhysicalPublish"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSmoke.java" \
    "readBackSourcePosition"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSmoke.java" \
    "Kafka Worker source-applied physical publish passed"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/WorkerShardRuntime.java" \
    "sameSourcePosition"
require_file_text "$delay_root/build.gradle" \
    "kafkaWorkerDestinationTopic"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "worker_destination_topic"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "source-applied physical publish with typed KAFKA_TRANSACTIONAL_RECEIPT Outcome"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "112522e6"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Kafka Worker source-applied physical publish passed"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Admission source offset=3"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "nereus-delay-kafka-e2e-1786812109-79794"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "21092,21093,21094"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/WorkerShardRuntime.java" \
    "bindActiveOwnerPublishGraph"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSmoke.java" \
    "runDueClaimPublishPhysicalTurn"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "e5cae7b8e7d9988cc6dca516212d011d49fea5fa"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "nereus-delay-kafka-e2e-1786814042-841"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "21492,21493,21494"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactRouteWorkerSmoke.java" \
    "Kafka signed Route -> guarded Fetch barrier"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactRouteWorkerSmoke.java" \
    "RocksDB apply/checkpoint"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactRouteWorkerSmoke.java" \
    "lastStableOffset"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactRouteWorkerSmoke.java" \
    "accepted-route broker failover"
require_file_text "$delay_root/build.gradle" \
    "runRealKafkaRouteWorkerSmoke"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "run_route_worker_smoke"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "NEREUS_DELAY_KAFKA_ROUTE_FAILOVER_ONLY"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "NEREUS_DELAY_KAFKA_K2_FAILOVER"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "NEREUS_DELAY_KAFKA_K2_FAILOVER_ONLY"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "NEREUS_DELAY_KAFKA_K2_RESPONSE_LOSS"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "NEREUS_DELAY_KAFKA_K2_RESPONSE_LOSS_ONLY"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactTransactionalDestinationTransport.java" \
    "NEREUS_DELAY_KAFKA_K2_COMMIT_GATE"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactTransactionalSmoke.java" \
    "K2 broker failover commit returned PUBLISHED"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactTransactionalSmoke.java" \
    "K2 broker failover commit resolved after UNKNOWN"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactTransactionalSmoke.java" \
    "NEREUS_DELAY_KAFKA_K2_COMMITTED_RESPONSE_LOSS"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactTransactionalSmoke.java" \
    "K2 committed response-loss smoke passed"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/adapter/KafkaTransactionalPublishEvidence.java" \
    "KAFKA_TRANSACTIONAL_RECEIPT"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactTransactionalReceiptEvidenceProvider.java" \
    "lastStableOffset"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactTransactionalReceiptEvidenceProvider.java" \
    "read_committed"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactTransactionalDestinationTransport.java" \
    "typed.requireBusinessMutation"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactTransactionalSmoke.java" \
    "typed KAFKA_TRANSACTIONAL_RECEIPT evidence"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "6912b940"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "3c7128eb6caecc50f3d6f4865ed2cdfa2838ad8a"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "nereus-delay-kafka-e2e-1786806083-13395"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "typed KAFKA_TRANSACTIONAL_RECEIPT evidence"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSmoke.java" \
    "NEREUS_DELAY_KAFKA_WORKER_DESTINATION_RESPONSE_LOSS"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSmoke.java" \
    "destinationResponseLossProducer"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSmoke.java" \
    "Kafka Worker destination response-loss smoke passed"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "NEREUS_DELAY_KAFKA_WORKER_DESTINATION_RESPONSE_LOSS_ONLY"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "Kafka Worker destination response-loss E2E passed"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "e95d1c0cbaf4b94c8523d6fd9994b6487102f400"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "nereus-delay-kafka-e2e-1786831579-93599"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Kafka Worker source-applied destination response-loss receipt audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Kafka Worker destination response-loss receipt"
require_file_text "$delay_root/e2e/README.md" \
    "NEREUS_DELAY_KAFKA_WORKER_DESTINATION_RESPONSE_LOSS=1"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSmoke.java" \
    "NEREUS_DELAY_KAFKA_SOURCE_ACK_RESPONSE_LOSS"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSmoke.java" \
    "sourceAckResponseLossConsumer"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSmoke.java" \
    "Kafka Worker source ACK response-loss smoke passed"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "NEREUS_DELAY_KAFKA_SOURCE_ACK_RESPONSE_LOSS_ONLY"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "Kafka Worker source ACK response-loss E2E passed"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "d165e73e457834be55af58d238980be65c2054c7"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "nereus-delay-kafka-e2e-1786832218-928"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Kafka Worker source ACK response-loss receipt audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Kafka Worker source ACK response-loss receipt"
require_file_text "$delay_root/e2e/README.md" \
    "NEREUS_DELAY_KAFKA_SOURCE_ACK_RESPONSE_LOSS=1"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "nereus-delay-kafka-e2e-1786790805-40581"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "127.0.0.1:19795 (id: 1)"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "nereus-delay-kafka-e2e-1786790805-40581"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "127.0.0.1:19797 (id: 3)"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "6912b940"
require_file_text "$delay_root/e2e/README.md" \
    "K2 broker failover commit returned PUBLISHED"
require_file_text "$delay_root/e2e/README.md" \
    "NEREUS_DELAY_KAFKA_K2_FAILOVER_ONLY=1"
require_file_text "$delay_root/e2e/README.md" \
    "NEREUS_DELAY_KAFKA_K2_RESPONSE_LOSS_ONLY=1"
require_file_text "$delay_root/e2e/README.md" \
    "K2 committed response-loss smoke passed"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "376252bae0faf6f2d5120e223886b3af8a54e636"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "nereus-delay-kafka-e2e-1786828912-64477"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "K2 committed response-loss E2E passed"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "typed KAFKA_TRANSACTIONAL_RECEIPT"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactRouteWorkerSmoke.java" \
    "Pulsar signed Route -> guarded SUBSCRIBE barrier"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactRouteWorkerSmoke.java" \
    "RocksDB apply/checkpoint"
require_file_text "$delay_root/build.gradle" \
    "runRealPulsarRouteWorkerSmoke"
require_file_text "$delay_root/e2e/run-pulsar-real-client-e2e.sh" \
    "run_route_worker_smoke"
require_file_text "$pulsar_checkout/pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/v2/PersistentTopics.java" \
    "resourceGuard"
require_file_text "$pulsar_checkout/pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/impl/PersistentTopicsBase.java" \
    "internalUpdateTopicResourceGuardPropertiesAsync"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/OxiaSyncOwnerLeaseBackend.java" \
    "establishSessionMarker"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/OxiaSyncOwnerLeaseBackend.java" \
    "connectUnchecked"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/SourceAssignment.java" \
    "canonicalBytes"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/WorkerAssignment.java" \
    "capacityEnvelopeDigest"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/OxiaSyncWorkerAssignmentBackend.java" \
    "nereus-delay-oxia-worker-assignment-record-v1"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/WorkerAssignmentCoordinator.java" \
    "requireAccepted"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/RouteWorkerAssignmentCoordinator.java" \
    "snapshotDigest"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/WorkerSchedulingRuntime.java" \
    "openForActiveOwner"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/WorkerShardRuntime.java" \
    "runSchedulingTurn"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/WorkerCommandRuntime.java" \
    "submitPublish"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/WorkerShardRuntime.java" \
    "runCommandTurn"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/scheduler/PersistentLaneScheduler.java" \
    "forActiveOwner"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/WorkerCheckpointRuntime.java" \
    "claimDue"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/CheckpointAtomicPublicationAuthority.java" \
    "publishUploadedCheckpointAtomically"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/OxiaSyncCheckpointPublicationBackend.java" \
    "checkpoint-publication-v1"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/OxiaRealCheckpointPublicationSmokeTest.java" \
    "workerCheckpointRuntimePublishesAtomicIntentAndCatalogAgainstRealService"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/OxiaRealCheckpointPublicationSmokeTest.java" \
    "checkpoint Owner Lease/session"
require_file_text "$delay_root/e2e/run-oxia-real-service.sh" \
    "OxiaRealCheckpointPublicationSmokeTest"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "nereus-delay-v1-oxia-e2e-1786787138-90186"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "ac72e43803806b9c309b62150c0aa54b43f8a3ea"
require_file_text "$delay_root/e2e/README.md" \
    "BUILD SUCCESSFUL in 11s"
require_file_text "$delay_root/build.gradle" \
    "runRealKafkaWorkerSmoke"
require_file_text "$delay_root/build.gradle" \
    "runRealKafkaFetchResponseLossSmoke"
require_file_text "$delay_root/build.gradle" \
    "runRealKafkaRetentionFloorSmoke"
require_file_text "$delay_root/build.gradle" \
    "runRealKafkaProcessCrashRecoverySmoke"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "runRealKafkaWorkerSmoke"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "NEREUS_DELAY_KAFKA_FETCH_RESPONSE_LOSS_ONLY"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "NEREUS_DELAY_KAFKA_RETENTION_FLOOR_ONLY"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "NEREUS_DELAY_KAFKA_PROCESS_CRASH_ONLY"
require_file_text "$delay_root/e2e/docker-compose.kafka.yml" \
    "KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS"
require_file_text "$delay_root/e2e/kafka-k1-entrypoint.sh" \
    "log.retention.check.interval.ms"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "bootstrap_survivors"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "restart_worker_topic"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "runRealKafkaMutationSmoke"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "NEREUS_DELAY_KAFKA_WITH_OXIA"
require_file_text "$delay_root/build.gradle" \
    "kafkaWorkerMode"
require_file_text "$delay_root/build.gradle" \
    "runRealKafkaMutationSmoke"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactSourceSmoke.java" \
    "PulsarClientArtifactRecoverySourceCursor"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSmoke.java" \
    "Pulsar Worker vertical smoke passed"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSmoke.java" \
    "prepare"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSmoke.java" \
    "Pulsar Worker assignment publication/acceptance passed"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSmoke.java" \
    "CheckpointFileInventory.collect"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSmoke.java" \
    "and final checkpoint"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSmoke.java" \
    "Kafka Worker assignment publication/acceptance passed"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/route/OxiaRealRouteWorkerAssignmentSmokeTest.java" \
    "signedRoutePublicationFeedsSessionBoundWorkerAssignmentAuthority"
require_file_text "$delay_root/e2e/run-oxia-real-service.sh" \
    "OxiaRealRouteWorkerAssignmentSmokeTest"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/route/OxiaRealRouteAuthoritySmokeTest.java" \
    "signedRouteProviderRecoversAfterRealOxiaRestart"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/route/OxiaRealRouteAuthoritySmokeTest.java" \
    "signedRouteNotificationsRecoverAfterRealOxiaRestart"
require_file_text "$delay_root/e2e/run-oxia-real-service.sh" \
    "NEREUS_DELAY_OXIA_ROUTE_RESTART"
require_file_text "$delay_root/e2e/run-oxia-real-service.sh" \
    "NEREUS_DELAY_OXIA_ROUTE_RESTART_NOTIFICATIONS"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "6a64ca894928a9a6f210129e2567b02f7df1329f"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Oxia Route notification restart recovery passed"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Route notification stream recovery after Oxia session rotation"
require_file_text "$delay_root/e2e/README.md" \
    "NEREUS_DELAY_OXIA_ROUTE_RESTART_NOTIFICATIONS=1"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "nereus-delay-v1-oxia-e2e-1786789198-22565"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "nereus-delay-v1-oxia-e2e-1786789198-22565"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "164597c39f1da6fc403c5283494b1f0c6b132802"
require_file_text "$delay_root/e2e/README.md" \
    "NEREUS_DELAY_OXIA_ROUTE_RESTART_ONLY=1"
require_file_text "$delay_root/build.gradle" \
    "runRealPulsarWorkerSmoke"
require_file_text "$delay_root/build.gradle" \
    "runRealPulsarMutationSmoke"
require_file_text "$delay_root/e2e/run-pulsar-real-client-e2e.sh" \
    "runRealPulsarWorkerSmoke"
require_file_text "$delay_root/e2e/run-pulsar-real-client-e2e.sh" \
    "runRealPulsarMutationSmoke"
require_file_text "$delay_root/e2e/run-pulsar-real-client-e2e.sh" \
    "restart_topic"
require_file_text "$delay_root/e2e/run-pulsar-real-client-e2e.sh" \
    "NEREUS_DELAY_PULSAR_WITH_OXIA"
require_file_text "$delay_root/build.gradle" \
    "pulsarWorkerMode"
require_file_text "$delay_root/build.gradle" \
    "pulsarWithOxia"

require_file_text "$delay_root/src/main/java/io/nereusstream/delay/adapter/PulsarSendAckEvidence.java" \
    "PULSAR_SEND_ACK"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactDestinationTransport.java" \
    "PulsarSendAckEvidence.published"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactDestinationSmoke.java" \
    "Pulsar destination typed-evidence smoke passed"
require_file_text "$delay_root/build.gradle" \
    "runRealPulsarDestinationSmoke"
require_file_text "$delay_root/e2e/run-pulsar-real-client-e2e.sh" \
    "destination_topic"
require_file_text "$delay_root/e2e/run-pulsar-real-client-e2e.sh" \
    "NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS_ONLY"
require_file_text "$delay_root/e2e/run-pulsar-real-client-e2e.sh" \
    "pulsar-p1-cluster-entrypoint.sh"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactDestinationTransport.java" \
    "PublishEvidenceProvider"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactDestinationTransport.java" \
    "PULSAR_SEND_ACK"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactDestinationSmoke.java" \
    "Pulsar committed response-loss smoke passed"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "12334f63"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Pulsar committed response-loss smoke passed"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Pulsar destination committed SEND response-loss receipt"
require_file_text "$delay_root/e2e/README.md" \
    "NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS=1"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "nereus-delay-pulsar-e2e-1786829967-75545"
require_file_text "$delay_root/e2e/run-pulsar-real-client-e2e.sh" \
    "NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS_ONLY"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSmoke.java" \
    "ACK_UNKNOWN"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSmoke.java" \
    "Pulsar Worker source ACK response-loss smoke passed"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "31145cc8"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Pulsar Worker source ACK response-loss smoke passed"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Pulsar Worker source ACK response-loss receipt"
require_file_text "$delay_root/e2e/README.md" \
    "NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS=1"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "nereus-delay-pulsar-e2e-1786830626-82754"
require_file_text "$delay_root/e2e/run-pulsar-real-client-e2e.sh" \
    "NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS_ONLY"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSmoke.java" \
    "resolveDestinationResponseLoss"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSmoke.java" \
    "Pulsar Worker destination response-loss smoke passed"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "c903fe34"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Pulsar Worker destination response-loss smoke passed"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Pulsar Worker source-applied destination response-loss receipt"
require_file_text "$delay_root/e2e/README.md" \
    "NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS=1"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "nereus-delay-pulsar-e2e-1786830983-86815"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "4f2297e1dc593f8b5e16f7733e6ed1109544cb4a"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Pulsar destination typed-evidence smoke passed"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "ledger=11, entry=0, batchIndex=0, sequence=0"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSmoke.java" \
    "runSourceAppliedPhysicalPublish"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSmoke.java" \
    "bindActiveOwnerPublishGraph"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSmoke.java" \
    "runDueClaimPublishPhysicalTurn"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSmoke.java" \
    "Pulsar Worker source-applied physical publish passed"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactLargePayloadGatewaySmoke.java" \
    "Pulsar + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/WorkerShardRuntime.java" \
    "runSourceBoundPhysicalPublish"
require_file_text "$delay_root/build.gradle" \
    "pulsarWorkerDestinationTopic"
require_file_text "$delay_root/build.gradle" \
    "runRealPulsarLargePayloadGatewaySmoke"
require_file_text "$delay_root/e2e/run-pulsar-real-client-e2e.sh" \
    "worker_destination_topic"
require_file_text "$delay_root/e2e/run-pulsar-real-client-e2e.sh" \
    "source-applied physical publish with typed Outcome"
require_file_text "$delay_root/e2e/run-pulsar-large-payload-gateway-e2e.sh" \
    "Pulsar + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed"
require_file_text "$delay_root/e2e/docker-compose.pulsar-large-payload-infra.yml" \
    "NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Pulsar Large-payload Gateway-to-destination authority receipt"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "nereus-delay-pulsar-large-e2e-1786879186-27914"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Pulsar Large-payload Gateway-to-destination authority audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Pulsar Large-payload Gateway-to-destination authority receipt"
require_file_text "$delay_root/e2e/README.md" \
    "Pulsar large-payload Gateway-to-destination authority E2E receipt"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "cb309d82"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Admission source ledger=22/3"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Admission source ledger=33/2"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "3c6e605a33cea2de85fce473af740b5e05fcf74e"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "nereus-delay-pulsar-e2e-1786814719-7983"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "21515,21516"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSmoke.java" \
    "real Oxia session-bound"
require_file_text "$delay_root/e2e/run-pulsar-real-client-e2e.sh" \
    "NEREUS_DELAY_PULSAR_WITH_OXIA"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "37a17bef17202d5fd6e23282da5fd26d94865484"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "nereus-delay-pulsar-e2e-1786815185-13398"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Admission source ledger=35/2"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSmoke.java" \
    "NEREUS_DELAY_PULSAR_LISTENER_NAME"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSmoke.java" \
    "followRedirects(HttpClient.Redirect.NORMAL)"
require_file_text "$delay_root/e2e/run-pulsar-multi-broker-failover-e2e.sh" \
    "NEREUS_DELAY_PULSAR_LISTENER_NAME=external"
require_file_text "$delay_root/e2e/run-pulsar-multi-broker-failover-e2e.sh" \
    "Pulsar multi-Broker failover E2E passed"
require_file_text "$delay_root/e2e/docker-compose.pulsar-cluster.yml" \
    "PULSAR_ADVERTISED_LISTENERS"
require_file_text "$delay_root/e2e/docker-compose.pulsar-cluster.yml" \
    "pulsar-cluster"
require_file_text "$delay_root/e2e/pulsar-p1-cluster-entrypoint.sh" \
    "PULSAR_ADVERTISED_LISTENERS"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "nereus-delay-pulsar-multi-e2e-1786819171-58253"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Admission source ledger=3/3"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Pulsar multi-Broker failover E2E passed"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "sha256:819a2a34b91d34468ac6caa048ec5cbf959fb9ecb40dbfd649a9fabf067318de"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "21985,21986,21987,21988"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Pulsar Worker source-applied physical publish passed: Admission source ledger=3/3"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "nereus-delay-kafka-e2e-1786815566-17636"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Kafka Worker source-applied physical publish passed: Admission source offset=3, typed KAFKA_TRANSACTIONAL_RECEIPT receipt offset=2"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "21792,21793,21794"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Kafka Worker authority smoke passed: real Oxia session-bound lease"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "37a17bef17202d5fd6e23282da5fd26d94865484"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "nereus-delay-kafka-e2e-1786815918-21809"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Kafka accepted-route broker failover E2E passed"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "fetch=v18, lso=1, routeRevision=1"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "NEREUS_DELAY_KAFKA_ROUTE_FAILOVER_ONLY"

require_file_text "$kafka_checkout/clients/src/main/java/org/apache/kafka/clients/producer/GuardedProducer.java" \
    "sendGuarded"
require_file_text "$kafka_checkout/clients/src/main/java/org/apache/kafka/clients/producer/GuardedTransactionalProducer.java" \
    "sendGuardedInTransaction"
require_file_text "$kafka_checkout/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java" \
    "transactionManager.maybeAddPartition"
require_file_text "$kafka_checkout/clients/src/main/java/org/apache/kafka/clients/producer/ProducerResourceGuard.java" \
    "expectedTopicId"
require_file_text "$kafka_checkout/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java" \
    ".setTopicId(topicId)"
require_file_text "$kafka_checkout/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java" \
    "Errors.UNKNOWN_TOPIC_ID"
require_file_text "$kafka_checkout/clients/src/main/java/org/apache/kafka/clients/producer/GuardedResponseEvidence.java" \
    "logAppendTimeMs < -1"

require_file_text "$pulsar_checkout/pulsar-client-api/src/main/java/org/apache/pulsar/client/api/TopicResourceGuard.java" \
    "RESOURCE_INCARNATION_BYTES = 32"
require_file_text "$pulsar_checkout/pulsar-client-api/src/main/java/org/apache/pulsar/client/api/GuardedMessageId.java" \
    "TopicResourceGuard resourceGuard()"
require_file_text "$pulsar_checkout/pulsar-common/src/main/proto/PulsarApi.proto" \
    "ResourceIncarnationMismatch = 26"
require_file_text "$pulsar_checkout/pulsar-common/src/main/proto/PulsarApi.proto" \
    "optional TopicResourceGuard resource_guard = 14"
require_file_text "$pulsar_checkout/pulsar-common/src/main/proto/PulsarApi.proto" \
    "optional TopicResourceGuard resource_guard = 20"
require_file_text "$pulsar_checkout/pulsar-common/src/main/proto/PulsarApi.proto" \
    "optional uint64 connection_generation = 4"
require_file_text "$pulsar_checkout/pulsar-client-api/src/main/java/org/apache/pulsar/client/api/GuardedConsumer.java" \
    "Optional<TopicResourceGuardAttestation> resourceGuardAttestation()"
require_file_text "$pulsar_checkout/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConsumerImpl.java" \
    "guardedSourceConnectionGeneration"
require_file_text "$pulsar_checkout/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConsumerImpl.java" \
    "small receiver queue can continue past a seek boundary"
require_file_text "$pulsar_checkout/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/ServerCnx.java" \
    "guardedSourceConnectionGeneration"
require_file_text "$pulsar_checkout/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/Producer.java" \
    "validateResourceGuardNow()"
require_file_text "$pulsar_checkout/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ProducerImpl.java" \
    "recoverResourceIncarnationMismatch"

require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapter.java" \
    "x-amz-content-sha256"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapter.java" \
    "verifyAfterAmbiguousPut"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapter.java" \
    "objectBytesLimit"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapter.java" \
    "credentialGate.requireBeforeProviderCall"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/ObjectStoreCredentialUseLeaseGate.java" \
    "requireBeforeProviderCall"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/runtime/OxiaSyncProfileCatalogBackend.java" \
    "issueCredentialUseLease"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/runtime/OxiaSyncProfileCatalogBackend.java" \
    "IfVersionIdEquals"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/runtime/CredentialAttestationTrustSet.java" \
    "semanticDigest"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/runtime/CredentialAttestationTrustSet.java" \
    "verifySignature"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/runtime/CredentialProfileAuthority.java" \
    "issueCredentialUseLease"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/OxiaObjectStoreCredentialLeaseActivator.java" \
    "activateS3Compatible"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/OxiaObjectStoreCredentialLeaseActivator.java" \
    "resolvedCredentialFingerprintDigest"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/RenewableS3CompatibleCheckpointObjectStoreAdapter.java" \
    "renewIfNeeded"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/RenewableS3CompatibleCheckpointObjectStoreAdapter.java" \
    "requires adapter quiescence"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/ObjectStoreCredentialUseLeaseGate.java" \
    "renewed Object Store credential lease moves expiry backwards"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/VerifiedCredentialMaterialCache.java" \
    "CacheKey.from"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/VerifiedCredentialMaterialCache.java" \
    "attestationTrustSet.verify"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapterTest.java" \
    "uploadsAndRestoresAfterManifestResponseLossWithBoundedSigV4Requests"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapterTest.java" \
    "rejectsImmutableObjectConflictAfterIfNoneMatchPrecondition"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/ObjectStoreCredentialUseLeaseGateTest.java" \
    "rejectsExpiredLeaseBeforeProviderCall"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/ObjectStoreCredentialUseLeaseGateTest.java" \
    "rejectsLoadedCredentialFingerprintDriftAtConstruction"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/runtime/OxiaSyncProfileCatalogBackendTest.java" \
    "responseLossIsAcceptedOnlyAfterExactReread"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/runtime/OxiaSyncProfileCatalogBackendTest.java" \
    "rejectsHeadCasDriftAndProfileSemanticCollision"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/runtime/CredentialAttestationTrustSetTest.java" \
    "verifiesExactVerifierIdentitySignatureAndWindow"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/runtime/CredentialAttestationTrustSetTest.java" \
    "rejectsUnknownVerifierAndOutOfWindowAttestation"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/runtime/OxiaRealProfileCatalogSmokeTest.java" \
    "profileHeadProtectionLeaseAndRotationReopenAgainstRealService"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/runtime/OxiaObjectStoreCredentialLeaseActivatorTest.java" \
    "rejectsResolverFingerprintDriftBeforeLeaseIssuance"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/runtime/OxiaObjectStoreCredentialLeaseActivatorTest.java" \
    "rejectsAuthorityLeaseThatIsNotProtectedByTheRereadProjection"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/RenewableS3CompatibleCheckpointObjectStoreAdapterTest.java" \
    "renewsOnlyInsideWindowAndReplacesTheLocalGate"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/RenewableS3CompatibleCheckpointObjectStoreAdapterTest.java" \
    "refusesToRenewAcrossAHeadRotationBeforeProviderIo"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/VerifiedCredentialMaterialCacheTest.java" \
    "resolvesOnlyTheExactVerifiedBindingAndSupportsRemoval"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/VerifiedCredentialMaterialCacheTest.java" \
    "rejectsUntrustedOrFingerprintDriftAndKeepsPreviousSnapshotOnFailedReplace"
require_file_text "$delay_root/e2e/run-oxia-real-service.sh" \
    "OxiaRealProfileCatalogSmokeTest"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "e01d3ee8708a53487747b0ef721d1f0d107ff677"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "078c66ce141a17a3e757aabb88bae5140d1d297a"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Object Store credential-use lease gate slice"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Oxia credential Profile CAS authority slice"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "37d8efb49876e8eb95b9d214f0ad9ec1afe48595"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Object Store adapter activation binding slice"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "138c1c0e5e0e9af9c3b8e93b223da5b3e322a6bb"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Credential attestation trust-set verification slice"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "f758d010b4d75f9c53d1f6e2cf01d573d655fd1c"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "S3-compatible checkpoint Object Store adapter audit"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Object Store credential-use lease gate audit"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Oxia credential Profile CAS authority audit"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Object Store adapter activation binding audit"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Credential attestation trust-set verification audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "S3-compatible checkpoint Object Store adapter implementation note"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Object Store credential-use lease gate implementation note"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Oxia credential Profile Head/Protection CAS implementation note"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Object Store authority-to-adapter activation implementation note"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Credential attestation trust-set implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "S3-compatible checkpoint adapter focused receipt"
require_file_text "$delay_root/e2e/README.md" \
    "Object Store credential-use lease gate focused receipt"
require_file_text "$delay_root/e2e/README.md" \
    "Oxia credential Profile Head/Protection/lease authority receipt"
require_file_text "$delay_root/e2e/README.md" \
    "Object Store authority-to-adapter activation focused receipt"
require_file_text "$delay_root/e2e/README.md" \
    "Credential attestation trust-set focused receipt"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Same-generation Object Store lease renewal slice"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "8307d690351af1699a6a9cb69e2cfe9bfe26a4a2"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Same-generation Object Store lease renewal audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Same-generation Object Store lease renewal implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Same-generation Object Store lease renewal focused receipt"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Verified credential material cache slice"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "d9b713a9159a8b2672a2b0aea5bd5243ca798c3e"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Verified credential material cache audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Verified credential material cache implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Verified credential material cache focused receipt"
require_file_text "$delay_root/e2e/run-minio-real-e2e.sh" \
    "S3CompatibleMinioRealSmokeTest"
require_file_text "$delay_root/e2e/run-minio-real-e2e.sh" \
    "--rerun-tasks"
require_file_text "$delay_root/e2e/run-minio-real-e2e.sh" \
    "sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/S3CompatibleMinioRealSmokeTest.java" \
    "immutableCheckpointUploadsIdempotentlyAndRestoresAgainstMinio"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "MinIO S3-compatible checkpoint provider smoke"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "tests=1 skipped=0 failures=0 errors=0"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Delay MinIO provider smoke slice"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "87b44d77344e564b46d9c5515472a581cad733ba"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "fecfd1cf7283a007efb7c8618bb8ae1f6f468bd8"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "660a3d0c4d909dd02e412f0153dd9e701c27bbdd"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "MinIO S3-compatible checkpoint provider implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "MinIO S3-compatible checkpoint real-service receipt"
require_file_text "$delay_root/e2e/README.md" \
    "Dockerized MinIO S3-compatible checkpoint smoke passed"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapter.java" \
    "responseVersionOrFallback"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapterTest.java" \
    "rejectsProviderThatOmitsExactVersionHeaders"
require_file_text "$delay_root/e2e/run-minio-real-e2e.sh" \
    "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Exact Object Store provider-version boundary"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Delay exact provider-version slice"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Exact Object Store provider-version implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Exact provider-version MinIO receipt"
require_file_text "$delay_root/e2e/README.md" \
    "780f1e1f-c7da-4dc1-ae4e-a7b9be4f801c"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapter.java" \
    "decodeProviderVersion"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapterTest.java" \
    "versionId="
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Catalog-bound manifest version readback"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Delay exact manifest-version readback slice"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Catalog-bound manifest version readback implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Catalog-bound manifest version readback receipt"
require_file_text "$delay_root/e2e/README.md" \
    "ac201fe8-ba70-4bcb-a49c-a75a6657be55"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapter.java" \
    "implements CheckpointUploadAdapter, CheckpointDownloadAdapter, CheckpointDeleteAdapter"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapter.java" \
    "private DeleteOperation deleteObject"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapter.java" \
    "aggregateDeleteRequestIds"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/CheckpointDeleteResult.java" \
    "externalEvidence"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapterTest.java" \
    "deletesEveryCheckpointObjectByExactProviderVersion"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapterTest.java" \
    "rejectsDeleteThatOmitsExactProviderVersionResponse"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/S3CompatibleMinioRealSmokeTest.java" \
    "CheckpointDeleteResult deleted"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Exact checkpoint object-set deletion slice"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "3bfe030a"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Delay exact checkpoint object-set deletion slice"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Exact checkpoint object-set deletion audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Exact checkpoint object-set deletion implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Exact checkpoint object-set deletion receipt"
require_file_text "$delay_root/e2e/README.md" \
    "e223584d-2863-45a1-8471-9b378c0899c5"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapter.java" \
    "aggregateProbeRequestIds"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapter.java" \
    "RemoteObjectObservation"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapterTest.java" \
    "deleteRetryConvergesAfterPartialResponseLossAndReportsAlreadyAbsent"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Checkpoint delete retry-convergence slice"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "220fc98a"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Delay checkpoint delete retry-convergence slice"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Checkpoint delete retry-convergence audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Checkpoint delete retry-convergence implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Checkpoint delete retry-convergence receipt"
require_file_text "$delay_root/e2e/README.md" \
    "1a81631c-3bd9-41e6-a132-8abe1da7ea2e"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/CheckpointPrefixSweepAdapter.java" \
    "CheckpointPrefixSweepResult sweep"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/CheckpointPrefixSweepRequest.java" \
    "MAX_SINGLE_PAGE_VERSIONS"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/CheckpointPrefixSweepResult.java" \
    "emptyAfterSweep"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapter.java" \
    "CheckpointPrefixSweepAdapter"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapter.java" \
    "private VersionList listVersions"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapter.java" \
    "parseVersionList"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapterTest.java" \
    "sweepsExactCheckpointPrefixVersionsAndProvesEmptyAfterward"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/S3CompatibleMinioRealSmokeTest.java" \
    "CheckpointReapingSweepResult reaping"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Bounded checkpoint prefix sweep provider seam"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "c32a98f328400c71346b98188930a6efa80da7c9"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "e0402eef46026c2ee91e4fe59337bb0e40cac723"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Delay checkpoint prefix sweep slice"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Checkpoint prefix sweep audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Bounded checkpoint prefix sweep implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Checkpoint prefix sweep receipt"
require_file_text "$delay_root/e2e/README.md" \
    "f905db1e-1a7e-455c-bb32-5fa90bb7ed1f"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/CheckpointReapingSweepCoordinator.java" \
    "quiescence.reapingEvidence(), catalog"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/CheckpointReapingSweepResult.java" \
    "requires a REAPING intent"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/CheckpointReapingSweepCoordinatorTest.java" \
    "responseLossLeavesReapingStateAndRetryUsesTheSamePrefix"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/CheckpointReapingSweepCoordinatorTest.java" \
    "catalogProtectionPreventsProviderSweep"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/S3CompatibleMinioRealSmokeTest.java" \
    "CheckpointReapingSweepResult reaping"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "REAPING-to-prefix sweep coordination slice"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "b9fcd2aa846329ed13986b122d287375a441b2fd"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "83bf17cea70b37fa42a507832693a0c43ed4d9fb"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Delay checkpoint REAPING sweep coordination slice"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Checkpoint REAPING sweep coordination audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "REAPING-to-prefix sweep coordination implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Checkpoint REAPING sweep coordination receipt"
require_file_text "$delay_root/e2e/README.md" \
    "f5404da4-4944-4581-a75d-80dccdad92c3"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/CheckpointReapingQuiescenceProof.java" \
    "maximumProviderOwnershipLifetimeMs"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/CheckpointReapingQuiescenceProof.java" \
    "requestQuiescenceHorizonMs < minimumHorizon"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/CheckpointReapingQuiescenceGuard.java" \
    "PROVIDER_OWNERSHIP_NOT_CLOSED"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/CheckpointReapingQuiescenceGuard.java" \
    "public static void require"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/CheckpointReapingSweepCoordinatorTest.java" \
    "providerOwnershipHorizonBlocksSweepAfterTheRequestWindow"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Checkpoint REAPING quiescence proof gate"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "7b8b73885c5ec26dfc96c1b5b8a1a6ab8ec0d1d9"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "065a233a48f07ee561e78d4d35fa35f82b8af0da"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Delay checkpoint REAPING quiescence proof slice"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Checkpoint REAPING quiescence proof audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "REAPING quiescence proof implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Checkpoint REAPING quiescence proof receipt"
require_file_text "$delay_root/e2e/README.md" \
    "9c4dcab9-c03c-4860-81de-07e62302d30e"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/CheckpointReapingOwnerProof.java" \
    "EXACT_OWNER_EXPLICIT_ABANDON"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/CheckpointReapingOwnerProof.java" \
    "recordedLease"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/CheckpointReapingOwnerProofGuard.java" \
    "OWNER_PROOF_DEADLINE_NOT_CLOSED"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/CheckpointReapingOwnerProofIssuer.java" \
    "explicitOwnerAbandon"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/CheckpointReapingSweepCoordinator.java" \
    "CheckpointReapingOwnerProof ownerProof"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/CheckpointReapingOwnerProofIssuerTest.java" \
    "anotherActorCanProveTheRecordedLeaseWasReplaced"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/CheckpointReapingSweepCoordinatorTest.java" \
    "quiescenceReceiptMustBindTheExactOwnerProof"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Checkpoint REAPING Owner proof gate"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "44cd3230709f5e87742cd94cd9a8b7bce314a184"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Delay checkpoint REAPING Owner proof slice"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Checkpoint REAPING Owner proof audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "REAPING Owner proof implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Checkpoint REAPING Owner proof receipt"
require_file_text "$delay_root/e2e/README.md" \
    "ea89d80e-e63e-4980-b225-94b070d3c36b"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/ObjectStoreProviderOwnershipTracker.java" \
    "maximumProviderOwnershipLifetimeMs"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/ObjectStoreProviderOwnershipTracker.java" \
    "beginQuiescence"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/ObjectStoreProviderOwnershipTracker.java" \
    "locallyQuiescent"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapter.java" \
    "beginProviderQuiescence"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapter.java" \
    "requireCredentialGate();"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/RenewableS3CompatibleCheckpointObjectStoreAdapter.java" \
    "requireProviderAdmission"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/ObjectStoreProviderOwnershipTrackerTest.java" \
    "uncertainCloseRetainsTheConfiguredProviderHorizon"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/S3CompatibleCheckpointObjectStoreAdapterTest.java" \
    "providerQuiescenceFenceStopsNewOperationsBeforeHttp"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Object Store provider-owned request horizon ledger"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "cc97c7654cb19f88c69045cd3c33a4d970a9fed3"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Delay Object Store provider ownership horizon slice"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Checkpoint provider-owned request horizon audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Object Store provider-owned request horizon implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Checkpoint provider-owned request horizon ledger receipt"
require_file_text "$delay_root/e2e/README.md" \
    "1b904a10-2104-46eb-a6fd-0bd2afe24524"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/CheckpointDeleteConfirmationComposer.java" \
    "does not authorize"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/CheckpointDeleteConfirmationComposer.java" \
    "confirmedAt.requireEarliestAtLeast(observedAt.latestEpochMs())"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/CheckpointDeleteConfirmationComposerTest.java" \
    "composesSignedDeletedConfirmationFromExactProviderReceipt"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Checkpoint delete-confirmation mutation composer"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "70e5f0da"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Delay checkpoint delete-confirmation composition slice"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Checkpoint delete-confirmed source mutation composition audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Checkpoint delete-confirmation mutation composition implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Checkpoint delete-confirmation mutation composition receipt"
require_file_text "$delay_root/e2e/README.md" \
    "io.nereusstream.delay.store.CheckpointDeleteConfirmationComposerTest"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/protocol/ResourceDeleteConfirmedBody.java" \
    "this.confirmedAt.requireEarliestAtLeast(this.evidence.observedAt().latestEpochMs())"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/runtime/ResourceDeleteConfirmedRecord.java" \
    "confirmedEvidence.requireEarliestAtLeast(observedEvidence.latestEpochMs())"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/protocol/ResourceDeleteConfirmedBodyTest.java" \
    "confirmationIntervalMustFollowTheCompleteObservationInterval"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/runtime/ResourceGcGuardTest.java" \
    "durableDeleteConfirmationRequiresConfirmationAfterObservation"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Delete-confirmation temporal evidence fence"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "a26c6816"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Delay delete-confirmation temporal evidence fence slice"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Delete-confirmed temporal evidence audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Delete-confirmation temporal evidence fence implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Delete-confirmation temporal evidence fence receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/GcWorkClassExecutor.java" \
    "submitDeleteConfirmation"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/GcWorkClassExecutor.java" \
    "requireStrictlyAfter"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/ownership/GcWorkClassExecutorTest.java" \
    "typedDeleteConfirmationHandoffRequiresReturnedSourceAfterRetire"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Source-ordered GC confirmation handoff"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "b225cef9"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Delay source-ordered GC confirmation handoff slice"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Source-ordered GC confirmation handoff audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Source-ordered GC confirmation handoff implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Source-ordered GC confirmation handoff receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/OxiaSessionBoundRecoveryPinStore.java" \
    "PutOption.AsEphemeralRecord"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/OxiaSessionBoundRecoveryPinStore.java" \
    "RecoveryPin create/release requires an identity-bearing connected Oxia session"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/OxiaSessionBoundRecoveryPinStore.java" \
    "requireCatalogGeneration(requested, currentCatalogGeneration)"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/OxiaSessionBoundRecoveryPinStore.java" \
    "DeleteOption.IfVersionIdEquals"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/OxiaSyncCheckpointPublicationBackend.java" \
    "new OxiaSessionBoundRecoveryPinStore(this.client, canonicalPrefix + PIN_SUFFIX)"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/OxiaSyncRecoveryCatalogBackendTest.java" \
    "recoveryPinUsesAnEphemeralSingletonCasAndExactRereadRelease"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/OxiaSyncRecoveryCatalogBackendTest.java" \
    "recoveryPinRequiresAnIdentityBearingCallerSession"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/OxiaRealRecoveryAuthoritySmokeTest.java" \
    "recoveryPinIsSessionBoundAndExpiresWithTheRealOxiaSession"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/OxiaSyncCheckpointPublicationBackendTest.java" \
    "recoveryPinUsesASeparateEphemeralRecordAlongsideAtomicPublication"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/OxiaRealCheckpointPublicationSmokeTest.java" \
    "recoveryPinIsSessionBoundAndExpiresWithTheRealPublicationSession"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Oxia session-bound Recovery Pin CAS"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "dedd03a94fb2ab1e8d12f19ba993408646426578"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Oxia Recovery Pin session-bound CAS audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Oxia Recovery Pin session-bound CAS implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Oxia Recovery Pin session-bound CAS receipt"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Atomic publication Recovery Pin CAS"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "04976375"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Atomic publication Recovery Pin CAS audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Atomic publication Recovery Pin CAS implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Atomic publication Recovery Pin CAS receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/OxiaSyncControlOperationBackend.java" \
    "handle.backend()::assertConnectedSession"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/OxiaSyncControlOperationBackend.java" \
    "successful CAS whose response is lost after the marker disappears"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/ownership/OxiaSyncControlOperationBackendTest.java" \
    "sessionFenceRejectsACommittedWriteAfterTheMarkerChanges"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/ownership/OxiaRealControlAuthoritySmokeTest.java" \
    "new OxiaSyncControlOperationBackend(client, prefix + \"/operation\")"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Oxia Control Operation session-bound CAS"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "cc8001b528bb9943a2f683c6ad14728c426cb8f2"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Oxia Control Operation session-bound CAS audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Oxia Control Operation session-bound CAS implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Oxia Control Operation session-bound CAS receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/OxiaSyncControlTargetRegistrationBackend.java" \
    "handle.backend()::assertConnectedSession"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/OxiaSyncControlTargetRegistrationBackend.java" \
    "successful registration whose response is lost after the marker"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/ownership/OxiaSyncControlTargetRegistrationBackendTest.java" \
    "sessionFenceRejectsACommittedRegistrationAfterTheMarkerChanges"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/ownership/OxiaRealControlAuthoritySmokeTest.java" \
    "new OxiaSyncControlTargetRegistrationBackend(client, prefix + \"/target\")"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Oxia Control Target Registration session-bound CAS"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "50435a1364d2e8f7d823cc05faa18e4766f5cbd6"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Oxia Control Target Registration session-bound CAS audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Oxia Control Target Registration session-bound CAS implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Oxia Control Target Registration session-bound CAS receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/runtime/OxiaSyncProfileCatalogBackend.java" \
    "handle.backend()::assertConnectedSession"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/runtime/OxiaSyncProfileCatalogBackend.java" \
    "profile mutation whose response is lost after the marker disappears"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/runtime/OxiaSyncProfileCatalogBackendTest.java" \
    "sessionFenceRejectsACommittedPublicationAfterTheMarkerChanges"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/runtime/OxiaRealProfileCatalogSmokeTest.java" \
    "client, prefix + \"/catalog\", 5_000, 10_000"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Oxia credential Profile catalog session-bound CAS"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "89020c97c29f99d98f7f3259ab7b27131644adcd"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Oxia credential Profile catalog session-bound CAS audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Oxia credential Profile catalog session-bound CAS implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Oxia credential Profile catalog session-bound CAS receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/OxiaSyncRecoveryCatalogBackend.java" \
    "handle.backend()::assertConnectedSession"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/OxiaSyncRecoveryCatalogBackend.java" \
    "catalog CAS whose response is lost after the marker"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/OxiaSyncRecoveryCatalogBackendTest.java" \
    "sessionFenceRejectsACommittedCatalogPublicationAfterTheMarkerChanges"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/OxiaRealRecoveryAuthoritySmokeTest.java" \
    "client, prefix + \"/catalog\", LIMITS);"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Oxia Recovery Catalog session-bound CAS"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "f04f58d15588662b71be68809e1a11a627baf540"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Oxia Recovery Catalog session-bound CAS audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Oxia Recovery Catalog session-bound CAS implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Oxia Recovery Catalog session-bound CAS receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/OxiaSyncCheckpointPublicationBackend.java" \
    "handle.backend()::assertConnectedSession"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/OxiaSyncCheckpointPublicationBackend.java" \
    "publication CAS whose response is lost after the marker"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/OxiaSyncCheckpointPublicationBackendTest.java" \
    "sessionFenceRejectsACommittedPublicationAfterTheMarkerChanges"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/OxiaRealCheckpointPublicationSmokeTest.java" \
    "new OxiaSyncCheckpointPublicationBackend(client, prefix + \"/publication\", LIMITS)"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Oxia Checkpoint Publication session-bound CAS"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "ffe0e5e15894ba377248068258444a1484bfb7f2"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Oxia Checkpoint Publication session-bound CAS audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Oxia Checkpoint Publication session-bound CAS implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Oxia Checkpoint Publication session-bound CAS receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/OxiaSyncCheckpointUploadIntentBackend.java" \
    "handle.backend()::assertConnectedSession"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/OxiaSyncCheckpointUploadIntentBackend.java" \
    "committed CAS whose response is lost after the marker"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/OxiaSyncCheckpointUploadIntentBackendTest.java" \
    "sessionFenceRejectsACommittedIntentAfterTheMarkerChanges"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/OxiaRealRecoveryAuthoritySmokeTest.java" \
    "new OxiaSyncCheckpointUploadIntentBackend(client, prefix + \"/intent\")"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Oxia Checkpoint Upload Intent session-bound CAS"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "0a1e6020"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Oxia Checkpoint Upload Intent session-bound CAS audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Oxia Checkpoint Upload Intent session-bound CAS implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Oxia Checkpoint Upload Intent session-bound CAS receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/OxiaSyncWorkerAssignmentBackend.java" \
    "Worker-assignment record"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/OxiaSyncWorkerAssignmentBackend.java" \
    "assignment CAS whose response is lost after the"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/ownership/OxiaSyncWorkerAssignmentBackendTest.java" \
    "sessionFenceRejectsACommittedAssignmentAfterTheMarkerChanges"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/route/OxiaRealRouteWorkerAssignmentSmokeTest.java" \
    "new OxiaSyncWorkerAssignmentBackend(assignmentHandle"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Oxia Worker assignment session-bound CAS"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "cca59a92df395c11cfdda23d24bb27a8b5269cca"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Oxia Worker assignment session-bound CAS audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Oxia Worker assignment session-bound CAS implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Oxia Worker assignment session-bound CAS receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/OxiaSyncOwnerLeaseBackend.java" \
    "owner epoch or lease"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/OxiaSyncOwnerLeaseBackend.java" \
    "committed ephemeral lease whose response is lost"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/ownership/OxiaSyncOwnerLeaseBackendTest.java" \
    "sessionFenceRejectsACommittedLeaseAfterTheMarkerChanges"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/ownership/OxiaRealServiceSmokeTest.java" \
    "OxiaSyncOwnerLeaseBackend.connect("
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Oxia Owner Lease session-bound CAS"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "7a76a3af61ea16bceb81cc566462c078ca8de2a5"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Oxia Owner Lease session-bound CAS audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Oxia Owner Lease session-bound CAS implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Oxia Owner Lease session-bound CAS receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/route/OxiaRouteAuthoritySession.java" \
    "SessionBoundIterable"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/route/OxiaRouteAuthoritySession.java" \
    "lazy iterator consumes Oxia data"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/route/OxiaSignedRouteSnapshotProviderTest.java" \
    "sessionFenceRejectsACommittedRouteHeadAfterTheMarkerChanges"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/route/OxiaRouteAuthoritySession.java" \
    "notificationDelegate.notifications(consumer);"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/route/OxiaSignedRouteSnapshotProviderTest.java" \
    "notificationReconnectRequiresTheCurrentSessionBeforeRegistration"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/route/OxiaSignedRouteSnapshotProviderTest.java" \
    "notificationReconnectRejectsACommittedRegistrationAfterTheMarkerChanges"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/route/OxiaSignedRouteSnapshotProvider.java" \
    "health == RouteCacheHealth.HEALTHY"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/route/OxiaSignedRouteSnapshotProviderTest.java" \
    "startRetriesNotificationRegistrationAfterACommittedRegistrationIsFenced"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/route/OxiaSignedRouteSnapshotProvider.java" \
    "client.notifications(this::onNotification);"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/route/OxiaSignedRouteSnapshotProviderTest.java" \
    "refreshAfterAnInitialRouteGapRestoresTheNotificationStream"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/route/OxiaRealRouteAuthoritySmokeTest.java" \
    "OxiaRouteAuthoritySession.connect("
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Oxia Route authority session-bound I/O fence"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "57e466786aea596cfdbd75020e48310415da0335"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Oxia Route authority session-bound I/O fence audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Oxia Route authority session-bound I/O fence implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Oxia Route authority session-bound I/O fence receipt"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Oxia Route notification reconnect session fence"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "de203e4dc14de32746ce73da75381843152af922"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Oxia Route notification reconnect session fence audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Oxia Route notification reconnect session fence implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Oxia Route notification reconnect session fence receipt"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Oxia Route provider start retry after notification fence"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "d241246eefc284fea9719c8e162afa8e2a8e4828"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Oxia Route provider start retry after notification fence audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Oxia Route provider start retry after notification fence implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Oxia Route provider start retry after notification fence receipt"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Oxia Route initial-refresh notification restoration"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "22780082d24e2011d44ead6ca62c38251a03633b"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Oxia Route initial-refresh notification restoration audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Oxia Route initial-refresh notification restoration implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Oxia Route initial-refresh notification restoration receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/WorkerShardFleetRuntime.java" \
    "Every admitted shard must get a close attempt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/route/OxiaRouteAuthoritySession.java" \
    "watch client is an independent Oxia session"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/ownership/WorkerShardFleetRuntimeTest.java" \
    "closeAttemptsEveryShardAndRetainsTheFirstDrainFailure"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/route/OxiaSignedRouteSnapshotProviderTest.java" \
    "sessionCloseAttemptsTheIndependentWatchClientAfterAuthorityCloseFails"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "fleet and Route resource close aggregation"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "eb47cb807ceb45d68a9f8db5f53ef3a7cc6ead4e"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "fleet and Route resource close aggregation audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "fleet and Route resource close aggregation implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Fleet and Route resource close aggregation receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/WorkerSourceApplyLoop.java" \
    "native close failure leaves the"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/ownership/SourceApplyCoordinatorTest.java" \
    "workerSourceLoopRetriesNativeCloseAfterAReleaseFailure"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Worker source close retry boundary"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "874fccb4fc521ad51b7954236ec5e37c1591e011"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Worker source close retry boundary audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Worker source close retry boundary implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Worker source close retry receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/route/OxiaRouteAuthoritySession.java" \
    "private boolean closeCompleted"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/route/OxiaSignedRouteSnapshotProvider.java" \
    "closeCompleted = true"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/route/OxiaSignedRouteSnapshotProviderTest.java" \
    "routeProviderRetriesClientCloseAfterAReleaseFailure"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Route client teardown retry boundary"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "9f24b2f38ba4f21962bebdaa2455d7f86ba0cd1b"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Route client teardown retry boundary audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Route client teardown retry boundary implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Route client teardown retry receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/client/DefaultDelayClient.java" \
    "private boolean closeCompleted"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/client/DefaultDelayClient.java" \
    "public synchronized void close()"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/client/DefaultDelayClientTest.java" \
    "closeRetriesEveryChildAfterTheFirstCloseFailure"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Direct SDK client teardown retry boundary"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "677026b3"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Direct SDK client teardown retry boundary audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Direct SDK client teardown retry boundary implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Direct SDK client teardown retry receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/route/OxiaRouteAuthoritySession.java" \
    "final String canonicalPrefix = canonicalKeyPrefix(keyPrefix)"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/route/OxiaRouteAuthoritySessionTest.java" \
    "connectRejectsAnInvalidKeyPrefixBeforeCreatingOxiaClients"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Route connect prefix validation boundary"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "4da7bcf46b0ab9350adebf1f614590851a1fadd8"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Route connect prefix validation boundary audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Route connect prefix validation boundary implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Route connect prefix validation receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/WorkerRuntimeResourceMonitor.java" \
    "private boolean closeCompleted"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/WorkerRocksDbUsageMonitor.java" \
    "private boolean closeCompleted"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/WorkerRuntimeResourceMonitorTest.java" \
    "closeRetriesExecutorShutdownAfterTheFirstFailure"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/WorkerRocksDbUsageMonitorTest.java" \
    "closeRetriesExecutorShutdownAfterTheFirstFailure"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Worker monitor teardown retry boundary"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "2f7d9d667547380355a27517ea2c1e4941962693"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Worker monitor teardown retry boundary audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Worker monitor teardown retry boundary implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Worker monitor teardown retry receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/transport/InMemoryCommandTransportRegistry.java" \
    "private boolean closeCompleted"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/transport/InMemoryCommandTransportRegistryTest.java" \
    "closeRetriesOnlyTheTransportThatFailedTheFirstTeardown"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "In-memory command transport registry teardown retry"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "0378e9a7585397e6f5e71a301f58c6d00835f2a0"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "In-memory command transport registry teardown retry audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "In-memory command transport registry teardown retry implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "In-memory command transport registry teardown retry receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/transport/GuardedPulsarCommandTransport.java" \
    "nativeSender.close()"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/transport/GuardedPulsarCommandTransport.java" \
    "appendCloseFailure"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/transport/GuardedTransportOwnershipTest.java" \
    "pulsarCloseAttemptsNativeSenderAfterManagedSenderFailure"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Guarded Pulsar transport teardown aggregation"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "9d164037f9ba3832cd1f83846813b44de18967ab"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Guarded Pulsar transport teardown aggregation audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Guarded Pulsar transport teardown aggregation implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Guarded Pulsar transport teardown aggregation receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/OxiaSyncOwnerLeaseBackend.java" \
    "final String canonicalPrefix = canonicalKeyPrefix(keyPrefix)"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/ownership/OxiaSyncOwnerLeaseBackendTest.java" \
    "connectRejectsAnInvalidKeyPrefixBeforeCreatingAnOxiaClient"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Owner connect prefix validation boundary"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "499e8439f2fe0f1b1c1114dbfd1bb7e55a06c43c"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Owner connect prefix validation boundary audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Owner connect prefix validation boundary implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Owner connect prefix validation receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/OxiaGatewayAdmissionController.java" \
    "owner.release(tenantScopeHash, lease)"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaGatewayAdmissionControllerTest.java" \
    "leaseCloseRemainsRetryableAfterReleaseCasDoesNotConverge"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Gateway admission lease release retry boundary"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "d5384b954e4d99ad291b2aea004910e1b1666ec8"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Gateway admission lease release retry boundary audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Gateway admission lease release retry boundary implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Gateway admission lease release retry receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/GatewayIdempotencyRecordV1.java" \
    "Gateway attempt terminal evidence conflict"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/OxiaGatewayIdempotencyStore.java" \
    "if (next == current.record())"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaGatewayIdempotencyStoreTest.java" \
    "lateQueuedEvidencePromotesUncertainWithoutChangingItsAttemptIdentity"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaGatewayIdempotencyStoreTest.java" \
    "retryUsesTheHighestUnresolvedAttemptWhenANewerAttemptIsDefinitive"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Gateway idempotency evidence monotonicity"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "b19f998ffe811d0a6dee1051491eae6c61131712"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Gateway idempotency evidence monotonicity audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Gateway idempotency evidence monotonicity implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Gateway idempotency evidence monotonicity receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/GatewayScheduleService.java" \
    "PREPARED_COMMAND_EXPIRED"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/InMemoryGatewayIdempotencyStore.java" \
    "current.retainUntilEpochMs()"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/OxiaGatewayIdempotencyStore.java" \
    "current.record().retainUntilEpochMs()"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/GatewayScheduleServiceTest.java" \
    "completedAggregateReplaysAfterRetryDeadlineWithoutAnotherCoordinatorCall"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaGatewayIdempotencyStoreTest.java" \
    "expiredPreparedRecordCannotCreateAnAttemptAtTheStoreBoundary"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Gateway prepared-expiry fence and aggregate replay"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "66508783f5e8230ace8bae37ff04c28dfb353653"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Gateway prepared-expiry fence and aggregate replay audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Gateway prepared-expiry fence and aggregate replay implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Gateway prepared-expiry fence and aggregate replay receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/GatewayPhysicalAttemptV1.java" \
    "Gateway STARTED attempt must not carry terminal evidence"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/GatewayIdempotencyRecordV1.java" \
    "Gateway physical attempt identity is duplicated"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/GatewayIdempotencyRecordV1.java" \
    "Gateway idempotency phase does not match attempts"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaGatewayIdempotencyStoreTest.java" \
    "gatewayProjectionRejectsImpossibleAttemptAndRecordShapes"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Gateway attempt projection integrity fence"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "52c6ed1c604a98b56668e510a3cf84ad364ec9cc"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Gateway attempt projection integrity fence audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Gateway attempt projection integrity fence implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Gateway attempt projection integrity fence receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/GatewayIdempotencyRecordV1.java" \
    "validateStoredProjection"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/GatewayIdempotencyRecordV1.java" \
    "Gateway attempt state does not match outcome"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/GatewayIdempotencyRecordV1.java" \
    "Gateway aggregate does not match attempt history"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaGatewayIdempotencyStoreTest.java" \
    "gatewayProjectionRejectsOutcomeStateAndAggregateMismatches"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Gateway stored evidence binding"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "380e279725e9ac5d31f98ad49ee711cd15c5b25c"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Gateway stored evidence binding audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Gateway stored evidence binding implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Gateway stored evidence binding receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/GatewayIdempotencyRecordV1.java" \
    "Gateway retry request hash does not bind to an earlier attempt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/GatewayIdempotencyRecordV1.java" \
    "GatewayIdempotencyHashV1.retryRequestHash"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaGatewayIdempotencyStoreTest.java" \
    "new Digest32(bytes(32, 22))"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Gateway retry evidence hash binding"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "5e1bd9f6b3e2bcf24972e7b9ecdd78db49520734"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Gateway retry evidence hash binding audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Gateway retry evidence hash binding implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Gateway retry evidence hash binding receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/GatewayIdempotencyRecordV1.java" \
    "Gateway operation does not match prepared submission"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaGatewayIdempotencyStoreTest.java" \
    "GatewayOperationKindV1.CANCEL"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/GatewayGrpcServiceTest.java" \
    "PreparedCommand.prepareLargeV1"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/GatewayScheduleServiceTest.java" \
    "PreparedCommand.rescheduleV1"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Gateway operation/prepared binding"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "f27800424a7cde3b8496b4fbbb4d4586cbeb07ca"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Gateway operation/prepared binding audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Gateway operation/prepared binding implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Gateway operation/prepared binding receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/GatewayAuditEventV1.java" \
    "Gateway audit outcome hash must match the completed phase"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaGatewayAuditSinkTest.java" \
    "auditOutcomeDigestIsPresentOnlyForCompletedEvents"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Gateway audit phase evidence"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "745da182c72af27dff09a8fb55db6cc15a4f20e3"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Gateway audit phase evidence audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Gateway audit phase evidence implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Gateway audit phase evidence receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/GatewayIdempotencyRecordV1.java" \
    "Gateway STARTED attempt must be the final attempt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/GatewayIdempotencyRecordV1.java" \
    "Gateway ACTIVE record must contain one STARTED attempt"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaGatewayIdempotencyStoreTest.java" \
    "gatewayProjectionRejectsImpossibleAttemptAndRecordShapes"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Gateway active attempt tail fence"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "a1a85f99471743c48126943fad92fbb80ce6be34"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Gateway active attempt tail fence audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Gateway active attempt tail fence implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Gateway active attempt tail fence receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/GatewayPhysicalAttemptV1.java" \
    "Gateway first attempt must not carry retry identity"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/GatewayPhysicalAttemptV1.java" \
    "Gateway retry attempt must carry retry identity"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/GatewayPhysicalAttemptV1.java" \
    "invalid Gateway physical attempt bounds"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Gateway attempt timing/retry shape"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "e0d5bc9761fea57103518819165d54eb60662b99"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Gateway attempt timing/retry shape audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Gateway attempt timing/retry shape implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Gateway attempt timing/retry shape receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/CheckpointPublicationCoordinator.java" \
    "intentIsAtomic || catalogIsAtomic"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/CheckpointUploadCoordinatorTest.java" \
    "rejectsMismatchedAtomicAuthorityRegardlessOfWhichSideDeclaresIt"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Atomic checkpoint publication authority pairing fence"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "920197ad41aaa6f0b88871f5ddf631f6899a53d3"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Atomic checkpoint publication authority pairing fence audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Atomic checkpoint publication authority pairing fence implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Atomic checkpoint publication authority pairing fence receipt"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/OxiaSyncRecoveryCatalogBackend.java" \
    "new OxiaSessionBoundRecoveryPinStore(this.client"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/store/OxiaSyncCheckpointPublicationBackend.java" \
    "new OxiaSessionBoundRecoveryPinStore(this.client"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/OxiaSyncRecoveryCatalogBackendTest.java" \
    "sessionFenceRejectsACommittedRecoveryPinAfterTheMarkerChanges"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/store/OxiaSyncCheckpointPublicationBackendTest.java" \
    "sessionFenceRejectsACommittedPublicationRecoveryPinAfterTheMarkerChanges"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "Recovery Pin session-fenced client wiring correction"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "f0e45cbdf6eb30d730c6678e71c4c19d34e06072"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Recovery Pin session-fenced client wiring correction audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "Recovery Pin session-fenced client wiring correction implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Recovery Pin session-fenced client wiring correction receipt"

echo "cross-repo contract audit passed"
echo "Delay:  $(git -C "$delay_root" rev-parse HEAD)"
echo "Kafka:  $kafka_branch@$kafka_head from $kafka_base"
echo "Pulsar: $pulsar_branch@$pulsar_head from $pulsar_base"
echo "Oxia:   $(git -C "$oxia_checkout" rev-parse HEAD)"
