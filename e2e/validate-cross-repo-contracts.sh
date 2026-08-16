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
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "runRealKafkaWorkerSmoke"
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
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/WorkerShardRuntime.java" \
    "runSourceBoundPhysicalPublish"
require_file_text "$delay_root/build.gradle" \
    "pulsarWorkerDestinationTopic"
require_file_text "$delay_root/e2e/run-pulsar-real-client-e2e.sh" \
    "worker_destination_topic"
require_file_text "$delay_root/e2e/run-pulsar-real-client-e2e.sh" \
    "source-applied physical publish with typed Outcome"
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
    "beginReaping(expectedPending, evidence, catalog)"
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
    "Delay checkpoint REAPING quiescence proof slice"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "Checkpoint REAPING quiescence proof audit"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "REAPING quiescence proof implementation note"
require_file_text "$delay_root/e2e/README.md" \
    "Checkpoint REAPING quiescence proof receipt"
require_file_text "$delay_root/e2e/README.md" \
    "9c4dcab9-c03c-4860-81de-07e62302d30e"

echo "cross-repo contract audit passed"
echo "Delay:  $(git -C "$delay_root" rev-parse HEAD)"
echo "Kafka:  $kafka_branch@$kafka_head from $kafka_base"
echo "Pulsar: $pulsar_branch@$pulsar_head from $pulsar_base"
echo "Oxia:   $(git -C "$oxia_checkout" rev-parse HEAD)"
