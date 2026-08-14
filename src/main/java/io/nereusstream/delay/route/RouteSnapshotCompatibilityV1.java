package io.nereusstream.delay.route;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.RoutePartitionPolicyV1;
import io.nereusstream.delay.protocol.RouteSnapshotV1;

import java.util.Objects;

/**
 * Verifies the immutable part of a same-incarnation Route snapshot successor.
 *
 * <p>Lifecycle, control version, validity evidence, quota grants and the
 * signed snapshot bytes may advance. Resource identity, partition barriers,
 * policy limits and credential proof digests may not drift in place.</p>
 */
public final class RouteSnapshotCompatibilityV1 {
    private RouteSnapshotCompatibilityV1() {
    }

    public static void requireCompatibleSuccessor(final RouteSnapshotV1 previous,
                                                   final RouteSnapshotV1 next) {
        Objects.requireNonNull(previous, "previous");
        Objects.requireNonNull(next, "next");
        if (!previous.routeIncarnation().equals(next.routeIncarnation())) {
            throw new IllegalArgumentException("Route successor has a different incarnation");
        }
        if (!Bytes.constantTimeEquals(previous.authenticatedTenantScopeHash(),
                next.authenticatedTenantScopeHash())
                || !Bytes.constantTimeEquals(previous.tenantRoutingScope(), next.tenantRoutingScope())) {
            throw new IllegalArgumentException("Route successor changed tenant scope");
        }
        if (!previous.ingress().equals(next.ingress())
                || previous.routingHashVersion() != next.routingHashVersion()
                || !previous.protocolTuple().equals(next.protocolTuple())
                || previous.queuedReceiptQueryWindowMs() != next.queuedReceiptQueryWindowMs()
                || previous.fullCommandResultRetentionMs() != next.fullCommandResultRetentionMs()
                || previous.maxInlinePayloadBytes() != next.maxInlinePayloadBytes()
                || previous.maxCommandBytes() != next.maxCommandBytes()
                || previous.maxBatchCommands() != next.maxBatchCommands()
                || previous.maxBatchBytes() != next.maxBatchBytes()
                || previous.maximumPreparationAgeMs() != next.maximumPreparationAgeMs()
                || !Bytes.constantTimeEquals(previous.routePrerequisiteDigest(), next.routePrerequisiteDigest())) {
            throw new IllegalArgumentException("Route successor changed immutable semantic fields");
        }
        if (previous.validFromEpochMs() != next.validFromEpochMs()
                || next.validUntilEpochMs() < previous.validUntilEpochMs()
                || Long.compareUnsigned(next.controlVersion(), previous.controlVersion()) <= 0) {
            throw new IllegalArgumentException("Route successor regressed control or validity bounds");
        }
        final var previousCredential = previous.credentialBinding();
        final var nextCredential = next.credentialBinding();
        if (!Bytes.constantTimeEquals(previousCredential.bindingId(), nextCredential.bindingId())
                || !Bytes.constantTimeEquals(previousCredential.bindingDigest(), nextCredential.bindingDigest())
                || !Bytes.constantTimeEquals(previousCredential.resolvedCredentialFingerprintDigest(),
                nextCredential.resolvedCredentialFingerprintDigest())
                || !Bytes.constantTimeEquals(previousCredential.authorizationScopeDigest(),
                nextCredential.authorizationScopeDigest())
                || Long.compareUnsigned(nextCredential.generation(), previousCredential.generation()) < 0) {
            throw new IllegalArgumentException("Route successor changed credential proof or regressed generation");
        }
        if (previous.partitions().size() != next.partitions().size()) {
            throw new IllegalArgumentException("Route successor changed partition count");
        }
        for (int index = 0; index < previous.partitions().size(); index++) {
            final RoutePartitionPolicyV1 oldPolicy = previous.partitions().get(index);
            final RoutePartitionPolicyV1 newPolicy = next.partitions().get(index);
            if (oldPolicy.partition() != newPolicy.partition()
                    || !oldPolicy.activationBarrier().equals(newPolicy.activationBarrier())
                    || oldPolicy.brokerGuardAttestationGeneration()
                    != newPolicy.brokerGuardAttestationGeneration()
                    || !Bytes.constantTimeEquals(oldPolicy.brokerGuardAttestationDigest(),
                    newPolicy.brokerGuardAttestationDigest())) {
                throw new IllegalArgumentException("Route successor changed immutable partition guard fields");
            }
        }
    }
}
