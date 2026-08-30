package com.nereusstream.delay.assessment;

import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DeliveryCapabilitySemantic;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.HandoffPath;
import com.nereusstream.delay.protocol.HandoffPolicyScope;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.OutcomeCapability;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarSourceLock;
import com.nereusstream.delay.protocol.TargetPartitionHashInput;
import com.nereusstream.delay.protocol.TargetPartitionPolicy;
import com.nereusstream.delay.protocol.TimingCapability;
import java.util.List;
import java.util.Objects;

/** Exact Profile, target and scope identity shared by the persistent staging policy and native canary. */
public final class PersistentStagingNativeCanaryIdentity {
    public static final int PHYSICAL_PARTITION = 0;
    public static final long MAX_HANDOFF_LEAD_MS = 15_000;
    private static final byte[] TENANT_ROUTE_SCOPE =
            Bytes.sha256(Bytes.utf8("ndip1-persistent-native-canary-tenant-route"));
    private static final byte[] PRINCIPAL_SCOPE = Bytes.sha256(Bytes.utf8("ndip1-persistent-native-canary-principal"));

    private PersistentStagingNativeCanaryIdentity() {}

    public static Identity create(
            final String authenticatedClusterId,
            final byte[] resourceIncarnation,
            final String physicalTopic,
            final long topicCreationTimestamp,
            final ArtifactGenerationSet artifacts) {
        Objects.requireNonNull(artifacts, "artifacts");
        final PulsarBrokerResourceIdentity target = new PulsarBrokerResourceIdentity(
                authenticatedClusterId, resourceIncarnation, physicalTopic, topicCreationTimestamp);
        final ProfileSemanticEnvelope capability = capability();
        final DestinationProfileSemantic destinationBody = new DestinationProfileSemantic(
                AdapterKind.PULSAR,
                BrokerResourceIdentity.pulsar(target),
                1,
                TargetPartitionPolicy.EXPLICIT_ONLY,
                TargetPartitionHashInput.ORDERING_KEY,
                List.of(PHYSICAL_PARTITION),
                capability.ref(),
                1,
                MAX_HANDOFF_LEAD_MS,
                Bytes.sha256(Bytes.utf8("ndip1-persistent-native-canary-credential-scope")),
                1 << 20,
                64 << 10,
                512 << 10,
                1,
                Bytes.utf8("ndip1-persistent-native-canary-destination"),
                0,
                0,
                ArtifactGenerationSet.ADAPTER_ENCODING_GENERATION,
                Bytes.sha256(Bytes.utf8("ndip1-persistent-native-canary-prerequisites")));
        final ProfileSemanticEnvelope destination = new ProfileSemanticEnvelope(
                ProfileKind.DESTINATION, Bytes.utf8("ndip1-persistent-native-canary-destination"), 1, destinationBody);
        final byte[] scope = HandoffPolicyScope.digest(
                TENANT_ROUTE_SCOPE,
                destination.ref(),
                capability.ref(),
                BrokerResourceIdentity.pulsar(target),
                PHYSICAL_PARTITION,
                OrderingMode.BEST_EFFORT,
                HandoffPath.VALID_MASK,
                artifacts);
        return new Identity(destination, capability, target, TENANT_ROUTE_SCOPE, PRINCIPAL_SCOPE, scope);
    }

    private static ProfileSemanticEnvelope capability() {
        final DeliveryCapabilitySemantic body = new DeliveryCapabilitySemantic(
                AdapterKind.PULSAR,
                OutcomeCapability.AT_LEAST_ONCE,
                TimingCapability.VALID_MASK,
                null,
                0,
                0,
                0,
                0,
                Bytes.sha256(Bytes.utf8("ndip1-persistent-native-canary-broker-prerequisites")),
                PulsarSourceLock.digest(),
                0,
                0);
        return new ProfileSemanticEnvelope(
                ProfileKind.DELIVERY_CAPABILITY, Bytes.utf8("ndip1-persistent-native-canary-capability"), 1, body);
    }

    public record Identity(
            ProfileSemanticEnvelope destination,
            ProfileSemanticEnvelope capability,
            PulsarBrokerResourceIdentity target,
            byte[] tenantRouteScopeDigest,
            byte[] principalScopeDigest,
            byte[] policyScopeDigest) {
        public Identity {
            Objects.requireNonNull(destination, "destination");
            Objects.requireNonNull(capability, "capability");
            Objects.requireNonNull(target, "target");
            tenantRouteScopeDigest = copyDigest(tenantRouteScopeDigest, "tenantRouteScopeDigest");
            principalScopeDigest = copyDigest(principalScopeDigest, "principalScopeDigest");
            policyScopeDigest = copyDigest(policyScopeDigest, "policyScopeDigest");
        }

        @Override
        public byte[] tenantRouteScopeDigest() {
            return Bytes.copy(tenantRouteScopeDigest);
        }

        @Override
        public byte[] principalScopeDigest() {
            return Bytes.copy(principalScopeDigest);
        }

        @Override
        public byte[] policyScopeDigest() {
            return Bytes.copy(policyScopeDigest);
        }

        private static byte[] copyDigest(final byte[] value, final String name) {
            Bytes.requireLength(value, 32, name);
            return Bytes.copy(value);
        }
    }
}
