package io.nereusstream.delay.semantic;

import io.nereusstream.delay.protocol.ActivationBarrierV1;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CredentialBindingProtectionV1;
import io.nereusstream.delay.protocol.CredentialBindingV1;
import io.nereusstream.delay.protocol.CredentialEquivalenceAttestationV1;
import io.nereusstream.delay.protocol.DeliveryCapabilitySemanticV1;
import io.nereusstream.delay.protocol.DestinationProfileSemanticV1;
import io.nereusstream.delay.protocol.IngressCredentialBindingRefV1;
import io.nereusstream.delay.protocol.OutcomeCapabilityV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.ProtocolTupleV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.PulsarIngressRouteResourceV1;
import io.nereusstream.delay.protocol.PulsarPhysicalPartitionIdentityV1;
import io.nereusstream.delay.protocol.QuotaGrantRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.RouteLifecycleV1;
import io.nereusstream.delay.protocol.RoutePartitionPolicyV1;
import io.nereusstream.delay.protocol.RouteSnapshotV1;
import io.nereusstream.delay.protocol.RoutingHashVersionV1;
import io.nereusstream.delay.protocol.TimingCapabilityV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.InMemoryProfileCatalog;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NativeCapabilitySnapshotIssuerTest {
    @Test
    void protectsBindingBeforeExposingIssuerSignedSnapshot() throws Exception {
        final Fixture fixture = fixture();
        final RecordingAuthority authority = new RecordingAuthority(fixture.target(), fixture.tenant());
        final NativeCapabilitySnapshotIssuer issuer = new NativeCapabilitySnapshotIssuer(fixture.catalog(), authority,
                fixture.routeKeys().getPublic(), fixture.attestationKeys().getPublic(),
                fixture.issuerKeys().getPrivate(), 7, 500);

        final NativePreparationSnapshotV1 prepared = issuer.issue(fixture.tenant(), fixture.route(),
                fixture.destination().ref(), 0, 420, fixture.issuedAt());

        assertEquals(1, authority.protectionCalls);
        assertEquals(800, authority.protectedUntil);
        assertEquals(0, prepared.physicalPartition());
        assertArrayEquals(fixture.target().resourceIncarnation(), prepared.target().resourceIncarnation());
        assertEquals(7, prepared.capabilitySnapshot().issuerSigningKeyVersion());
        assertEquals(800, prepared.capabilitySnapshot().notAfterEpochMs());
        assertEquals(true, prepared.capabilitySnapshot().verifySignature(fixture.issuerKeys().getPublic()));
    }

    @Test
    void rejectsProtectionThatDoesNotCoverTheSignedSnapshotLifetime() throws Exception {
        final Fixture fixture = fixture();
        final RecordingAuthority authority = new RecordingAuthority(fixture.target(), fixture.tenant());
        authority.returnShortProtection = true;
        final NativeCapabilitySnapshotIssuer issuer = new NativeCapabilitySnapshotIssuer(fixture.catalog(), authority,
                fixture.routeKeys().getPublic(), fixture.attestationKeys().getPublic(),
                fixture.issuerKeys().getPrivate(), 7, 500);

        assertThrows(IllegalArgumentException.class, () -> issuer.issue(fixture.tenant(), fixture.route(),
                fixture.destination().ref(), 0, 420, fixture.issuedAt()));
        assertEquals(1, authority.protectionCalls);
    }

    @Test
    void rejectsForeignGuardEvidenceBeforeProtection() throws Exception {
        final Fixture fixture = fixture();
        final RecordingAuthority authority = new RecordingAuthority(
                new PulsarBrokerResourceIdentityV1("other-cluster", bytes(32, 130),
                        "persistent://other/ns/topic", 131), fixture.tenant());
        final NativeCapabilitySnapshotIssuer issuer = new NativeCapabilitySnapshotIssuer(fixture.catalog(), authority,
                fixture.routeKeys().getPublic(), fixture.attestationKeys().getPublic(),
                fixture.issuerKeys().getPrivate(), 7, 500);

        assertThrows(IllegalArgumentException.class, () -> issuer.issue(fixture.tenant(), fixture.route(),
                fixture.destination().ref(), 0, 420, fixture.issuedAt()));
        assertEquals(0, authority.protectionCalls);
    }

    private static Fixture fixture() throws Exception {
        final KeyPair issuerKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final KeyPair attestationKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final AuthenticatedTenantContext tenant = new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2),
                bytes(32, 3));
        final PulsarBrokerResourceIdentityV1 target = new PulsarBrokerResourceIdentityV1(
                "target-cluster", bytes(32, 10), "persistent://tenant/ns/destination", 20);
        final ProfileSemanticEnvelopeV1 capability = capability();
        final DestinationProfileSemanticV1 destinationBody = new DestinationProfileSemanticV1(
                AdapterKindV1.PULSAR, io.nereusstream.delay.protocol.BrokerResourceIdentityV1.pulsar(target), 1,
                io.nereusstream.delay.protocol.TargetPartitionPolicyV1.EXPLICIT_ONLY,
                io.nereusstream.delay.protocol.TargetPartitionHashInputV1.ORDERING_KEY, List.of(0),
                capability.ref(), 1, 0, 20, bytes(32, 40), 1024, 512, 512, 1,
                Bytes.utf8("destination"), 0, 0, 1, bytes(32, 41));
        final ProfileSemanticEnvelopeV1 destination = new ProfileSemanticEnvelopeV1(ProfileKindV1.DESTINATION,
                Bytes.utf8("destination"), 1, destinationBody);
        final byte[] secretReference = Bytes.utf8("secret-reference-v1");
        final TrustedUtcIntervalEvidence attestationTime = new TrustedUtcIntervalEvidence(200, 210,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, bytes(8, 44), 1, 2, 3,
                bytes(32, 45), 0, null);
        final CredentialEquivalenceAttestationV1 attestation = CredentialEquivalenceAttestationV1.signed(
                destination.ref(), 1, Bytes.sha256(secretReference), destinationBody.credentialAuthorizationScopeDigest(),
                bytes(32, 46), 1, Bytes.utf8("credential-verifier-v1"), attestationTime, 900, bytes(32, 47), 1,
                attestationKeys.getPrivate());
        final CredentialBindingV1 binding = CredentialBindingV1.create(destination.ref(), 1, secretReference,
                attestation);
        final InMemoryProfileCatalog catalog = new InMemoryProfileCatalog();
        catalog.publish(capability);
        catalog.publish(destination, binding);
        final KeyPair routeKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        return new Fixture(issuerKeys, attestationKeys, routeKeys, tenant, target, destination, catalog,
                route(routeKeys));
    }

    private static ProfileSemanticEnvelopeV1 capability() {
        final DeliveryCapabilitySemanticV1 body = new DeliveryCapabilitySemanticV1(AdapterKindV1.PULSAR,
                OutcomeCapabilityV1.AT_LEAST_ONCE,
                TimingCapabilityV1.ORDINARY_MANAGED | TimingCapabilityV1.PULSAR_AUTO_FAST, null, 0, 0, 0, 0,
                bytes(32, 60), bytes(32, 61), 0, 0);
        return new ProfileSemanticEnvelopeV1(ProfileKindV1.DELIVERY_CAPABILITY, Bytes.utf8("capability"), 1, body);
    }

    private static RouteSnapshotV1 route(final KeyPair keys) {
        final PulsarPhysicalPartitionIdentityV1 physical = new PulsarPhysicalPartitionIdentityV1(0,
                "persistent://tenant/ns/commands-partition-0", bytes(32, 80), 81);
        final PulsarIngressRouteResourceV1 ingress = new PulsarIngressRouteResourceV1("source-cluster",
                "persistent://tenant/ns/commands", List.of(physical));
        final PulsarBrokerResourceIdentityV1 source = new PulsarBrokerResourceIdentityV1("source-cluster",
                physical.resourceIncarnation(), physical.physicalTopic(), physical.physicalTopicCreationTimestamp());
        final QuotaGrantRefV1 quota = new QuotaGrantRefV1(bytes(32, 82), 1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        final RoutePartitionPolicyV1 policy = new RoutePartitionPolicyV1(0,
                ActivationBarrierV1.empty(io.nereusstream.delay.protocol.BrokerResourceIdentityV1.pulsar(source),
                        0, 1L, bytes(32, 83)), quota, 1, bytes(32, 84));
        return RouteSnapshotV1.create(new RouteIncarnation(bytes(16, 85)), bytes(32, 1), bytes(32, 2),
                RouteLifecycleV1.ACTIVE_FOR_NEW, 900, ingress, RoutingHashVersionV1.ROUTING_HASH_V1,
                new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1), 1, List.of(policy), 100, 200,
                1024, 4096, 10, 8192, 500, 100, 1000,
                new IngressCredentialBindingRefV1(bytes(32, 86), 1, bytes(32, 87), bytes(32, 88), bytes(32, 89)),
                bytes(32, 90), new TrustedUtcIntervalEvidence(200, 201,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, bytes(8, 91), 1, 2, 3,
                        bytes(32, 92), 0, null), 1, keys.getPrivate());
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private record Fixture(KeyPair issuerKeys, KeyPair attestationKeys, KeyPair routeKeys,
                           AuthenticatedTenantContext tenant,
                           PulsarBrokerResourceIdentityV1 target, ProfileSemanticEnvelopeV1 destination,
                           InMemoryProfileCatalog catalog, RouteSnapshotV1 route) {
        private TrustedUtcIntervalEvidence issuedAt() {
            return new TrustedUtcIntervalEvidence(300, 310,
                    TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, bytes(8, 93), 1, 2, 3,
                    bytes(32, 94), 0, null);
        }
    }

    private static final class RecordingAuthority implements NativeCapabilityIssuanceAuthority {
        private final PulsarBrokerResourceIdentityV1 target;
        private final AuthenticatedTenantContext tenant;
        private int protectionCalls;
        private long protectedUntil;
        private boolean returnShortProtection;

        private RecordingAuthority(final PulsarBrokerResourceIdentityV1 target,
                                   final AuthenticatedTenantContext tenant) {
            this.target = target;
            this.tenant = tenant;
        }

        @Override
        public GuardEvidence resolveGuard(final io.nereusstream.delay.protocol.ProfileRefV1 destination,
                                          final io.nereusstream.delay.protocol.ProfileRefV1 capability,
                                          final int physicalPartition, final byte[] principalScopeDigest,
                                          final TrustedUtcIntervalEvidence issuedAt) {
            return new GuardEvidence(target, physicalPartition, bytes(32, 100), 1,
                    tenant.principalScopeHash(), 900);
        }

        @Override
        public CredentialBindingProtectionV1 protectNativeCapability(final CredentialBindingV1 binding,
                                                                       final long notAfterEpochMs) {
            protectionCalls++;
            protectedUntil = notAfterEpochMs;
            final long horizon = returnShortProtection ? notAfterEpochMs - 1 : notAfterEpochMs;
            return CredentialBindingProtectionV1.forBinding(binding, 0, 0, horizon, 0, 2);
        }
    }
}
