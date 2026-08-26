package com.nereusstream.delay.semantic;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.ActivationBarrier;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CredentialBinding;
import com.nereusstream.delay.protocol.CredentialBindingProtection;
import com.nereusstream.delay.protocol.CredentialEquivalenceAttestation;
import com.nereusstream.delay.protocol.DeliveryCapabilitySemantic;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.IngressCredentialBindingRef;
import com.nereusstream.delay.protocol.OutcomeCapability;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarIngressRouteResource;
import com.nereusstream.delay.protocol.PulsarPhysicalPartitionIdentity;
import com.nereusstream.delay.protocol.QuotaGrantRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.RouteLifecycle;
import com.nereusstream.delay.protocol.RoutePartitionPolicy;
import com.nereusstream.delay.protocol.RouteSnapshot;
import com.nereusstream.delay.protocol.RoutingHashVersion;
import com.nereusstream.delay.protocol.TimingCapability;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.InMemoryProfileCatalog;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import org.junit.jupiter.api.Test;

class NativeCapabilitySnapshotIssuerTest {
    @Test
    void protectsBindingBeforeExposingIssuerSignedSnapshot() throws Exception {
        final Fixture fixture = fixture();
        final RecordingAuthority authority = new RecordingAuthority(fixture.target(), fixture.tenant());
        final NativeCapabilitySnapshotIssuer issuer = new NativeCapabilitySnapshotIssuer(
                fixture.catalog(),
                authority,
                fixture.routeKeys().getPublic(),
                fixture.attestationKeys().getPublic(),
                fixture.issuerKeys().getPrivate(),
                7,
                500);

        final NativePreparationSnapshot prepared = issuer.issue(
                fixture.tenant(), fixture.route(), fixture.destination().ref(), 0, 420, fixture.issuedAt());

        assertEquals(1, authority.protectionCalls);
        assertEquals(800, authority.protectedUntil);
        assertEquals(0, prepared.physicalPartition());
        assertArrayEquals(
                fixture.target().resourceIncarnation(), prepared.target().resourceIncarnation());
        assertEquals(7, prepared.capabilitySnapshot().issuerSigningKeyVersion());
        assertEquals(800, prepared.capabilitySnapshot().notAfterEpochMs());
        assertEquals(
                true,
                prepared.capabilitySnapshot()
                        .verifySignature(fixture.issuerKeys().getPublic()));
    }

    @Test
    void rejectsProtectionThatDoesNotCoverTheSignedSnapshotLifetime() throws Exception {
        final Fixture fixture = fixture();
        final RecordingAuthority authority = new RecordingAuthority(fixture.target(), fixture.tenant());
        authority.returnShortProtection = true;
        final NativeCapabilitySnapshotIssuer issuer = new NativeCapabilitySnapshotIssuer(
                fixture.catalog(),
                authority,
                fixture.routeKeys().getPublic(),
                fixture.attestationKeys().getPublic(),
                fixture.issuerKeys().getPrivate(),
                7,
                500);

        assertThrows(
                IllegalArgumentException.class,
                () -> issuer.issue(
                        fixture.tenant(), fixture.route(), fixture.destination().ref(), 0, 420, fixture.issuedAt()));
        assertEquals(1, authority.protectionCalls);
    }

    @Test
    void rejectsForeignGuardEvidenceBeforeProtection() throws Exception {
        final Fixture fixture = fixture();
        final RecordingAuthority authority = new RecordingAuthority(
                new PulsarBrokerResourceIdentity("other-cluster", bytes(32, 130), "persistent://other/ns/topic", 131),
                fixture.tenant());
        final NativeCapabilitySnapshotIssuer issuer = new NativeCapabilitySnapshotIssuer(
                fixture.catalog(),
                authority,
                fixture.routeKeys().getPublic(),
                fixture.attestationKeys().getPublic(),
                fixture.issuerKeys().getPrivate(),
                7,
                500);

        assertThrows(
                IllegalArgumentException.class,
                () -> issuer.issue(
                        fixture.tenant(), fixture.route(), fixture.destination().ref(), 0, 420, fixture.issuedAt()));
        assertEquals(0, authority.protectionCalls);
    }

    private static Fixture fixture() throws Exception {
        final KeyPair issuerKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final KeyPair attestationKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final AuthenticatedTenantContext tenant =
                new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3));
        final PulsarBrokerResourceIdentity target = new PulsarBrokerResourceIdentity(
                "target-cluster", bytes(32, 10), "persistent://tenant/ns/destination", 20);
        final ProfileSemanticEnvelope capability = capability();
        final DestinationProfileSemantic destinationBody = new DestinationProfileSemantic(
                AdapterKind.PULSAR,
                com.nereusstream.delay.protocol.BrokerResourceIdentity.pulsar(target),
                1,
                com.nereusstream.delay.protocol.TargetPartitionPolicy.EXPLICIT_ONLY,
                com.nereusstream.delay.protocol.TargetPartitionHashInput.ORDERING_KEY,
                List.of(0),
                capability.ref(),
                1,
                0,
                20,
                bytes(32, 40),
                1024,
                512,
                512,
                1,
                Bytes.utf8("destination"),
                0,
                0,
                1,
                bytes(32, 41));
        final ProfileSemanticEnvelope destination =
                new ProfileSemanticEnvelope(ProfileKind.DESTINATION, Bytes.utf8("destination"), 1, destinationBody);
        final byte[] secretReference = Bytes.utf8("secret-reference");
        final TrustedUtcIntervalEvidence attestationTime = new TrustedUtcIntervalEvidence(
                200,
                210,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                bytes(8, 44),
                1,
                2,
                3,
                bytes(32, 45),
                0,
                null);
        final CredentialEquivalenceAttestation attestation = CredentialEquivalenceAttestation.signed(
                destination.ref(),
                1,
                Bytes.sha256(secretReference),
                destinationBody.credentialAuthorizationScopeDigest(),
                bytes(32, 46),
                1,
                Bytes.utf8("credential-verifier"),
                attestationTime,
                900,
                bytes(32, 47),
                1,
                attestationKeys.getPrivate());
        final CredentialBinding binding = CredentialBinding.create(destination.ref(), 1, secretReference, attestation);
        final InMemoryProfileCatalog catalog = new InMemoryProfileCatalog();
        catalog.publish(capability);
        catalog.publish(destination, binding);
        final KeyPair routeKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        return new Fixture(
                issuerKeys, attestationKeys, routeKeys, tenant, target, destination, catalog, route(routeKeys));
    }

    private static ProfileSemanticEnvelope capability() {
        final DeliveryCapabilitySemantic body = new DeliveryCapabilitySemantic(
                AdapterKind.PULSAR,
                OutcomeCapability.AT_LEAST_ONCE,
                TimingCapability.ORDINARY_MANAGED | TimingCapability.PULSAR_AUTO_FAST,
                null,
                0,
                0,
                0,
                0,
                bytes(32, 60),
                bytes(32, 61),
                0,
                0);
        return new ProfileSemanticEnvelope(ProfileKind.DELIVERY_CAPABILITY, Bytes.utf8("capability"), 1, body);
    }

    private static RouteSnapshot route(final KeyPair keys) {
        final PulsarPhysicalPartitionIdentity physical = new PulsarPhysicalPartitionIdentity(
                0, "persistent://tenant/ns/commands-partition-0", bytes(32, 80), 81);
        final PulsarIngressRouteResource ingress =
                new PulsarIngressRouteResource("source-cluster", "persistent://tenant/ns/commands", List.of(physical));
        final PulsarBrokerResourceIdentity source = new PulsarBrokerResourceIdentity(
                "source-cluster",
                physical.resourceIncarnation(),
                physical.physicalTopic(),
                physical.physicalTopicCreationTimestamp());
        final QuotaGrantRef quota = new QuotaGrantRef(
                bytes(32, 82),
                1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        final RoutePartitionPolicy policy = new RoutePartitionPolicy(
                0,
                ActivationBarrier.empty(
                        com.nereusstream.delay.protocol.BrokerResourceIdentity.pulsar(source), 0, 1L, bytes(32, 83)),
                quota,
                1,
                bytes(32, 84));
        return RouteSnapshot.create(
                new RouteIncarnation(bytes(16, 85)),
                bytes(32, 1),
                bytes(32, 2),
                RouteLifecycle.ACTIVE_FOR_NEW,
                900,
                ingress,
                RoutingHashVersion.ROUTING_HASH,
                new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 1),
                1,
                List.of(policy),
                100,
                200,
                1024,
                4096,
                10,
                8192,
                500,
                100,
                1000,
                new IngressCredentialBindingRef(bytes(32, 86), 1, bytes(32, 87), bytes(32, 88), bytes(32, 89)),
                bytes(32, 90),
                new TrustedUtcIntervalEvidence(
                        200,
                        201,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                        bytes(8, 91),
                        1,
                        2,
                        3,
                        bytes(32, 92),
                        0,
                        null),
                1,
                keys.getPrivate());
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private record Fixture(
            KeyPair issuerKeys,
            KeyPair attestationKeys,
            KeyPair routeKeys,
            AuthenticatedTenantContext tenant,
            PulsarBrokerResourceIdentity target,
            ProfileSemanticEnvelope destination,
            InMemoryProfileCatalog catalog,
            RouteSnapshot route) {
        private TrustedUtcIntervalEvidence issuedAt() {
            return new TrustedUtcIntervalEvidence(
                    300,
                    310,
                    TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                    bytes(8, 93),
                    1,
                    2,
                    3,
                    bytes(32, 94),
                    0,
                    null);
        }
    }

    private static final class RecordingAuthority implements NativeCapabilityIssuanceAuthority {
        private final PulsarBrokerResourceIdentity target;
        private final AuthenticatedTenantContext tenant;
        private int protectionCalls;
        private long protectedUntil;
        private boolean returnShortProtection;

        private RecordingAuthority(final PulsarBrokerResourceIdentity target, final AuthenticatedTenantContext tenant) {
            this.target = target;
            this.tenant = tenant;
        }

        @Override
        public GuardEvidence resolveGuard(
                final com.nereusstream.delay.protocol.ProfileRef destination,
                final com.nereusstream.delay.protocol.ProfileRef capability,
                final int physicalPartition,
                final byte[] principalScopeDigest,
                final TrustedUtcIntervalEvidence issuedAt) {
            return new GuardEvidence(target, physicalPartition, bytes(32, 100), 1, tenant.principalScopeHash(), 900);
        }

        @Override
        public CredentialBindingProtection protectNativeCapability(
                final CredentialBinding binding, final long notAfterEpochMs) {
            protectionCalls++;
            protectedUntil = notAfterEpochMs;
            final long horizon = returnShortProtection ? notAfterEpochMs - 1 : notAfterEpochMs;
            return CredentialBindingProtection.forBinding(binding, 0, 0, horizon, 0, 2);
        }
    }
}
