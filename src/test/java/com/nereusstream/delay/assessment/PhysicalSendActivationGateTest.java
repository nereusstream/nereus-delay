package com.nereusstream.delay.assessment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ProtocolCapabilityDeclaration;
import com.nereusstream.delay.protocol.PulsarSourceLock;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import org.junit.jupiter.api.Test;

class PhysicalSendActivationGateTest {
    private static final String SOURCE_COMMIT = "0123456789abcdef0123456789abcdef01234567";

    @Test
    void disposableModeRequiresACompleteAttestationAndExactArtifacts() {
        final ArtifactGenerationSet artifacts = artifacts(7);
        final DisposableEnvironmentAttestation complete = new DisposableEnvironmentAttestation(
                "local-a", "run-a", true, true, true, true, hash("local-attestation"));
        final PhysicalSendActivationGate gate =
                PhysicalSendActivationGate.disposableLocal(DeploymentSafetyGate.GateBStatus.PASS, complete, artifacts);

        gate.requirePhysicalSend(artifacts, 1);
        assertEquals(PhysicalSendActivationGate.Mode.DISPOSABLE_LOCAL, gate.mode());
        assertThrows(IllegalStateException.class, () -> gate.requirePhysicalSend(artifacts(8), 1));
        assertThrows(
                IllegalStateException.class,
                () -> PhysicalSendActivationGate.disposableLocal(
                        DeploymentSafetyGate.GateBStatus.PENDING, complete, artifacts));
        assertThrows(
                IllegalStateException.class,
                () -> PhysicalSendActivationGate.disposableLocal(
                        DeploymentSafetyGate.GateBStatus.PASS,
                        new DisposableEnvironmentAttestation(
                                "local-a", "run-a", true, true, false, true, hash("incomplete")),
                        artifacts));
    }

    @Test
    void persistentModeRequiresGateCShadowManifestAndExactSource() throws Exception {
        final PersistentFixture fixture = persistentFixture("prod-a");
        final GateCAuthorization gateC = new GateCAuthorization(
                "prod-a",
                EnvironmentClassification.PRODUCTION,
                GateCAuthorization.Resolution.RESET,
                hash("assessment-scope"),
                hash("assessment-receipt"),
                hash("gate-c-receipt"));

        final PhysicalSendActivationGate gate = PhysicalSendActivationGate.persistentEnabled(
                DeploymentSafetyGate.GateBStatus.PASS,
                EnvironmentClassification.PRODUCTION,
                gateC,
                DeploymentSafetyGate.ShadowReadiness.REQUIREMENTS_PASS,
                fixture.manifestGate,
                SOURCE_COMMIT);

        gate.requirePhysicalSend(fixture.artifacts, 250);
        assertEquals(PhysicalSendActivationGate.Mode.PERSISTENT_ENABLED, gate.mode());
        assertThrows(IllegalStateException.class, () -> gate.requirePhysicalSend(fixture.artifacts, 300));
        assertThrows(
                IllegalStateException.class,
                () -> PhysicalSendActivationGate.persistentEnabled(
                        DeploymentSafetyGate.GateBStatus.PASS,
                        EnvironmentClassification.PRODUCTION,
                        gateC,
                        DeploymentSafetyGate.ShadowReadiness.NOT_STARTED,
                        fixture.manifestGate,
                        SOURCE_COMMIT));
        assertThrows(
                IllegalArgumentException.class,
                () -> PhysicalSendActivationGate.persistentEnabled(
                        DeploymentSafetyGate.GateBStatus.PASS,
                        EnvironmentClassification.PRODUCTION,
                        gateC,
                        DeploymentSafetyGate.ShadowReadiness.REQUIREMENTS_PASS,
                        fixture.manifestGate,
                        "fedcba9876543210fedcba9876543210fedcba98"));
        assertThrows(
                NullPointerException.class,
                () -> PhysicalSendActivationGate.persistentEnabled(
                        DeploymentSafetyGate.GateBStatus.PASS,
                        EnvironmentClassification.PRODUCTION,
                        null,
                        DeploymentSafetyGate.ShadowReadiness.REQUIREMENTS_PASS,
                        fixture.manifestGate,
                        SOURCE_COMMIT));
    }

    @Test
    void persistentGateCAndManifestEnvironmentMismatchFailsClosed() throws Exception {
        final PersistentFixture fixture = persistentFixture("prod-b");
        final GateCAuthorization anotherEnvironment = new GateCAuthorization(
                "prod-a",
                EnvironmentClassification.PRODUCTION,
                GateCAuthorization.Resolution.RETAIN,
                hash("scope"),
                hash("receipt"),
                hash("gate"));

        assertThrows(
                IllegalArgumentException.class,
                () -> PhysicalSendActivationGate.persistentEnabled(
                        DeploymentSafetyGate.GateBStatus.PASS,
                        EnvironmentClassification.PRODUCTION,
                        anotherEnvironment,
                        DeploymentSafetyGate.ShadowReadiness.REQUIREMENTS_PASS,
                        fixture.manifestGate,
                        SOURCE_COMMIT));
    }

    private static PersistentFixture persistentFixture(final String environmentId) throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ArtifactGenerationSet artifacts = artifacts(7);
        final ShardId shard = new ShardId(new RouteIncarnation(bytes(16, 1)), 0);
        final DataResetManifest.ManifestScope scope = new DataResetManifest.ManifestScope(
                environmentId, "deployment-a", hash("tenant"), hash("route"), new ShardSubject(shard));
        final ProtocolCapabilityDeclaration declaration = new ProtocolCapabilityDeclaration(
                "worker-a",
                hash("worker"),
                List.of(artifacts.clientCommandTuple(), artifacts.systemMutationTuple()),
                artifacts,
                1,
                hash("session"));
        final DataResetManifest.WorkerCapability worker = new DataResetManifest.WorkerCapability(
                "worker-a", hash("worker"), hash("session"), declaration, hash("worker-evidence"));
        final DataResetManifest.ResourceIncarnation resource = new DataResetManifest.ResourceIncarnation(
                "PULSAR", "native-topic", hash("incarnation"), true, hash("resource-evidence"));
        final DataResetManifest manifest = DataResetManifest.create(
                scope,
                SOURCE_COMMIT,
                7,
                artifacts,
                List.of(resource),
                hash("fresh-resources"),
                new DataResetManifest.ObligationZeroProof(0, 0, 0, hash("zero-obligations")),
                List.of(worker),
                evidence(100),
                new DataResetManifest.ActivationWindow(200, 300),
                1,
                keys.getPrivate());
        return new PersistentFixture(
                artifacts, new DataResetActivationGate(manifest, keys.getPublic(), environmentId, artifacts));
    }

    private static ArtifactGenerationSet artifacts(final long resetGeneration) {
        return ArtifactGenerationSet.current(
                resetGeneration, PulsarSourceLock.digest(), hash("schema-" + resetGeneration));
    }

    private static TrustedUtcIntervalEvidence evidence(final long time) {
        return new TrustedUtcIntervalEvidence(
                time,
                time + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("clock-" + time),
                1,
                time,
                time,
                hash("time-" + time),
                0,
                null);
    }

    private static byte[] hash(final String value) {
        return Bytes.sha256(Bytes.utf8(value));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private record PersistentFixture(ArtifactGenerationSet artifacts, DataResetActivationGate manifestGate) {}
}
