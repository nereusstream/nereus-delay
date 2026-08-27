package com.nereusstream.delay.assessment;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.WorkerAssignment;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.ProtocolCapabilityDeclaration;
import com.nereusstream.delay.protocol.PulsarSourceLock;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DataResetManifestTest {
    @Test
    void signedManifestRoundTripsAndGatesExactStartupAssignmentAndSend() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final Fixture fixture = fixture(keys);
        final DataResetManifest decoded = DataResetManifest.decode(fixture.manifest.canonicalBytes());

        assertTrue(decoded.verifySignature(keys.getPublic()));
        assertArrayEquals(fixture.manifest.manifestDigest(), decoded.manifestDigest());
        assertTrue(decoded.isCurrentGeneration());

        final DataResetActivationGate gate = new DataResetActivationGate(
                decoded, keys.getPublic(), fixture.scope.environmentId(), fixture.artifacts);
        gate.requireStartup(fixture.startup, 250);
        gate.requireAssignment(fixture.assignment, fixture.declaration, 250);
        gate.requireSourceApply(
                fixture.artifacts.clientCommandTuple(), fixture.artifacts, fixture.manifest.manifestDigest(), 250);
        gate.requirePhysicalSend(fixture.artifacts, fixture.manifest.manifestDigest(), 250);
    }

    @Test
    void signatureTamperingAndWindowBoundariesFailClosed() throws Exception {
        final Fixture fixture = fixture(KeyPairGenerator.getInstance("Ed25519").generateKeyPair());
        final byte[] tampered = fixture.manifest.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        final DataResetManifest decoded = DataResetManifest.decode(tampered);

        assertFalse(decoded.verifySignature(fixture.keys.getPublic()));
        final DataResetActivationGate gate = new DataResetActivationGate(
                fixture.manifest, fixture.keys.getPublic(), fixture.scope.environmentId(), fixture.artifacts);
        assertThrows(IllegalStateException.class, () -> gate.requireManifest(199));
        assertThrows(IllegalStateException.class, () -> gate.requireManifest(300));
    }

    @Test
    void nonFreshResourcesNonZeroObligationsAndLegacyWorkersCannotFormManifest() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final Fixture fixture = fixture(keys);
        final DataResetManifest.ObligationZeroProof nonZero =
                new DataResetManifest.ObligationZeroProof(0, 0, 1, bytes(32, 71));
        assertThrows(
                IllegalArgumentException.class,
                () -> DataResetManifest.create(
                        fixture.scope,
                        fixture.manifest.sourceBaselineCommit(),
                        fixture.artifacts.environmentResetGeneration(),
                        fixture.artifacts,
                        fixture.manifest.resources(),
                        fixture.manifest.freshResourceEvidenceDigest(),
                        nonZero,
                        fixture.manifest.workerCapabilities(),
                        fixture.manifest.createdAt(),
                        fixture.manifest.activationWindow(),
                        1,
                        keys.getPrivate()));

        final DataResetManifest.ResourceIncarnation staleResource = new DataResetManifest.ResourceIncarnation(
                "ROCKSDB", "delay-store", bytes(32, 72), false, bytes(32, 73));
        assertThrows(
                IllegalArgumentException.class,
                () -> DataResetManifest.create(
                        fixture.scope,
                        fixture.manifest.sourceBaselineCommit(),
                        fixture.artifacts.environmentResetGeneration(),
                        fixture.artifacts,
                        List.of(staleResource),
                        fixture.manifest.freshResourceEvidenceDigest(),
                        fixture.manifest.obligationZeroProof(),
                        fixture.manifest.workerCapabilities(),
                        fixture.manifest.createdAt(),
                        fixture.manifest.activationWindow(),
                        1,
                        keys.getPrivate()));

        final ProtocolCapabilityDeclaration legacy = new ProtocolCapabilityDeclaration(
                fixture.declaration.workerId(),
                fixture.declaration.workerIdentity(),
                List.of(fixture.artifacts.clientCommandTuple()),
                1,
                fixture.declaration.sessionIdentity());
        final DataResetManifest.WorkerCapability legacyWorker = new DataResetManifest.WorkerCapability(
                fixture.declaration.workerId(),
                fixture.declaration.workerIdentity(),
                fixture.declaration.sessionIdentity(),
                legacy,
                bytes(32, 74));
        assertThrows(
                IllegalArgumentException.class,
                () -> DataResetManifest.create(
                        fixture.scope,
                        fixture.manifest.sourceBaselineCommit(),
                        fixture.artifacts.environmentResetGeneration(),
                        fixture.artifacts,
                        fixture.manifest.resources(),
                        fixture.manifest.freshResourceEvidenceDigest(),
                        fixture.manifest.obligationZeroProof(),
                        List.of(legacyWorker),
                        fixture.manifest.createdAt(),
                        fixture.manifest.activationWindow(),
                        1,
                        keys.getPrivate()));
    }

    private static Fixture fixture(final KeyPair keys) {
        final ArtifactGenerationSet artifacts =
                ArtifactGenerationSet.current(7, PulsarSourceLock.digest(), Bytes.sha256(Bytes.utf8("schema-bundle")));
        final ShardId shard = new ShardId(new RouteIncarnation(bytes(16, 9)), 3);
        final DataResetManifest.ManifestScope scope = new DataResetManifest.ManifestScope(
                "env-a", "deployment-a", bytes(32, 10), bytes(32, 11), new ShardSubject(shard));
        final ProtocolCapabilityDeclaration declaration = new ProtocolCapabilityDeclaration(
                "worker-a",
                bytes(32, 12),
                List.of(artifacts.clientCommandTuple(), artifacts.systemMutationTuple()),
                artifacts,
                1,
                bytes(32, 13));
        final DataResetManifest.WorkerCapability worker = new DataResetManifest.WorkerCapability(
                "worker-a", bytes(32, 12), bytes(32, 13), declaration, bytes(32, 14));
        final DataResetManifest.ResourceIncarnation resource =
                new DataResetManifest.ResourceIncarnation("ROCKSDB", "delay-store", bytes(32, 15), true, bytes(32, 16));
        final DataResetManifest manifest = DataResetManifest.create(
                scope,
                "0123456789abcdef0123456789abcdef01234567",
                7,
                artifacts,
                List.of(resource),
                bytes(32, 17),
                new DataResetManifest.ObligationZeroProof(0, 0, 0, bytes(32, 18)),
                List.of(worker),
                evidence(100),
                new DataResetManifest.ActivationWindow(200, 300),
                1,
                keys.getPrivate());
        final KafkaActivationBarrier barrier =
                new KafkaActivationBarrier(shard, "activation-cluster", UUID.randomUUID(), 0);
        final SourceAssignment sourceAssignment = new SourceAssignment(shard, bytes(32, 19), 1, barrier);
        final WorkerAssignment assignment =
                new WorkerAssignment("worker-a", sourceAssignment, 1, bytes(32, 20), scope.routeSnapshotDigest());
        final DataResetActivationGate.StartupIdentity startup = new DataResetActivationGate.StartupIdentity(
                scope.environmentId(),
                scope.deploymentId(),
                scope.tenantScopeDigest(),
                scope.routeSnapshotDigest(),
                scope.shard(),
                "worker-a",
                bytes(32, 12),
                bytes(32, 13),
                declaration,
                List.of(resource));
        return new Fixture(keys, artifacts, scope, declaration, manifest, assignment, startup);
    }

    private static TrustedUtcIntervalEvidence evidence(final long time) {
        return new TrustedUtcIntervalEvidence(
                time,
                time + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("manifest-clock"),
                1,
                time,
                time,
                Bytes.sha256(Bytes.utf8("manifest-time-" + time)),
                0,
                null);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] result = new byte[length];
        for (int index = 0; index < length; index++) {
            result[index] = (byte) (seed + index);
        }
        return result;
    }

    private record Fixture(
            KeyPair keys,
            ArtifactGenerationSet artifacts,
            DataResetManifest.ManifestScope scope,
            ProtocolCapabilityDeclaration declaration,
            DataResetManifest manifest,
            WorkerAssignment assignment,
            DataResetActivationGate.StartupIdentity startup) {}
}
