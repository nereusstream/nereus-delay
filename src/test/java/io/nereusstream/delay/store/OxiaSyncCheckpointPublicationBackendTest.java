package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CheckpointResourceV1;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.CheckpointUploadStateV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.RecoveryCandidateKindV1;
import io.nereusstream.delay.protocol.RecoveryCandidateRefV1;
import io.nereusstream.delay.protocol.RecoveryFloorRefV1;
import io.nereusstream.delay.protocol.RecoveryPinV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OxiaSyncCheckpointPublicationBackendTest {
    private static final CheckpointManifestLimits LIMITS = new CheckpointManifestLimits(
            100, Long.MAX_VALUE, Long.MAX_VALUE, 4_096, 1 << 20, 100, 4_096);

    @Test
    void atomicallyBindsPublishedIntentAndCatalogManifest() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncCheckpointPublicationBackend backend = new OxiaSyncCheckpointPublicationBackend(records,
                "delay/publication", LIMITS);
        final CheckpointUploadIntentV1 seedPending = pending(1, null);
        final CheckpointManifest parent = parentManifest(seedPending);
        backend.publish(parent, 0);
        final CheckpointUploadIntentV1 pending = pending(1, parent);
        final CheckpointManifest manifest = manifest(pending, parent);
        final CheckpointResourceV1 resource = resource(pending, manifest);

        assertEquals(pending, backend.create(pending));
        final CheckpointUploadIntentV1 published = backend.publishUploadedCheckpointAtomically(pending, resource,
                manifest, 1);

        assertEquals(CheckpointUploadStateV1.PUBLISHED, published.state());
        assertEquals(published, backend.currentPublishedFor(pending).orElseThrow());
        assertArrayEquals(manifest.canonicalJsonBytes(),
                backend.manifest(manifest.checkpointId()).orElseThrow().canonicalJsonBytes());
        assertEquals(2, backend.publishUploadedCheckpoint(published, manifest, 1).catalogGeneration());
        assertEquals(3, records.putCount);
    }

    @Test
    void responseLossIsAcceptedOnlyAfterExactCombinedStateReread() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncCheckpointPublicationBackend backend = new OxiaSyncCheckpointPublicationBackend(records,
                "delay/publication-loss", LIMITS);
        final CheckpointUploadIntentV1 seedPending = pending(2, null);
        final CheckpointManifest parent = parentManifest(seedPending);
        backend.publish(parent, 0);
        final CheckpointUploadIntentV1 pending = pending(2, parent);
        final CheckpointManifest manifest = manifest(pending, parent);
        final CheckpointResourceV1 resource = resource(pending, manifest);
        backend.create(pending);

        records.failNextPutAfterCommit = true;
        final CheckpointUploadIntentV1 published = backend.publishUploadedCheckpointAtomically(pending, resource,
                manifest, 1);
        assertEquals(CheckpointUploadStateV1.PUBLISHED, published.state());
        assertEquals(published, backend.current(pending).orElseThrow());
        assertEquals(3, records.putCount);
    }

    @Test
    void rejectsASecondIntentOrManifestForTheSameAtomicIdentity() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncCheckpointPublicationBackend backend = new OxiaSyncCheckpointPublicationBackend(records,
                "delay/publication-conflict", LIMITS);
        final CheckpointUploadIntentV1 seedPending = pending(3, null);
        final CheckpointManifest parent = parentManifest(seedPending);
        backend.publish(parent, 0);
        final CheckpointUploadIntentV1 pending = pending(3, parent);
        backend.create(pending);

        final CheckpointUploadIntentV1 conflicting = new CheckpointUploadIntentV1(
                pending.shard(), pending.recoveryLineageId(), pending.checkpointId(), pending.owner(),
                pending.sourceStoreIncarnation(), id32(80), pending.baseCatalogGeneration(),
                pending.parentCheckpointId(), pending.parentManifestSha256(), pending.objectStoreProfile(),
                pending.checkpointCreatedAt(), pending.uploadDeadlineEpochMs(), CheckpointUploadStateV1.PENDING_UPLOAD,
                pending.stateRevision(), null, null);
        assertThrows(IllegalStateException.class, () -> backend.create(conflicting));
    }

    @Test
    void recoveryPinUsesASeparateEphemeralRecordAlongsideAtomicPublication() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncCheckpointPublicationBackend backend = new OxiaSyncCheckpointPublicationBackend(records,
                "delay/publication-pin", LIMITS);
        final CheckpointUploadIntentV1 seedPending = pending(4, null);
        final CheckpointManifest parent = parentManifest(seedPending);
        assertEquals(1, backend.publish(parent, 0).catalogGeneration());
        final RecoveryFloorRefV1 floor = backend.advanceFloor(parent.checkpointId(), 1,
                java.util.List.<io.nereusstream.delay.protocol.EvidenceCursorV1>of());
        final RecoveryPinV1 pin = pin(parent, floor, 81, 82);

        records.failNextPutAfterCommit = true;
        assertEquals(pin, backend.createRecoveryPin(pin));
        assertEquals(pin, backend.activeRecoveryPin().orElseThrow());

        final OxiaSyncCheckpointPublicationBackend reopened = new OxiaSyncCheckpointPublicationBackend(records,
                "delay/publication-pin", LIMITS);
        assertEquals(pin, reopened.activeRecoveryPin().orElseThrow());
        reopened.releaseRecoveryPin(pin);
        assertTrue(reopened.activeRecoveryPin().isEmpty());
    }

    private static CheckpointUploadIntentV1 pending(final int seed, final CheckpointManifest parent) {
        final ShardId shard = new ShardId(new RouteIncarnation(id16(seed + 1)), seed);
        return new CheckpointUploadIntentV1(new ShardSubjectV1(shard), id16(seed + 2), id16(seed + 3),
                new OwnerIdentityV1(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1, id32(seed + 4)),
                id16(seed + 5), id32(seed + 6), 1,
                parent == null ? null : parent.checkpointId(),
                parent == null ? null : parent.manifestSha256(),
                new ProfileRefV1(Bytes.utf8("checkpoint-store"), 1, id32(seed + 7), ProfileKindV1.OBJECT_STORE),
                evidence(1_000), 4_000, CheckpointUploadStateV1.PENDING_UPLOAD, 1, null, null);
    }

    private static CheckpointManifest parentManifest(final CheckpointUploadIntentV1 pending) {
        final ShardId shard = pending.shard().shardId();
        final KafkaSourcePosition position = new KafkaSourcePosition(shard, "cluster",
                uuid(pending.recoveryLineageId()), 0,
                null, 1_000);
        final CheckpointManifest.FileEntry file = new CheckpointManifest.FileEntry("CURRENT", 1, id32(39),
                Bytes.utf8("object/parent"), Bytes.utf8("version/parent"), null);
        return new CheckpointManifest(id16(70), pending.recoveryLineageId(), 0, null, null,
                new CheckpointManifest.CreatedBy(pending.owner().deploymentId(), pending.owner().workerRunId(),
                        pending.owner().ownerEpoch()),
                new CheckpointManifest.CreatedAt(900, 901, "CERTIFIED_HOST_CLOCK", id32(38), 1, 0, 0,
                        id32(37), 0, null), shard, id32(36), uuid(pending.sourceStoreIncarnation()), 1, 0, position,
                id32(35), id32(34), java.util.List.of(), java.util.List.of(file));
    }

    private static CheckpointManifest manifest(final CheckpointUploadIntentV1 pending,
                                               final CheckpointManifest parent) {
        final ShardId shard = pending.shard().shardId();
        final KafkaSourcePosition position = new KafkaSourcePosition(shard, "cluster",
                uuid(pending.recoveryLineageId()), 1,
                null, 1_001);
        final CheckpointManifest.FileEntry file = new CheckpointManifest.FileEntry("CURRENT", 1, id32(40),
                Bytes.utf8("object/current"), Bytes.utf8("version/current"), null);
        return new CheckpointManifest(pending.checkpointId(), pending.recoveryLineageId(), 1,
                new CheckpointManifest.ParentCheckpoint(parent.checkpointId(), Bytes.hex(parent.manifestSha256())), null,
                new CheckpointManifest.CreatedBy(pending.owner().deploymentId(), pending.owner().workerRunId(),
                        pending.owner().ownerEpoch()),
                new CheckpointManifest.CreatedAt(1_000, 1_001, "CERTIFIED_HOST_CLOCK", id32(41), 1, 1, 1,
                        id32(42), 0, null), shard, id32(43), uuid(pending.sourceStoreIncarnation()), 1, 1, position,
                id32(44), id32(45), java.util.List.of(), java.util.List.of(file));
    }

    private static CheckpointResourceV1 resource(final CheckpointUploadIntentV1 pending,
                                                 final CheckpointManifest manifest) {
        return new CheckpointResourceV1(pending.recoveryLineageId(), pending.checkpointId(),
                pending.objectStoreProfile(), Bytes.utf8("bucket"), Bytes.utf8("manifest"), Bytes.utf8("version"),
                manifest.canonicalJsonBytes().length, manifest.manifestSha256());
    }

    private static TrustedUtcIntervalEvidence evidence(final long earliest) {
        return new TrustedUtcIntervalEvidence(earliest, earliest + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 1, 1,
                id32(60), 0, null);
    }

    private static RecoveryPinV1 pin(final CheckpointManifest manifest, final RecoveryFloorRefV1 floor,
                                     final int pinId, final int ownerId) {
        final RecoveryCandidateRefV1 candidate = new RecoveryCandidateRefV1(
                RecoveryCandidateKindV1.CATALOG_CHECKPOINT, manifest.recoveryLineageId(), manifest.checkpointId(),
                manifest.manifestSha256(), null);
        return new RecoveryPinV1(id16(pinId), new ShardSubjectV1(manifest.shardId()),
                new OwnerIdentityV1(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1, id32(ownerId)), candidate,
                floor, floor.catalogGeneration(), fakeSessionIdentity());
    }

    private static UUID uuid(final byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return new UUID(buffer.getLong(), buffer.getLong());
    }

    private static byte[] id16(final int value) {
        final byte[] bytes = new byte[16];
        bytes[15] = (byte) value;
        return bytes;
    }

    private static byte[] id32(final int value) {
        final byte[] bytes = new byte[32];
        bytes[31] = (byte) value;
        return bytes;
    }

    private static byte[] fakeSessionIdentity() {
        return Bytes.sha256(Bytes.utf8("nereus-delay-oxia-session-identity-v1\0"), Bytes.u64be(101),
                Bytes.lp32(Bytes.utf8("fake-publication-session")));
    }

    private static final class FakeRecordClient implements OxiaSyncCheckpointPublicationBackend.RecordClient {
        private final Map<String, GetResult> records = new HashMap<>();
        private long nextVersion = 1;
        private int putCount;
        private boolean failNextPutAfterCommit;

        @Override
        public byte[] sessionIdentity() {
            return fakeSessionIdentity();
        }

        @Override
        public GetResult get(final String key) {
            return records.get(key);
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            final GetResult current = records.get(key);
            final OptionVersionId condition = options.stream()
                    .filter(OptionVersionId.class::isInstance)
                    .map(OptionVersionId.class::cast)
                    .findFirst().orElse(null);
            if (condition != null && condition.versionId() == OptionVersionId.KEY_NOT_EXISTS && current != null) {
                throw new KeyAlreadyExistsException(key);
            }
            if (condition != null && condition.versionId() != OptionVersionId.KEY_NOT_EXISTS
                    && (current == null || current.version().versionId() != condition.versionId())) {
                throw new UnexpectedVersionIdException(key,
                        current == null ? OptionVersionId.KEY_NOT_EXISTS : current.version().versionId());
            }
            final Version version = new Version(nextVersion++, 0, 0, 1, Optional.of(101L),
                    Optional.of("fake-publication-session"));
            records.put(key, new GetResult(key, Bytes.copy(value), version));
            putCount++;
            if (failNextPutAfterCommit) {
                failNextPutAfterCommit = false;
                throw new IllegalStateException("simulated response loss");
            }
            return new PutResult(key, version);
        }

        @Override
        public boolean delete(final String key, final Set<DeleteOption> options)
                throws UnexpectedVersionIdException {
            final GetResult current = records.get(key);
            if (current == null) {
                return false;
            }
            final OptionVersionId condition = options.stream()
                    .filter(OptionVersionId.class::isInstance)
                    .map(OptionVersionId.class::cast)
                    .findFirst().orElse(null);
            if (condition != null && (current.version() == null
                    || current.version().versionId() != condition.versionId())) {
                throw new UnexpectedVersionIdException(key, current.version() == null
                        ? OptionVersionId.KEY_NOT_EXISTS : current.version().versionId());
            }
            records.remove(key);
            return true;
        }
    }
}
