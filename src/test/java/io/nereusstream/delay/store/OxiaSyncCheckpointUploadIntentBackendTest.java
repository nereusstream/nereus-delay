package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CheckpointResourceV1;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.CheckpointUploadStateV1;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OxiaSyncCheckpointUploadIntentBackendTest {
    @Test
    void persistsPendingPublishedAndReapingSuccessorsWithExactCas() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncCheckpointUploadIntentBackend backend =
                new OxiaSyncCheckpointUploadIntentBackend(records, "delay/upload");
        final CheckpointUploadIntentV1 pending = pending(1);

        assertEquals(pending, backend.create(pending));
        final CheckpointResourceV1 resource = resource(pending);
        final CheckpointUploadIntentV1 published = backend.publish(pending, resource);
        assertEquals(CheckpointUploadStateV1.PUBLISHED, published.state());
        assertEquals(published, backend.currentPublishedFor(pending).orElseThrow());

        final CheckpointUploadIntentV1 pendingReaping = pending(2);
        backend.create(pendingReaping);
        final TrustedUtcIntervalEvidence evidence = evidence(5_000);
        final CheckpointUploadIntentV1 reaping = backend.beginReaping(pendingReaping, evidence);
        assertEquals(CheckpointUploadStateV1.REAPING, reaping.state());
        assertEquals(reaping, backend.current(pendingReaping).orElseThrow());
    }

    @Test
    void responseLossAndCorruptionFailClosedExceptExactReread() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncCheckpointUploadIntentBackend backend =
                new OxiaSyncCheckpointUploadIntentBackend(records, "delay/lost-upload");
        final CheckpointUploadIntentV1 pending = pending(3);
        records.failNextPutAfterCommit = true;
        assertEquals(pending, backend.create(pending));
        assertEquals(pending, backend.current(pending).orElseThrow());

        records.putRaw("delay/lost-upload/intent/" + keyToken(pending), new byte[]{0x08, 0x02});
        assertThrows(IllegalStateException.class, () -> backend.current(pending));
    }

    @Test
    void sessionFenceRejectsACommittedIntentAfterTheMarkerChanges() {
        final FakeRecordClient records = new FakeRecordClient();
        final AtomicBoolean sessionAlive = new AtomicBoolean(true);
        final OxiaSyncCheckpointUploadIntentBackend backend =
                new OxiaSyncCheckpointUploadIntentBackend(records, "delay/fenced-upload", () -> {
                    if (!sessionAlive.get()) {
                        throw new IllegalStateException("simulated Oxia session fence");
                    }
                });
        final CheckpointUploadIntentV1 pending = pending(5);
        records.afterPut = () -> sessionAlive.set(false);

        assertThrows(IllegalStateException.class, () -> backend.create(pending));

        final OxiaSyncCheckpointUploadIntentBackend reopened =
                new OxiaSyncCheckpointUploadIntentBackend(records, "delay/fenced-upload");
        assertEquals(pending, reopened.current(pending).orElseThrow());
    }

    @Test
    void expiredEvidenceAndWrongSuccessorDoNotAdvanceState() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncCheckpointUploadIntentBackend backend =
                new OxiaSyncCheckpointUploadIntentBackend(records, "delay/strict-upload");
        final CheckpointUploadIntentV1 pending = pending(4);
        backend.create(pending);
        assertThrows(IllegalArgumentException.class, () -> backend.beginReaping(pending, evidence(999)));
        assertEquals(CheckpointUploadStateV1.PENDING_UPLOAD, backend.current(pending).orElseThrow().state());
        assertTrue(backend.currentPublishedFor(pending).isEmpty());
    }

    private static CheckpointUploadIntentV1 pending(final int seed) {
        final ShardId shard = new ShardId(new RouteIncarnation(id16(seed + 1)), seed);
        return new CheckpointUploadIntentV1(new ShardSubjectV1(shard), id16(seed + 2), id16(seed + 3),
                new OwnerIdentityV1(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1, id32(seed + 4)),
                id16(seed + 5), id32(seed + 6), 1, null, null,
                new ProfileRefV1(Bytes.utf8("checkpoint-store"), 1, id32(seed + 7), ProfileKindV1.OBJECT_STORE),
                evidence(1_000), 4_000, CheckpointUploadStateV1.PENDING_UPLOAD, 1, null, null);
    }

    private static CheckpointResourceV1 resource(final CheckpointUploadIntentV1 pending) {
        return new CheckpointResourceV1(pending.recoveryLineageId(), pending.checkpointId(),
                pending.objectStoreProfile(), Bytes.utf8("bucket"), Bytes.utf8("checkpoint/manifest"),
                Bytes.utf8("version"), 10, id32(20));
    }

    private static TrustedUtcIntervalEvidence evidence(final long earliest) {
        return new TrustedUtcIntervalEvidence(earliest, earliest + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 1, 1,
                id32(30), 0, null);
    }

    private static String keyToken(final CheckpointUploadIntentV1 intent) {
        return Bytes.hex(Bytes.concat(intent.shard().canonicalHashBytes(), intent.checkpointId()));
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

    private static final class FakeRecordClient implements OxiaSyncCheckpointUploadIntentBackend.RecordClient {
        private final Map<String, GetResult> records = new HashMap<>();
        private long nextVersion = 1;
        private boolean failNextPutAfterCommit;
        private Runnable afterPut = () -> {
        };

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
            final Version version = new Version(nextVersion++, 0, 0, 1, Optional.empty(), Optional.empty());
            records.put(key, new GetResult(key, Bytes.copy(value), version));
            afterPut.run();
            if (failNextPutAfterCommit) {
                failNextPutAfterCommit = false;
                throw new IllegalStateException("simulated response loss");
            }
            return new PutResult(key, version);
        }

        private void putRaw(final String key, final byte[] value) {
            final Version version = new Version(nextVersion++, 0, 0, 1, Optional.empty(), Optional.empty());
            records.put(key, new GetResult(key, Bytes.copy(value), version));
        }
    }
}
