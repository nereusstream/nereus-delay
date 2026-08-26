package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointResource;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class OxiaSyncCheckpointUploadIntentBackendTest {
    @Test
    void persistsPendingPublishedAndReapingSuccessorsWithExactCas() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncCheckpointUploadIntentBackend backend =
                new OxiaSyncCheckpointUploadIntentBackend(records, "delay/upload");
        final CheckpointUploadIntent pending = pending(1);

        assertEquals(pending, backend.create(pending));
        final CheckpointResource resource = resource(pending);
        final CheckpointUploadIntent published = backend.publish(pending, resource);
        assertEquals(CheckpointUploadState.PUBLISHED, published.state());
        assertEquals(published, backend.currentPublishedFor(pending).orElseThrow());

        final CheckpointUploadIntent pendingReaping = pending(2);
        backend.create(pendingReaping);
        final TrustedUtcIntervalEvidence evidence = evidence(5_000);
        final CheckpointUploadIntent reaping = backend.beginReaping(pendingReaping, evidence);
        assertEquals(CheckpointUploadState.REAPING, reaping.state());
        assertEquals(reaping, backend.current(pendingReaping).orElseThrow());
    }

    @Test
    void responseLossAndCorruptionFailClosedExceptExactReread() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncCheckpointUploadIntentBackend backend =
                new OxiaSyncCheckpointUploadIntentBackend(records, "delay/lost-upload");
        final CheckpointUploadIntent pending = pending(3);
        records.failNextPutAfterCommit = true;
        assertEquals(pending, backend.create(pending));
        assertEquals(pending, backend.current(pending).orElseThrow());

        records.putRaw("delay/lost-upload/intent/" + keyToken(pending), new byte[] {0x08, 0x02});
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
        final CheckpointUploadIntent pending = pending(5);
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
        final CheckpointUploadIntent pending = pending(4);
        backend.create(pending);
        assertThrows(IllegalArgumentException.class, () -> backend.beginReaping(pending, evidence(999)));
        assertEquals(
                CheckpointUploadState.PENDING_UPLOAD,
                backend.current(pending).orElseThrow().state());
        assertTrue(backend.currentPublishedFor(pending).isEmpty());
    }

    private static CheckpointUploadIntent pending(final int seed) {
        final ShardId shard = new ShardId(new RouteIncarnation(id16(seed + 1)), seed);
        return new CheckpointUploadIntent(
                new ShardSubject(shard),
                id16(seed + 2),
                id16(seed + 3),
                new OwnerIdentity(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1, id32(seed + 4)),
                id16(seed + 5),
                id32(seed + 6),
                1,
                null,
                null,
                new ProfileRef(Bytes.utf8("checkpoint-store"), 1, id32(seed + 7), ProfileKind.OBJECT_STORE),
                evidence(1_000),
                4_000,
                CheckpointUploadState.PENDING_UPLOAD,
                1,
                null,
                null);
    }

    private static CheckpointResource resource(final CheckpointUploadIntent pending) {
        return new CheckpointResource(
                pending.recoveryLineageId(),
                pending.checkpointId(),
                pending.objectStoreProfile(),
                Bytes.utf8("bucket"),
                Bytes.utf8("checkpoint/manifest"),
                Bytes.utf8("version"),
                10,
                id32(20));
    }

    private static TrustedUtcIntervalEvidence evidence(final long earliest) {
        return new TrustedUtcIntervalEvidence(
                earliest,
                earliest + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("clock"),
                1,
                1,
                1,
                id32(30),
                0,
                null);
    }

    private static String keyToken(final CheckpointUploadIntent intent) {
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
        private Runnable afterPut = () -> {};

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
                    .findFirst()
                    .orElse(null);
            if (condition != null && condition.versionId() == OptionVersionId.KEY_NOT_EXISTS && current != null) {
                throw new KeyAlreadyExistsException(key);
            }
            if (condition != null
                    && condition.versionId() != OptionVersionId.KEY_NOT_EXISTS
                    && (current == null || current.version().versionId() != condition.versionId())) {
                throw new UnexpectedVersionIdException(
                        key,
                        current == null
                                ? OptionVersionId.KEY_NOT_EXISTS
                                : current.version().versionId());
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
