package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class OxiaSyncWorkerAssignmentBackendTest {
    @Test
    void publishRereadsAnExactRecordWhenThePutResponseIsLost() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncWorkerAssignmentBackend backend = new OxiaSyncWorkerAssignmentBackend(records, "delay/test");
        final WorkerAssignment assignment = assignment("worker-a", 1, "first");
        records.failNextPutAfterCommit = true;

        final WorkerAssignmentAuthority.Publication publication = backend.publish(assignment, 0);

        assertEquals(1, publication.revision());
        assertEquals(
                publication,
                backend.current(assignment.sourceAssignment().shardId()).orElseThrow());
        assertEquals(publication, backend.publish(assignment, 1));
    }

    @Test
    void sessionFenceRejectsACommittedAssignmentAfterTheMarkerChanges() {
        final FakeRecordClient records = new FakeRecordClient();
        final AtomicBoolean sessionAlive = new AtomicBoolean(true);
        final OxiaSyncWorkerAssignmentBackend backend =
                new OxiaSyncWorkerAssignmentBackend(records, "delay/fenced-assignment", () -> {
                    if (!sessionAlive.get()) {
                        throw new IllegalStateException("simulated Oxia session fence");
                    }
                });
        final WorkerAssignment assignment = assignment("worker-a", 1, "fenced");
        records.afterPut = () -> sessionAlive.set(false);

        assertThrows(IllegalStateException.class, () -> backend.publish(assignment, 0));

        final OxiaSyncWorkerAssignmentBackend reopened =
                new OxiaSyncWorkerAssignmentBackend(records, "delay/fenced-assignment");
        assertEquals(
                assignment,
                reopened.current(assignment.sourceAssignment().shardId())
                        .orElseThrow()
                        .assignment());
    }

    @Test
    void withdrawRereadsAbsenceWhenTheDeleteResponseIsLost() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncWorkerAssignmentBackend backend = new OxiaSyncWorkerAssignmentBackend(records, "delay/test");
        final WorkerAssignmentAuthority.Publication publication =
                backend.publish(assignment("worker-a", 1, "withdraw"), 0);
        records.failNextDeleteAfterCommit = true;

        assertTrue(backend.withdraw(publication));
        assertTrue(backend.current(publication.assignment().sourceAssignment().shardId())
                .isEmpty());
    }

    @Test
    void replacementRequiresAFreshEpochAndTheExpectedRevision() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncWorkerAssignmentBackend backend = new OxiaSyncWorkerAssignmentBackend(records, "delay/test");
        final WorkerAssignment first = assignment("worker-a", 1, "first");
        backend.publish(first, 0);

        assertThrows(
                IllegalStateException.class, () -> backend.publish(assignment("worker-b", 2, "stale-revision"), 0));
        assertThrows(IllegalArgumentException.class, () -> backend.publish(assignment("worker-b", 1, "same-epoch"), 1));
        assertEquals(2, backend.publish(assignment("worker-b", 2, "second"), 1).revision());
    }

    @Test
    void wrongResponseAndMalformedRecordFailClosed() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncWorkerAssignmentBackend backend = new OxiaSyncWorkerAssignmentBackend(records, "delay/test");
        final WorkerAssignment assignment = assignment("worker-a", 1, "wrong-response");
        records.wrongKeyOnNextPut = true;
        records.wrongKeyOnNextGet = true;
        assertThrows(IllegalStateException.class, () -> backend.publish(assignment, 0));

        final WorkerAssignment malformed = assignment("worker-a", 1, "malformed");
        records.putRaw(assignmentKey(malformed.sourceAssignment().shardId()), new byte[] {1, 2, 3});
        assertThrows(
                IllegalStateException.class,
                () -> backend.current(malformed.sourceAssignment().shardId()));
    }

    private static WorkerAssignment assignment(final String workerId, final long epoch, final String seed) {
        final ShardId shard =
                new ShardId(RouteIncarnation.fromUuid(UUID.fromString("10213243-5465-7687-98a9-bacbdcedfe0f")), 2);
        final SourceAssignment source = new SourceAssignment(
                shard,
                Bytes.sha256(Bytes.utf8("source")),
                1,
                new KafkaActivationBarrier(
                        shard, "cluster-a", UUID.fromString("00112233-4455-6677-8899-aabbccddeeff"), 0));
        return new WorkerAssignment(workerId, source, epoch, Bytes.sha256(Bytes.utf8(seed)));
    }

    private static String assignmentKey(final ShardId shard) {
        return "delay/test/assignment/"
                + Bytes.hex(Bytes.concat(shard.routeIncarnation().bytes(), Bytes.u32beBits(shard.partition())));
    }

    private static final class FakeRecordClient implements OxiaSyncWorkerAssignmentBackend.RecordClient {
        private final Map<String, Entry> records = new HashMap<>();
        private long nextVersion = 1;
        private boolean failNextPutAfterCommit;
        private boolean failNextDeleteAfterCommit;
        private boolean wrongKeyOnNextGet;
        private boolean wrongKeyOnNextPut;
        private Runnable afterPut = () -> {};

        @Override
        public GetResult get(final String key) {
            final Entry entry = records.get(key);
            if (entry == null) {
                return null;
            }
            if (wrongKeyOnNextGet) {
                wrongKeyOnNextGet = false;
                return new GetResult(key + "/wrong", Bytes.copy(entry.result.value()), entry.result.version());
            }
            return entry.result;
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            final Entry current = records.get(key);
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
                    && (current == null || current.result.version().versionId() != condition.versionId())) {
                throw new UnexpectedVersionIdException(
                        key,
                        current == null
                                ? OptionVersionId.KEY_NOT_EXISTS
                                : current.result.version().versionId());
            }
            final Version version = new Version(nextVersion++, 0, 0, 1, Optional.empty(), Optional.empty());
            final GetResult result = new GetResult(key, Bytes.copy(value), version);
            records.put(key, new Entry(result));
            afterPut.run();
            if (failNextPutAfterCommit) {
                failNextPutAfterCommit = false;
                throw new IllegalStateException("simulated response loss");
            }
            if (wrongKeyOnNextPut) {
                wrongKeyOnNextPut = false;
                return new PutResult(key + "/wrong", version);
            }
            return new PutResult(key, version);
        }

        @Override
        public boolean delete(final String key, final Set<DeleteOption> options) throws UnexpectedVersionIdException {
            final Entry current = records.get(key);
            final OptionVersionId condition = options.stream()
                    .filter(OptionVersionId.class::isInstance)
                    .map(OptionVersionId.class::cast)
                    .findFirst()
                    .orElse(null);
            if (condition != null
                    && (current == null || current.result.version().versionId() != condition.versionId())) {
                throw new UnexpectedVersionIdException(
                        key,
                        current == null
                                ? OptionVersionId.KEY_NOT_EXISTS
                                : current.result.version().versionId());
            }
            final boolean removed = records.remove(key) != null;
            if (removed && failNextDeleteAfterCommit) {
                failNextDeleteAfterCommit = false;
                throw new IllegalStateException("simulated response loss");
            }
            return removed;
        }

        private void putRaw(final String key, final byte[] value) {
            final Version version = new Version(nextVersion++, 0, 0, 1, Optional.empty(), Optional.empty());
            records.put(key, new Entry(new GetResult(key, Bytes.copy(value), version)));
        }

        private record Entry(GetResult result) {}
    }
}
