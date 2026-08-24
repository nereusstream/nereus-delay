package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class OxiaSyncOwnerLeaseBackendTest {
    @Test
    void connectRejectsAnInvalidKeyPrefixBeforeCreatingAnOxiaClient() {
        assertThrows(
                IllegalArgumentException.class,
                () -> OxiaSyncOwnerLeaseBackend.connect(
                        "invalid-endpoint", "default", "owner-client", Duration.ofSeconds(1), "delay/owner/"));
    }

    @Test
    void contextBoundCasUsesDurableEpochAndEphemeralSessionIdentity() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncOwnerLeaseBackend backend = new OxiaSyncOwnerLeaseBackend(records, "delay/test");
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final SourceAssignment assignment = new SourceAssignment(
                shard,
                Bytes.sha256(Bytes.utf8("assignment")),
                4,
                new KafkaActivationBarrier(shard, "cluster", UUID.randomUUID(), 0));
        final byte[] session = OxiaSyncOwnerLeaseBackend.sessionIdentity(records.ephemeralVersion());

        final OwnerLease acquired =
                backend.acquire(assignment, "worker-a", session, 100, 50).orElseThrow();
        assertEquals(1, acquired.ownerEpoch());
        assertEquals(ShardLifecycleState.ACQUIRING, acquired.state());
        assertArrayEquals(session, acquired.sessionIdentity());
        assertTrue(backend.acquire(assignment, "worker-b", session, 101, 50).isEmpty());

        final OwnerLease renewed = backend.renew(acquired, 110, 50).orElseThrow();
        assertEquals(160, renewed.expiresAtEpochMs());
        final OwnerLease restoring =
                backend.transition(renewed, ShardLifecycleState.RESTORING).orElseThrow();
        assertEquals(ShardLifecycleState.RESTORING, restoring.state());
        assertTrue(backend.release(restoring));
        assertTrue(backend.current(shard).isEmpty());

        final OwnerLease reacquired =
                backend.acquire(assignment, "worker-b", session, 200, 50).orElseThrow();
        assertEquals(2, reacquired.ownerEpoch());
        assertTrue(reacquired.ownerEpoch() > acquired.ownerEpoch());
    }

    @Test
    void acquireResponseLossRereadsExactCommittedEphemeralLease() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncOwnerLeaseBackend backend = new OxiaSyncOwnerLeaseBackend(records, "delay/test");
        final ShardId shard = new ShardId(RouteIncarnation.random(), 11);
        records.failNextPutAfterCommit = true;

        final OwnerLease acquired =
                backend.acquire(shard, "worker-response-loss", 100, 50).orElseThrow();

        assertEquals(1, acquired.ownerEpoch());
        assertEquals(150, acquired.expiresAtEpochMs());
        assertTrue(acquired.sameIdentity(backend.current(shard).orElseThrow()));
    }

    @Test
    void sessionFenceRejectsACommittedLeaseAfterTheMarkerChanges() {
        final FakeRecordClient records = new FakeRecordClient();
        final AtomicBoolean sessionAlive = new AtomicBoolean(true);
        final OxiaSyncOwnerLeaseBackend backend = new OxiaSyncOwnerLeaseBackend(records, "delay/fenced-owner", () -> {
            if (!sessionAlive.get()) {
                throw new IllegalStateException("simulated Oxia session fence");
            }
        });
        final ShardId shard = new ShardId(RouteIncarnation.random(), 20);
        records.afterPut = () -> sessionAlive.set(false);

        assertThrows(IllegalStateException.class, () -> backend.acquire(shard, "worker-fenced", 100, 50));

        final OxiaSyncOwnerLeaseBackend reopened = new OxiaSyncOwnerLeaseBackend(records, "delay/fenced-owner");
        assertTrue(reopened.current(shard).isPresent());
    }

    @Test
    void renewalResponseLossRereadsExactCommittedSuccessor() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncOwnerLeaseBackend backend = new OxiaSyncOwnerLeaseBackend(records, "delay/test");
        final ShardId shard = new ShardId(RouteIncarnation.random(), 12);
        final OwnerLease acquired =
                backend.acquire(shard, "worker-renew-loss", 100, 50).orElseThrow();
        records.failNextPutAfterCommit = true;

        final OwnerLease renewed = backend.renew(acquired, 110, 60).orElseThrow();

        assertEquals(170, renewed.expiresAtEpochMs());
        assertTrue(renewed.sameIdentity(backend.current(shard).orElseThrow()));
    }

    @Test
    void transitionResponseLossRereadsExactCommittedSuccessor() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncOwnerLeaseBackend backend = new OxiaSyncOwnerLeaseBackend(records, "delay/test");
        final ShardId shard = new ShardId(RouteIncarnation.random(), 13);
        final OwnerLease acquired =
                backend.acquire(shard, "worker-transition-loss", 100, 50).orElseThrow();
        records.failNextPutAfterCommit = true;

        final OwnerLease restoring =
                backend.transition(acquired, ShardLifecycleState.RESTORING).orElseThrow();

        assertEquals(ShardLifecycleState.RESTORING, restoring.state());
        assertTrue(restoring.sameIdentity(backend.current(shard).orElseThrow()));
    }

    @Test
    void releaseResponseLossRereadsAbsenceAfterCommittedDelete() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncOwnerLeaseBackend backend = new OxiaSyncOwnerLeaseBackend(records, "delay/test");
        final ShardId shard = new ShardId(RouteIncarnation.random(), 14);
        final OwnerLease acquired =
                backend.acquire(shard, "worker-release-loss", 100, 50).orElseThrow();
        records.failNextDeleteAfterCommit = true;

        assertTrue(backend.release(acquired));
        assertTrue(backend.current(shard).isEmpty());
    }

    @Test
    void wrongSessionIdentityFailsClosedAndCleansTheEphemeralRecord() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncOwnerLeaseBackend backend = new OxiaSyncOwnerLeaseBackend(records, "delay/test");
        final ShardId shard = new ShardId(RouteIncarnation.random(), 8);
        final SourceAssignment assignment = new SourceAssignment(
                shard,
                Bytes.sha256(Bytes.utf8("assignment-2")),
                1,
                new KafkaActivationBarrier(shard, "cluster", UUID.randomUUID(), 0));

        assertThrows(
                IllegalStateException.class,
                () -> backend.acquire(assignment, "worker-a", Bytes.sha256(Bytes.utf8("wrong")), 100, 50));
        assertTrue(backend.current(shard).isEmpty());
    }

    @Test
    void malformedEpochAndNonCanonicalLeaseAreRejected() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncOwnerLeaseBackend backend = new OxiaSyncOwnerLeaseBackend(records, "delay/test");
        final ShardId shard = new ShardId(RouteIncarnation.random(), 9);
        records.putRaw("delay/test/epoch/" + shardToken(shard), new byte[8]);
        assertThrows(IllegalStateException.class, () -> backend.acquire(shard, "worker-a", 100, 50));
    }

    @Test
    void epochReadRejectsARecordWithoutExactIdentityOrVersion() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncOwnerLeaseBackend backend = new OxiaSyncOwnerLeaseBackend(records, "delay/test");
        final ShardId shard = new ShardId(RouteIncarnation.random(), 15);
        final String epochKey = "delay/test/epoch/" + shardToken(shard);
        records.putRawWithoutVersion(epochKey, Bytes.u64be(1));

        assertThrows(IllegalStateException.class, () -> backend.acquire(shard, "worker-a", 100, 50));
    }

    @Test
    void leaseReadRejectsAResponseForAnotherRecordKey() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncOwnerLeaseBackend backend = new OxiaSyncOwnerLeaseBackend(records, "delay/test");
        final ShardId shard = new ShardId(RouteIncarnation.random(), 16);
        backend.acquire(shard, "worker-a", 100, 50).orElseThrow();
        records.wrongKeyOnNextGet = true;

        assertThrows(IllegalStateException.class, () -> backend.current(shard));
        assertTrue(records.records.containsKey("delay/test/lease/" + shardToken(shard)));
    }

    @Test
    void leaseWriteRejectsWrongResponseAndWrongRereadIdentity() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncOwnerLeaseBackend backend = new OxiaSyncOwnerLeaseBackend(records, "delay/test");
        final ShardId shard = new ShardId(RouteIncarnation.random(), 17);
        records.wrongKeyOnNextPut = true;
        records.wrongKeyOnNextGet = true;

        assertThrows(IllegalStateException.class, () -> backend.acquire(shard, "worker-a", 100, 50));
        assertTrue(backend.current(shard).isEmpty());
    }

    @Test
    void epochCreateResponseLossUsesOnlyAnExactCommittedReread() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncOwnerLeaseBackend backend = new OxiaSyncOwnerLeaseBackend(records, "delay/test");
        final ShardId shard = new ShardId(RouteIncarnation.random(), 18);
        records.failNextEpochPutAfterCommit = true;

        final OwnerLease acquired =
                backend.acquire(shard, "worker-epoch-create-loss", 100, 50).orElseThrow();

        assertEquals(1, acquired.ownerEpoch());
    }

    @Test
    void epochUpdateResponseLossUsesOnlyAnExactCommittedReread() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncOwnerLeaseBackend backend = new OxiaSyncOwnerLeaseBackend(records, "delay/test");
        final ShardId shard = new ShardId(RouteIncarnation.random(), 19);
        final OwnerLease first =
                backend.acquire(shard, "worker-epoch-update-loss", 100, 50).orElseThrow();
        assertTrue(backend.release(first));
        records.failNextEpochPutAfterCommit = true;

        final OwnerLease reacquired =
                backend.acquire(shard, "worker-epoch-update-loss-2", 200, 50).orElseThrow();

        assertEquals(2, reacquired.ownerEpoch());
    }

    @Test
    void ownerEpochUsesTheCompleteUnsigned64Domain() {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncOwnerLeaseBackend backend = new OxiaSyncOwnerLeaseBackend(records, "delay/test");
        final ShardId shard = new ShardId(RouteIncarnation.random(), 10);
        records.putRaw("delay/test/epoch/" + shardToken(shard), Bytes.u64beBits(Long.MAX_VALUE));

        final OwnerLease acquired = backend.acquire(shard, "worker-a", 100, 50).orElseThrow();
        assertEquals(Long.MIN_VALUE, acquired.ownerEpoch());
    }

    private static String shardToken(final ShardId shard) {
        return Bytes.hex(Bytes.concat(shard.routeIncarnation().bytes(), Bytes.u32beBits(shard.partition())));
    }

    private static final class FakeRecordClient implements OxiaSyncOwnerLeaseBackend.RecordClient {
        private final Map<String, Entry> records = new HashMap<>();
        private long nextVersion = 1;
        private boolean failNextPutAfterCommit;
        private boolean failNextDeleteAfterCommit;
        private boolean wrongKeyOnNextGet;
        private boolean wrongKeyOnNextPut;
        private boolean failNextEpochPutAfterCommit;
        private Runnable afterPut;

        private Version ephemeralVersion() {
            return new Version(1, 0, 0, 1, Optional.of(7L), Optional.of("worker-session"));
        }

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
            if (condition != null) {
                if (condition.versionId() == OptionVersionId.KEY_NOT_EXISTS && current != null) {
                    throw new KeyAlreadyExistsException(key);
                }
                if (condition.versionId() != OptionVersionId.KEY_NOT_EXISTS
                        && (current == null || current.result.version().versionId() != condition.versionId())) {
                    throw new UnexpectedVersionIdException(
                            key,
                            current == null
                                    ? OptionVersionId.KEY_NOT_EXISTS
                                    : current.result.version().versionId());
                }
            }
            final boolean ephemeral = options.stream().anyMatch(option -> option == PutOption.AsEphemeralRecord);
            final Version version = new Version(
                    nextVersion++,
                    0,
                    0,
                    1,
                    ephemeral ? Optional.of(7L) : Optional.empty(),
                    ephemeral ? Optional.of("worker-session") : Optional.empty());
            final GetResult result = new GetResult(key, Bytes.copy(value), version);
            records.put(key, new Entry(result));
            if (afterPut != null && ephemeral) {
                final Runnable callback = afterPut;
                afterPut = null;
                callback.run();
            }
            if (failNextPutAfterCommit && ephemeral) {
                failNextPutAfterCommit = false;
                throw new IllegalStateException("simulated response loss");
            }
            if (wrongKeyOnNextPut && ephemeral) {
                wrongKeyOnNextPut = false;
                return new PutResult(key + "/wrong", version);
            }
            if (failNextEpochPutAfterCommit && !ephemeral) {
                failNextEpochPutAfterCommit = false;
                throw new IllegalStateException("simulated epoch response loss");
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
            records.put(key, new Entry(new GetResult(key, value, version)));
        }

        private void putRawWithoutVersion(final String key, final byte[] value) {
            records.put(key, new Entry(new GetResult(key, value, null)));
        }

        private record Entry(GetResult result) {}
    }
}
