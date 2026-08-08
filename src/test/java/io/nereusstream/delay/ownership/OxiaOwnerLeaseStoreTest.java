package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OxiaOwnerLeaseStoreTest {
    @Test
    void contextBoundLeaseCarriesAssignmentSessionAndLifecycleCas() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 10);
        final SourceAssignment assignment = new SourceAssignment(shard,
                Bytes.sha256(Bytes.utf8("source-assignment")), 3,
                new KafkaActivationBarrier(shard, "cluster", UUID.randomUUID(), 0));
        final byte[] session = Bytes.sha256(Bytes.utf8("oxia-session"));
        final OxiaOwnerLeaseStore store = new OxiaOwnerLeaseStore(new InMemoryOwnerLeaseStore());

        final OwnerLease acquired = store.acquire(assignment, "worker-a", session, 100, 50).orElseThrow();
        assertArrayEquals(assignment.assignmentId(), acquired.sourceAssignmentId());
        assertEquals(assignment.assignmentEpoch(), acquired.sourceAssignmentEpoch());
        assertArrayEquals(session, acquired.sessionIdentity());
        assertEquals(ShardLifecycleState.ACQUIRING, acquired.state());

        final OwnerLease renewed = store.renew(acquired, 110, 50).orElseThrow();
        assertEquals(acquired.context(), renewed.context());
        assertEquals(acquired.state(), renewed.state());
        final OwnerLease restoring = store.transition(renewed, ShardLifecycleState.RESTORING).orElseThrow();
        assertEquals(ShardLifecycleState.RESTORING, restoring.state());
        assertTrue(store.transition(restoring, ShardLifecycleState.ACQUIRING).isEmpty());
        assertTrue(store.transition(renewed, ShardLifecycleState.CATCHING_UP).isEmpty());
        assertFalse(store.acquire(assignment, "worker-b", session, 120, 50).isPresent());
    }

    @Test
    void backendWithoutContextBoundAcquireCannotAllocateAShardOnlyLease() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 11);
        final SourceAssignment assignment = new SourceAssignment(shard,
                Bytes.sha256(Bytes.utf8("backend-assignment")), 1,
                new KafkaActivationBarrier(shard, "cluster", UUID.randomUUID(), 0));
        final AtomicBoolean shardOnlyAcquireCalled = new AtomicBoolean();
        final OxiaOwnerLeaseStore.LeaseCasBackend backend = new OxiaOwnerLeaseStore.LeaseCasBackend() {
            @Override
            public Optional<OwnerLease> acquire(final ShardId ignored, final String ownerId,
                                                 final long nowEpochMs, final long leaseDurationMs) {
                shardOnlyAcquireCalled.set(true);
                return Optional.of(new OwnerLease(ignored, ownerId, 1, nonZero(32, 10),
                        nowEpochMs + leaseDurationMs));
            }

            @Override
            public Optional<OwnerLease> renew(final OwnerLease expected, final long nowEpochMs,
                                              final long leaseDurationMs) {
                return Optional.empty();
            }

            @Override
            public boolean release(final OwnerLease expected) {
                return false;
            }

            @Override
            public Optional<OwnerLease> current(final ShardId requested) {
                return Optional.empty();
            }
        };

        assertTrue(new OxiaOwnerLeaseStore(backend)
                .acquire(assignment, "worker-a", nonZero(32, 11), 100, 100).isEmpty());
        assertFalse(shardOnlyAcquireCalled.get());
    }

    @Test
    void delegatesCasAndPreservesFencedIdentity() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final OxiaOwnerLeaseStore store = new OxiaOwnerLeaseStore(new InMemoryOwnerLeaseStore());
        final OwnerLease first = store.acquire(shard, "worker-a", 100, 20).orElseThrow();
        final OwnerLease renewed = store.renew(first, 110, 20).orElseThrow();

        assertEquals(first.ownerEpoch(), renewed.ownerEpoch());
        assertTrue(store.current(shard).isPresent());
        assertTrue(store.release(renewed));
        assertTrue(store.current(shard).isEmpty());
    }

    @Test
    void rejectsBackendAcquireResultForAnotherOwner() {
        final ShardId requested = new ShardId(RouteIncarnation.random(), 1);
        final ShardId returned = new ShardId(RouteIncarnation.random(), 2);
        final OxiaOwnerLeaseStore.LeaseCasBackend backend = new OxiaOwnerLeaseStore.LeaseCasBackend() {
            @Override
            public Optional<OwnerLease> acquire(final ShardId shardId, final String ownerId, final long now,
                                                 final long duration) {
                return Optional.of(new OwnerLease(returned, "other", 1, nonZero(32, 1), now + duration));
            }

            @Override
            public Optional<OwnerLease> renew(final OwnerLease expected, final long now, final long duration) {
                return Optional.empty();
            }

            @Override
            public boolean release(final OwnerLease expected) {
                return false;
            }

            @Override
            public Optional<OwnerLease> current(final ShardId shardId) {
                return Optional.empty();
            }
        };
        final OxiaOwnerLeaseStore store = new OxiaOwnerLeaseStore(backend);
        assertThrows(IllegalStateException.class, () -> store.acquire(requested, "worker-a", 100, 20));
    }

    @Test
    void rejectsBackendAcquireResultThatSkipsAcquiringState() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 19);
        final OxiaOwnerLeaseStore.LeaseCasBackend backend = new OxiaOwnerLeaseStore.LeaseCasBackend() {
            @Override
            public Optional<OwnerLease> acquire(final ShardId ignored, final String ownerId, final long now,
                                                 final long duration) {
                return Optional.of(new OwnerLease(shard, ownerId, 1, nonZero(32, 12), now + duration,
                        null, ShardLifecycleState.RESTORING));
            }

            @Override
            public Optional<OwnerLease> renew(final OwnerLease ignored, final long now, final long duration) {
                return Optional.empty();
            }

            @Override
            public boolean release(final OwnerLease ignored) {
                return false;
            }

            @Override
            public Optional<OwnerLease> current(final ShardId ignored) {
                return Optional.empty();
            }
        };

        assertThrows(IllegalStateException.class,
                () -> new OxiaOwnerLeaseStore(backend).acquire(shard, "worker-a", 100, 20));
    }

    @Test
    void rejectsRenewalThatChangesEpochOrToken() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final OwnerLease expected = new OwnerLease(shard, "worker-a", 4, nonZero(32, 2), 200);
        final OxiaOwnerLeaseStore.LeaseCasBackend backend = new OxiaOwnerLeaseStore.LeaseCasBackend() {
            @Override
            public Optional<OwnerLease> acquire(final ShardId ignored, final String ownerId, final long now,
                                                 final long duration) {
                return Optional.empty();
            }

            @Override
            public Optional<OwnerLease> renew(final OwnerLease ignored, final long now, final long duration) {
                return Optional.of(new OwnerLease(shard, "worker-a", 5, nonZero(32, 2), now + duration));
            }

            @Override
            public boolean release(final OwnerLease ignored) {
                return false;
            }

            @Override
            public Optional<OwnerLease> current(final ShardId ignored) {
                return Optional.empty();
            }
        };
        final OxiaOwnerLeaseStore store = new OxiaOwnerLeaseStore(backend);
        assertThrows(IllegalStateException.class, () -> store.renew(expected, 150, 20));
    }

    @Test
    void rejectsRenewalThatChangesLifecycleState() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final OwnerLease expected = new OwnerLease(shard, "worker-a", 4, nonZero(32, 3), 200,
                null, ShardLifecycleState.RESTORING);
        final OxiaOwnerLeaseStore.LeaseCasBackend backend = new OxiaOwnerLeaseStore.LeaseCasBackend() {
            @Override
            public Optional<OwnerLease> acquire(final ShardId ignored, final String ownerId, final long now,
                                                 final long duration) {
                return Optional.empty();
            }

            @Override
            public Optional<OwnerLease> renew(final OwnerLease ignored, final long now, final long duration) {
                return Optional.of(new OwnerLease(shard, "worker-a", 4, nonZero(32, 3), now + duration,
                        null, ShardLifecycleState.FENCED));
            }

            @Override
            public boolean release(final OwnerLease ignored) {
                return false;
            }

            @Override
            public Optional<OwnerLease> current(final ShardId ignored) {
                return Optional.empty();
            }
        };
        final OxiaOwnerLeaseStore store = new OxiaOwnerLeaseStore(backend);
        assertThrows(IllegalStateException.class, () -> store.renew(expected, 150, 20));
    }

    @Test
    void transitionOrReadAcceptsOnlyExactSuccessorAfterResponseLoss() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 16);
        final OwnerLease acquiring = new OwnerLease(shard, "worker-response-loss", 1, nonZero(32, 7), 200);
        final OwnerLease active = new OwnerLease(shard, "worker-response-loss", 1, acquiring.leaseToken(), 200,
                acquiring.context(), ShardLifecycleState.ACTIVE_FOR_COMMANDS);
        final class Backend implements OxiaOwnerLeaseStore.LeaseCasBackend {
            private OwnerLease current = acquiring;

            @Override
            public Optional<OwnerLease> acquire(final ShardId ignored, final String ownerId, final long now,
                                                final long duration) {
                return Optional.empty();
            }

            @Override
            public Optional<OwnerLease> renew(final OwnerLease ignored, final long now, final long duration) {
                return Optional.empty();
            }

            @Override
            public boolean release(final OwnerLease ignored) {
                return false;
            }

            @Override
            public Optional<OwnerLease> transition(final OwnerLease ignored, final ShardLifecycleState next) {
                current = active;
                return Optional.empty();
            }

            @Override
            public Optional<OwnerLease> current(final ShardId ignored) {
                return Optional.of(current);
            }
        }
        final OxiaOwnerLeaseStore store = new OxiaOwnerLeaseStore(new Backend());
        assertEquals(active, store.transitionOrRead(acquiring, ShardLifecycleState.ACTIVE_FOR_COMMANDS)
                .orElseThrow());
    }

    @Test
    void transitionOrReadDoesNotTurnAnIllegalTransitionIntoSuccess() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 17);
        final OwnerLease restoring = new OwnerLease(shard, "worker-illegal-transition", 1,
                nonZero(32, 8), 200, null, ShardLifecycleState.RESTORING);
        final OwnerLease acquiring = new OwnerLease(shard, "worker-illegal-transition", 1,
                restoring.leaseToken(), 200, null, ShardLifecycleState.ACQUIRING);
        final OxiaOwnerLeaseStore.LeaseCasBackend backend = new OxiaOwnerLeaseStore.LeaseCasBackend() {
            @Override
            public Optional<OwnerLease> acquire(final ShardId ignored, final String ownerId, final long now,
                                                final long duration) {
                return Optional.empty();
            }

            @Override
            public Optional<OwnerLease> renew(final OwnerLease ignored, final long now, final long duration) {
                return Optional.empty();
            }

            @Override
            public boolean release(final OwnerLease ignored) {
                return false;
            }

            @Override
            public Optional<OwnerLease> transition(final OwnerLease ignored, final ShardLifecycleState next) {
                return Optional.empty();
            }

            @Override
            public Optional<OwnerLease> current(final ShardId ignored) {
                return Optional.of(acquiring);
            }
        };
        final OxiaOwnerLeaseStore store = new OxiaOwnerLeaseStore(backend);
        assertTrue(store.transitionOrRead(restoring, ShardLifecycleState.ACQUIRING).isEmpty());
    }

    @Test
    void transitionOrReadRejectsAResponseLossSuccessorWithShorterExpiry() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 18);
        final OwnerLease expected = new OwnerLease(shard, "worker-short-expiry", 1,
                nonZero(32, 9), 200, null, ShardLifecycleState.ACQUIRING);
        final OwnerLease shortened = new OwnerLease(shard, "worker-short-expiry", 1,
                expected.leaseToken(), 199, null, ShardLifecycleState.RESTORING);
        final OxiaOwnerLeaseStore.LeaseCasBackend backend = new OxiaOwnerLeaseStore.LeaseCasBackend() {
            @Override
            public Optional<OwnerLease> acquire(final ShardId ignored, final String ownerId, final long now,
                                                final long duration) {
                return Optional.empty();
            }

            @Override
            public Optional<OwnerLease> renew(final OwnerLease ignored, final long now, final long duration) {
                return Optional.empty();
            }

            @Override
            public boolean release(final OwnerLease ignored) {
                return false;
            }

            @Override
            public Optional<OwnerLease> transition(final OwnerLease ignored, final ShardLifecycleState next) {
                return Optional.empty();
            }

            @Override
            public Optional<OwnerLease> current(final ShardId ignored) {
                return Optional.of(shortened);
            }
        };
        final OxiaOwnerLeaseStore store = new OxiaOwnerLeaseStore(backend);
        assertTrue(store.transitionOrRead(expected, ShardLifecycleState.RESTORING).isEmpty());
    }

    private static byte[] nonZero(final int length, final int firstByte) {
        final byte[] value = new byte[length];
        value[0] = (byte) firstByte;
        return value;
    }
}
