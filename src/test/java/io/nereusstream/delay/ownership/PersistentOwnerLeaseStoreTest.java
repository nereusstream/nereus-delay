package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PersistentOwnerLeaseStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void contextBoundLeaseAndLifecycleSurviveReopen() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 17);
        final SourceAssignment assignment = assignment(shard, "persistent-assignment", 4);
        final byte[] session = Bytes.sha256(Bytes.utf8("persistent-session"));
        final PersistentOwnerLeaseStore first = new PersistentOwnerLeaseStore(tempDir);
        final OwnerLease acquired = first.acquire(assignment, "worker-a", session, 100, 100).orElseThrow();

        final PersistentOwnerLeaseStore reopened = new PersistentOwnerLeaseStore(tempDir);
        final OwnerLease observed = reopened.current(shard).orElseThrow();
        assertSameLease(acquired, observed);
        assertArrayEquals(assignment.assignmentId(), observed.sourceAssignmentId());
        assertEquals(assignment.assignmentEpoch(), observed.sourceAssignmentEpoch());
        assertArrayEquals(session, observed.sessionIdentity());

        final OwnerLease restoring = reopened.transition(observed, ShardLifecycleState.RESTORING).orElseThrow();
        final PersistentOwnerLeaseStore reopenedAgain = new PersistentOwnerLeaseStore(tempDir);
        assertEquals(ShardLifecycleState.RESTORING, reopenedAgain.current(shard).orElseThrow().state());
        assertTrue(reopenedAgain.release(restoring));
        assertTrue(new PersistentOwnerLeaseStore(tempDir).current(shard).isEmpty());
    }

    @Test
    void releasedLeaseKeepsEpochHistoryAcrossRestart() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 18);
        final PersistentOwnerLeaseStore store = new PersistentOwnerLeaseStore(tempDir);
        final OwnerLease first = store.acquire(shard, "worker-a", 100, 50).orElseThrow();
        assertTrue(store.release(first));

        final PersistentOwnerLeaseStore reopened = new PersistentOwnerLeaseStore(tempDir);
        final OwnerLease second = reopened.acquire(shard, "worker-b", 200, 50).orElseThrow();
        assertEquals(first.ownerEpoch() + 1, second.ownerEpoch());
        assertFalse(first.sameIdentity(second));
    }

    @Test
    void expiredLeaseCanBeReplacedButStaleReleaseCannotRemoveSuccessor() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 19);
        final PersistentOwnerLeaseStore store = new PersistentOwnerLeaseStore(tempDir);
        final OwnerLease first = store.acquire(shard, "worker-a", 0, 10).orElseThrow();
        final OwnerLease second = store.acquire(shard, "worker-b", 10, 10).orElseThrow();

        assertEquals(2, second.ownerEpoch());
        assertFalse(store.release(first));
        assertSameLease(second, store.current(shard).orElseThrow());
    }

    @Test
    void renewalCannotMoveExpiryBackwardsAndRetainsContext() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 20);
        final SourceAssignment assignment = assignment(shard, "renew-assignment", 7);
        final byte[] session = Bytes.sha256(Bytes.utf8("renew-session"));
        final PersistentOwnerLeaseStore store = new PersistentOwnerLeaseStore(tempDir);
        final OwnerLease acquired = store.acquire(assignment, "worker-renew", session, 100, 100).orElseThrow();

        assertTrue(store.renew(acquired, 110, 20).isEmpty());
        assertEquals(200, store.current(shard).orElseThrow().expiresAtEpochMs());
        final OwnerLease renewed = store.renew(acquired, 110, 100).orElseThrow();
        assertEquals(210, renewed.expiresAtEpochMs());
        assertEquals(acquired.context(), renewed.context());
        assertEquals(acquired.state(), renewed.state());
        assertEquals(220, store.renew(acquired, 120, 100).orElseThrow().expiresAtEpochMs());
    }

    @Test
    void invalidRequestAndExpiryOverflowDoNotConsumeEpoch() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 21);
        final PersistentOwnerLeaseStore store = new PersistentOwnerLeaseStore(tempDir);
        assertThrows(IllegalArgumentException.class, () -> store.acquire(shard, "", 0, 10));
        assertThrows(ArithmeticException.class, () -> store.acquire(shard, "worker-overflow", Long.MAX_VALUE, 1));

        final OwnerLease acquired = store.acquire(shard, "worker-retry", 0, 10).orElseThrow();
        assertEquals(1, acquired.ownerEpoch());
    }

    @Test
    void mismatchedContextCannotTransitionOrRelease() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 22);
        final PersistentOwnerLeaseStore store = new PersistentOwnerLeaseStore(tempDir);
        final OwnerLease acquired = store.acquire(assignment(shard, "context-a", 1), "worker-a",
                Bytes.sha256(Bytes.utf8("session-a")), 100, 100).orElseThrow();
        final OwnerLease mismatched = new OwnerLease(shard, acquired.ownerId(), acquired.ownerEpoch(),
                acquired.leaseToken(), acquired.expiresAtEpochMs(),
                new OwnerLeaseContext(Bytes.sha256(Bytes.utf8("context-b")), 2,
                        Bytes.sha256(Bytes.utf8("session-b"))), acquired.state());

        assertTrue(store.transition(mismatched, ShardLifecycleState.RESTORING).isEmpty());
        assertFalse(store.release(mismatched));
        assertSameLease(acquired, store.current(shard).orElseThrow());
    }

    @Test
    void corruptStateFailsClosedBeforeReturningCurrentLease() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 23);
        final PersistentOwnerLeaseStore store = new PersistentOwnerLeaseStore(tempDir);
        store.acquire(shard, "worker-corrupt", 100, 100).orElseThrow();
        final Path state = tempDir.resolve(Bytes.hex(shardKey(shard)) + ".state");
        Files.write(state, new byte[]{0x01, 0x02, 0x03});

        final PersistentOwnerLeaseStore reopened = new PersistentOwnerLeaseStore(tempDir);
        assertThrows(IllegalStateException.class, () -> reopened.current(shard));
    }

    @Test
    void rejectsSymbolicParentComponentBeforeCreatingLeaseStateOutsideBoundary() throws Exception {
        final Path parentRoot = tempDir.resolve("lease-parent");
        final Path outside = tempDir.resolve("lease-outside");
        Files.createDirectories(parentRoot);
        Files.createDirectories(outside);
        Files.createSymbolicLink(parentRoot.resolve("nested"), outside);

        assertThrows(IllegalStateException.class,
                () -> new PersistentOwnerLeaseStore(parentRoot.resolve("nested/state")));
        assertFalse(Files.exists(outside.resolve("state"), java.nio.file.LinkOption.NOFOLLOW_LINKS));
    }

    @Test
    void copiedStateForAnotherShardIsRejected() throws Exception {
        final ShardId firstShard = new ShardId(RouteIncarnation.random(), 24);
        final ShardId secondShard = new ShardId(RouteIncarnation.random(), 25);
        final PersistentOwnerLeaseStore store = new PersistentOwnerLeaseStore(tempDir);
        store.acquire(firstShard, "worker-copy", 100, 100).orElseThrow();
        final Path firstState = tempDir.resolve(Bytes.hex(shardKey(firstShard)) + ".state");
        final Path secondState = tempDir.resolve(Bytes.hex(shardKey(secondShard)) + ".state");
        Files.copy(firstState, secondState);

        assertThrows(IllegalStateException.class, () -> store.current(secondShard));
    }

    private static SourceAssignment assignment(final ShardId shard, final String seed, final long epoch) {
        return new SourceAssignment(shard, Bytes.sha256(Bytes.utf8(seed)), epoch,
                new KafkaActivationBarrier(shard, "cluster", UUID.randomUUID(), 0));
    }

    private static byte[] shardKey(final ShardId shard) {
        return Bytes.concat(shard.routeIncarnation().bytes(), Bytes.u32beBits(shard.partition()));
    }

    private static void assertSameLease(final OwnerLease expected, final OwnerLease actual) {
        assertEquals(expected.shardId(), actual.shardId());
        assertEquals(expected.ownerId(), actual.ownerId());
        assertEquals(expected.ownerEpoch(), actual.ownerEpoch());
        assertArrayEquals(expected.leaseToken(), actual.leaseToken());
        assertEquals(expected.expiresAtEpochMs(), actual.expiresAtEpochMs());
        assertEquals(expected.context(), actual.context());
        assertEquals(expected.state(), actual.state());
    }
}
