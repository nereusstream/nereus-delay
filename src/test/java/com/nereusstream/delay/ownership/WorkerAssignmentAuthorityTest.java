package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class WorkerAssignmentAuthorityTest {
    @Test
    void inMemoryAuthorityUsesRevisionCasAndIdempotentCanonicalAssignment() {
        final InMemoryWorkerAssignmentAuthority authority = new InMemoryWorkerAssignmentAuthority();
        final WorkerAssignment first = assignment("worker-a", 1, "first");

        final WorkerAssignmentAuthority.Publication published = authority.publish(first, 0);
        assertEquals(1, published.revision());
        assertSame(published, authority.publish(first, 1));
        assertThrows(IllegalStateException.class, () -> authority.publish(first, 0));

        final WorkerAssignment moved = assignment("worker-b", 2, "moved");
        final WorkerAssignmentAuthority.Publication successor = authority.publish(moved, 1);
        assertEquals(2, successor.revision());
        assertEquals(
                moved,
                authority
                        .current(first.sourceAssignment().shardId())
                        .orElseThrow()
                        .assignment());
        assertThrows(
                IllegalArgumentException.class, () -> authority.publish(assignment("worker-c", 2, "same-epoch"), 2));
    }

    @Test
    void withdrawRequiresTheExactCurrentRevisionAndIdentity() {
        final InMemoryWorkerAssignmentAuthority authority = new InMemoryWorkerAssignmentAuthority();
        final WorkerAssignment assignment = assignment("worker-a", 1, "withdraw");
        final WorkerAssignmentAuthority.Publication publication = authority.publish(assignment, 0);

        assertFalse(authority.withdraw(new WorkerAssignmentAuthority.Publication(2, assignment)));
        assertTrue(authority.withdraw(publication));
        assertTrue(authority.current(assignment.sourceAssignment().shardId()).isEmpty());
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
}
