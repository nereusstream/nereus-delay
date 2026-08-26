package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class CompatibleControlSnapshotTest {
    @Test
    void canonicalSnapshotRoundTripsAndSortsReferences() {
        final ShardId shardId = new ShardId(new RouteIncarnation(bytes(16, 1)), 7);
        final ProfileRef capability = profile(bytes(32, 3), ProfileKind.DELIVERY_CAPABILITY, 2);
        final ProfileRef destination = profile(bytes(32, 2), ProfileKind.DESTINATION, 1);
        final CompatibleControlSnapshot snapshot = new CompatibleControlSnapshot(
                new ShardSubject(shardId),
                List.of(
                        new ProtocolTuple(1, 1, ProtocolTuple.SYSTEM_MUTATION, 1, 1),
                        new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 1)),
                List.of(capability, destination),
                new QuotaGrantRef(
                        bytes(32, 4),
                        1,
                        new PublishAdmissionBody.ChargeVector(
                                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)));

        final CompatibleControlSnapshot decoded = CompatibleControlSnapshot.decode(snapshot.canonicalBytes());

        assertEquals(snapshot, decoded);
        assertEquals(snapshot.profiles(), decoded.profiles());
        assertArrayEquals(snapshot.snapshotDigest(), decoded.snapshotDigest());
    }

    @Test
    void rejectsDuplicateReferencesShardMismatchAndDigestTampering() {
        final ShardSubject shard = new ShardSubject(new ShardId(new RouteIncarnation(bytes(16, 10)), 1));
        final ProtocolTuple tuple = new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 1);
        final ProfileRef profile = profile(bytes(32, 2), ProfileKind.DESTINATION, 1);
        final QuotaGrantRef grant = new QuotaGrantRef(
                bytes(32, 5),
                1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> new CompatibleControlSnapshot(shard, List.of(tuple, tuple), List.of(profile), grant));
        assertThrows(
                IllegalArgumentException.class,
                () -> new CompatibleControlSnapshot(shard, List.of(tuple), List.of(profile, profile), grant));

        final byte[] tampered =
                new CompatibleControlSnapshot(shard, List.of(tuple), List.of(profile), grant).canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> CompatibleControlSnapshot.decode(tampered));
    }

    private static ProfileRef profile(final byte[] id, final ProfileKind kind, final long version) {
        return new ProfileRef(id, version, Bytes.sha256(id, Bytes.u64beBits(version)), kind);
    }

    private static byte[] bytes(final int length, final int seed) {
        return Arrays.copyOf(Bytes.sha256(Bytes.utf8("control-snapshot-" + seed)), length);
    }
}
