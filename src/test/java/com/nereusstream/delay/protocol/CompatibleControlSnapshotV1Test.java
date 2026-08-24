package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class CompatibleControlSnapshotV1Test {
    @Test
    void canonicalSnapshotRoundTripsAndSortsReferences() {
        final ShardId shardId = new ShardId(new RouteIncarnation(bytes(16, 1)), 7);
        final ProfileRefV1 capability = profile(bytes(32, 3), ProfileKindV1.DELIVERY_CAPABILITY, 2);
        final ProfileRefV1 destination = profile(bytes(32, 2), ProfileKindV1.DESTINATION, 1);
        final CompatibleControlSnapshotV1 snapshot = new CompatibleControlSnapshotV1(
                new ShardSubjectV1(shardId),
                List.of(
                        new ProtocolTupleV1(1, 1, ProtocolTupleV1.SYSTEM_MUTATION, 1, 1),
                        new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1)),
                List.of(capability, destination),
                new QuotaGrantRefV1(
                        bytes(32, 4),
                        1,
                        new PublishAdmissionBody.ChargeVector(
                                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)));

        final CompatibleControlSnapshotV1 decoded = CompatibleControlSnapshotV1.decode(snapshot.canonicalBytes());

        assertEquals(snapshot, decoded);
        assertEquals(snapshot.profiles(), decoded.profiles());
        assertArrayEquals(snapshot.snapshotDigest(), decoded.snapshotDigest());
    }

    @Test
    void rejectsDuplicateReferencesShardMismatchAndDigestTampering() {
        final ShardSubjectV1 shard = new ShardSubjectV1(new ShardId(new RouteIncarnation(bytes(16, 10)), 1));
        final ProtocolTupleV1 tuple = new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1);
        final ProfileRefV1 profile = profile(bytes(32, 2), ProfileKindV1.DESTINATION, 1);
        final QuotaGrantRefV1 grant = new QuotaGrantRefV1(
                bytes(32, 5),
                1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> new CompatibleControlSnapshotV1(shard, List.of(tuple, tuple), List.of(profile), grant));
        assertThrows(
                IllegalArgumentException.class,
                () -> new CompatibleControlSnapshotV1(shard, List.of(tuple), List.of(profile, profile), grant));

        final byte[] tampered =
                new CompatibleControlSnapshotV1(shard, List.of(tuple), List.of(profile), grant).canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> CompatibleControlSnapshotV1.decode(tampered));
    }

    private static ProfileRefV1 profile(final byte[] id, final ProfileKindV1 kind, final long version) {
        return new ProfileRefV1(id, version, Bytes.sha256(id, Bytes.u64beBits(version)), kind);
    }

    private static byte[] bytes(final int length, final int seed) {
        return Arrays.copyOf(Bytes.sha256(Bytes.utf8("control-snapshot-" + seed)), length);
    }
}
