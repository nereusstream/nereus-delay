package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KeyCodecTest {
    @Test
    void controlReserveKeyUsesRegisteredClassAndLengthPrefixedGrantIdentity() {
        final byte[] grantId = Bytes.sha256(Bytes.utf8("control-reserve-grant"));

        assertArrayEquals(Bytes.concat(new byte[]{6, 1, 2}, Bytes.lp32(grantId)),
                KeyCodec.metaControlReserve(2, grantId));
    }

    @Test
    void controlReserveKeyRejectsUnknownClassAndInvalidGrantIdentity() {
        final byte[] grantId = Bytes.sha256(Bytes.utf8("control-reserve-grant"));

        assertThrows(IllegalArgumentException.class, () -> KeyCodec.metaControlReserve(0, grantId));
        assertThrows(IllegalArgumentException.class, () -> KeyCodec.metaControlReserve(7, grantId));
        assertThrows(IllegalArgumentException.class, () -> KeyCodec.metaControlReserve(1, new byte[31]));
        assertThrows(IllegalArgumentException.class, () -> KeyCodec.metaControlReserve(1, new byte[32]));
    }

    @Test
    void terminalDlqExportKeyUsesRegisteredIdentityLayout() {
        final byte[] exportId = Bytes.sha256(Bytes.utf8("dlq-export"));

        assertArrayEquals(Bytes.concat(new byte[]{2, 1}, exportId), KeyCodec.terminalDlqExport(exportId));
        assertThrows(IllegalArgumentException.class, () -> KeyCodec.terminalDlqExport(new byte[32]));
    }

    @Test
    void remainingRegisteredKeyNamespacesUseExactLayouts() {
        final byte[] proofId = Bytes.sha256(Bytes.utf8("fence-proof"));
        final byte[] resourceId = Bytes.sha256(Bytes.utf8("protected-resource"));
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("lane"));

        assertArrayEquals(Bytes.concat(new byte[]{4, 1}, proofId), KeyCodec.dedupeFence(proofId));
        assertArrayEquals(Bytes.concat(new byte[]{2, 1, 3}, Bytes.lp32(resourceId), Bytes.u64be(9)),
                KeyCodec.gcProtection(3, resourceId, 9));
        assertArrayEquals(Bytes.concat(new byte[]{4, 1}, lane.bytes(), Bytes.u32be(17), Bytes.u32be(2)),
                KeyCodec.metaProducer(lane, 17, 2));
        assertArrayEquals(new byte[]{7, 1, 4}, KeyCodec.metaRecovery(4));
        assertThrows(IllegalArgumentException.class, () -> KeyCodec.timelineSystem((byte) 5, 0, proofId, 0));
        assertThrows(IllegalArgumentException.class, () -> KeyCodec.metaQuota(6));
        assertThrows(IllegalArgumentException.class, () -> KeyCodec.dedupePosition(new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> KeyCodec.gcTask(0, (byte) 11, proofId, 0));
    }

    @Test
    void inflightKeyPreservesUnsignedOwnerEpochBits() {
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("owner-epoch-attempt"));
        assertArrayEquals(Bytes.concat(new byte[]{1, 1}, Bytes.u64beBits(Long.MIN_VALUE), Bytes.lp32(attemptId)),
                KeyCodec.inflight((byte) 1, Long.MIN_VALUE, attemptId));
    }

    @Test
    void gcRetireIntentKeyPreservesUnsignedResourceStateVersionBits() {
        final byte[] resourceId = Bytes.sha256(Bytes.utf8("resource-state-version"));
        assertArrayEquals(Bytes.concat(new byte[]{1, 1}, Bytes.u64be(0), new byte[]{1}, Bytes.lp32(resourceId),
                        Bytes.u64beBits(Long.MIN_VALUE)),
                KeyCodec.gcRetireIntent(io.nereusstream.delay.protocol.ResourceKind.PAYLOAD_OBJECT, resourceId,
                        Long.MIN_VALUE));
    }

    @Test
    void remainingRegisteredKeyNamespacesRejectInvalidComponents() {
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("lane"));
        final byte[] resourceId = Bytes.sha256(Bytes.utf8("protected-resource"));

        assertThrows(IllegalArgumentException.class, () -> KeyCodec.dedupeFence(new byte[31]));
        assertThrows(IllegalArgumentException.class, () -> KeyCodec.gcProtection(0, resourceId, 1));
        assertThrows(IllegalArgumentException.class, () -> KeyCodec.gcProtection(7, resourceId, 1));
        assertThrows(IllegalArgumentException.class, () -> KeyCodec.gcProtection(1, new byte[0], 1));
        assertThrows(IllegalArgumentException.class, () -> KeyCodec.gcProtection(1, resourceId, -1));
        assertThrows(IllegalArgumentException.class, () -> KeyCodec.metaProducer(lane, -1, 0));
        assertThrows(IllegalArgumentException.class, () -> KeyCodec.metaProducer(lane, 0x1_0000_0000L, 0));
        assertThrows(IllegalArgumentException.class, () -> KeyCodec.metaProducer(lane, 0, 0x1_0000_0000L));
        assertThrows(IllegalArgumentException.class, () -> KeyCodec.metaRecovery(0));
        assertThrows(IllegalArgumentException.class, () -> KeyCodec.metaRecovery(5));
    }
}
