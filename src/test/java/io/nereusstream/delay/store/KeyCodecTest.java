package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
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
}
