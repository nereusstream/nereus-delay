package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetPartitionId;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TargetKeyCodecTest {
    @TempDir
    Path tempDir;

    @Test
    void laneFormatReaderRefusesTargetFormatBeforeOpeningForApplication() throws IOException {
        final Properties vectors = vectors();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        for (boolean identityAlsoConverted : new boolean[] {false, true}) {
            final ShardStoreConfig config =
                    ShardStoreConfig.defaults(tempDir.resolve("format-" + identityAlsoConverted));
            try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
                try (ShardStore store = ShardStore.open(config, shard, resources)) {
                    final byte[] identity = ValueEnvelope.decode(store.get(ColumnFamily.META, KeyCodec.metaFixed(2)), 1)
                            .payload();
                    if (identityAlsoConverted) {
                        ByteBuffer.wrap(identity).putInt(2);
                    }
                    store.write(batch -> {
                        batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(1), Bytes.u32be(2));
                        batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(2), identity);
                        batch.put(
                                ColumnFamily.TIMELINE,
                                HexFormat.of().parseHex(vectors.getProperty("due.kafka.key")),
                                new byte[] {1});
                    });
                }
                final RuntimeException failure = assertThrows(RuntimeException.class, () -> {
                    try (ShardStore reopened = ShardStore.open(config, shard, resources)) {
                        throw new AssertionError("Lane reader opened Target format: " + reopened.operationStatistics());
                    }
                });
                Throwable cause = failure;
                while (cause.getCause() != null) {
                    cause = cause.getCause();
                }
                assertTrue(cause.getMessage().contains("unsupported store format"), cause.toString());
            }
        }
    }

    @Test
    void candidateExpiryAndStateMatchIndependentGoldenVectors() throws IOException {
        final Properties vectors = vectors();
        final TargetPartitionId target = target(vectors);
        final DelayMessageId message = message(vectors);
        final var domain = new TargetKeyCodec.Domain(0, 1);
        final byte[] kafkaToken = Bytes.concat(new byte[] {1}, Bytes.u64be(7));
        final byte[] pulsarToken = Bytes.concat(new byte[] {2}, Bytes.u64be(7), Bytes.u64be(11), Bytes.u32be(2));
        final byte[] due =
                TargetKeyCodec.candidate(TargetKeyCodec.CandidateKind.DUE, target, domain, 100, kafkaToken, message, 2);
        final byte[] nativeKey = TargetKeyCodec.candidate(
                TargetKeyCodec.CandidateKind.NATIVE, target, domain, 100, pulsarToken, message, 2);
        assertEquals(vectors.getProperty("due.kafka.key"), Bytes.hex(due));
        assertEquals(vectors.getProperty("native.pulsar.key"), Bytes.hex(nativeKey));
        assertEquals(vectors.getProperty("expiry.key"), Bytes.hex(TargetKeyCodec.expiry(200, target, message, 2)));
        assertEquals(vectors.getProperty("state.key"), Bytes.hex(TargetKeyCodec.state(target)));
        for (byte[] key : new byte[][] {due, nativeKey}) {
            final var decoded = TargetKeyCodec.decodeCandidate(key);
            assertArrayEquals(key, decoded.encodedKey());
            assertEquals(domain, decoded.domain());
            assertEquals(target, decoded.target());
            assertEquals(message, decoded.messageId());
        }
        assertEquals(106, due.length);
        assertEquals(118, nativeKey.length);
    }

    @Test
    void domainGenerationAndSlotAreInThePrefixBeforeTimeAndUnsignedSourceOrder() throws IOException {
        final Properties vectors = vectors();
        final TargetPartitionId target = target(vectors);
        final DelayMessageId message = message(vectors);
        final byte[] lowToken = Bytes.concat(new byte[] {1}, Bytes.u64beBits(Long.MAX_VALUE));
        final byte[] highToken = Bytes.concat(new byte[] {1}, Bytes.u64beBits(Long.MIN_VALUE));
        final var domain = new TargetKeyCodec.Domain(0, 1);
        final byte[] low =
                TargetKeyCodec.candidate(TargetKeyCodec.CandidateKind.DUE, target, domain, 100, lowToken, message, 0);
        final byte[] high =
                TargetKeyCodec.candidate(TargetKeyCodec.CandidateKind.DUE, target, domain, 100, highToken, message, 0);
        assertTrue(Arrays.compareUnsigned(low, high) < 0);
        final byte[] earlier =
                TargetKeyCodec.candidate(TargetKeyCodec.CandidateKind.DUE, target, domain, 99, highToken, message, -1);
        assertTrue(Arrays.compareUnsigned(earlier, low) < 0);
        final byte[] reused = TargetKeyCodec.candidate(
                TargetKeyCodec.CandidateKind.DUE, target, new TargetKeyCodec.Domain(0, 2), 0, lowToken, message, 0);
        assertTrue(Arrays.compareUnsigned(high, reused) < 0);
        final byte[] anotherSlot = TargetKeyCodec.candidatePrefix(
                TargetKeyCodec.CandidateKind.DUE, target, new TargetKeyCodec.Domain(1, 1));
        assertTrue(
                Arrays.compareUnsigned(Arrays.copyOf(reused, TargetKeyCodec.CANDIDATE_PREFIX_BYTES), anotherSlot) < 0);
        final byte[] maximum = TargetKeyCodec.candidate(
                TargetKeyCodec.CandidateKind.NATIVE,
                target,
                new TargetKeyCodec.Domain(65535, -1),
                Long.MAX_VALUE,
                highToken,
                message,
                -1);
        assertEquals(-1L, TargetKeyCodec.decodeCandidate(maximum).domain().generation());
        assertEquals(-1, TargetKeyCodec.decodeCandidate(maximum).generation());
    }

    @Test
    void closedDecoderRejectsLegacyTagsUnknownFormatsAndInvalidComponents() throws IOException {
        final Properties vectors = vectors();
        final byte[] good = HexFormat.of().parseHex(vectors.getProperty("due.kafka.key"));
        for (int tag : new int[] {1, 2, 3, 7, 10, 255}) {
            final byte[] changed = good.clone();
            changed[0] = (byte) tag;
            assertThrows(IllegalArgumentException.class, () -> TargetKeyCodec.decodeCandidate(changed));
        }
        for (int offset : new int[] {1, 52}) {
            final byte[] changed = good.clone();
            changed[offset] = 3;
            assertThrows(IllegalArgumentException.class, () -> TargetKeyCodec.decodeCandidate(changed));
        }
        final byte[] zeroGeneration = good.clone();
        Arrays.fill(zeroGeneration, 36, 44, (byte) 0);
        assertThrows(IllegalArgumentException.class, () -> TargetKeyCodec.decodeCandidate(zeroGeneration));
        assertThrows(IllegalArgumentException.class, () -> TargetKeyCodec.decodeCandidate(Arrays.copyOf(good, 117)));
        assertThrows(IllegalArgumentException.class, () -> new TargetKeyCodec.Domain(0, 0));
        assertThrows(IllegalArgumentException.class, () -> new TargetKeyCodec.Domain(-1, 1));
        assertThrows(IllegalArgumentException.class, () -> new TargetKeyCodec.Domain(65536, 1));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetKeyCodec.candidate(
                        TargetKeyCodec.CandidateKind.DUE,
                        target(vectors),
                        new TargetKeyCodec.Domain(0, 1),
                        -1,
                        new byte[9],
                        message(vectors),
                        0));
    }

    @Test
    void candidateTokenIsCopiedOnConstructionAndAccess() throws IOException {
        final Properties vectors = vectors();
        final var candidate =
                TargetKeyCodec.decodeCandidate(HexFormat.of().parseHex(vectors.getProperty("due.kafka.key")));
        final byte[] token = candidate.sourceOrderToken();
        token[1] ^= 1;
        assertNotEquals(Bytes.hex(token), Bytes.hex(candidate.sourceOrderToken()));
        assertEquals(vectors.getProperty("due.kafka.key"), Bytes.hex(candidate.encodedKey()));
    }

    private static TargetPartitionId target(final Properties vectors) {
        return new TargetPartitionId(HexFormat.of().parseHex(vectors.getProperty("kafka.id")));
    }

    private static DelayMessageId message(final Properties vectors) {
        return new DelayMessageId(HexFormat.of().parseHex(vectors.getProperty("message.id")));
    }

    private static Properties vectors() throws IOException {
        final Properties properties = new Properties();
        try (var input = TargetKeyCodecTest.class.getResourceAsStream("/ndip3/target-identity-vectors.properties")) {
            properties.load(input);
        }
        return properties;
    }
}
