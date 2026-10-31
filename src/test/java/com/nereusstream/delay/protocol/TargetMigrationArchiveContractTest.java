package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.io.File;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TargetMigrationArchiveContractTest {
    private final TargetQuotaIncarnation payer = TargetQuotaIncarnation.decode(vector("target.open"));

    @Test
    void originalRowAndHeadKeepTheirIndependentPreUpgradeBytesAndDigests() {
        final var row = copyRow(TargetMigrationArchiveRow.V1_SCHEMA_VERSION);
        final var head = head(TargetMigrationArchiveState.V2_SCHEMA_VERSION, stamp(2), null);

        assertArrayEquals(hex(LEGACY_ROW), row.canonicalBytes());
        assertArrayEquals(hex(LEGACY_ROW_DIGEST), row.digest());
        assertArrayEquals(
                hex(LEGACY_ROW),
                TargetMigrationArchiveRow.decode(hex(LEGACY_ROW)).canonicalBytes());
        assertArrayEquals(hex(LEGACY_HEAD), head.canonicalBytes());
        assertArrayEquals(hex(LEGACY_HEAD_DIGEST), head.digest());
        assertArrayEquals(
                hex(LEGACY_HEAD),
                TargetMigrationArchiveState.decode(hex(LEGACY_HEAD)).canonicalBytes());
        assertEquals(32 << 20, TargetMigrationArchiveRow.maximumSourceFrameBytes(1));
        assertEquals(32 << 20, TargetMigrationArchiveState.maximumSourceFrameBytes(2));
    }

    @Test
    void newSchemasRoundTripAndRejectUnknownVersions() {
        final var row = copyRow(TargetMigrationArchiveRow.SCHEMA_VERSION);
        final byte[] rowBytes = row.canonicalBytes();
        final var decodedRow = TargetMigrationArchiveRow.decode(TargetValueEnvelope.decode(
                        TargetValueEnvelope.encode(TargetMigrationArchiveRow.VALUE_TYPE, rowBytes),
                        TargetMigrationArchiveRow.VALUE_TYPE)
                .payload());
        assertArrayEquals(rowBytes, decodedRow.canonicalBytes());
        assertEquals(rowBytes.length, row.canonicalLength());
        assertEquals(rowBytes.length + 12L, row.nativeValueLength());
        assertFalse(Arrays.equals(hex(LEGACY_ROW_DIGEST), row.digest()));

        final var head = head(TargetMigrationArchiveState.SCHEMA_VERSION, stamp(2), null);
        final byte[] headBytes = head.canonicalBytes();
        final var decodedHead = TargetMigrationArchiveState.decode(TargetValueEnvelope.decode(
                        TargetValueEnvelope.encode(TargetMigrationArchiveState.VALUE_TYPE, headBytes),
                        TargetMigrationArchiveState.VALUE_TYPE)
                .payload());
        assertArrayEquals(headBytes, decodedHead.canonicalBytes());
        assertEquals(headBytes.length, head.canonicalLength());
        assertEquals(headBytes.length + 12L, head.nativeValueLength());
        assertFalse(Arrays.equals(hex(LEGACY_HEAD_DIGEST), head.digest()));

        rowBytes[1] = 3;
        headBytes[1] = 4;
        assertThrows(IllegalArgumentException.class, () -> TargetMigrationArchiveRow.decode(rowBytes));
        assertThrows(IllegalArgumentException.class, () -> TargetMigrationArchiveState.decode(headBytes));
        assertThrows(IllegalArgumentException.class, () -> TargetMigrationArchiveRow.maximumCanonicalBytes(0));
        assertThrows(IllegalArgumentException.class, () -> TargetMigrationArchiveState.maximumCanonicalBytes(1));
    }

    @Test
    void readerAndDeclarationVersionsKeepTheExplicitArchiveMatrix() {
        final var beforeArchive = descriptor(19);
        final var oldReader =
                TargetProtocolActivationDescriptor.decode(descriptor(20).canonicalBytes());
        final var fullReader =
                TargetProtocolActivationDescriptor.decode(descriptor(21).canonicalBytes());
        assertEquals(ProtocolTuple.targetMigrationLogicalFirstArchive(), oldReader.tuple());
        assertEquals(ProtocolTuple.targetMigrationFullSourceArchive(), fullReader.tuple());
        assertFalse(oldReader.tuple().equals(fullReader.tuple()));
        for (int version : new int[] {0, 1, 2, 3}) {
            assertFalse(beforeArchive.supportsMigrationArchiveRowSchema(version));
            assertEquals(version == 1, oldReader.supportsMigrationArchiveRowSchema(version));
            assertEquals(version == 1 || version == 2, fullReader.supportsMigrationArchiveRowSchema(version));
        }
        for (int version : new int[] {1, 2, 3, 4}) {
            assertFalse(beforeArchive.supportsMigrationArchiveStateSchema(version));
            assertEquals(version == 2, oldReader.supportsMigrationArchiveStateSchema(version));
            assertEquals(version == 2 || version == 3, fullReader.supportsMigrationArchiveStateSchema(version));
        }
        assertEquals(32 << 20, oldReader.maximumMigrationArchiveSourceFrameBytes());
        assertEquals(Integer.MAX_VALUE, fullReader.maximumMigrationArchiveSourceFrameBytes());
        assertEquals(72, fullReader.nativeMaximum());
        final var originalDeclaration = ProtocolCapabilityDeclaration.extendedTarget(
                "worker",
                repeat(32, 1),
                List.of(oldReader.tuple()),
                List.of(oldReader.base()),
                List.of(oldReader),
                1,
                repeat(32, 2));
        assertEquals(
                4,
                ProtocolCapabilityDeclaration.decode(originalDeclaration.canonicalBytes())
                        .version());
        assertFalse(originalDeclaration.supports(fullReader));
        assertThrows(
                IllegalArgumentException.class,
                () -> ProtocolCapabilityDeclaration.extendedTarget(
                        "worker",
                        repeat(32, 1),
                        List.of(fullReader.tuple()),
                        List.of(fullReader.base()),
                        List.of(fullReader),
                        1,
                        repeat(32, 2)));
        final var declaration = ProtocolCapabilityDeclaration.fullSourceArchiveTarget(
                "worker",
                repeat(32, 1),
                List.of(oldReader.tuple(), fullReader.tuple()),
                List.of(fullReader.base()),
                List.of(oldReader, fullReader),
                1,
                repeat(32, 2));
        final var decoded = ProtocolCapabilityDeclaration.decode(declaration.canonicalBytes());
        assertEquals(5, decoded.version());
        assertTrue(decoded.supports(oldReader));
        assertTrue(decoded.supports(fullReader));
        final byte[] unknownDeclaration = declaration.canonicalBytes();
        unknownDeclaration[1] = 127;
        assertThrows(IllegalArgumentException.class, () -> ProtocolCapabilityDeclaration.decode(unknownDeclaration));
        final byte[] unknownDescriptor = fullReader.canonicalBytes();
        unknownDescriptor[1] = 127;
        assertThrows(
                IllegalArgumentException.class, () -> TargetProtocolActivationDescriptor.decode(unknownDescriptor));
    }

    @Test
    void completeSignedUnknownFrameBeyondOldLimitRoundTripsInBothNewFormats(@TempDir Path directory) throws Exception {
        // Full inline codec replay needs several simultaneous frame-sized copies; keep its heap local to this case.
        final Path output = directory.resolve("full-source-archive-frame.log");
        final var process = new ProcessBuilder(
                        Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                        "-Xmx2g",
                        "-cp",
                        largeFrameClasspath(),
                        TargetMigrationArchiveContractTest.class.getName(),
                        "large-frame")
                .redirectErrorStream(true)
                .redirectOutput(output.toFile())
                .start();
        final boolean completed;
        try {
            completed = process.waitFor(120, TimeUnit.SECONDS);
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(10, TimeUnit.SECONDS);
            }
        }
        final String result = Files.readString(output);
        assertTrue(completed, "full Source archive frame subprocess timed out: " + result);
        assertEquals(0, process.exitValue(), result);
        assertTrue(result.contains("Full Source archive frame contract passed"), result);
    }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 1 || !arguments[0].equals("large-frame")) {
            throw new IllegalArgumentException("expected the dedicated large-frame contract entrypoint");
        }
        new TargetMigrationArchiveContractTest().requireCompleteSignedUnknownFrameRoundTrips();
        System.out.println("Full Source archive frame contract passed");
    }

    private static String largeFrameClasspath() throws Exception {
        final var entries = new LinkedHashSet<String>();
        for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
            if (!entry.isBlank()) {
                entries.add(Path.of(entry).toAbsolutePath().toString());
            }
        }
        // Gradle isolates the test runtime in a URLClassLoader; java.class.path can contain only its worker JAR.
        for (ClassLoader loader = TargetMigrationArchiveContractTest.class.getClassLoader();
                loader != null;
                loader = loader.getParent()) {
            if (loader instanceof URLClassLoader urls) {
                for (var url : urls.getURLs()) {
                    entries.add(Path.of(url.toURI()).toAbsolutePath().toString());
                }
            }
        }
        return String.join(File.pathSeparator, entries);
    }

    private void requireCompleteSignedUnknownFrameRoundTrips() throws Exception {
        final byte[] frame = largeUnknownFrame();
        assertTrue(frame.length > (32 << 20));
        final var at = largeFrameMutation(frame);
        assertThrows(IllegalArgumentException.class, () -> sourceRow(1, at, frame));
        assertThrows(IllegalArgumentException.class, () -> head(2, at, frame));
        requireFullRowRoundTrip(frame, at);
        requireFullHeadRoundTrip(frame, at);
    }

    private TargetQuotaMutation largeFrameMutation(byte[] frame) {
        return new TargetQuotaMutation(
                3,
                source(3),
                Bytes.sha256(SystemMutation.decodeTargetFrame(frame).canonicalEnvelope()));
    }

    @Test
    void checkedRepresentabilityIncludesNativeMetadataWithoutClaimingServiceCapacity() {
        assertEquals(
                Integer.MAX_VALUE,
                TargetMigrationArchiveSourceFrame.frameBytes(TargetMigrationArchiveSourceFrame.MAX_ENVELOPE_BYTES));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMigrationArchiveSourceFrame.frameBytes(
                        (long) TargetMigrationArchiveSourceFrame.MAX_ENVELOPE_BYTES + 1));
        assertEquals(
                Integer.MAX_VALUE - 12,
                TargetMigrationArchiveSourceFrame.requireNativeCanonical(Integer.MAX_VALUE - 12L, "archive"));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMigrationArchiveSourceFrame.requireNativeCanonical(Integer.MAX_VALUE - 11L, "archive"));
        for (long canonicalBytes : new long[] {
            TargetMigrationArchiveRow.maximumCanonicalBytes(2, Integer.MAX_VALUE),
            TargetMigrationArchiveState.maximumCanonicalBytes(3, Integer.MAX_VALUE)
        }) {
            assertTrue(canonicalBytes > TargetMigrationArchiveSourceFrame.MAX_NATIVE_CANONICAL_BYTES);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetMigrationArchiveSourceFrame.requireNativeCanonical(canonicalBytes, "archive"));
        }
        assertThrows(IllegalArgumentException.class, () -> TargetMigrationArchiveRow.maximumCanonicalBytes(2, -1));
        assertThrows(IllegalArgumentException.class, () -> TargetMigrationArchiveState.maximumCanonicalBytes(3, -1));
    }

    private void requireFullRowRoundTrip(byte[] frame, TargetQuotaMutation at) {
        final var row = sourceRow(2, at, frame);
        final byte[] encoded = row.canonicalBytes();
        assertEquals(encoded.length, row.canonicalLength());
        final var decoded = TargetMigrationArchiveRow.decode(encoded);
        assertArrayEquals(frame, decoded.sourceFrame());
        assertArrayEquals(encoded, decoded.canonicalBytes());
    }

    private void requireFullHeadRoundTrip(byte[] frame, TargetQuotaMutation at) {
        final var head = head(3, at, frame);
        final byte[] encoded = head.canonicalBytes();
        assertEquals(encoded.length, head.canonicalLength());
        final var decoded = TargetMigrationArchiveState.decode(encoded);
        assertArrayEquals(frame, decoded.tailFrame());
        assertArrayEquals(encoded, decoded.canonicalBytes());
    }

    private byte[] largeUnknownFrame() throws Exception {
        // Legacy UNKNOWN transfer/retry fields are opaque canonical messages, with no 32 MiB body cap.
        final byte[] transfer =
                CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, 1, new byte[(32 << 20) + 1]));
        final byte[] retry = CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, 1, Bytes.utf8("unknown")));
        final var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final byte[] frame = encodeUnknownFrame(transfer, retry, keys);
        final var decoded = SystemMutation.decodeTargetFrame(frame);
        assertTrue(decoded.verifySignature(keys.getPublic()));
        final var outcome = PublishOutcomeBody.decode(decoded.canonicalBody());
        assertArrayEquals(transfer, outcome.transfer());
        assertArrayEquals(retry, outcome.retryDecision().canonicalBytes());
        assertArrayEquals(frame, decoded.encodeFrame());
        return frame;
    }

    private byte[] encodeUnknownFrame(byte[] transfer, byte[] retry, KeyPair keys) {
        final byte[] attempt = repeat(32, 0xa1);
        final byte[] body = PublishOutcomeBody.encodeInitial(
                payer.scope().shard(),
                9_000,
                attempt,
                3,
                4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                null,
                transfer,
                new TrustedUtcIntervalEvidence(
                        2_000,
                        2_000,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                        Bytes.utf8("clock"),
                        1,
                        1,
                        1,
                        repeat(32, 0xa2),
                        0,
                        null),
                retry);
        final var input = SystemMutation.signed(
                payer.scope().shard(),
                SystemMutationType.PUBLISH_OUTCOME,
                9_000,
                attempt,
                body,
                AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 7, repeat(32, 0xa3))
                        .canonicalBytes(),
                1,
                keys.getPrivate());
        return input.encodeFrame();
    }

    private TargetMigrationArchiveRow copyRow(int version) {
        return new TargetMigrationArchiveRow(
                repeat(32, 0x81),
                repeat(32, 0x82),
                repeat(16, 0x83),
                repeat(32, 0x84),
                repeat(16, 0x85),
                payer,
                entry(),
                TargetMigrationArchiveRow.SourceBranch.COPY_BATCH,
                1,
                repeat(32, 0x66),
                stamp(2),
                null,
                version);
    }

    private TargetMigrationArchiveRow sourceRow(int version, TargetQuotaMutation at, byte[] frame) {
        return new TargetMigrationArchiveRow(
                repeat(32, 0x81),
                repeat(32, 0x82),
                repeat(16, 0x83),
                repeat(32, 0x84),
                repeat(16, 0x85),
                payer,
                entry(),
                TargetMigrationArchiveRow.SourceBranch.TARGET_FIRST,
                0,
                null,
                at,
                frame,
                version);
    }

    private TargetMigrationArchiveState head(int version, TargetQuotaMutation at, byte[] frame) {
        final var coverage = new TargetMigrationArchiveCoverage(IntStream.rangeClosed(1, 5)
                .mapToObj(n ->
                        new TargetMigrationArchiveCoverage.Range(n, null, null, null, 0, 0, repeat(32, 0x90 + n), true))
                .toList());
        return new TargetMigrationArchiveState(
                payer,
                repeat(32, 0x81),
                repeat(32, 0x82),
                repeat(32, 0x86),
                stamp(1),
                1,
                repeat(32, 0x87),
                coverage,
                stamp(1),
                at,
                repeat(32, 0x66),
                stamp(2),
                false,
                0,
                0,
                null,
                null,
                at,
                frame,
                null,
                version);
    }

    private TargetMigrationArchiveEntry entry() {
        final var object = new TargetMigrationObjectReference(new CheckpointResource(
                repeat(16, 0x77),
                repeat(16, 0x78),
                new ProfileRef(Bytes.utf8("archive-object"), 1, repeat(32, 0x79), ProfileKind.OBJECT_STORE),
                Bytes.utf8("bucket"),
                Bytes.utf8("archive"),
                Bytes.utf8("version"),
                2,
                repeat(32, 0x7a)));
        return new TargetMigrationArchiveEntry(
                new byte[] {1, 1, 0x55},
                new byte[] {8, 1},
                TargetMigrationArchiveEntry.Origin.COMPACT_BASELINE,
                null,
                stamp(1),
                new TargetMigrationRecordReference(object, 0));
    }

    private TargetQuotaMutation stamp(long sequence) {
        return new TargetQuotaMutation(sequence, source(sequence), repeat(32, 0x66));
    }

    private KafkaSourcePosition source(long sequence) {
        final var allocation = (KafkaSourcePosition) payer.allocation().source();
        return new KafkaSourcePosition(
                allocation.shardId(),
                allocation.authenticatedClusterId(),
                allocation.nativeTopicUuid(),
                9 + sequence,
                null,
                100);
    }

    private static TargetProtocolActivationDescriptor descriptor(int version) {
        return new TargetProtocolActivationDescriptor(
                ArtifactGenerationSet.current(1, PulsarSourceLock.digest(), repeat(32, 4)),
                repeat(32, 5),
                repeat(32, 6),
                "1".repeat(40),
                "2".repeat(40),
                repeat(32, 7),
                repeat(32, 8),
                version);
    }

    private static byte[] vector(String name) {
        final var vectors = new Properties();
        try (var input = TargetMigrationArchiveContractTest.class.getResourceAsStream(
                "/ndip3/target-quota-incarnation-vectors.properties")) {
            vectors.load(input);
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
        return hex(vectors.getProperty(name));
    }

    private static byte[] hex(String value) {
        return HexFormat.of().parseHex(value);
    }

    private static byte[] repeat(int count, int value) {
        final byte[] result = new byte[count];
        Arrays.fill(result, (byte) value);
        return result;
    }

    // Frozen independently with generate-ndip3-target-quota-incarnation-vectors.py's Python u/b primitives
    // and the exact Row1/Head2 fields and digest domains in 0a8e4fcd, before the full Source format upgrade.
    private static final String LEGACY_ROW_DIGEST = "ea660bfb8c97506033abff45f0ad29a0b468a6bf4ddde4a3b85a91813b5e3c05";
    private static final String LEGACY_HEAD_DIGEST = "c2442505471642b1d486422e00b8756ffb4244b214a341d5f9265c0e629a3bda";
    private static final String LEGACY_ROW =
            ("0801122081818181818181818181818181818181818181818181818181818181818181811a208282"
                            + "82828282828282828282828282828282")
                    + ("82828282828282828282828282822210838383838383838383838383838383832a20848484848484"
                            + "84848484848484848484848484848484")
                    + ("848484848484848484843210858585858585858585858585858585853a810308011270080110011a"
                            + "14111111111111111111111111111111")
                    + ("110000000322102cdbd7f406e52838e86dedbaff1a2d942a20333333333333333333333333333333"
                            + "33333333333333333333333333333333")
                    + ("333a20f6f90ee73422f7d218426ef19185eaee16d4f5a5ba2eb0b4866eac8f15edac151a20444444"
                            + "44444444444444444444444444444444")
                    + ("44444444444444444444444444225008011220bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                            + "bbbbbbbbbbbbbbbbbbbbbb1801202028")
                    + ("18302838404220ff12256c88f393f3b2027d44c7ae37b5863820edcb9b7804393c32b4f0ccd3a32a"
                            + "630801123d0111111111111111111111")
                    + ("111111111111000000036b666b555555555555555555555555555555550000000300000000000000"
                            + "0a0000000000000000641a2066666666")
                    + ("666666666666666666666666666666666666666666666666666666663210cccccccccccccccccccc"
                            + "cccccccccccc4220b4826d5856fd72ad")
                    + ("e75f1028674d9e911497cd08861442390da7f306b4667c0d429602080112030101551a0208012001"
                            + "32630801123d01111111111111111111")
                    + ("11111111111111000000036b666b5555555555555555555555555555555500000003000000000000"
                            + "000a0000000000000000641a20666666")
                    + ("66666666666666666666666666666666666666666666666666666666663aa1010801129a010a1077"
                            + "77777777777777777777777777777712")
                    + ("10787878787878787878787878787878781a360a0e617263686976652d6f626a65637410011a2079"
                            + "79797979797979797979797979797979")
                    + ("797979797979797979797979797979200322066275636b65742a0761726368697665320776657273"
                            + "696f6e380242207a7a7a7a7a7a7a7a7a")
                    + ("7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a1800480150015a20666666666666666666"
                            + "66666666666666666666666666666666")
                    + ("6666666666666662630802123d0111111111111111111111111111111111000000036b666b555555"
                            + "55555555555555555555555555000000")
                    + ("03000000000000000b0000000000000000641a206666666666666666666666666666666666666666"
                            + "6666666666666666666666667220ea66")
                    + "0bfb8c97506033abff45f0ad29a0b468a6bf4ddde4a3b85a91813b5e3c05";
    private static final String LEGACY_HEAD =
            ("080210011a810308011270080110011a14111111111111111111111111111111110000000322102c"
                            + "dbd7f406e52838e86dedbaff1a2d942a")
                    + ("2033333333333333333333333333333333333333333333333333333333333333333a20f6f90ee734"
                            + "22f7d218426ef19185eaee16d4f5a5ba")
                    + ("2eb0b4866eac8f15edac151a20444444444444444444444444444444444444444444444444444444"
                            + "4444444444225008011220bbbbbbbbbb")
                    + ("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb180120202818302838404220ff"
                            + "12256c88f393f3b2027d44c7ae37b586")
                    + ("3820edcb9b7804393c32b4f0ccd3a32a630801123d01111111111111111111111111111111110000"
                            + "00036b666b5555555555555555555555")
                    + ("555555555500000003000000000000000a0000000000000000641a20666666666666666666666666"
                            + "66666666666666666666666666666666")
                    + ("666666663210cccccccccccccccccccccccccccccccc4220b4826d5856fd72ade75f1028674d9e91"
                            + "1497cd08861442390da7f306b4667c0d")
                    + ("222081818181818181818181818181818181818181818181818181818181818181812a2082828282"
                            + "82828282828282828282828282828282")
                    + ("82828282828282828282828232208686868686868686868686868686868686868686868686868686"
                            + "8686868686863a630801123d01111111")
                    + ("11111111111111111111111111000000036b666b5555555555555555555555555555555500000003"
                            + "000000000000000a0000000000000000")
                    + ("641a20666666666666666666666666666666666666666666666666666666666666666640014a2087"
                            + "87878787878787878787878787878787")
                    + ("87878787878787878787878787878752e8010801122c080110013000380042209191919191919191"
                            + "91919191919191919191919191919191")
                    + ("91919191919191914801122c08011002300038004220929292929292929292929292929292929292"
                            + "92929292929292929292929292924801")
                    + ("122c0801100330003800422093939393939393939393939393939393939393939393939393939393"
                            + "939393934801122c0801100430003800")
                    + ("422094949494949494949494949494949494949494949494949494949494949494944801122c0801"
                            + "10053000380042209595959595959595")
                    + ("95959595959595959595959595959595959595959595959548015a630801123d0111111111111111"
                            + "111111111111111111000000036b666b")
                    + ("5555555555555555555555555555555500000003000000000000000a0000000000000000641a2066"
                            + "66666666666666666666666666666666")
                    + ("66666666666666666666666666666662630802123d01111111111111111111111111111111110000"
                            + "00036b666b5555555555555555555555")
                    + ("555555555500000003000000000000000b0000000000000000641a20666666666666666666666666"
                            + "66666666666666666666666666666666")
                    + ("666666666a2066666666666666666666666666666666666666666666666666666666666666667263"
                            + "0802123d011111111111111111111111")
                    + ("1111111111000000036b666b5555555555555555555555555555555500000003000000000000000b"
                            + "0000000000000000641a206666666666")
                    + ("66666666666666666666666666666666666666666666666666666678008001008801009201630802"
                            + "123d0111111111111111111111111111")
                    + ("111111000000036b666b5555555555555555555555555555555500000003000000000000000b0000"
                            + "000000000000641a2066666666666666")
                    + ("66666666666666666666666666666666666666666666666666b20120c2442505471642b1d486422e"
                            + "00b8756ffb4244b214a341d5f9265c0e")
                    + "629a3bda";
}
