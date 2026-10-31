package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Bounded Native71 child of the original retirement: one mutable head and immutable actual management steps. */
public record TargetMigrationArchiveState(TargetQuotaIncarnation payer, byte[] migrationId, byte[] planDigest,
        byte[] intentDigest, TargetQuotaMutation intentAt, long generation, byte[] archiveRoot,
        TargetMigrationArchiveCoverage coverage, TargetQuotaMutation shadowCut, TargetQuotaMutation coveredThrough,
        byte[] managementDigest, TargetQuotaMutation managementAt, boolean taken, long tailRows, long tailRawBytes,
        PreparedControlOperation control, SystemMutation input, TargetQuotaMutation mutation, byte[] tailFrame, TargetMigrationObjectReference inventory, int schemaVersion) {
    public static final int VALUE_TYPE = 71;
    public static final int V2_SCHEMA_VERSION = 2;
    public static final int SCHEMA_VERSION = 3;
    public static final int V2_MAX_CANONICAL_BYTES = 2 * TargetMigrationArchiveRequest.MAX_CANONICAL_BYTES
            + TargetQuotaIncarnation.MAX_CANONICAL_BYTES + TargetMigrationArchiveCoverage.MAX_CANONICAL_BYTES
            + 4 * TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES + 8192;
    public static final int MAX_CANONICAL_BYTES = TargetMigrationArchiveSourceFrame.MAX_NATIVE_CANONICAL_BYTES;
    private static final byte[] V3_DOMAIN = Bytes.utf8("nereus-delay-migration-archive-state-v3\0");
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-migration-archive-state-v1\0");
    public TargetMigrationArchiveState(TargetQuotaIncarnation payer, byte[] migrationId, byte[] planDigest,
            byte[] intentDigest, TargetQuotaMutation intentAt, long generation, byte[] archiveRoot,
            TargetMigrationArchiveCoverage coverage, TargetQuotaMutation shadowCut, TargetQuotaMutation coveredThrough,
            byte[] managementDigest, TargetQuotaMutation managementAt, boolean taken, long tailRows, long tailRawBytes,
            PreparedControlOperation control, SystemMutation input, TargetQuotaMutation mutation, byte[] tailFrame) {
        this(payer, migrationId, planDigest, intentDigest, intentAt, generation, archiveRoot, coverage, shadowCut, coveredThrough,
                managementDigest, managementAt, taken, tailRows, tailRawBytes, control, input, mutation, tailFrame, null);
    }
    public TargetMigrationArchiveState(TargetQuotaIncarnation payer, byte[] migrationId, byte[] planDigest,
            byte[] intentDigest, TargetQuotaMutation intentAt, long generation, byte[] archiveRoot,
            TargetMigrationArchiveCoverage coverage, TargetQuotaMutation shadowCut, TargetQuotaMutation coveredThrough,
            byte[] managementDigest, TargetQuotaMutation managementAt, boolean taken, long tailRows, long tailRawBytes,
            PreparedControlOperation control, SystemMutation input, TargetQuotaMutation mutation, byte[] tailFrame,
            TargetMigrationObjectReference inventory) {
        this(payer, migrationId, planDigest, intentDigest, intentAt, generation, archiveRoot, coverage, shadowCut,
                coveredThrough, managementDigest, managementAt, taken, tailRows, tailRawBytes, control, input, mutation,
                tailFrame, inventory, SCHEMA_VERSION);
    }
    public static int maximumCanonicalBytes(int version) {
        if (version == V2_SCHEMA_VERSION) return V2_MAX_CANONICAL_BYTES;
        if (version == SCHEMA_VERSION) return MAX_CANONICAL_BYTES;
        throw new IllegalArgumentException("unknown archive Native71 schema");
    }
    public static int maximumSourceFrameBytes(int version) {
        maximumCanonicalBytes(version);
        return version == V2_SCHEMA_VERSION ? TargetMigrationArchiveRequest.MAX_CANONICAL_BYTES
                : TargetMigrationArchiveSourceFrame.MAX_FRAME_BYTES;
    }
    /** A supplied genuine Source maximum remains distinct from this format-only size calculation. */
    public static long maximumCanonicalBytes(int version, long sourceFrameMaximum) {
        maximumCanonicalBytes(version);
        if (version == V2_SCHEMA_VERSION) return V2_MAX_CANONICAL_BYTES;
        if (sourceFrameMaximum < 0) throw new IllegalArgumentException("negative frozen Source maximum");
        final long mutation = TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES;
        final long[] mutations = {mutation, mutation, mutation, mutation, mutation};
        final long head = encodedLength(version, TargetQuotaIncarnation.MAX_CANONICAL_BYTES, mutations, -1L, true,
                Long.MAX_VALUE, Long.MAX_VALUE, TargetMigrationArchiveCoverage.MAX_CANONICAL_BYTES, -1, -1,
                sourceFrameMaximum, TargetMigrationObjectReference.MAX_CANONICAL_BYTES);
        final long managementInput = Math.subtractExact(TargetMigrationArchiveSourceFrame.controlFrameBytes(
                Math.addExact((long) TargetMigrationArchiveRequest.MAX_CANONICAL_BYTES, 192L)),
                ShardLogFrame.HEADER_LENGTH + ShardLogFrame.TRAILER_LENGTH);
        final long step = encodedLength(version, TargetQuotaIncarnation.MAX_CANONICAL_BYTES, mutations, -1L, true,
                Long.MAX_VALUE, Long.MAX_VALUE, TargetMigrationArchiveCoverage.MAX_CANONICAL_BYTES,
                Math.addExact((long) TargetMigrationArchiveRequest.MAX_CANONICAL_BYTES, 2048L), managementInput,
                -1, TargetMigrationObjectReference.MAX_CANONICAL_BYTES);
        return Math.max(head, step);
    }
    private static long encodedLength(int version, long payerBytes, long[] mutations, long generation, boolean taken,
            long rows, long rawBytes, long coverageBytes, long controlBytes, long inputBytes, long frameBytes, long inventoryBytes) {
        long size = TargetMigrationArchiveSourceFrame.scalarFieldBytes(1, version);
        size = Math.addExact(size, TargetMigrationArchiveSourceFrame.scalarFieldBytes(2, inputBytes >= 0 ? 2 : 1));
        size = Math.addExact(size, TargetMigrationArchiveSourceFrame.bytesFieldBytes(3, payerBytes));
        for (int field : new int[] {4, 5, 6, 9, 13}) size = Math.addExact(size, TargetMigrationArchiveSourceFrame.bytesFieldBytes(field, 32));
        final int[] mutationFields = {7, 11, 12, 14, 18};
        for (int i = 0; i < mutationFields.length; i++)
            size = Math.addExact(size, TargetMigrationArchiveSourceFrame.bytesFieldBytes(mutationFields[i], mutations[i]));
        size = Math.addExact(size, TargetMigrationArchiveSourceFrame.scalarFieldBytes(8, generation));
        size = Math.addExact(size, TargetMigrationArchiveSourceFrame.bytesFieldBytes(10, coverageBytes));
        size = Math.addExact(size, TargetMigrationArchiveSourceFrame.scalarFieldBytes(15, taken ? 1 : 0));
        size = Math.addExact(size, TargetMigrationArchiveSourceFrame.scalarFieldBytes(16, rows));
        size = Math.addExact(size, TargetMigrationArchiveSourceFrame.scalarFieldBytes(17, rawBytes));
        if (inputBytes >= 0) {
            size = Math.addExact(size, TargetMigrationArchiveSourceFrame.bytesFieldBytes(19, controlBytes));
            size = Math.addExact(size, TargetMigrationArchiveSourceFrame.bytesFieldBytes(20, inputBytes));
        }
        if (frameBytes >= 0) size = Math.addExact(size, TargetMigrationArchiveSourceFrame.bytesFieldBytes(21, frameBytes));
        if (inventoryBytes >= 0) size = Math.addExact(size, TargetMigrationArchiveSourceFrame.bytesFieldBytes(22, inventoryBytes));
        return Math.addExact(size, TargetMigrationArchiveSourceFrame.bytesFieldBytes(inventoryBytes >= 0 ? 23 : 22, 32));
    }
    public TargetMigrationArchiveState {
        maximumCanonicalBytes(schemaVersion);
        Objects.requireNonNull(payer); migrationId = TargetCompatibilityCodec.assigned(migrationId, 32, "migration ID");
        planDigest = TargetCompatibilityCodec.assigned(planDigest, 32, "original migration plan"); intentDigest = TargetCompatibilityCodec.assigned(intentDigest, 32, "original retirement intent");
        archiveRoot = TargetCompatibilityCodec.assigned(archiveRoot, 32, "actual archive rolling root"); managementDigest = TargetCompatibilityCodec.assigned(managementDigest, 32, "actual archive management frame");
        Objects.requireNonNull(intentAt).requireSourceApplied(); Objects.requireNonNull(shadowCut).requireSourceApplied();
        Objects.requireNonNull(coveredThrough).requireSourceApplied(); Objects.requireNonNull(managementAt).requireSourceApplied(); Objects.requireNonNull(mutation).requireSourceApplied();
        Objects.requireNonNull(coverage); managementAt.requireAfter(intentAt); managementAt.requireAtOrBefore(mutation); shadowCut.requireAtOrBefore(coveredThrough);
        if (generation == 0 || tailRows < 0 || tailRawBytes < 0 || (control == null) != (input == null)
                || !coveredThrough.equals(mutation) || !Arrays.equals(managementAt.mutationDigest(), managementDigest))
            throw new IllegalArgumentException("archive state changed actual management/Source coverage");
        if (schemaVersion == SCHEMA_VERSION)
            TargetMigrationArchiveSourceFrame.requireNativeCanonical(encodedLength(schemaVersion, payer.canonicalBytes().length,
                    new long[] {intentAt.canonicalBytes().length, shadowCut.canonicalBytes().length, coveredThrough.canonicalBytes().length,
                            managementAt.canonicalBytes().length, mutation.canonicalBytes().length}, generation, taken, tailRows, tailRawBytes,
                    coverage.canonicalBytes().length, control == null ? -1 : control.canonicalBytes().length,
                    input == null ? -1 : input.canonicalEnvelope().length, tailFrame == null ? -1 : tailFrame.length,
                    inventory == null ? -1 : inventory.canonicalBytes().length), "archive Native71 state");
        if (input != null) {
            final var body = TargetMigrationControlBody.decode(input.canonicalBody()); body.requireInput(input);
            if (!(body.request() instanceof TargetMigrationArchiveRequest request) || !control.request().equals(request.operationRequest())
                    || !request.scope().equals(payer.scope()) || generation != request.generation() || !migration(request.migrationId(), migrationId)
                    || !migration(request.planDigest(), planDigest) || !migration(request.intentDigest(), intentDigest)
                    || !request.intentAt().equals(intentAt) || !request.coverage().equals(coverage) || !request.shadowCut().equals(shadowCut)
                    || !Arrays.equals(request.resultingRoot(), archiveRoot) || !mutation.equals(managementAt) || taken != request.takeover()
                    || !Arrays.equals(Bytes.sha256(input.canonicalEnvelope()), managementDigest) || tailFrame != null)
                throw new IllegalArgumentException("archive management step changed exact request/actual Source/root");
            managementAt.requireAfter(request.coveredThrough());
            if (managementAt.sequence() != TargetQuotaMutation.increment(request.coveredThrough().sequence()))
                throw new IllegalArgumentException("archive step does not follow its exact actual Source frontier");
            ControlTargetMutationBinding.validate(control, control.targets().getFirst(), input);
        } else if (!mutation.equals(managementAt)) {
            tailFrame = TargetMigrationRetirementInventory.material(tailFrame, maximumSourceFrameBytes(schemaVersion), "actual archive tail frame");
            requireFrame(tailFrame, mutation);
        } else if (tailFrame != null) throw new IllegalArgumentException("management head cannot invent an archive tail frame");
    }
    private static boolean migration(byte[] a, byte[] b) { return Arrays.equals(a, b); }
    public boolean history() { return input != null; }
    public TargetMigrationArchiveRequest request() {
        if (input == null) throw new IllegalStateException("archive head is only a pointer to its actual management history");
        return (TargetMigrationArchiveRequest) TargetMigrationControlBody.decode(input.canonicalBody()).request();
    }
    public TargetMigrationArchiveState head() {
        return new TargetMigrationArchiveState(payer, migrationId, planDigest, intentDigest, intentAt, generation, archiveRoot,
                coverage, shadowCut, coveredThrough, managementDigest, managementAt, taken, tailRows, tailRawBytes, null, null, mutation, tailFrame, inventory, schemaVersion);
    }
    public TargetMigrationArchiveState tail(byte[] newRoot, long rows, long bytes, TargetQuotaMutation at, byte[] frame) {
        return tail(newRoot, rows, bytes, at, frame, inventory);
    }
    public TargetMigrationArchiveState tail(byte[] newRoot, long rows, long bytes, TargetQuotaMutation at, byte[] frame,
            TargetMigrationObjectReference actualInventory) {
        if (history()) throw new IllegalStateException("archive tail advances the original head only");
        at.requireAfter(mutation);
        return new TargetMigrationArchiveState(payer, migrationId, planDigest, intentDigest, intentAt, generation, newRoot,
                coverage, shadowCut, at, managementDigest, managementAt, taken, Math.addExact(tailRows, rows), Math.addExact(tailRawBytes, bytes), null, null, at, frame, Objects.requireNonNull(actualInventory, "actual received archive tail inventory"));
    }
    public void requireRetirement(TargetMigrationRoot root, TargetMigrationRetirementRecord retirement) {
        retirement.requireRoot(root);
        if (!Arrays.equals(migrationId, root.plan().migrationId()) || !Arrays.equals(planDigest, root.plan().digest())
                || !Arrays.equals(intentDigest, retirement.intention().digest()) || !intentAt.equals(retirement.intentAt())
                || !payer.identity().equals(root.owner().identity()) || !payer.accounting().equals(root.owner().accounting())
                || !payer.allocation().equals(root.owner().allocation())) throw new IllegalStateException("archive state changed its original root/retirement/payer");
    }
    @Override public byte[] migrationId() { return Bytes.copy(migrationId); }
    @Override public byte[] planDigest() { return Bytes.copy(planDigest); }
    @Override public byte[] intentDigest() { return Bytes.copy(intentDigest); }
    @Override public byte[] archiveRoot() { return Bytes.copy(archiveRoot); }
    @Override public byte[] managementDigest() { return Bytes.copy(managementDigest); }
    @Override public byte[] tailFrame() { return tailFrame == null ? null : Bytes.copy(tailFrame); }
    public static byte[] headKey(byte[] id) { return Bytes.concat(TargetMigrationRetirementRecord.keyFor(id), new byte[] {1}); }
    public static byte[] historyKey(byte[] id, long generation) {
        if (generation == 0) throw new IllegalArgumentException("archive history generation unassigned");
        return Bytes.concat(TargetMigrationRetirementRecord.keyFor(id), new byte[] {2}, Bytes.u64beBits(generation));
    }
    public static boolean isKey(byte[] key) {
        return key.length == 35 && key[0] == TargetKeyCodec.MIGRATION_RETIREMENT_TAG && key[1] == 1 && key[34] == 1
                || key.length == 43 && key[0] == TargetKeyCodec.MIGRATION_RETIREMENT_TAG && key[1] == 1 && key[34] == 2;
    }
    public byte[] key() { return history() ? historyKey(migrationId, managementAt.sequence()) : headKey(migrationId); }
    public static void requireFrame(byte[] frame, TargetQuotaMutation at) {
        TargetMigrationArchiveSourceFrame.requireFrame(frame, at);
    }
    public long canonicalLength() {
        return encodedLength(schemaVersion, payer.canonicalBytes().length,
                new long[] {intentAt.canonicalBytes().length, shadowCut.canonicalBytes().length, coveredThrough.canonicalBytes().length,
                        managementAt.canonicalBytes().length, mutation.canonicalBytes().length}, generation, taken, tailRows, tailRawBytes,
                coverage.canonicalBytes().length, control == null ? -1 : control.canonicalBytes().length,
                input == null ? -1 : input.canonicalEnvelope().length, tailFrame == null ? -1 : tailFrame.length,
                inventory == null ? -1 : inventory.canonicalBytes().length);
    }
    public long nativeValueLength() { return TargetMigrationArchiveSourceFrame.nativeValueBytes(canonicalLength()); }
    private byte[] fields() {
        if (schemaVersion == SCHEMA_VERSION)
            TargetMigrationArchiveSourceFrame.requireNativeCanonical(canonicalLength(), "archive Native71 state");
        return CanonicalProtobuf.message(out -> {
        CanonicalProtobuf.uint32(out, 1, schemaVersion); CanonicalProtobuf.uint32(out, 2, history() ? 2 : 1);
        CanonicalProtobuf.bytes(out, 3, payer.canonicalBytes()); CanonicalProtobuf.bytes(out, 4, migrationId); CanonicalProtobuf.bytes(out, 5, planDigest);
        CanonicalProtobuf.bytes(out, 6, intentDigest); CanonicalProtobuf.bytes(out, 7, intentAt.canonicalBytes()); CanonicalProtobuf.uint64Bits(out, 8, generation);
        CanonicalProtobuf.bytes(out, 9, archiveRoot); CanonicalProtobuf.bytes(out, 10, coverage.canonicalBytes()); CanonicalProtobuf.bytes(out, 11, shadowCut.canonicalBytes());
        CanonicalProtobuf.bytes(out, 12, coveredThrough.canonicalBytes()); CanonicalProtobuf.bytes(out, 13, managementDigest); CanonicalProtobuf.bytes(out, 14, managementAt.canonicalBytes());
        CanonicalProtobuf.uint32(out, 15, taken ? 1 : 0); CanonicalProtobuf.uint64(out, 16, tailRows); CanonicalProtobuf.uint64(out, 17, tailRawBytes);
        CanonicalProtobuf.bytes(out, 18, mutation.canonicalBytes());
        if (history()) { CanonicalProtobuf.bytes(out, 19, control.canonicalBytes()); CanonicalProtobuf.bytes(out, 20, input.canonicalEnvelope()); }
        if (tailFrame != null) CanonicalProtobuf.bytes(out, 21, tailFrame);
        if (inventory != null) CanonicalProtobuf.bytes(out, 22, inventory.canonicalBytes());
    }); }
    public byte[] digest() { return schemaVersion == V2_SCHEMA_VERSION ? Bytes.sha256(DOMAIN, fields())
            : TargetMigrationArchiveSourceFrame.digest(V3_DOMAIN, fields()); }
    public byte[] canonicalBytes() {
        if (schemaVersion == V2_SCHEMA_VERSION)
            return CanonicalProtobuf.message(out -> { out.writeBytes(fields()); CanonicalProtobuf.bytes(out, inventory == null ? 22 : 23, digest()); });
        final byte[] fields = fields();
        final byte[] digest = TargetMigrationArchiveSourceFrame.digest(V3_DOMAIN, fields);
        return CanonicalProtobuf.message(out -> { out.writeBytes(fields); CanonicalProtobuf.bytes(out, inventory == null ? 22 : 23, digest); });
    }
    public static TargetMigrationArchiveState decode(byte[] encoded) {
        final int version = TargetMigrationArchiveSourceFrame.schemaVersion(encoded, "migration archive state");
        final var f = TargetCompatibilityCodec.read(encoded, maximumCanonicalBytes(version), 23, false, "migration archive state");
        if (f.size() < 19) throw new IllegalArgumentException("incomplete archive state");
        for (int i = 0; i < 18; i++) if (f.get(i).number() != i + 1) throw new IllegalArgumentException("unknown archive state field");
        
        final int kind = QueryCodecSupport.uint32(f.get(1), 2), taken = QueryCodecSupport.uint32(f.get(14), 15);
        if (kind != 1 && kind != 2 || taken > 1) throw new IllegalArgumentException("unknown archive state kind");
        int i = 18; final var control = kind == 2 ? PreparedControlOperation.decode(QueryCodecSupport.bytes(f.get(i++), 19)) : null;
        final var input = kind == 2 ? SystemMutation.decodeTargetEnvelope(QueryCodecSupport.bytes(f.get(i++), 20), SystemMutationType.APPLY_SHARD_CONTROL) : null;
        final byte[] tail = i < f.size() && f.get(i).number() == 21 ? QueryCodecSupport.bytes(f.get(i++), 21) : null;
        final boolean hasInventory = f.getLast().number() == 23;
        final var inventory = hasInventory ? TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.get(i++), 22)) : null;
        if (f.size() != i + 1) throw new IllegalArgumentException("archive state trailing fields");
        final var value = new TargetMigrationArchiveState(TargetQuotaIncarnation.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                QueryCodecSupport.fixed(f.get(3), 4, 32), QueryCodecSupport.fixed(f.get(4), 5, 32), QueryCodecSupport.fixed(f.get(5), 6, 32),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(6), 7)), QueryCodecSupport.uint64Bits(f.get(7), 8), QueryCodecSupport.fixed(f.get(8), 9, 32),
                TargetMigrationArchiveCoverage.decode(QueryCodecSupport.bytes(f.get(9), 10)), TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(10), 11)),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(11), 12)), QueryCodecSupport.fixed(f.get(12), 13, 32), TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(13), 14)),
                taken == 1, QueryCodecSupport.uint(f.get(15), 16), QueryCodecSupport.uint(f.get(16), 17), control, input,
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(17), 18)), tail, inventory, version);
        if (!Arrays.equals(value.digest(), QueryCodecSupport.fixed(f.get(i), hasInventory ? 23 : 22, 32))) throw new IllegalArgumentException("archive state digest changed");
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration archive state"); return value;
    }
    @Override public boolean equals(Object other) { return other instanceof TargetMigrationArchiveState v && Arrays.equals(canonicalBytes(), v.canonicalBytes()); }
    @Override public int hashCode() { return Arrays.hashCode(digest()); }
}
