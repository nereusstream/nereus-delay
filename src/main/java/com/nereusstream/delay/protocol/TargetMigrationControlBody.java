package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Independently versioned registered CONTROL branch. Merely decoding/signing it never activates a Store. */
public record TargetMigrationControlBody(ShardId shard, long retryUntil, ControlRef controlRef, TargetMigrationRequest request) {
    public static final int LEGACY_MAX_CANONICAL_BYTES = Math.max(TargetMigrationActivateRequest.V1_MAX_CANONICAL_BYTES,
            TargetMigrationSettlementRequest.MAX_CANONICAL_BYTES) + 192;
    public static final int LEGACY_MAX_INPUT_BYTES = LEGACY_MAX_CANONICAL_BYTES + 512;
    public static final int ACTIVATE_V2_MAX_CANONICAL_BYTES=Math.max(TargetMigrationActivateRequest.MAX_CANONICAL_BYTES,TargetMigrationSettlementRequest.MAX_CANONICAL_BYTES)+192;
    public static final int ACTIVATE_V2_MAX_INPUT_BYTES=ACTIVATE_V2_MAX_CANONICAL_BYTES+512;
    public static final int MAX_CANONICAL_BYTES = Math.max(Math.max(TargetMigrationActivateRequest.MAX_CANONICAL_BYTES,
            TargetMigrationSettlementRequest.MAX_CANONICAL_BYTES), Math.max(TargetMigrationRetirementIntentRequest.MAX_CANONICAL_BYTES,
                    Math.max(TargetMigrationRetirementConfirmRequest.MAX_CANONICAL_BYTES, TargetMigrationArchiveRequest.MAX_CANONICAL_BYTES))) + 192;
    /** Common SystemMutation fields, fixed CONTROL author and Ed25519 signature, including protobuf tag/lengths. */
    public static final int MAX_INPUT_BYTES = MAX_CANONICAL_BYTES + 512;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-migration-control-v1\0");
    public TargetMigrationControlBody {
        Objects.requireNonNull(shard); Objects.requireNonNull(controlRef); Objects.requireNonNull(request);
        TargetCompatibilityCodec.assigned(controlRef.operationId(), 32, "migration Control operation ID");
        if (retryUntil < 0 || retryUntil == Long.MAX_VALUE || controlRef.targetIndex() != 0
                || !shard.equals(request.scope().shard())
                || !Arrays.equals(controlRef.requestHash(), PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()))) {
            throw new IllegalArgumentException("migration Control changed exact registered request, Source or finite retry limit");
        }
    }
    public byte[] logicalIdentity() { return controlRef.logicalOperationIdentity(request.controlKind()); }
    public byte[] semanticHash() { return Bytes.sha256(DOMAIN, Bytes.u16be(request.controlKind()), request.canonicalBytes()); }
    public ProtocolTuple tuple() { return request.tuple(); }
    public int expectedRecordState() { return 0; }
    public static boolean isControl(SystemMutation input) {
        if (input.type() != SystemMutationType.APPLY_SHARD_CONTROL) return false;
        final var reader = new CanonicalProtobuf.Reader(input.canonicalBody());
        for (int n = 0; n < 5 && reader.hasRemaining(); n++) {
            final var f = reader.next();
            if (f.number() == 11) {
                if (f.wireType() != 0) throw new IllegalArgumentException("migration Control kind is not varint");
                return f.unsignedValue() == TargetMigrationActivateRequest.CONTROL_KIND
                        || f.unsignedValue() == TargetMigrationSettlementRequest.CONTROL_KIND
                        || f.unsignedValue() == TargetMigrationRetirementIntentRequest.CONTROL_KIND
                        || f.unsignedValue() == TargetMigrationRetirementConfirmRequest.CONTROL_KIND
                        || f.unsignedValue() == TargetMigrationArchiveRequest.CONTROL_KIND;
            }
        }
        return false;
    }
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, new ShardSubject(shard).canonicalBytes());
            CanonicalProtobuf.uint32(out, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue()); CanonicalProtobuf.uint64(out, 3, retryUntil);
            CanonicalProtobuf.bytes(out, 10, controlRef.canonicalBytes()); CanonicalProtobuf.uint32(out, 11, request.controlKind());
            CanonicalProtobuf.uint32(out, 12, request instanceof TargetMigrationActivateRequest activation?activation.schemaVersion():1); CanonicalProtobuf.bytes(out, 13, semanticHash()); CanonicalProtobuf.uint32(out, 14, 0);
            CanonicalProtobuf.bytes(out, 15, CanonicalProtobuf.message(payload -> CanonicalProtobuf.bytes(payload, request.controlKind(), request.canonicalBytes())));
        });
    }
    public void requireInput(SystemMutation input) {
        final var author = AuthorIdentity.decode(input.authorIdentity());
        if (input.type() != SystemMutationType.APPLY_SHARD_CONTROL || !input.shardId().equals(shard)
                || input.retryUntilEpochMs() != retryUntil || author.kind() != AuthorIdentity.Kind.CONTROL
                || !Arrays.equals(input.canonicalBody(), canonicalBytes()) || !Arrays.equals(input.logicalOperationIdentity(), logicalIdentity())
                || input.canonicalEnvelope().length > MAX_INPUT_BYTES) {
            throw new IllegalArgumentException("migration requires its complete exact signed CONTROL input");
        }
    }
    public static TargetMigrationControlBody decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 9, false, "migration Control body");
        QueryCodecSupport.requireNumbers(f, new int[] {1,2,3,10,11,12,13,14,15}, "migration Control body");
        final int kind = QueryCodecSupport.uint32(f.get(4), 11);
        if (QueryCodecSupport.uint32(f.get(1), 2) != SystemMutationType.APPLY_SHARD_CONTROL.wireValue()
                || (QueryCodecSupport.uint32(f.get(5),12)!=1&&!(kind==TargetMigrationActivateRequest.CONTROL_KIND&&QueryCodecSupport.uint32(f.get(5),12)==2)) || QueryCodecSupport.uint32(f.get(7), 14) != 0
                || kind != TargetMigrationActivateRequest.CONTROL_KIND && kind != TargetMigrationSettlementRequest.CONTROL_KIND
                    && kind != TargetMigrationRetirementIntentRequest.CONTROL_KIND && kind != TargetMigrationRetirementConfirmRequest.CONTROL_KIND && kind != TargetMigrationArchiveRequest.CONTROL_KIND) {
            throw new IllegalArgumentException("unknown migration Control type/kind/schema/precondition");
        }
        final var payload = TargetCompatibilityCodec.read(QueryCodecSupport.bytes(f.get(8), 15), MAX_CANONICAL_BYTES, 1, false, "migration Control payload");
        QueryCodecSupport.requireNumbers(payload, new int[] {kind}, "migration Control payload");
        final TargetMigrationRequest request = switch (kind) {
            case TargetMigrationArchiveRequest.CONTROL_KIND -> TargetMigrationArchiveRequest.decode(QueryCodecSupport.bytes(payload.getFirst(), kind));
            case TargetMigrationActivateRequest.CONTROL_KIND -> TargetMigrationActivateRequest.decode(QueryCodecSupport.bytes(payload.getFirst(), kind));
            case TargetMigrationSettlementRequest.CONTROL_KIND -> TargetMigrationSettlementRequest.decode(QueryCodecSupport.bytes(payload.getFirst(), kind));
            case TargetMigrationRetirementIntentRequest.CONTROL_KIND -> TargetMigrationRetirementIntentRequest.decode(QueryCodecSupport.bytes(payload.getFirst(), kind));
            case TargetMigrationRetirementConfirmRequest.CONTROL_KIND -> TargetMigrationRetirementConfirmRequest.decode(QueryCodecSupport.bytes(payload.getFirst(), kind));
            default -> throw new IllegalArgumentException("unknown migration purpose");
        };
        final var value = new TargetMigrationControlBody(ShardSubject.decode(QueryCodecSupport.bytes(f.getFirst(), 1)).shardId(),
                QueryCodecSupport.uint(f.get(2), 3), ControlRef.decode(QueryCodecSupport.bytes(f.get(3), 10)), request);
        if (!Arrays.equals(QueryCodecSupport.fixed(f.get(6), 13, 32), value.semanticHash())) throw new IllegalArgumentException("migration Control semantic hash changed");
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration Control body"); return value;
    }
}
