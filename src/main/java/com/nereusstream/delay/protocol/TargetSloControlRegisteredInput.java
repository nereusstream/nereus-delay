package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Full immutable original Scope entry. Existence of these bytes is not MEMBER admission or authentication. */
public final class TargetSloControlRegisteredInput {
    public static final int VERSION = 1;
    public static final int MAX_PREPARED_BYTES = 8 * 1024 * 1024;
    public static final int MAX_FRAME_BYTES = 8 * 1024 * 1024;
    public static final int MAX_CANONICAL_BYTES = MAX_PREPARED_BYTES + MAX_FRAME_BYTES + 512;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-slo-control-registered-input\0");

    private final int slot;
    private final TargetSloControlScopeHeader.Reference headerReference;
    private final PreparedControlOperation prepared;
    private final SystemMutation mutation;
    private final byte[] digest;
    private final int entryBytes;

    public TargetSloControlRegisteredInput(
            int slot,
            TargetSloControlScopeHeader.Reference headerReference,
            PreparedControlOperation prepared,
            SystemMutation mutation) {
        if (slot < 0 || slot >= TargetSloControlScopeHeader.MAX_SLOTS) {
            throw new IllegalArgumentException("Scope entry slot exceeds schema bound");
        }
        this.slot = slot;
        this.headerReference = Objects.requireNonNull(headerReference);
        this.prepared = Objects.requireNonNull(prepared);
        this.mutation = Objects.requireNonNull(mutation);
        if (prepared.targets().size() != 1
                || prepared.targets().getFirst().targetIndex() != 0
                || prepared.targets().getFirst().targetKind() != ControlTargetKind.SHARD
                || !prepared.targets().getFirst().shard().shardId().equals(mutation.shardId())
                || mutation.type() != SystemMutationType.APPLY_SHARD_CONTROL
                || !(prepared.request().branch() instanceof TargetSloWriterControlRequest)
                        && !(prepared.request().branch() instanceof TargetSloObservationControlRequest)) {
            throw new IllegalArgumentException("Scope entry needs one exact original SLO Control Shard target");
        }
        if (prepared.canonicalBytes().length > MAX_PREPARED_BYTES || mutation.encodeFrame().length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("Scope entry original Prepared/frame exceeds the closed codec bound");
        }
        ControlTargetMutationBinding.validate(prepared, prepared.targets().getFirst(), mutation);
        final var scoped = prepared.request().branch() instanceof TargetSloWriterControlRequest
                ? TargetSloWriterControlBody.decode(mutation.canonicalBody()).admissionScope()
                : TargetSloObservationControlBody.decode(mutation.canonicalBody())
                        .admissionScope();
        if (!headerReference.equals(scoped)) {
            throw new IllegalArgumentException("entry reference differs from its exact signed scoped Control frame");
        }
        digest = Bytes.sha256(DOMAIN, fields());
        entryBytes = canonicalBytes().length;
        if (entryBytes > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Scope entry canonical bytes exceed schema bound");
        }
    }

    public int slot() {
        return slot;
    }

    public TargetSloControlScopeHeader.Reference headerReference() {
        return headerReference;
    }

    public PreparedControlOperation prepared() {
        return prepared;
    }

    public SystemMutation mutation() {
        return mutation;
    }

    public byte[] operationId() {
        return prepared.operationId();
    }
    /** Actual signed Source retry boundary; Prepared.registrationRetryUntil is not the legal SERVICE deadline D. */
    public long sourceRetryUntil() {
        return mutation.retryUntilEpochMs();
    }

    public int entryBytes() {
        return entryBytes;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public void requireHeader(TargetSloControlScopeHeader header) {
        Objects.requireNonNull(header).requireInput(this);
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, headerReference.canonicalBytes());
            CanonicalProtobuf.uint32(out, 3, slot);
            CanonicalProtobuf.bytes(out, 4, prepared.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, mutation.encodeFrame());
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 6, digest);
        });
    }

    public static TargetSloControlRegisteredInput decode(byte[] encoded) {
        final var f =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 6, false, "SLO Control registered input");
        QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3, 4, 5, 6}, "SLO Control registered input");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != VERSION) {
            throw new IllegalArgumentException("unknown Scope entry schema");
        }
        final var original = QueryCodecSupport.bytes(f.get(3), 4);
        final var frame = QueryCodecSupport.bytes(f.get(4), 5);
        if (original.length > MAX_PREPARED_BYTES || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("Scope original Prepared/frame exceeds bound");
        }
        // This schema permits exactly one target; do not first allocate an unbounded repeated target list.
        final var preparedFields = TargetCompatibilityCodec.read(
                original, MAX_PREPARED_BYTES, 13, false, "single-Shard original Prepared");
        QueryCodecSupport.requireNumbers(
                preparedFields,
                new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
                "single-Shard original Prepared");
        final var value = new TargetSloControlRegisteredInput(
                QueryCodecSupport.uint32(f.get(2), 3),
                TargetSloControlScopeHeader.Reference.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                PreparedControlOperation.decode(original),
                SystemMutation.decodeTargetControlFrame(frame));
        if (!Arrays.equals(value.digest, QueryCodecSupport.fixed(f.get(5), 6, 32))) {
            throw new IllegalArgumentException("Scope entry full original-input digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "SLO Control registered input");
        return value;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetSloControlRegisteredInput that
                && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
