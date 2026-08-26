package com.nereusstream.delay.protocol;

import java.util.Objects;

/** Closed managed/native submission outcome union. */
public final class SubmissionOutcomeMessage {
    private final SubmissionOutcomeKind kind;
    private final EnqueueOutcomeMessage managed;
    private final NativeDeliveryReceipt nativeReceipt;
    private final NativeDefinitelyNotQueued nativeDefinitelyNotQueued;
    private final NativeEnqueueUncertain nativeUncertain;

    private SubmissionOutcomeMessage(
            final SubmissionOutcomeKind kind,
            final EnqueueOutcomeMessage managed,
            final NativeDeliveryReceipt nativeReceipt,
            final NativeDefinitelyNotQueued nativeDefinitelyNotQueued,
            final NativeEnqueueUncertain nativeUncertain) {
        this.kind = Objects.requireNonNull(kind, "kind");
        final int branches = (managed == null ? 0 : 1)
                + (nativeReceipt == null ? 0 : 1)
                + (nativeDefinitelyNotQueued == null ? 0 : 1)
                + (nativeUncertain == null ? 0 : 1);
        if (branches != 1
                || (kind == SubmissionOutcomeKind.MANAGED) != (managed != null)
                || (kind == SubmissionOutcomeKind.NATIVE_RECEIPT) != (nativeReceipt != null)
                || (kind == SubmissionOutcomeKind.NATIVE_DEFINITELY_NOT_QUEUED) != (nativeDefinitelyNotQueued != null)
                || (kind == SubmissionOutcomeKind.NATIVE_ENQUEUE_UNCERTAIN) != (nativeUncertain != null)) {
            throw new IllegalArgumentException("SubmissionOutcome branch does not match outcome kind");
        }
        this.managed = managed;
        this.nativeReceipt = nativeReceipt;
        this.nativeDefinitelyNotQueued = nativeDefinitelyNotQueued;
        this.nativeUncertain = nativeUncertain;
    }

    public static SubmissionOutcomeMessage managed(final EnqueueOutcomeMessage outcome) {
        return new SubmissionOutcomeMessage(
                SubmissionOutcomeKind.MANAGED, Objects.requireNonNull(outcome, "outcome"), null, null, null);
    }

    public static SubmissionOutcomeMessage nativeReceipt(final NativeDeliveryReceipt receipt) {
        return new SubmissionOutcomeMessage(
                SubmissionOutcomeKind.NATIVE_RECEIPT, null, Objects.requireNonNull(receipt, "receipt"), null, null);
    }

    public static SubmissionOutcomeMessage nativeDefinitelyNotQueued(final NativeDefinitelyNotQueued outcome) {
        return new SubmissionOutcomeMessage(
                SubmissionOutcomeKind.NATIVE_DEFINITELY_NOT_QUEUED,
                null,
                null,
                Objects.requireNonNull(outcome, "outcome"),
                null);
    }

    public static SubmissionOutcomeMessage nativeUncertain(final NativeEnqueueUncertain outcome) {
        return new SubmissionOutcomeMessage(
                SubmissionOutcomeKind.NATIVE_ENQUEUE_UNCERTAIN,
                null,
                null,
                null,
                Objects.requireNonNull(outcome, "outcome"));
    }

    public SubmissionOutcomeKind kind() {
        return kind;
    }

    public EnqueueOutcomeMessage managed() {
        return managed;
    }

    public NativeDeliveryReceipt nativeReceipt() {
        return nativeReceipt;
    }

    public NativeDefinitelyNotQueued nativeDefinitelyNotQueued() {
        return nativeDefinitelyNotQueued;
    }

    public NativeEnqueueUncertain nativeUncertain() {
        return nativeUncertain;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind.wireValue());
            if (managed != null) {
                CanonicalProtobuf.bytes(output, 10, managed.canonicalBytes());
            } else if (nativeReceipt != null) {
                CanonicalProtobuf.bytes(output, 11, nativeReceipt.payload());
            } else if (nativeDefinitelyNotQueued != null) {
                CanonicalProtobuf.bytes(output, 12, nativeDefinitelyNotQueued.canonicalBytes());
            } else {
                CanonicalProtobuf.bytes(output, 13, nativeUncertain.canonicalBytes());
            }
        });
    }

    public static SubmissionOutcomeMessage decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "SubmissionOutcomeMessage");
        if (fields.size() != 2 || fields.get(0).number() != 1) {
            throw new IllegalArgumentException("SubmissionOutcomeMessage must contain one branch");
        }
        final SubmissionOutcomeKind kind = SubmissionOutcomeKind.fromWire(QueryCodecSupport.uint(fields.get(0), 1));
        final SubmissionOutcomeMessage result =
                switch (fields.get(1).number()) {
                    case 10 -> {
                        if (kind != SubmissionOutcomeKind.MANAGED) {
                            throw new IllegalArgumentException("managed submission branch has wrong outcome kind");
                        }
                        yield managed(EnqueueOutcomeMessage.decode(QueryCodecSupport.nested(fields.get(1), 10)));
                    }
                    case 11 -> {
                        if (kind != SubmissionOutcomeKind.NATIVE_RECEIPT) {
                            throw new IllegalArgumentException("native receipt branch has wrong outcome kind");
                        }
                        yield nativeReceipt(
                                NativeDeliveryReceipt.decodePayload(QueryCodecSupport.nested(fields.get(1), 11)));
                    }
                    case 12 -> {
                        if (kind != SubmissionOutcomeKind.NATIVE_DEFINITELY_NOT_QUEUED) {
                            throw new IllegalArgumentException("native definite branch has wrong outcome kind");
                        }
                        yield nativeDefinitelyNotQueued(
                                NativeDefinitelyNotQueued.decode(QueryCodecSupport.nested(fields.get(1), 12)));
                    }
                    case 13 -> {
                        if (kind != SubmissionOutcomeKind.NATIVE_ENQUEUE_UNCERTAIN) {
                            throw new IllegalArgumentException("native uncertain branch has wrong outcome kind");
                        }
                        yield nativeUncertain(
                                NativeEnqueueUncertain.decode(QueryCodecSupport.nested(fields.get(1), 13)));
                    }
                    default -> throw new IllegalArgumentException("unknown SubmissionOutcome branch");
                };
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "SubmissionOutcomeMessage");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof SubmissionOutcomeMessage that
                && kind == that.kind
                && Objects.equals(managed, that.managed)
                && Objects.equals(nativeReceipt, that.nativeReceipt)
                && Objects.equals(nativeDefinitelyNotQueued, that.nativeDefinitelyNotQueued)
                && Objects.equals(nativeUncertain, that.nativeUncertain);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, managed, nativeReceipt, nativeDefinitelyNotQueued, nativeUncertain);
    }
}
