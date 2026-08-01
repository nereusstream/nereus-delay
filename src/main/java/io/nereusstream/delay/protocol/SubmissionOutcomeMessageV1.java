package io.nereusstream.delay.protocol;

import java.util.Objects;

/** Closed managed/native submission outcome union. */
public final class SubmissionOutcomeMessageV1 {
    private final SubmissionOutcomeKindV1 kind;
    private final EnqueueOutcomeMessageV1 managed;
    private final NativeDeliveryReceiptV1 nativeReceipt;
    private final NativeDefinitelyNotQueuedV1 nativeDefinitelyNotQueued;
    private final NativeEnqueueUncertainV1 nativeUncertain;

    private SubmissionOutcomeMessageV1(final SubmissionOutcomeKindV1 kind,
                                       final EnqueueOutcomeMessageV1 managed,
                                       final NativeDeliveryReceiptV1 nativeReceipt,
                                       final NativeDefinitelyNotQueuedV1 nativeDefinitelyNotQueued,
                                       final NativeEnqueueUncertainV1 nativeUncertain) {
        this.kind = Objects.requireNonNull(kind, "kind");
        final int branches = (managed == null ? 0 : 1) + (nativeReceipt == null ? 0 : 1)
                + (nativeDefinitelyNotQueued == null ? 0 : 1) + (nativeUncertain == null ? 0 : 1);
        if (branches != 1 || (kind == SubmissionOutcomeKindV1.MANAGED) != (managed != null)
                || (kind == SubmissionOutcomeKindV1.NATIVE_RECEIPT) != (nativeReceipt != null)
                || (kind == SubmissionOutcomeKindV1.NATIVE_DEFINITELY_NOT_QUEUED)
                != (nativeDefinitelyNotQueued != null)
                || (kind == SubmissionOutcomeKindV1.NATIVE_ENQUEUE_UNCERTAIN) != (nativeUncertain != null)) {
            throw new IllegalArgumentException("SubmissionOutcome branch does not match outcome kind");
        }
        this.managed = managed;
        this.nativeReceipt = nativeReceipt;
        this.nativeDefinitelyNotQueued = nativeDefinitelyNotQueued;
        this.nativeUncertain = nativeUncertain;
    }

    public static SubmissionOutcomeMessageV1 managed(final EnqueueOutcomeMessageV1 outcome) {
        return new SubmissionOutcomeMessageV1(SubmissionOutcomeKindV1.MANAGED,
                Objects.requireNonNull(outcome, "outcome"), null, null, null);
    }

    public static SubmissionOutcomeMessageV1 nativeReceipt(final NativeDeliveryReceiptV1 receipt) {
        return new SubmissionOutcomeMessageV1(SubmissionOutcomeKindV1.NATIVE_RECEIPT, null,
                Objects.requireNonNull(receipt, "receipt"), null, null);
    }

    public static SubmissionOutcomeMessageV1 nativeDefinitelyNotQueued(final NativeDefinitelyNotQueuedV1 outcome) {
        return new SubmissionOutcomeMessageV1(SubmissionOutcomeKindV1.NATIVE_DEFINITELY_NOT_QUEUED, null, null,
                Objects.requireNonNull(outcome, "outcome"), null);
    }

    public static SubmissionOutcomeMessageV1 nativeUncertain(final NativeEnqueueUncertainV1 outcome) {
        return new SubmissionOutcomeMessageV1(SubmissionOutcomeKindV1.NATIVE_ENQUEUE_UNCERTAIN, null, null, null,
                Objects.requireNonNull(outcome, "outcome"));
    }

    public SubmissionOutcomeKindV1 kind() {
        return kind;
    }

    public EnqueueOutcomeMessageV1 managed() {
        return managed;
    }

    public NativeDeliveryReceiptV1 nativeReceipt() {
        return nativeReceipt;
    }

    public NativeDefinitelyNotQueuedV1 nativeDefinitelyNotQueued() {
        return nativeDefinitelyNotQueued;
    }

    public NativeEnqueueUncertainV1 nativeUncertain() {
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

    public static SubmissionOutcomeMessageV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "SubmissionOutcomeMessageV1");
        if (fields.size() != 2 || fields.get(0).number() != 1) {
            throw new IllegalArgumentException("SubmissionOutcomeMessageV1 must contain one branch");
        }
        final SubmissionOutcomeKindV1 kind = SubmissionOutcomeKindV1.fromWire(
                QueryCodecSupport.uint(fields.get(0), 1));
        final SubmissionOutcomeMessageV1 result = switch (fields.get(1).number()) {
            case 10 -> {
                if (kind != SubmissionOutcomeKindV1.MANAGED) {
                    throw new IllegalArgumentException("managed submission branch has wrong outcome kind");
                }
                yield managed(EnqueueOutcomeMessageV1.decode(QueryCodecSupport.nested(fields.get(1), 10)));
            }
            case 11 -> {
                if (kind != SubmissionOutcomeKindV1.NATIVE_RECEIPT) {
                    throw new IllegalArgumentException("native receipt branch has wrong outcome kind");
                }
                yield nativeReceipt(NativeDeliveryReceiptV1.decodePayload(QueryCodecSupport.nested(fields.get(1), 11)));
            }
            case 12 -> {
                if (kind != SubmissionOutcomeKindV1.NATIVE_DEFINITELY_NOT_QUEUED) {
                    throw new IllegalArgumentException("native definite branch has wrong outcome kind");
                }
                yield nativeDefinitelyNotQueued(
                        NativeDefinitelyNotQueuedV1.decode(QueryCodecSupport.nested(fields.get(1), 12)));
            }
            case 13 -> {
                if (kind != SubmissionOutcomeKindV1.NATIVE_ENQUEUE_UNCERTAIN) {
                    throw new IllegalArgumentException("native uncertain branch has wrong outcome kind");
                }
                yield nativeUncertain(NativeEnqueueUncertainV1.decode(QueryCodecSupport.nested(fields.get(1), 13)));
            }
            default -> throw new IllegalArgumentException("unknown SubmissionOutcome branch");
        };
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "SubmissionOutcomeMessageV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof SubmissionOutcomeMessageV1 that && kind == that.kind
                && Objects.equals(managed, that.managed) && Objects.equals(nativeReceipt, that.nativeReceipt)
                && Objects.equals(nativeDefinitelyNotQueued, that.nativeDefinitelyNotQueued)
                && Objects.equals(nativeUncertain, that.nativeUncertain);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, managed, nativeReceipt, nativeDefinitelyNotQueued, nativeUncertain);
    }
}
