package com.nereusstream.delay.protocol;

import java.util.Objects;

/** Closed managed enqueue outcome union. */
public final class EnqueueOutcomeMessageV1 {
    private final EnqueueOutcomeKindV1 kind;
    private final CommandQueuedReceiptV1 queued;
    private final DefinitelyNotQueuedV1 definitelyNotQueued;
    private final EnqueueUncertainV1 uncertain;

    private EnqueueOutcomeMessageV1(
            final EnqueueOutcomeKindV1 kind,
            final CommandQueuedReceiptV1 queued,
            final DefinitelyNotQueuedV1 definitelyNotQueued,
            final EnqueueUncertainV1 uncertain) {
        this.kind = Objects.requireNonNull(kind, "kind");
        final int branches =
                (queued == null ? 0 : 1) + (definitelyNotQueued == null ? 0 : 1) + (uncertain == null ? 0 : 1);
        if (branches != 1
                || (kind == EnqueueOutcomeKindV1.QUEUED) != (queued != null)
                || (kind == EnqueueOutcomeKindV1.DEFINITELY_NOT_QUEUED) != (definitelyNotQueued != null)
                || (kind == EnqueueOutcomeKindV1.ENQUEUE_UNCERTAIN) != (uncertain != null)) {
            throw new IllegalArgumentException("EnqueueOutcome branch does not match outcome kind");
        }
        this.queued = queued;
        this.definitelyNotQueued = definitelyNotQueued;
        this.uncertain = uncertain;
    }

    public static EnqueueOutcomeMessageV1 queued(final CommandQueuedReceiptV1 receipt) {
        return new EnqueueOutcomeMessageV1(
                EnqueueOutcomeKindV1.QUEUED, Objects.requireNonNull(receipt, "receipt"), null, null);
    }

    public static EnqueueOutcomeMessageV1 definitelyNotQueued(final DefinitelyNotQueuedV1 outcome) {
        return new EnqueueOutcomeMessageV1(
                EnqueueOutcomeKindV1.DEFINITELY_NOT_QUEUED, null, Objects.requireNonNull(outcome, "outcome"), null);
    }

    public static EnqueueOutcomeMessageV1 uncertain(final EnqueueUncertainV1 outcome) {
        return new EnqueueOutcomeMessageV1(
                EnqueueOutcomeKindV1.ENQUEUE_UNCERTAIN, null, null, Objects.requireNonNull(outcome, "outcome"));
    }

    public EnqueueOutcomeKindV1 kind() {
        return kind;
    }

    public CommandQueuedReceiptV1 queued() {
        return queued;
    }

    public DefinitelyNotQueuedV1 definitelyNotQueued() {
        return definitelyNotQueued;
    }

    public EnqueueUncertainV1 uncertain() {
        return uncertain;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind.wireValue());
            if (queued != null) {
                CanonicalProtobuf.bytes(output, 10, queued.payload());
            } else if (definitelyNotQueued != null) {
                CanonicalProtobuf.bytes(output, 11, definitelyNotQueued.canonicalBytes());
            } else {
                CanonicalProtobuf.bytes(output, 12, uncertain.canonicalBytes());
            }
        });
    }

    public static EnqueueOutcomeMessageV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "EnqueueOutcomeMessageV1");
        if (fields.size() != 2 || fields.get(0).number() != 1) {
            throw new IllegalArgumentException("EnqueueOutcomeMessageV1 must contain one branch");
        }
        final EnqueueOutcomeKindV1 kind = EnqueueOutcomeKindV1.fromWire(QueryCodecSupport.uint(fields.get(0), 1));
        final EnqueueOutcomeMessageV1 result =
                switch (fields.get(1).number()) {
                    case 10 -> {
                        if (kind != EnqueueOutcomeKindV1.QUEUED) {
                            throw new IllegalArgumentException("managed queued branch has wrong outcome kind");
                        }
                        yield queued(CommandQueuedReceiptV1.decodePayload(QueryCodecSupport.nested(fields.get(1), 10)));
                    }
                    case 11 -> {
                        if (kind != EnqueueOutcomeKindV1.DEFINITELY_NOT_QUEUED) {
                            throw new IllegalArgumentException("managed definite branch has wrong outcome kind");
                        }
                        yield definitelyNotQueued(
                                DefinitelyNotQueuedV1.decode(QueryCodecSupport.nested(fields.get(1), 11)));
                    }
                    case 12 -> {
                        if (kind != EnqueueOutcomeKindV1.ENQUEUE_UNCERTAIN) {
                            throw new IllegalArgumentException("managed uncertain branch has wrong outcome kind");
                        }
                        yield uncertain(EnqueueUncertainV1.decode(QueryCodecSupport.nested(fields.get(1), 12)));
                    }
                    default -> throw new IllegalArgumentException("unknown EnqueueOutcome branch");
                };
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "EnqueueOutcomeMessageV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof EnqueueOutcomeMessageV1 that
                && kind == that.kind
                && Objects.equals(queued, that.queued)
                && Objects.equals(definitelyNotQueued, that.definitelyNotQueued)
                && Objects.equals(uncertain, that.uncertain);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, queued, definitelyNotQueued, uncertain);
    }
}
