package com.nereusstream.delay.protocol;

import java.util.Objects;

/** Closed managed enqueue outcome union. */
public final class EnqueueOutcomeMessage {
    private final EnqueueOutcomeKind kind;
    private final CanonicalCommandQueuedReceipt queued;
    private final DefinitelyNotQueued definitelyNotQueued;
    private final EnqueueUncertain uncertain;

    private EnqueueOutcomeMessage(
            final EnqueueOutcomeKind kind,
            final CanonicalCommandQueuedReceipt queued,
            final DefinitelyNotQueued definitelyNotQueued,
            final EnqueueUncertain uncertain) {
        this.kind = Objects.requireNonNull(kind, "kind");
        final int branches =
                (queued == null ? 0 : 1) + (definitelyNotQueued == null ? 0 : 1) + (uncertain == null ? 0 : 1);
        if (branches != 1
                || (kind == EnqueueOutcomeKind.QUEUED) != (queued != null)
                || (kind == EnqueueOutcomeKind.DEFINITELY_NOT_QUEUED) != (definitelyNotQueued != null)
                || (kind == EnqueueOutcomeKind.ENQUEUE_UNCERTAIN) != (uncertain != null)) {
            throw new IllegalArgumentException("EnqueueOutcome branch does not match outcome kind");
        }
        this.queued = queued;
        this.definitelyNotQueued = definitelyNotQueued;
        this.uncertain = uncertain;
    }

    public static EnqueueOutcomeMessage queued(final CanonicalCommandQueuedReceipt receipt) {
        return new EnqueueOutcomeMessage(
                EnqueueOutcomeKind.QUEUED, Objects.requireNonNull(receipt, "receipt"), null, null);
    }

    public static EnqueueOutcomeMessage definitelyNotQueued(final DefinitelyNotQueued outcome) {
        return new EnqueueOutcomeMessage(
                EnqueueOutcomeKind.DEFINITELY_NOT_QUEUED, null, Objects.requireNonNull(outcome, "outcome"), null);
    }

    public static EnqueueOutcomeMessage uncertain(final EnqueueUncertain outcome) {
        return new EnqueueOutcomeMessage(
                EnqueueOutcomeKind.ENQUEUE_UNCERTAIN, null, null, Objects.requireNonNull(outcome, "outcome"));
    }

    public EnqueueOutcomeKind kind() {
        return kind;
    }

    public CanonicalCommandQueuedReceipt queued() {
        return queued;
    }

    public DefinitelyNotQueued definitelyNotQueued() {
        return definitelyNotQueued;
    }

    public EnqueueUncertain uncertain() {
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

    public static EnqueueOutcomeMessage decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "EnqueueOutcomeMessage");
        if (fields.size() != 2 || fields.get(0).number() != 1) {
            throw new IllegalArgumentException("EnqueueOutcomeMessage must contain one branch");
        }
        final EnqueueOutcomeKind kind = EnqueueOutcomeKind.fromWire(QueryCodecSupport.uint(fields.get(0), 1));
        final EnqueueOutcomeMessage result =
                switch (fields.get(1).number()) {
                    case 10 -> {
                        if (kind != EnqueueOutcomeKind.QUEUED) {
                            throw new IllegalArgumentException("managed queued branch has wrong outcome kind");
                        }
                        yield queued(CanonicalCommandQueuedReceipt.decodePayload(
                                QueryCodecSupport.nested(fields.get(1), 10)));
                    }
                    case 11 -> {
                        if (kind != EnqueueOutcomeKind.DEFINITELY_NOT_QUEUED) {
                            throw new IllegalArgumentException("managed definite branch has wrong outcome kind");
                        }
                        yield definitelyNotQueued(
                                DefinitelyNotQueued.decode(QueryCodecSupport.nested(fields.get(1), 11)));
                    }
                    case 12 -> {
                        if (kind != EnqueueOutcomeKind.ENQUEUE_UNCERTAIN) {
                            throw new IllegalArgumentException("managed uncertain branch has wrong outcome kind");
                        }
                        yield uncertain(EnqueueUncertain.decode(QueryCodecSupport.nested(fields.get(1), 12)));
                    }
                    default -> throw new IllegalArgumentException("unknown EnqueueOutcome branch");
                };
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "EnqueueOutcomeMessage");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof EnqueueOutcomeMessage that
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
