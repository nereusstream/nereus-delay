package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Immutable current-generation routing/accounting projection embedded in the Target Message. */
public final class TargetMessageLocator {
    public static final int VERSION = 1;
    public static final int MAX_CANONICAL_BYTES = 2 + 43 + 6 + 34 + 4 + 11 + 18 + 2 + 34 + 34 + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-message-locator\0");
    private final DelayMessageId messageId;
    private final int generation;
    private final TargetPartitionId target;
    private final TargetKeyCodec.Domain domain;
    private final byte[] accountingIncarnation;
    private final OrderingMode orderingMode;
    private final byte[] orderingDomain;
    private final byte[] scheduleBindingDigest;
    private final byte[] digest;

    public TargetMessageLocator(
            final DelayMessageId messageId,
            final int generation,
            final TargetPartitionId target,
            final TargetKeyCodec.Domain domain,
            final byte[] accountingIncarnation,
            final OrderingMode orderingMode,
            final byte[] orderingDomain,
            final byte[] scheduleBindingDigest) {
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        this.generation = generation;
        this.target = Objects.requireNonNull(target, "target");
        this.domain = Objects.requireNonNull(domain, "domain");
        if (domain.slot() >= TargetQueueState.MAX_DOMAIN_SLOTS) {
            throw new IllegalArgumentException("Target locator slot exceeds the schema bound");
        }
        this.accountingIncarnation = nonzero(accountingIncarnation, 16, "accountingIncarnation");
        this.orderingMode = Objects.requireNonNull(orderingMode, "orderingMode");
        if ((orderingMode == OrderingMode.DELIVERY_TIME_FIFO) != (orderingDomain != null)) {
            throw new IllegalArgumentException("Target ordering identity presence disagrees with ordering mode");
        }
        this.orderingDomain = orderingDomain == null ? null : nonzero(orderingDomain, 32, "orderingDomain");
        this.scheduleBindingDigest = nonzero(scheduleBindingDigest, 32, "scheduleBindingDigest");
        this.digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public DelayMessageId messageId() {
        return messageId;
    }

    public int generation() {
        return generation;
    }

    public TargetPartitionId target() {
        return target;
    }

    public TargetKeyCodec.Domain domain() {
        return domain;
    }

    public byte[] accountingIncarnation() {
        return Bytes.copy(accountingIncarnation);
    }

    public OrderingMode orderingMode() {
        return orderingMode;
    }

    public byte[] orderingDomain() {
        return orderingDomain == null ? null : Bytes.copy(orderingDomain);
    }

    public byte[] scheduleBindingDigest() {
        return Bytes.copy(scheduleBindingDigest);
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, messageId.bytes());
            CanonicalProtobuf.uint32Bits(out, 3, generation);
            CanonicalProtobuf.bytes(out, 4, target.bytes());
            CanonicalProtobuf.uint32(out, 5, domain.slot());
            CanonicalProtobuf.uint64Bits(out, 6, domain.generation());
            CanonicalProtobuf.bytes(out, 7, accountingIncarnation);
            CanonicalProtobuf.uint32(out, 8, orderingMode.wireValue());
            if (orderingDomain != null) {
                CanonicalProtobuf.bytes(out, 9, orderingDomain);
            }
            CanonicalProtobuf.bytes(out, 10, scheduleBindingDigest);
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 11, digest);
        });
    }

    public static TargetMessageLocator decode(final byte[] encoded) {
        if (encoded == null || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Target locator exceeds its encoding bound");
        }
        final var fields = QueryCodecSupport.read(encoded, "TargetMessageLocator");
        final boolean ordered = fields.size() == 11;
        QueryCodecSupport.requireNumbers(
                fields,
                ordered ? new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11} : new int[] {1, 2, 3, 4, 5, 6, 7, 8, 10, 11},
                "TargetMessageLocator");
        if (QueryCodecSupport.uint32(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unknown Target locator schema");
        }
        final TargetMessageLocator result = new TargetMessageLocator(
                new DelayMessageId(QueryCodecSupport.fixed(fields.get(1), 2, DelayMessageId.LENGTH)),
                QueryCodecSupport.uint32Bits(fields.get(2), 3),
                new TargetPartitionId(QueryCodecSupport.fixed(fields.get(3), 4, 32)),
                new TargetKeyCodec.Domain(
                        QueryCodecSupport.uint32(fields.get(4), 5), QueryCodecSupport.uint64Bits(fields.get(5), 6)),
                QueryCodecSupport.fixed(fields.get(6), 7, 16),
                OrderingMode.fromWire(QueryCodecSupport.uint(fields.get(7), 8)),
                ordered ? QueryCodecSupport.fixed(fields.get(8), 9, 32) : null,
                QueryCodecSupport.fixed(fields.get(ordered ? 9 : 8), 10, 32));
        if (!Bytes.constantTimeEquals(result.digest, QueryCodecSupport.fixed(fields.get(ordered ? 10 : 9), 11, 32))) {
            throw new IllegalArgumentException("Target locator digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetMessageLocator");
        return result;
    }

    /** Checks a live routing projection; historical terminal locators do not require a still-bound slot. */
    public void requireQueueProjection(final TargetQueueState queue) {
        Objects.requireNonNull(queue, "queue");
        if (!target.equals(queue.targetId())
                || !Arrays.equals(accountingIncarnation, queue.accountingIncarnation())
                || domain.slot() >= queue.domains().size()) {
            throw new IllegalArgumentException("Target locator queue/accounting/slot mismatch");
        }
        final TargetDomainState bound = queue.domains().get(domain.slot());
        if (!domain.equals(bound.domain()) || bound.lifecycle() == TargetDomainState.Lifecycle.VACANT) {
            throw new IllegalArgumentException("Target locator references a released or reused slot");
        }
    }

    public void requireMessageProjection(final DelayMessageId expectedMessage, final int expectedGeneration) {
        if (!messageId.equals(expectedMessage) || generation != expectedGeneration) {
            throw new IllegalArgumentException("Target locator belongs to another Message generation");
        }
    }

    private static byte[] nonzero(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        if (Arrays.equals(value, new byte[length])) {
            throw new IllegalArgumentException(name + " is unassigned");
        }
        return Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetMessageLocator that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
