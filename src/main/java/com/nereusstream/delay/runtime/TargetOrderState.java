package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetDomainState;
import com.nereusstream.delay.protocol.TargetHeadRef;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Closed strict-domain projection; source-ordered mutation and release authority remain external. */
public final class TargetOrderState {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 17;
    public static final int MAX_CANONICAL_BYTES = 2
            + 34
            + 34
            + 22
            + 3
            + 11
            + 18
            + 2
            + 11
            + 11
            + 2
            + 3
            + TargetOrderBarrier.MAX_ORDERED_KEY_BYTES
            + Math.max(3 + TargetHeadRef.MAX_CANONICAL_BYTES, 3 + TargetOrderBarrier.MAX_CANONICAL_BYTES)
            + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-order-state\0");

    public enum OrderingContract {
        LEGACY_DELIVERY_TIME_FIFO(1),
        ADMISSION_WATERMARK(2);

        private final int wireValue;

        OrderingContract(final int wireValue) {
            this.wireValue = wireValue;
        }

        public int wireValue() {
            return wireValue;
        }

        private static OrderingContract fromWire(final long wire) {
            for (OrderingContract contract : values()) {
                if (contract.wireValue == wire) {
                    return contract;
                }
            }
            throw new IllegalArgumentException("unknown ordering contract");
        }
    }

    public enum Gate {
        OPEN(1),
        ORDERING_BROKEN(2),
        CLOSED(3);

        private final int wireValue;

        Gate(final int wireValue) {
            this.wireValue = wireValue;
        }

        public int wireValue() {
            return wireValue;
        }

        private static Gate fromWire(final long wire) {
            for (Gate gate : values()) {
                if (gate.wireValue == wire) {
                    return gate;
                }
            }
            throw new IllegalArgumentException("unknown ordering gate");
        }
    }

    private final TargetPartitionId target;
    private final byte[] orderingDomain;
    private final ShardId sourceShard;
    private final TargetKeyCodec.Domain executionDomain;
    private final byte[] accountingIncarnation;
    private final OrderingContract orderingContract;
    private final long stateRevision;
    private final long controlVersion;
    private final Gate gate;
    private final TargetKeyCodec.Ordered lastAdmittedOrder;
    private final TargetHeadRef serviceableHead;
    private final TargetOrderBarrier barrier;
    private final byte[] digest;

    public TargetOrderState(
            final TargetPartitionId target,
            final byte[] orderingDomain,
            final ShardId sourceShard,
            final TargetKeyCodec.Domain executionDomain,
            final byte[] accountingIncarnation,
            final OrderingContract orderingContract,
            final long stateRevision,
            final long controlVersion,
            final Gate gate,
            final byte[] lastAdmittedOrder,
            final TargetHeadRef serviceableHead,
            final TargetOrderBarrier barrier) {
        this.target = Objects.requireNonNull(target, "target");
        TargetKeyCodec.orderState(target, orderingDomain);
        this.orderingDomain = Bytes.copy(orderingDomain);
        this.sourceShard = Objects.requireNonNull(sourceShard, "sourceShard");
        this.executionDomain = Objects.requireNonNull(executionDomain, "executionDomain");
        Bytes.requireLength(accountingIncarnation, 16, "accountingIncarnation");
        if (Arrays.equals(accountingIncarnation, new byte[16])
                || executionDomain.slot() >= TargetQueueState.MAX_DOMAIN_SLOTS
                || stateRevision == 0
                || controlVersion == 0) {
            throw new IllegalArgumentException("invalid Target order state incarnation/slot/revisions");
        }
        this.accountingIncarnation = Bytes.copy(accountingIncarnation);
        this.orderingContract = Objects.requireNonNull(orderingContract, "orderingContract");
        this.stateRevision = stateRevision;
        this.controlVersion = controlVersion;
        this.gate = Objects.requireNonNull(gate, "gate");
        this.lastAdmittedOrder = lastAdmittedOrder == null ? null : TargetKeyCodec.decodeOrdered(lastAdmittedOrder);
        if (this.lastAdmittedOrder != null) {
            if (orderingContract != OrderingContract.ADMISSION_WATERMARK) {
                throw new IllegalArgumentException("legacy ordering contract cannot acquire a retroactive watermark");
            }
            requireOrderProjection(this.lastAdmittedOrder);
        }
        if (serviceableHead != null) {
            final var headKey = TargetKeyCodec.decodeOrderedHead(serviceableHead.key());
            if (gate != Gate.OPEN
                    || barrier != null
                    || !target.equals(headKey.target())
                    || !executionDomain.equals(headKey.domain())
                    || !Arrays.equals(this.orderingDomain, headKey.orderingDomain())
                    || !sourceShard.equals(
                            serviceableHead.messageId().routingId().shardId())) {
                throw new IllegalArgumentException("Target serviceable strict head identity/gate/barrier mismatch");
            }
        }
        this.serviceableHead = serviceableHead;
        this.barrier = barrier;
        if (barrier != null) {
            requireLocatorProjection(barrier.locator());
            requireOrderProjection(barrier.order());
        }
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    private void requireOrderProjection(final TargetKeyCodec.Ordered order) {
        if (!target.equals(order.target())
                || !Arrays.equals(orderingDomain, order.orderingDomain())
                || !sourceShard.equals(order.messageId().routingId().shardId())) {
            throw new IllegalArgumentException("Target order key crosses target/ordering domain/Shard");
        }
    }

    private void requireLocatorProjection(final TargetMessageLocator locator) {
        if (locator.orderingMode() != OrderingMode.DELIVERY_TIME_FIFO
                || !target.equals(locator.target())
                || !Arrays.equals(orderingDomain, locator.orderingDomain())
                || !sourceShard.equals(locator.messageId().routingId().shardId())
                || !executionDomain.equals(locator.domain())
                || !Arrays.equals(accountingIncarnation, locator.accountingIncarnation())) {
            throw new IllegalArgumentException("Target order state locator projection mismatch");
        }
    }

    public TargetPartitionId target() {
        return target;
    }

    public byte[] orderingDomain() {
        return Bytes.copy(orderingDomain);
    }

    public ShardId sourceShard() {
        return sourceShard;
    }

    public TargetKeyCodec.Domain executionDomain() {
        return executionDomain;
    }

    public byte[] accountingIncarnation() {
        return Bytes.copy(accountingIncarnation);
    }

    public OrderingContract orderingContract() {
        return orderingContract;
    }

    public long stateRevision() {
        return stateRevision;
    }

    public long controlVersion() {
        return controlVersion;
    }

    public Gate gate() {
        return gate;
    }

    public TargetKeyCodec.Ordered lastAdmittedOrder() {
        return lastAdmittedOrder;
    }

    public TargetHeadRef serviceableHead() {
        return serviceableHead;
    }

    public TargetOrderBarrier barrier() {
        return barrier;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public byte[] encodedKey() {
        return TargetKeyCodec.orderState(target, orderingDomain);
    }

    /** A structural Store check. OPEN here does not bypass the Target/domain's live admission gates. */
    public static TargetOrderState decodeForStore(
            final byte[] key, final byte[] encoded, final ShardId shard, final TargetQueueState queue) {
        final TargetOrderState state = decode(encoded);
        if (!Arrays.equals(key, state.encodedKey())
                || !state.sourceShard.equals(shard)
                || !state.target.equals(queue.targetId())
                || !Arrays.equals(state.accountingIncarnation, queue.accountingIncarnation())
                || state.executionDomain.slot() >= queue.domains().size()) {
            throw new IllegalArgumentException("Target order state Store key/Shard/queue mismatch");
        }
        final TargetDomainState domain = queue.domains().get(state.executionDomain.slot());
        if (!state.executionDomain.equals(domain.domain())
                || domain.lifecycle() == TargetDomainState.Lifecycle.VACANT) {
            throw new IllegalArgumentException("Target order state belongs to a released or reused domain");
        }
        return state;
    }

    /** ORDER_HEAD uses the same NV 14 work body as ORDERED; every selected copy must match Message and state. */
    public TargetTimelineWorkRef requireServiceableProjection(
            final byte[] orderedHeadKey, final byte[] workBytes, final TargetMessageRecord message) {
        if (gate != Gate.OPEN
                || serviceableHead == null
                || barrier != null
                || !Arrays.equals(orderedHeadKey, serviceableHead.key())) {
            throw new IllegalArgumentException("strict domain has no matching serviceable head");
        }
        requireLocatorProjection(message.locator());
        final TargetTimelineWorkRef work = TargetTimelineWorkRef.decode(workBytes);
        message.requireTimelineProjection(work.ordinaryKey(), workBytes);
        work.requireHeadProjection(serviceableHead);
        if (message.runtime().terminal()
                || !message.runtime().attemptObligations().isEmpty()) {
            throw new IllegalArgumentException("strict head still has unresolved attempts or is terminal");
        }
        return work;
    }

    public void requireBarrierProjection(final TargetMessageRecord message) {
        if (barrier == null) {
            throw new IllegalArgumentException("strict domain has no barrier");
        }
        barrier.requireMessageProjection(message);
        if (orderingContract == OrderingContract.ADMISSION_WATERMARK
                && !message.runtime().attemptObligations().isEmpty()
                && !barrier.order().equals(lastAdmittedOrder)) {
            throw new IllegalArgumentException("unresolved Admission differs from the strict domain watermark");
        }
    }

    /** Does not prove source CAS, control authorization, head minimality, or barrier-release evidence. */
    public void requireSuccessorOf(final TargetOrderState previous) {
        if (!target.equals(previous.target)
                || !Arrays.equals(orderingDomain, previous.orderingDomain)
                || !sourceShard.equals(previous.sourceShard)
                || !executionDomain.equals(previous.executionDomain)
                || !Arrays.equals(accountingIncarnation, previous.accountingIncarnation)
                || orderingContract != previous.orderingContract
                || stateRevision != TargetQueueState.nextRevision(previous.stateRevision)) {
            throw new IllegalArgumentException("Target order successor changes identity/contract or skips revision");
        }
        if (controlVersion != previous.controlVersion
                && controlVersion != TargetQueueState.nextRevision(previous.controlVersion)) {
            throw new IllegalArgumentException("Target order successor skips control version");
        }
        if ((gate != previous.gate && controlVersion == previous.controlVersion)
                || (previous.gate == Gate.CLOSED && gate != Gate.CLOSED)
                || (previous.gate == Gate.ORDERING_BROKEN && gate == Gate.OPEN)) {
            throw new IllegalArgumentException(
                    "Target order successor reopens or changes gate without control revision");
        }
        if (previous.lastAdmittedOrder != null
                && (lastAdmittedOrder == null
                        || Arrays.compareUnsigned(
                                        lastAdmittedOrder.encodedKey(), previous.lastAdmittedOrder.encodedKey())
                                < 0)) {
            throw new IllegalArgumentException("Target order successor loses or rewinds Admission watermark");
        }
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, target.bytes());
            CanonicalProtobuf.bytes(out, 3, orderingDomain);
            CanonicalProtobuf.bytes(
                    out,
                    4,
                    Bytes.concat(sourceShard.routeIncarnation().bytes(), Bytes.u32be(sourceShard.unsignedPartition())));
            CanonicalProtobuf.uint32(out, 5, executionDomain.slot());
            CanonicalProtobuf.uint64Bits(out, 6, executionDomain.generation());
            CanonicalProtobuf.bytes(out, 7, accountingIncarnation);
            CanonicalProtobuf.uint32(out, 8, orderingContract.wireValue());
            CanonicalProtobuf.uint64Bits(out, 9, stateRevision);
            CanonicalProtobuf.uint64Bits(out, 10, controlVersion);
            CanonicalProtobuf.uint32(out, 11, gate.wireValue());
            if (lastAdmittedOrder != null) {
                CanonicalProtobuf.bytes(out, 12, lastAdmittedOrder.encodedKey());
            }
            if (serviceableHead != null) {
                CanonicalProtobuf.bytes(out, 13, serviceableHead.canonicalBytes());
            }
            if (barrier != null) {
                CanonicalProtobuf.bytes(out, 14, barrier.canonicalBytes());
            }
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 15, digest);
        });
    }

    public static TargetOrderState decode(final byte[] encoded) {
        if (encoded == null || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Target order state exceeds its byte bound");
        }
        final var fields = QueryCodecSupport.read(encoded, "TargetOrderState");
        for (var field : fields) {
            if (field.number() > 15) {
                throw new IllegalArgumentException("unknown Target order state field");
            }
        }
        if (QueryCodecSupport.uint32(QueryCodecSupport.field(fields, 1), 1) != VERSION) {
            throw new IllegalArgumentException("unknown Target order state schema");
        }
        final byte[] shard = QueryCodecSupport.fixed(QueryCodecSupport.field(fields, 4), 4, 20);
        final var watermark = QueryCodecSupport.optional(fields, 12);
        final var head = QueryCodecSupport.optional(fields, 13);
        final var barrier = QueryCodecSupport.optional(fields, 14);
        final TargetOrderState state = new TargetOrderState(
                new TargetPartitionId(QueryCodecSupport.fixed(QueryCodecSupport.field(fields, 2), 2, 32)),
                QueryCodecSupport.fixed(QueryCodecSupport.field(fields, 3), 3, 32),
                new ShardId(new RouteIncarnation(Arrays.copyOf(shard, 16)), (int) Bytes.readU32be(shard, 16)),
                new TargetKeyCodec.Domain(
                        QueryCodecSupport.uint32(QueryCodecSupport.field(fields, 5), 5),
                        QueryCodecSupport.uint64Bits(QueryCodecSupport.field(fields, 6), 6)),
                QueryCodecSupport.fixed(QueryCodecSupport.field(fields, 7), 7, 16),
                OrderingContract.fromWire(QueryCodecSupport.uint(QueryCodecSupport.field(fields, 8), 8)),
                QueryCodecSupport.uint64Bits(QueryCodecSupport.field(fields, 9), 9),
                QueryCodecSupport.uint64Bits(QueryCodecSupport.field(fields, 10), 10),
                Gate.fromWire(QueryCodecSupport.uint(QueryCodecSupport.field(fields, 11), 11)),
                watermark == null ? null : QueryCodecSupport.bytes(watermark, 12),
                head == null ? null : TargetHeadRef.decode(QueryCodecSupport.bytes(head, 13)),
                barrier == null ? null : TargetOrderBarrier.decode(QueryCodecSupport.bytes(barrier, 14)));
        if (!Bytes.constantTimeEquals(
                state.digest, QueryCodecSupport.fixed(QueryCodecSupport.field(fields, 15), 15, 32))) {
            throw new IllegalArgumentException("Target order state digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, state.canonicalBytes(), "TargetOrderState");
        return state;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetOrderState that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
