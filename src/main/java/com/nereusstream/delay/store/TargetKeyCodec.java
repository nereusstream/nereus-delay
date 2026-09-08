package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.TargetPartitionId;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/** NDIP-3 key reservations. Writers require the separately activated Target store format. */
public final class TargetKeyCodec {
    public static final int KEY_FORMAT = 1;
    public static final int DUE_TAG = 8;
    public static final int NATIVE_TAG = 9;
    public static final int EXPIRY_TAG = 10;
    public static final int ORDERED_TAG = 11;
    public static final int ORDER_HEAD_TAG = 12;
    public static final int STATE_TAG = 9;
    public static final int ORDER_STATE_TAG = 10;
    public static final int IDENTITY_TAG = 11;
    public static final int DISPATCH_COMPATIBILITY_TAG = 12;
    public static final int CONTROL_SCOPE_TAG = 13;
    public static final int CHANNEL_IDENTITY_TAG = 14;
    public static final int MEMBERSHIP_GRANT_TAG = 15;
    public static final int MEMBERSHIP_POLICY_TAG = 16;
    public static final int NATIVE_POLICY_SCOPE_TAG = 17;
    public static final int NATIVE_POLICY_SNAPSHOT_TAG = 18;
    public static final int QUOTA_COUNTER_TAG = 19;
    public static final int QUOTA_AGGREGATE_TAG = 20;
    public static final int QUOTA_ATTEMPT_BUDGET_TAG = 21;
    public static final int QUOTA_TOTAL_TAG = 22;
    public static final int QUOTA_GRANT_ACTIVATION_TAG = 23;
    public static final int QUOTA_BOOKKEEPING_TAG = 24;
    public static final int QUOTA_PAYLOAD_OWNER_TAG = 25;
    public static final int QUOTA_INCARNATION_TAG = 26;
    public static final int MESSAGE_TAG = 5;
    public static final int SCHEDULE_BINDING_TAG = 6;
    public static final int MAX_DOMAIN_SLOT = 0xffff;
    public static final int CANDIDATE_PREFIX_BYTES = 2 + TargetPartitionId.LENGTH + 2 + 8;

    private TargetKeyCodec() {}

    public enum CandidateKind {
        DUE(DUE_TAG),
        NATIVE(NATIVE_TAG);

        private final int tag;

        CandidateKind(final int tag) {
            this.tag = tag;
        }

        public int tag() {
            return tag;
        }
    }

    /** Slot zero is the first domain; its nonzero uint64 generation prevents reuse ABA. */
    public record Domain(int slot, long generation) {
        public Domain {
            if (slot < 0 || slot > MAX_DOMAIN_SLOT || generation == 0) {
                throw new IllegalArgumentException("invalid target execution domain slot/generation");
            }
        }
    }

    public static byte[] candidatePrefix(
            final CandidateKind kind, final TargetPartitionId target, final Domain domain) {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(domain, "domain");
        return ByteBuffer.allocate(CANDIDATE_PREFIX_BYTES)
                .put((byte) kind.tag())
                .put((byte) KEY_FORMAT)
                .put(target.bytes())
                .putShort((short) domain.slot())
                .putLong(domain.generation())
                .array();
    }

    public static byte[] candidate(
            final CandidateKind kind,
            final TargetPartitionId target,
            final Domain domain,
            final long timeEpochMs,
            final byte[] sourceOrderToken,
            final DelayMessageId messageId,
            final int generation) {
        if (timeEpochMs < 0) {
            throw new IllegalArgumentException("target candidate time must be non-negative");
        }
        requireSourceOrderToken(sourceOrderToken);
        return Bytes.concat(
                candidatePrefix(kind, target, domain),
                Bytes.u64be(timeEpochMs),
                sourceOrderToken,
                Objects.requireNonNull(messageId, "messageId").bytes(),
                Bytes.u32beBits(generation));
    }

    public static Candidate decodeCandidate(final byte[] key) {
        Objects.requireNonNull(key, "key");
        final int tokenOffset = CANDIDATE_PREFIX_BYTES + Long.BYTES;
        final int fixedBytes = tokenOffset + DelayMessageId.LENGTH + Integer.BYTES;
        if (key.length != fixedBytes + 9 && key.length != fixedBytes + 21) {
            throw new IllegalArgumentException("invalid target candidate key length");
        }
        final ByteBuffer input = ByteBuffer.wrap(key);
        final CandidateKind kind =
                switch (Byte.toUnsignedInt(input.get())) {
                    case DUE_TAG -> CandidateKind.DUE;
                    case NATIVE_TAG -> CandidateKind.NATIVE;
                    default -> throw new IllegalArgumentException("unknown target candidate tag");
                };
        if (Byte.toUnsignedInt(input.get()) != KEY_FORMAT) {
            throw new IllegalArgumentException("unknown target candidate key format");
        }
        final byte[] target = new byte[TargetPartitionId.LENGTH];
        input.get(target);
        final Domain domain = new Domain(Short.toUnsignedInt(input.getShort()), input.getLong());
        final long time = input.getLong();
        final byte[] token = new byte[key.length - fixedBytes];
        input.get(token);
        final byte[] message = new byte[DelayMessageId.LENGTH];
        input.get(message);
        return new Candidate(
                kind, new TargetPartitionId(target), domain, time, token, new DelayMessageId(message), input.getInt());
    }

    public record Candidate(
            CandidateKind kind,
            TargetPartitionId target,
            Domain domain,
            long timeEpochMs,
            byte[] sourceOrderToken,
            DelayMessageId messageId,
            int generation) {
        public Candidate {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(target, "target");
            Objects.requireNonNull(domain, "domain");
            Objects.requireNonNull(messageId, "messageId");
            if (timeEpochMs < 0) {
                throw new IllegalArgumentException("negative target candidate time");
            }
            requireSourceOrderToken(sourceOrderToken);
            sourceOrderToken = Bytes.copy(sourceOrderToken);
        }

        @Override
        public byte[] sourceOrderToken() {
            return Bytes.copy(sourceOrderToken);
        }

        public byte[] encodedKey() {
            return candidate(kind, target, domain, timeEpochMs, sourceOrderToken, messageId, generation);
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof Candidate that
                    && kind == that.kind
                    && target.equals(that.target)
                    && domain.equals(that.domain)
                    && timeEpochMs == that.timeEpochMs
                    && Arrays.equals(sourceOrderToken, that.sourceOrderToken)
                    && messageId.equals(that.messageId)
                    && generation == that.generation;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    kind, target, domain, timeEpochMs, Arrays.hashCode(sourceOrderToken), messageId, generation);
        }
    }

    public static byte[] expiry(
            final long expireAtEpochMs,
            final TargetPartitionId target,
            final DelayMessageId messageId,
            final int generation) {
        if (expireAtEpochMs < 0) {
            throw new IllegalArgumentException("negative target expiry time");
        }
        return Bytes.concat(
                new byte[] {EXPIRY_TAG, KEY_FORMAT},
                Bytes.u64be(expireAtEpochMs),
                target.bytes(),
                messageId.bytes(),
                Bytes.u32beBits(generation));
    }

    public static byte[] state(final TargetPartitionId target) {
        return Bytes.concat(new byte[] {STATE_TAG, KEY_FORMAT}, target.bytes());
    }

    /** Full FIFO order, independent of retry eligibility and execution-slot assignment. */
    public static byte[] ordered(
            final TargetPartitionId target,
            final byte[] orderingDomain,
            final long deliverAtEpochMs,
            final byte[] sourceOrderToken,
            final DelayMessageId messageId,
            final int generation) {
        requireOrderingDomain(orderingDomain);
        requireSourceOrderToken(sourceOrderToken);
        if (deliverAtEpochMs < 0) {
            throw new IllegalArgumentException("negative target FIFO delivery time");
        }
        return Bytes.concat(
                new byte[] {ORDERED_TAG, KEY_FORMAT},
                target.bytes(),
                orderingDomain,
                Bytes.u64be(deliverAtEpochMs),
                sourceOrderToken,
                messageId.bytes(),
                Bytes.u32beBits(generation));
    }

    public static Ordered decodeOrdered(final byte[] key) {
        final int fixedBytes = 2 + 32 + 32 + 8 + DelayMessageId.LENGTH + 4;
        if (key == null
                || (key.length != fixedBytes + 9 && key.length != fixedBytes + 21)
                || key[0] != ORDERED_TAG
                || key[1] != KEY_FORMAT) {
            throw new IllegalArgumentException("invalid Target FIFO key");
        }
        final ByteBuffer input = ByteBuffer.wrap(key);
        input.position(2);
        final byte[] target = new byte[32];
        final byte[] orderingDomain = new byte[32];
        input.get(target).get(orderingDomain);
        final long deliverAt = input.getLong();
        final byte[] token = new byte[key.length - fixedBytes];
        final byte[] message = new byte[DelayMessageId.LENGTH];
        input.get(token).get(message);
        return new Ordered(
                new TargetPartitionId(target),
                orderingDomain,
                deliverAt,
                token,
                new DelayMessageId(message),
                input.getInt());
    }

    public record Ordered(
            TargetPartitionId target,
            byte[] orderingDomain,
            long deliverAtEpochMs,
            byte[] sourceOrderToken,
            DelayMessageId messageId,
            int generation) {
        public Ordered {
            Objects.requireNonNull(target, "target");
            Objects.requireNonNull(messageId, "messageId");
            requireOrderingDomain(orderingDomain);
            requireSourceOrderToken(sourceOrderToken);
            if (deliverAtEpochMs < 0) {
                throw new IllegalArgumentException("negative Target FIFO delivery time");
            }
            orderingDomain = Bytes.copy(orderingDomain);
            sourceOrderToken = Bytes.copy(sourceOrderToken);
        }

        @Override
        public byte[] orderingDomain() {
            return Bytes.copy(orderingDomain);
        }

        @Override
        public byte[] sourceOrderToken() {
            return Bytes.copy(sourceOrderToken);
        }

        public byte[] encodedKey() {
            return ordered(target, orderingDomain, deliverAtEpochMs, sourceOrderToken, messageId, generation);
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof Ordered that
                    && target.equals(that.target)
                    && Arrays.equals(orderingDomain, that.orderingDomain)
                    && deliverAtEpochMs == that.deliverAtEpochMs
                    && Arrays.equals(sourceOrderToken, that.sourceOrderToken)
                    && messageId.equals(that.messageId)
                    && generation == that.generation;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    target,
                    Arrays.hashCode(orderingDomain),
                    deliverAtEpochMs,
                    Arrays.hashCode(sourceOrderToken),
                    messageId,
                    generation);
        }
    }

    /** One eligible summary per ordering domain; a blocked FIFO successor never enters this index. */
    public static byte[] orderedHead(
            final TargetPartitionId target,
            final Domain executionDomain,
            final long eligibleAtEpochMs,
            final byte[] orderingDomain) {
        requireOrderingDomain(orderingDomain);
        if (eligibleAtEpochMs < 0) {
            throw new IllegalArgumentException("negative ordered head eligibility");
        }
        final byte[] prefix = candidatePrefix(CandidateKind.DUE, target, executionDomain);
        prefix[0] = ORDER_HEAD_TAG;
        return Bytes.concat(prefix, Bytes.u64be(eligibleAtEpochMs), orderingDomain);
    }

    public static OrderedHead decodeOrderedHead(final byte[] key) {
        if (key == null
                || key.length != CANDIDATE_PREFIX_BYTES + Long.BYTES + 32
                || key[0] != ORDER_HEAD_TAG
                || key[1] != KEY_FORMAT) {
            throw new IllegalArgumentException("invalid target ordered head key");
        }
        final ByteBuffer input = ByteBuffer.wrap(key);
        input.position(2);
        final byte[] target = new byte[32];
        input.get(target);
        final Domain domain = new Domain(Short.toUnsignedInt(input.getShort()), input.getLong());
        final long eligibleAt = input.getLong();
        final byte[] orderingDomain = new byte[32];
        input.get(orderingDomain);
        return new OrderedHead(new TargetPartitionId(target), domain, eligibleAt, orderingDomain);
    }

    public record OrderedHead(TargetPartitionId target, Domain domain, long eligibleAtEpochMs, byte[] orderingDomain) {
        public OrderedHead {
            Objects.requireNonNull(target, "target");
            Objects.requireNonNull(domain, "domain");
            requireOrderingDomain(orderingDomain);
            if (eligibleAtEpochMs < 0) {
                throw new IllegalArgumentException("negative target ordered head eligibility");
            }
            orderingDomain = Bytes.copy(orderingDomain);
        }

        @Override
        public byte[] orderingDomain() {
            return Bytes.copy(orderingDomain);
        }

        public byte[] encodedKey() {
            return orderedHead(target, domain, eligibleAtEpochMs, orderingDomain);
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof OrderedHead that
                    && target.equals(that.target)
                    && domain.equals(that.domain)
                    && eligibleAtEpochMs == that.eligibleAtEpochMs
                    && Arrays.equals(orderingDomain, that.orderingDomain);
        }

        @Override
        public int hashCode() {
            return Objects.hash(target, domain, eligibleAtEpochMs, Arrays.hashCode(orderingDomain));
        }
    }

    public static byte[] orderState(final TargetPartitionId target, final byte[] orderingDomain) {
        requireOrderingDomain(orderingDomain);
        return Bytes.concat(new byte[] {ORDER_STATE_TAG, KEY_FORMAT}, target.bytes(), orderingDomain);
    }

    public static byte[] message(final DelayMessageId messageId) {
        return Bytes.concat(new byte[] {MESSAGE_TAG, KEY_FORMAT}, messageId.bytes());
    }

    public static byte[] identity(final TargetPartitionId target) {
        return Bytes.concat(new byte[] {IDENTITY_TAG, KEY_FORMAT}, target.bytes());
    }

    public static byte[] dispatchCompatibility(final byte[] digest) {
        requireAssignedDigest(digest, "dispatchCompatibilityDigest");
        return Bytes.concat(new byte[] {DISPATCH_COMPATIBILITY_TAG, KEY_FORMAT}, digest);
    }

    public static byte[] controlScope(final byte[] digest) {
        requireAssignedDigest(digest, "controlScopeDigest");
        return Bytes.concat(new byte[] {CONTROL_SCOPE_TAG, KEY_FORMAT}, digest);
    }

    public static byte[] channelIdentity(final byte[] digest) {
        requireAssignedDigest(digest, "channelIdentityDigest");
        return Bytes.concat(new byte[] {CHANNEL_IDENTITY_TAG, KEY_FORMAT}, digest);
    }

    public static byte[] scheduleBinding(final byte[] digest) {
        requireAssignedDigest(digest, "scheduleBindingDigest");
        return Bytes.concat(new byte[] {SCHEDULE_BINDING_TAG, KEY_FORMAT}, digest);
    }

    public static byte[] membershipGrant(final byte[] digest) {
        requireAssignedDigest(digest, "membershipGrantDigest");
        return Bytes.concat(new byte[] {MEMBERSHIP_GRANT_TAG, KEY_FORMAT}, digest);
    }

    public static byte[] membershipPolicy(final byte[] digest) {
        requireAssignedDigest(digest, "membershipPolicyDigest");
        return Bytes.concat(new byte[] {MEMBERSHIP_POLICY_TAG, KEY_FORMAT}, digest);
    }

    public static byte[] nativePolicyScope(final byte[] digest) {
        requireAssignedDigest(digest, "nativePolicyScopeDigest");
        return Bytes.concat(new byte[] {NATIVE_POLICY_SCOPE_TAG, KEY_FORMAT}, digest);
    }

    public static byte[] nativePolicySnapshot(final byte[] digest) {
        requireAssignedDigest(digest, "nativePolicySnapshotDigest");
        return Bytes.concat(new byte[] {NATIVE_POLICY_SNAPSHOT_TAG, KEY_FORMAT}, digest);
    }

    private static void requireAssignedDigest(final byte[] digest, final String name) {
        Bytes.requireLength(digest, 32, name);
        if (Arrays.equals(digest, new byte[32])) {
            throw new IllegalArgumentException(name + " is unassigned");
        }
    }

    private static void requireOrderingDomain(final byte[] orderingDomain) {
        Bytes.requireLength(orderingDomain, 32, "orderingDomain");
        if (Arrays.equals(orderingDomain, new byte[32])) {
            throw new IllegalArgumentException("ordering domain identity is unassigned");
        }
    }

    private static void requireSourceOrderToken(final byte[] token) {
        Objects.requireNonNull(token, "sourceOrderToken");
        if (!((token.length == 9 && token[0] == 1) || (token.length == 21 && token[0] == 2))) {
            throw new IllegalArgumentException("target source-order token is not a registered variant");
        }
    }
}
