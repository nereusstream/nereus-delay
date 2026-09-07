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
    public static final int STATE_TAG = 9;
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

    private static void requireSourceOrderToken(final byte[] token) {
        Objects.requireNonNull(token, "sourceOrderToken");
        if (!((token.length == 9 && token[0] == 1) || (token.length == 21 && token[0] == 2))) {
            throw new IllegalArgumentException("target source-order token is not a registered variant");
        }
    }
}
