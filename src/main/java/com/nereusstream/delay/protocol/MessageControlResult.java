package com.nereusstream.delay.protocol;

import java.util.Objects;

/** Public-safe result for one Message control target. */
public final class MessageControlResult {
    private final DelayMessageId messageId;
    private final int generation;
    private final long stateVersion;
    private final MessageGenerationState state;
    private final StableCode stableCode;
    private final PublicEvidenceRef evidence;

    public MessageControlResult(
            final DelayMessageId messageId,
            final int generation,
            final long stateVersion,
            final MessageGenerationState state,
            final StableCode stableCode,
            final PublicEvidenceRef evidence) {
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        if (stateVersion < 0) {
            throw new IllegalArgumentException("invalid Message control generation/state version");
        }
        this.generation = generation;
        this.stateVersion = stateVersion;
        this.state = Objects.requireNonNull(state, "state");
        this.stableCode = Objects.requireNonNull(stableCode, "stableCode");
        this.evidence = evidence;
    }

    public DelayMessageId messageId() {
        return messageId;
    }

    public int generation() {
        return generation;
    }

    public long stateVersion() {
        return stateVersion;
    }

    public MessageGenerationState state() {
        return state;
    }

    public StableCode stableCode() {
        return stableCode;
    }

    public PublicEvidenceRef evidence() {
        return evidence;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, messageId.bytes());
            CanonicalProtobuf.uint32Bits(output, 2, generation);
            CanonicalProtobuf.uint64(output, 3, stateVersion);
            CanonicalProtobuf.uint32(output, 4, state.wireValue());
            CanonicalProtobuf.uint32(output, 5, stableCode.wireValue());
            if (evidence != null) {
                CanonicalProtobuf.bytes(output, 6, evidence.canonicalBytes());
            }
        });
    }

    public static MessageControlResult decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "MessageControlResult");
        if (fields.size() != 5 && fields.size() != 6) {
            throw new IllegalArgumentException("invalid MessageControlResult field count");
        }
        if (fields.get(0).number() != 1
                || fields.get(1).number() != 2
                || fields.get(2).number() != 3
                || fields.get(3).number() != 4
                || fields.get(4).number() != 5
                || (fields.size() == 6 && fields.get(5).number() != 6)) {
            throw new IllegalArgumentException("invalid MessageControlResult field order");
        }
        final MessageControlResult result = new MessageControlResult(
                new DelayMessageId(QueryCodecSupport.fixed(fields.get(0), 1, DelayMessageId.LENGTH)),
                QueryCodecSupport.uint32Bits(fields.get(1), 2),
                QueryCodecSupport.uint(fields.get(2), 3),
                MessageGenerationState.fromWire(QueryCodecSupport.uint(fields.get(3), 4)),
                StableCode.fromWire(QueryCodecSupport.uint32(fields.get(4), 5)),
                fields.size() == 6 ? PublicEvidenceRef.decode(QueryCodecSupport.nested(fields.get(5), 6)) : null);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "MessageControlResult");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof MessageControlResult that
                && generation == that.generation
                && stateVersion == that.stateVersion
                && messageId.equals(that.messageId)
                && state == that.state
                && stableCode == that.stableCode
                && Objects.equals(evidence, that.evidence);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId, generation, stateVersion, state, stableCode, evidence);
    }
}
