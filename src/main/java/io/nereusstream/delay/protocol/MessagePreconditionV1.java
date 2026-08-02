package io.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Optional generation/state-version precondition with independent presence. */
public final class MessagePreconditionV1 {
    private final Long expectedGeneration;
    private final Long expectedStateVersion;

    public MessagePreconditionV1(final Long expectedGeneration, final Long expectedStateVersion) {
        if (expectedGeneration != null && (expectedGeneration < 0 || expectedGeneration > 0xffff_ffffL)) {
            throw new IllegalArgumentException("expectedGeneration must be an unsigned uint32");
        }
        if (expectedStateVersion != null && expectedStateVersion < 0) {
            throw new IllegalArgumentException("expectedStateVersion must be non-negative");
        }
        this.expectedGeneration = expectedGeneration;
        this.expectedStateVersion = expectedStateVersion;
    }

    public Long expectedGeneration() {
        return expectedGeneration;
    }

    public Long expectedStateVersion() {
        return expectedStateVersion;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            if (expectedGeneration != null) {
                CanonicalProtobuf.uint32(output, 1, expectedGeneration);
            }
            if (expectedStateVersion != null) {
                CanonicalProtobuf.uint64(output, 2, expectedStateVersion);
            }
        });
    }

    public static MessagePreconditionV1 decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() > 2 || !fields.isEmpty() && fields.get(0).number() != 1
                || fields.size() == 2 && fields.get(1).number() != 2) {
            throw new IllegalArgumentException("MessagePreconditionV1 has invalid field order");
        }
        final Long generation = fields.isEmpty() ? null : QueryCodecSupport.uint(fields.get(0), 1);
        final Long stateVersion = fields.size() < 2 ? null : QueryCodecSupport.uint(fields.get(1), 2);
        final MessagePreconditionV1 result = new MessagePreconditionV1(generation, stateVersion);
        if (!Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical MessagePreconditionV1");
        }
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof MessagePreconditionV1 that
                && Objects.equals(expectedGeneration, that.expectedGeneration)
                && Objects.equals(expectedStateVersion, that.expectedStateVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expectedGeneration, expectedStateVersion);
    }
}
