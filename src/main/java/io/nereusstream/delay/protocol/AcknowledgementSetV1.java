package io.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Canonical repeated acknowledgement set, strictly sorted by kind. */
public final class AcknowledgementSetV1 {
    private final List<AcknowledgementV1> acknowledgements;

    public AcknowledgementSetV1(final List<AcknowledgementV1> acknowledgements) {
        Objects.requireNonNull(acknowledgements, "acknowledgements");
        final List<AcknowledgementV1> copy = new ArrayList<>(acknowledgements.size());
        int previousKind = 0;
        for (AcknowledgementV1 acknowledgement : acknowledgements) {
            Objects.requireNonNull(acknowledgement, "acknowledgement");
            if (acknowledgement.kind().wireValue() <= previousKind) {
                throw new IllegalArgumentException("acknowledgements must be sorted and unique by kind");
            }
            copy.add(acknowledgement);
            previousKind = acknowledgement.kind().wireValue();
        }
        this.acknowledgements = List.copyOf(copy);
    }

    public static AcknowledgementSetV1 empty() {
        return new AcknowledgementSetV1(List.of());
    }

    public List<AcknowledgementV1> acknowledgements() {
        return acknowledgements;
    }

    public boolean has(final AcknowledgementKindV1 kind) {
        Objects.requireNonNull(kind, "kind");
        return acknowledgements.stream().anyMatch(value -> value.kind() == kind);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            for (AcknowledgementV1 acknowledgement : acknowledgements) {
                CanonicalProtobuf.bytes(output, 1, acknowledgement.canonicalBytes());
            }
        });
    }

    public static AcknowledgementSetV1 decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "AcknowledgementSetV1");
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<AcknowledgementV1> values = new ArrayList<>();
        while (reader.hasRemaining()) {
            final CanonicalProtobuf.Reader.Field field = reader.next();
            if (field.number() != 1 || field.wireType() != 2) {
                throw new IllegalArgumentException("AcknowledgementSetV1 contains an invalid field");
            }
            values.add(AcknowledgementV1.decode(field.rawValue()));
        }
        final AcknowledgementSetV1 result = new AcknowledgementSetV1(values);
        if (!java.util.Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("AcknowledgementSetV1 is not canonical");
        }
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof AcknowledgementSetV1 that && acknowledgements.equals(that.acknowledgements);
    }

    @Override
    public int hashCode() {
        return acknowledgements.hashCode();
    }
}
