package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Canonical repeated acknowledgement set, strictly sorted by kind. */
public final class AcknowledgementSet {
    private final List<Acknowledgement> acknowledgements;

    public AcknowledgementSet(final List<Acknowledgement> acknowledgements) {
        Objects.requireNonNull(acknowledgements, "acknowledgements");
        final List<Acknowledgement> copy = new ArrayList<>(acknowledgements.size());
        int previousKind = 0;
        for (Acknowledgement acknowledgement : acknowledgements) {
            Objects.requireNonNull(acknowledgement, "acknowledgement");
            if (acknowledgement.kind().wireValue() <= previousKind) {
                throw new IllegalArgumentException("acknowledgements must be sorted and unique by kind");
            }
            copy.add(acknowledgement);
            previousKind = acknowledgement.kind().wireValue();
        }
        this.acknowledgements = List.copyOf(copy);
    }

    public static AcknowledgementSet empty() {
        return new AcknowledgementSet(List.of());
    }

    public List<Acknowledgement> acknowledgements() {
        return acknowledgements;
    }

    public boolean has(final AcknowledgementKind kind) {
        Objects.requireNonNull(kind, "kind");
        return acknowledgements.stream().anyMatch(value -> value.kind() == kind);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            for (Acknowledgement acknowledgement : acknowledgements) {
                CanonicalProtobuf.bytes(output, 1, acknowledgement.canonicalBytes());
            }
        });
    }

    public static AcknowledgementSet decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "AcknowledgementSet");
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<Acknowledgement> values = new ArrayList<>();
        while (reader.hasRemaining()) {
            final CanonicalProtobuf.Reader.Field field = reader.next();
            if (field.number() != 1 || field.wireType() != 2) {
                throw new IllegalArgumentException("AcknowledgementSet contains an invalid field");
            }
            values.add(Acknowledgement.decode(field.rawValue()));
        }
        final AcknowledgementSet result = new AcknowledgementSet(values);
        if (!java.util.Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("AcknowledgementSet is not canonical");
        }
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof AcknowledgementSet that && acknowledgements.equals(that.acknowledgements);
    }

    @Override
    public int hashCode() {
        return acknowledgements.hashCode();
    }
}
