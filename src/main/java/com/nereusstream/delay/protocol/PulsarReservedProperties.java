package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Canonical projection of the closed nine-property Nereus Pulsar registry. */
public final class PulsarReservedProperties {
    public static final String PREFIX = "nereus.delay.";
    public static final int IDENTITY_PROPERTY_COUNT = 8;
    public static final int TOTAL_PROPERTY_COUNT = 9;

    private PulsarReservedProperties() {}

    /** Returns the eight identity/time properties that precede prepared_hash. */
    public static List<PulsarMetadata.Property> identityProperties(
            final ReservedPublishMetadata metadata, final byte[] attemptId) {
        final ReservedPublishMetadata checked = Objects.requireNonNull(metadata, "metadata");
        final byte[] checkedAttempt = fixed(attemptId, "attemptId");
        final List<PulsarMetadata.Property> result = new ArrayList<>(IDENTITY_PROPERTY_COUNT);
        result.add(reserved("route", encode(checked.routeIncarnation().bytes())));
        result.add(reserved("partition", Long.toUnsignedString(checked.shardPartition())));
        result.add(reserved("message_id", encode(checked.messageId().bytes())));
        result.add(reserved("generation", Long.toUnsignedString(checked.generation())));
        result.add(reserved("attempt_id", encode(checkedAttempt)));
        result.add(reserved("destination_profile_hash", encode(checked.destinationProfileSemanticHash())));
        result.add(reserved("capability_profile_hash", encode(checked.capabilityProfileSemanticHash())));
        result.add(reserved("deliver_at", Long.toString(checked.deliverAtEpochMs())));
        result.sort(Comparator.comparing(PulsarMetadata.Property::keyUtf8, Arrays::compareUnsigned));
        return Collections.unmodifiableList(result);
    }

    /** Returns all nine properties in strict unsigned-byte key order. */
    public static List<PulsarMetadata.Property> all(
            final ReservedPublishMetadata metadata, final byte[] attemptId, final byte[] preparedHash) {
        final List<PulsarMetadata.Property> result = new ArrayList<>(identityProperties(metadata, attemptId));
        result.add(reserved("prepared_hash", encode(fixed(preparedHash, "preparedHash"))));
        result.sort(Comparator.comparing(PulsarMetadata.Property::keyUtf8, Arrays::compareUnsigned));
        return Collections.unmodifiableList(result);
    }

    public static String encode(final byte[] bytes) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(Objects.requireNonNull(bytes, "bytes"));
    }

    private static PulsarMetadata.Property reserved(final String suffix, final String value) {
        return PulsarMetadata.Property.reserved(PREFIX + suffix, value);
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, 32, name);
        return Bytes.copy(value);
    }
}
