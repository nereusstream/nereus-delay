package com.nereusstream.delay.protocol;

/** Allocation bounds before invoking the existing exact Schedule/Prepare decoders. Their wire bytes are preserved. */
final class TargetScheduleBody {
    static final int MAX_INLINE_BYTES = 1 << 24;
    static final int MAX_AUXILIARY_BYTES = 1 << 20;
    static final int MAX_METADATA_ENTRIES = 1024;
    static final int MAX_DESCRIPTOR_BYTES = 4 * MAX_AUXILIARY_BYTES + 1024;
    static final int MAX_BODY_BYTES = MAX_INLINE_BYTES + 3 * MAX_AUXILIARY_BYTES + 4096;

    private TargetScheduleBody() {}

    static CanonicalScheduleIntent validate(final byte[] body, final CommandType type, final DelayMessageId message) {
        final var fields = TargetCompatibilityCodec.read(body, MAX_BODY_BYTES, 9, false, "Target Schedule body");
        for (var field : fields) {
            if (field.number() == 10) {
                requireIntent(QueryCodecSupport.bytes(field, 10));
            } else if (field.number() == 14) {
                TargetCompatibilityCodec.read(
                        QueryCodecSupport.bytes(field, 14), 45, 2, false, "Target payload trust ref");
            } else if (field.number() == 15) {
                requireProfile(QueryCodecSupport.bytes(field, 15));
            }
        }
        if (type == CommandType.SCHEDULE) {
            final var decoded = ScheduleCommandBody.decode(body);
            if (!message.equals(decoded.delayMessageId())) {
                throw new IllegalArgumentException("Target binding Schedule message mismatch");
            }
            return decoded.intent();
        }
        if (type == CommandType.PREPARE_LARGE_SCHEDULE) {
            final var decoded = PrepareLargeScheduleBody.decode(body);
            if (!message.equals(decoded.delayMessageId())) {
                throw new IllegalArgumentException("Target binding Prepare message mismatch");
            }
            return decoded.intentWithoutPayload();
        }
        throw new IllegalArgumentException("Target binding must retain a Schedule or Prepare body");
    }

    private static void requireIntent(final byte[] bytes) {
        final var fields = TargetCompatibilityCodec.read(bytes, MAX_BODY_BYTES, 14, false, "Target Schedule intent");
        for (var field : fields) {
            switch (field.number()) {
                case 1 -> requireProfile(QueryCodecSupport.bytes(field, 1));
                case 2 -> {
                    final var retry = TargetCompatibilityCodec.read(
                            QueryCodecSupport.bytes(field, 2), 304, 3, false, "Target retry reference");
                    if (!retry.isEmpty() && retry.getFirst().number() == 1) {
                        requireLength(QueryCodecSupport.bytes(retry.getFirst(), 1), 256, "Target retry policy ID");
                    }
                }
                case 7, 11 ->
                    requireLength(
                            QueryCodecSupport.bytes(field, field.number()),
                            MAX_AUXILIARY_BYTES,
                            "Target ordering/business key");
                case 8 -> requireLength(QueryCodecSupport.bytes(field, 8), MAX_INLINE_BYTES, "Target inline payload");
                case 9 -> {
                    final var descriptor = TargetCompatibilityCodec.read(
                            QueryCodecSupport.bytes(field, 9),
                            MAX_DESCRIPTOR_BYTES,
                            9,
                            false,
                            "Target committed descriptor");
                    for (var part : descriptor) {
                        if (part.number() == 1) {
                            requireProfile(QueryCodecSupport.bytes(part, 1));
                        } else if (part.number() >= 2 && part.number() <= 5) {
                            requireLength(
                                    QueryCodecSupport.bytes(part, part.number()),
                                    MAX_AUXILIARY_BYTES,
                                    "Target object identity component");
                        }
                    }
                }
                case 10 -> requireMetadata(QueryCodecSupport.bytes(field, 10));
                default -> {
                    /* The existing closed decoder validates scalar/presence semantics. */
                }
            }
        }
    }

    private static void requireProfile(final byte[] bytes) {
        final var fields = TargetCompatibilityCodec.read(
                bytes, TargetChannelIdentity.MAX_PROFILE_REF_BYTES, 4, false, "Target ProfileRef");
        if (!fields.isEmpty() && fields.getFirst().number() == 1) {
            requireLength(QueryCodecSupport.bytes(fields.getFirst(), 1), 256, "Target Profile ID");
        }
    }

    private static void requireMetadata(final byte[] bytes) {
        final var union = TargetCompatibilityCodec.read(bytes, MAX_AUXILIARY_BYTES, 1, false, "Target AdapterMetadata");
        if (union.size() != 1) {
            throw new IllegalArgumentException("Target metadata must select one branch");
        }
        final int branch = union.getFirst().number();
        final var fields = TargetCompatibilityCodec.read(
                QueryCodecSupport.bytes(union.getFirst(), branch),
                MAX_AUXILIARY_BYTES,
                MAX_METADATA_ENTRIES + 3,
                true,
                "Target metadata branch");
        int entries = 0;
        for (var field : fields) {
            if ((branch == 1 && field.number() == 2) || (branch == 2 && field.number() == 4)) {
                if (++entries > MAX_METADATA_ENTRIES) {
                    throw new IllegalArgumentException("Target metadata entry count exceeds bound");
                }
                TargetCompatibilityCodec.read(
                        QueryCodecSupport.bytes(field, field.number()),
                        MAX_AUXILIARY_BYTES,
                        2,
                        false,
                        "Target metadata entry");
            }
        }
    }

    private static void requireLength(final byte[] bytes, final int maximum, final String name) {
        if (bytes.length > maximum) {
            throw new IllegalArgumentException(name + " exceeds its byte bound");
        }
    }
}
