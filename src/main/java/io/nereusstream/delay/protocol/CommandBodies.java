package io.nereusstream.delay.protocol;

import java.nio.ByteBuffer;

/**
 * Command body codecs.
 *
 * <p>The original methods remain legacy embedded adapters. The explicit
 * {@code *V1} methods below emit Registry-shaped Client Body common fields.
 * Runtime application is enabled per command type as its semantic resolver is
 * implemented; callers must not silently fall back from a malformed V1 body
 * to a legacy body.</p>
 */
public final class CommandBodies {
    private CommandBodies() {
    }

    /**
     * Returns whether a body is shaped like a Registry Client Body V1.
     *
     * <p>All Client Body V1 branches begin with required bytes field 1, whose
     * canonical protobuf tag is {@code 0x0a}. The legacy fixed-width adapters
     * begin with a four-byte version integer and therefore cannot be mistaken
     * for this discriminator. The branch codec remains responsible for full
     * canonical validation.</p>
     */
    public static boolean isRegistryClientBodyV1(final byte[] body) {
        return body != null && body.length > 0 && (body[0] & 0xff) == 0x0a;
    }

    public static byte[] schedule(final ScheduleIntent intent) {
        return intent.canonicalBytes();
    }

    public static ScheduleIntent decodeSchedule(final byte[] body) {
        final ByteBuffer input = ByteBuffer.wrap(body);
        if (input.remaining() < 4 + 8 + 8 + 32 + 1 + 4) {
            throw new IllegalArgumentException("truncated schedule body");
        }
        final int version = input.getInt();
        if (version != 1) {
            throw new IllegalArgumentException("unsupported schedule body version: " + version);
        }
        final long deliverAt = input.getLong();
        final long expireAt = input.getLong();
        final byte[] lane = new byte[DestinationLaneId.LENGTH];
        input.get(lane);
        final int ordering = input.get() & 0xff;
        final int payloadLength = input.getInt();
        if (payloadLength < 0 || payloadLength != input.remaining()) {
            throw new IllegalArgumentException("schedule payload length mismatch");
        }
        final byte[] payload = new byte[payloadLength];
        input.get(payload);
        final OrderingMode mode = switch (ordering) {
            case 1 -> OrderingMode.BEST_EFFORT;
            case 2 -> OrderingMode.DELIVERY_TIME_FIFO;
            default -> throw new IllegalArgumentException("unknown ordering mode: " + ordering);
        };
        final ScheduleIntent result = new ScheduleIntent(new DestinationLaneId(lane), deliverAt, expireAt, mode, payload);
        if (!java.util.Arrays.equals(body, result.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical schedule body");
        }
        return result;
    }

    /** Encodes the Registry-shaped {@code ScheduleV1} body. */
    public static byte[] scheduleV1(final DelayMessageId delayMessageId, final long retryUntilEpochMs,
                                    final ScheduleIntentV1 intent) {
        return new ScheduleCommandBodyV1(delayMessageId, retryUntilEpochMs, intent).canonicalBytes();
    }

    /** Decodes the Registry-shaped {@code ScheduleV1} body. */
    public static ScheduleCommandBodyV1 decodeScheduleV1(final byte[] body) {
        return ScheduleCommandBodyV1.decode(body);
    }

    public static byte[] prepareLarge(final LargeScheduleIntent intent) {
        return intent.canonicalBytes();
    }

    public static LargeScheduleIntent decodePrepareLarge(final byte[] body) {
        final ByteBuffer input = ByteBuffer.wrap(body);
        final int expectedLength = 4 + 8 + 8 + DestinationLaneId.LENGTH + 1 + 8 + 32 + 8 + 8;
        if (body.length != expectedLength || input.getInt() != 1) {
            throw new IllegalArgumentException("invalid large schedule prepare body");
        }
        final long deliverAt = input.getLong();
        final long expireAt = input.getLong();
        final byte[] lane = new byte[DestinationLaneId.LENGTH];
        input.get(lane);
        final OrderingMode ordering = switch (input.get() & 0xff) {
            case 1 -> OrderingMode.BEST_EFFORT;
            case 2 -> OrderingMode.DELIVERY_TIME_FIFO;
            default -> throw new IllegalArgumentException("unknown ordering mode");
        };
        final long payloadLength = input.getLong();
        final byte[] payloadSha = new byte[32];
        input.get(payloadSha);
        final long reservationTtl = input.getLong();
        final long trustSetVersion = input.getLong();
        final LargeScheduleIntent result = new LargeScheduleIntent(new DestinationLaneId(lane), deliverAt,
                expireAt, ordering, payloadLength, payloadSha, reservationTtl, trustSetVersion);
        if (!java.util.Arrays.equals(body, result.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical large schedule prepare body");
        }
        return result;
    }

    /** Encodes the Registry-shaped {@code PrepareLargeScheduleV1} body. */
    public static byte[] prepareLargeV1(final DelayMessageId delayMessageId, final long retryUntilEpochMs,
                                        final ScheduleIntentV1 intentWithoutPayload,
                                        final long expectedPayloadLength, final byte[] payloadSha256,
                                        final long reservationTtlMs, final PayloadProofTrustSetRefV1 trustSet) {
        return new PrepareLargeScheduleBodyV1(delayMessageId, retryUntilEpochMs, intentWithoutPayload,
                expectedPayloadLength, payloadSha256, reservationTtlMs, trustSet).canonicalBytes();
    }

    /** Decodes the Registry-shaped {@code PrepareLargeScheduleV1} body. */
    public static PrepareLargeScheduleBodyV1 decodePrepareLargeV1(final byte[] body) {
        return PrepareLargeScheduleBodyV1.decode(body);
    }

    public static byte[] commitLarge(final PayloadCommitProof proof) {
        return proof.canonicalBytes();
    }

    public static PayloadCommitProof decodeCommitLarge(final byte[] body) {
        return PayloadCommitProof.decode(body);
    }

    /** Encodes the Registry-shaped CommitLargeSchedule body around the legacy proof projection. */
    public static byte[] commitLargeV1(final DelayMessageId delayMessageId, final long retryUntilEpochMs,
                                       final byte[] reservationId, final PayloadCommitProof proof) {
        return new CommitLargeScheduleBodyV1(delayMessageId, retryUntilEpochMs, reservationId, proof)
                .canonicalBytes();
    }

    /** Decodes the Registry-shaped CommitLargeSchedule body. */
    public static CommitLargeScheduleBodyV1 decodeCommitLargeV1(final byte[] body) {
        return CommitLargeScheduleBodyV1.decode(body);
    }

    public static byte[] cancel(final int expectedGeneration) {
        final ByteBuffer result = ByteBuffer.allocate(8);
        result.putInt(1).putInt(expectedGeneration).flip();
        return result.array();
    }

    public static int decodeCancel(final byte[] body) {
        if (body.length != 8) {
            throw new IllegalArgumentException("invalid cancel body");
        }
        final ByteBuffer input = ByteBuffer.wrap(body);
        if (input.getInt() != 1) {
            throw new IllegalArgumentException("unsupported cancel body version");
        }
        return input.getInt();
    }

    /** Encodes the Registry-shaped {@code CancelV1} body. */
    public static byte[] cancelV1(final DelayMessageId delayMessageId, final long retryUntilEpochMs,
                                  final MessagePreconditionV1 precondition) {
        return new CancelCommandBodyV1(delayMessageId, retryUntilEpochMs, precondition).canonicalBytes();
    }

    /** Decodes the Registry-shaped {@code CancelV1} body. */
    public static CancelCommandBodyV1 decodeCancelV1(final byte[] body) {
        return CancelCommandBodyV1.decode(body);
    }

    public static byte[] reschedule(final int expectedGeneration, final long deliverAt, final long expireAt) {
        if (expectedGeneration < -1 || deliverAt < 0 || expireAt < deliverAt) {
            throw new IllegalArgumentException("invalid reschedule body");
        }
        final ByteBuffer result = ByteBuffer.allocate(4 + 4 + 8 + 8);
        result.putInt(1).putInt(expectedGeneration).putLong(deliverAt).putLong(expireAt);
        return result.array();
    }

    public static RescheduleValues decodeReschedule(final byte[] body) {
        if (body.length != 24) {
            throw new IllegalArgumentException("invalid reschedule body");
        }
        final ByteBuffer input = ByteBuffer.wrap(body);
        if (input.getInt() != 1) {
            throw new IllegalArgumentException("unsupported reschedule body version");
        }
        final RescheduleValues result = new RescheduleValues(input.getInt(), input.getLong(), input.getLong());
        if (result.deliverAtEpochMs() < 0 || result.expireAtEpochMs() < result.deliverAtEpochMs()) {
            throw new IllegalArgumentException("invalid reschedule window");
        }
        return result;
    }

    /** Encodes the Registry-shaped {@code RescheduleV1} body. */
    public static byte[] rescheduleV1(final DelayMessageId delayMessageId, final long retryUntilEpochMs,
                                      final MessagePreconditionV1 precondition, final long deliverAt,
                                      final long expireAt) {
        return new RescheduleCommandBodyV1(delayMessageId, retryUntilEpochMs, precondition, deliverAt,
                expireAt).canonicalBytes();
    }

    /** Decodes the Registry-shaped {@code RescheduleV1} body. */
    public static RescheduleCommandBodyV1 decodeRescheduleV1(final byte[] body) {
        return RescheduleCommandBodyV1.decode(body);
    }

    public record RescheduleValues(int expectedGeneration, long deliverAtEpochMs, long expireAtEpochMs) {
    }
}
