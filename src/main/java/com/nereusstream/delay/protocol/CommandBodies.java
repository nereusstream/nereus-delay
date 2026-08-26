package com.nereusstream.delay.protocol;

import java.nio.ByteBuffer;

/**
 * Command body codecs.
 *
 * <p>The production methods emit or decode the current Registry-shaped Client Body. Compact methods are an
 * explicit embedded-service API shape, not a project version or fallback decoder. The caller selects one shape
 * before decoding, and malformed input never falls through to the other shape.</p>
 */
public final class CommandBodies {
    private CommandBodies() {}

    /** Encodes the compact embedded Schedule body used by direct shard fixtures. */
    public static byte[] schedule(final ScheduleIntent intent) {
        return intent.canonicalBytes();
    }

    /** Decodes the compact embedded Schedule body. */
    public static ScheduleIntent decodeDirectSchedule(final byte[] body) {
        final ByteBuffer input = ByteBuffer.wrap(body);
        if (input.remaining() < 4 + 8 + 8 + 32 + 1 + 4) {
            throw new IllegalArgumentException("truncated schedule body");
        }
        final int format = input.getInt();
        if (format != 1) {
            throw new IllegalArgumentException("unsupported schedule body format: " + format);
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
        final OrderingMode mode =
                switch (ordering) {
                    case 1 -> OrderingMode.BEST_EFFORT;
                    case 2 -> OrderingMode.DELIVERY_TIME_FIFO;
                    default -> throw new IllegalArgumentException("unknown ordering mode: " + ordering);
                };
        final ScheduleIntent result =
                new ScheduleIntent(new DestinationLaneId(lane), deliverAt, expireAt, mode, payload);
        if (!java.util.Arrays.equals(body, result.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical schedule body");
        }
        return result;
    }

    /**
     * Returns whether a body is shaped like a Registry Client Body.
     *
     * <p>All Client Body branches begin with required bytes field 1, whose
     * canonical protobuf tag is {@code 0x0a}. The branch codec remains
     * responsible for full canonical validation.</p>
     */
    public static boolean isRegistryClientBody(final byte[] body) {
        return body != null && body.length > 0 && (body[0] & 0xff) == 0x0a;
    }

    /** Encodes the Registry-shaped {@code Schedule} body. */
    public static byte[] schedule(
            final DelayMessageId delayMessageId, final long retryUntilEpochMs, final CanonicalScheduleIntent intent) {
        return new ScheduleCommandBody(delayMessageId, retryUntilEpochMs, intent).canonicalBytes();
    }

    /** Decodes the Registry-shaped {@code Schedule} body. */
    public static ScheduleCommandBody decodeSchedule(final byte[] body) {
        return ScheduleCommandBody.decode(body);
    }

    /** Encodes the Registry-shaped {@code PrepareLargeSchedule} body. */
    public static byte[] prepareLarge(
            final DelayMessageId delayMessageId,
            final long retryUntilEpochMs,
            final CanonicalScheduleIntent intentWithoutPayload,
            final long expectedPayloadLength,
            final byte[] payloadSha256,
            final long reservationTtlMs,
            final PayloadProofTrustSetRef trustSet,
            final ProfileRef objectStoreProfile) {
        return new PrepareLargeScheduleBody(
                        delayMessageId,
                        retryUntilEpochMs,
                        intentWithoutPayload,
                        expectedPayloadLength,
                        payloadSha256,
                        reservationTtlMs,
                        trustSet,
                        objectStoreProfile)
                .canonicalBytes();
    }

    /** Decodes the Registry-shaped {@code PrepareLargeSchedule} body. */
    public static PrepareLargeScheduleBody decodePrepareLarge(final byte[] body) {
        return PrepareLargeScheduleBody.decode(body);
    }

    /** Encodes the compact embedded large-payload preparation body. */
    public static byte[] prepareLarge(final LargeScheduleIntent intent) {
        return intent.canonicalBytes();
    }

    /** Decodes the compact embedded large-payload preparation body. */
    public static LargeScheduleIntent decodeDirectPrepareLarge(final byte[] body) {
        final ByteBuffer input = ByteBuffer.wrap(body);
        final int expectedLength = 4 + 8 + 8 + DestinationLaneId.LENGTH + 1 + 8 + 32 + 8 + 8;
        if (body.length != expectedLength || input.getInt() != 1) {
            throw new IllegalArgumentException("invalid large schedule prepare body");
        }
        final long deliverAt = input.getLong();
        final long expireAt = input.getLong();
        final byte[] lane = new byte[DestinationLaneId.LENGTH];
        input.get(lane);
        final OrderingMode ordering =
                switch (input.get() & 0xff) {
                    case 1 -> OrderingMode.BEST_EFFORT;
                    case 2 -> OrderingMode.DELIVERY_TIME_FIFO;
                    default -> throw new IllegalArgumentException("unknown ordering mode");
                };
        final long payloadLength = input.getLong();
        final byte[] payloadSha = new byte[32];
        input.get(payloadSha);
        final long reservationTtl = input.getLong();
        final long trustSetVersion = input.getLong();
        final LargeScheduleIntent result = new LargeScheduleIntent(
                new DestinationLaneId(lane),
                deliverAt,
                expireAt,
                ordering,
                payloadLength,
                payloadSha,
                reservationTtl,
                trustSetVersion);
        if (!java.util.Arrays.equals(body, result.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical large schedule prepare body");
        }
        return result;
    }

    /** Encodes the Registry-shaped CommitLargeSchedule body with a typed proof. */
    public static byte[] commitLarge(
            final DelayMessageId delayMessageId,
            final long retryUntilEpochMs,
            final byte[] reservationId,
            final CanonicalPayloadCommitProof proof) {
        return new CommitLargeScheduleBody(delayMessageId, retryUntilEpochMs, reservationId, proof).canonicalBytes();
    }

    /** Decodes the Registry-shaped CommitLargeSchedule body. */
    public static CommitLargeScheduleBody decodeCommitLarge(final byte[] body) {
        return CommitLargeScheduleBody.decode(body);
    }

    /** Encodes the compact embedded payload-commit body. */
    public static byte[] commitLarge(final PayloadCommitProof proof) {
        return proof.canonicalBytes();
    }

    /** Decodes the compact embedded payload-commit body. */
    public static PayloadCommitProof decodeDirectCommitLarge(final byte[] body) {
        return PayloadCommitProof.decode(body);
    }

    /** Encodes the Registry-shaped {@code Cancel} body. */
    public static byte[] cancel(
            final DelayMessageId delayMessageId, final long retryUntilEpochMs, final MessagePrecondition precondition) {
        return new CancelCommandBody(delayMessageId, retryUntilEpochMs, precondition).canonicalBytes();
    }

    /** Decodes the Registry-shaped {@code Cancel} body. */
    public static CancelCommandBody decodeCancel(final byte[] body) {
        return CancelCommandBody.decode(body);
    }

    /** Encodes the compact embedded cancel body. */
    public static byte[] cancel(final int expectedGeneration) {
        final ByteBuffer result = ByteBuffer.allocate(8);
        result.putInt(1).putInt(expectedGeneration).flip();
        return result.array();
    }

    /** Decodes the compact embedded cancel body. */
    public static int decodeDirectCancel(final byte[] body) {
        if (body.length != 8) {
            throw new IllegalArgumentException("invalid cancel body");
        }
        final ByteBuffer input = ByteBuffer.wrap(body);
        if (input.getInt() != 1) {
            throw new IllegalArgumentException("unsupported cancel body format");
        }
        return input.getInt();
    }

    /** Encodes the Registry-shaped {@code Reschedule} body. */
    public static byte[] reschedule(
            final DelayMessageId delayMessageId,
            final long retryUntilEpochMs,
            final MessagePrecondition precondition,
            final long deliverAt,
            final long expireAt) {
        return new RescheduleCommandBody(delayMessageId, retryUntilEpochMs, precondition, deliverAt, expireAt)
                .canonicalBytes();
    }

    /** Decodes the Registry-shaped {@code Reschedule} body. */
    public static RescheduleCommandBody decodeReschedule(final byte[] body) {
        return RescheduleCommandBody.decode(body);
    }

    /** Encodes the compact embedded reschedule body. */
    public static byte[] reschedule(final int expectedGeneration, final long deliverAt, final long expireAt) {
        if (expectedGeneration < -1 || deliverAt < 0 || expireAt < deliverAt) {
            throw new IllegalArgumentException("invalid reschedule body");
        }
        final ByteBuffer result = ByteBuffer.allocate(4 + 4 + 8 + 8);
        result.putInt(1).putInt(expectedGeneration).putLong(deliverAt).putLong(expireAt);
        return result.array();
    }

    /** Decodes the compact embedded reschedule body. */
    public static DirectRescheduleValues decodeDirectReschedule(final byte[] body) {
        if (body.length != 24) {
            throw new IllegalArgumentException("invalid reschedule body");
        }
        final ByteBuffer input = ByteBuffer.wrap(body);
        if (input.getInt() != 1) {
            throw new IllegalArgumentException("unsupported reschedule body format");
        }
        final DirectRescheduleValues result =
                new DirectRescheduleValues(input.getInt(), input.getLong(), input.getLong());
        if (result.deliverAtEpochMs() < 0 || result.expireAtEpochMs() < result.deliverAtEpochMs()) {
            throw new IllegalArgumentException("invalid reschedule window");
        }
        return result;
    }

    public record DirectRescheduleValues(int expectedGeneration, long deliverAtEpochMs, long expireAtEpochMs) {}
}
