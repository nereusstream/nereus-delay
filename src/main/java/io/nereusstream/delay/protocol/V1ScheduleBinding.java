package io.nereusstream.delay.protocol;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Durable semantic binding retained for a Registry Schedule or large-payload
 * reservation.
 *
 * <p>The legacy {@code MessageRecord}/{@code PayloadReservation} projections
 * intentionally keep their compact compatibility shape.  This sidecar keeps
 * the exact V1 body, the resolver's canonical Lane tuple and the derived Lane
 * identity in the same shard WriteBatch, so a reopen cannot silently forget
 * the Profile/Retry/Adapter binding that was accepted at the source
 * position.</p>
 */
public final class V1ScheduleBinding {
    private static final int VERSION = 1;
    private static final int MAX_TUPLE_BYTES = 1 << 20;
    private static final int MAX_BODY_BYTES = 1 << 24;

    private final CommandType commandType;
    private final DelayMessageId delayMessageId;
    private final DestinationLaneId laneId;
    private final byte[] canonicalLaneTuple;
    private final byte[] canonicalBody;

    public V1ScheduleBinding(final CommandType commandType, final DelayMessageId delayMessageId,
                             final DestinationLaneId laneId, final byte[] canonicalLaneTuple,
                             final byte[] canonicalBody) {
        if (commandType != CommandType.SCHEDULE && commandType != CommandType.PREPARE_LARGE_SCHEDULE) {
            throw new IllegalArgumentException("V1 binding command type must be Schedule or PrepareLargeSchedule");
        }
        this.commandType = commandType;
        this.delayMessageId = Objects.requireNonNull(delayMessageId, "delayMessageId");
        this.laneId = Objects.requireNonNull(laneId, "laneId");
        this.canonicalLaneTuple = boundedNonEmpty(canonicalLaneTuple, MAX_TUPLE_BYTES, "canonicalLaneTuple");
        if (!laneId.equals(DestinationLaneId.derive(this.canonicalLaneTuple))) {
            throw new IllegalArgumentException("V1 binding Lane ID does not match canonical tuple");
        }
        this.canonicalBody = boundedNonEmpty(canonicalBody, MAX_BODY_BYTES, "canonicalBody");
        validateBody(commandType, delayMessageId, this.canonicalBody);
    }

    public static V1ScheduleBinding fromCommand(final PreparedCommand command,
                                                final DestinationLaneId laneId,
                                                final byte[] canonicalLaneTuple) {
        Objects.requireNonNull(command, "command");
        return new V1ScheduleBinding(command.type(), command.delayMessageId(), laneId, canonicalLaneTuple,
                command.canonicalBody());
    }

    public CommandType commandType() {
        return commandType;
    }

    public DelayMessageId delayMessageId() {
        return delayMessageId;
    }

    public DestinationLaneId laneId() {
        return laneId;
    }

    public byte[] canonicalLaneTuple() {
        return Bytes.copy(canonicalLaneTuple);
    }

    public byte[] canonicalBody() {
        return Bytes.copy(canonicalBody);
    }

    /** Requires one Claim projection to preserve the immutable resolved Lane tuple. */
    public void requireClaimLaneProjection(final ClaimMaterializationV1 materialization) {
        CanonicalLaneTupleV1.requireClaimProjection(canonicalLaneTuple,
                Objects.requireNonNull(materialization, "materialization"));
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof V1ScheduleBinding that && commandType == that.commandType
                && delayMessageId.equals(that.delayMessageId) && laneId.equals(that.laneId)
                && Arrays.equals(canonicalLaneTuple, that.canonicalLaneTuple)
                && Arrays.equals(canonicalBody, that.canonicalBody);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commandType, delayMessageId, laneId, Arrays.hashCode(canonicalLaneTuple),
                Arrays.hashCode(canonicalBody));
    }

    public byte[] encode() {
        return Bytes.concat(Bytes.u32be(VERSION), Bytes.u8(commandType.wireValue()), delayMessageId.bytes(),
                laneId.bytes(), Bytes.lp32(canonicalLaneTuple), Bytes.lp32(canonicalBody));
    }

    public static V1ScheduleBinding decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        if (input.remaining() < 4 + 1 + DelayMessageId.LENGTH + DestinationLaneId.LENGTH + 4 + 1 + 4 + 1) {
            throw new IllegalArgumentException("V1 Schedule binding is truncated");
        }
        if (input.getInt() != VERSION) {
            throw new IllegalArgumentException("unsupported V1 Schedule binding version");
        }
        final CommandType commandType = switch (input.get() & 0xff) {
            case 1 -> CommandType.SCHEDULE;
            case 2 -> CommandType.PREPARE_LARGE_SCHEDULE;
            default -> throw new IllegalArgumentException("invalid V1 Schedule binding command type");
        };
        final byte[] message = readFixed(input, DelayMessageId.LENGTH);
        final byte[] lane = readFixed(input, DestinationLaneId.LENGTH);
        final byte[] tuple = readLp32(input, MAX_TUPLE_BYTES, "canonicalLaneTuple");
        final byte[] body = readLp32(input, MAX_BODY_BYTES, "canonicalBody");
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("V1 Schedule binding has trailing bytes");
        }
        final V1ScheduleBinding result = new V1ScheduleBinding(commandType, new DelayMessageId(message),
                new DestinationLaneId(lane), tuple, body);
        if (!Arrays.equals(encoded, result.encode())) {
            throw new IllegalArgumentException("non-canonical V1 Schedule binding");
        }
        return result;
    }

    private static void validateBody(final CommandType commandType, final DelayMessageId messageId,
                                     final byte[] body) {
        if (!CommandBodies.isRegistryClientBodyV1(body)) {
            throw new IllegalArgumentException("V1 binding body is not a Registry Client Body");
        }
        if (commandType == CommandType.SCHEDULE) {
            final ScheduleCommandBodyV1 decoded = ScheduleCommandBodyV1.decode(body);
            if (!messageId.equals(decoded.delayMessageId())) {
                throw new IllegalArgumentException("V1 binding Schedule message identity mismatch");
            }
        } else {
            final PrepareLargeScheduleBodyV1 decoded = PrepareLargeScheduleBodyV1.decode(body);
            if (!messageId.equals(decoded.delayMessageId())) {
                throw new IllegalArgumentException("V1 binding Prepare message identity mismatch");
            }
        }
    }

    private static byte[] boundedNonEmpty(final byte[] value, final int maxLength, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0 || value.length > maxLength) {
            throw new IllegalArgumentException(name + " length is outside bounds");
        }
        return Bytes.copy(value);
    }

    private static byte[] readFixed(final ByteBuffer input, final int length) {
        if (input.remaining() < length) {
            throw new IllegalArgumentException("truncated V1 Schedule binding");
        }
        final byte[] result = new byte[length];
        input.get(result);
        return result;
    }

    private static byte[] readLp32(final ByteBuffer input, final int maxLength, final String name) {
        if (input.remaining() < 4) {
            throw new IllegalArgumentException("truncated V1 Schedule binding " + name + " length");
        }
        final long length = Integer.toUnsignedLong(input.getInt());
        if (length == 0 || length > maxLength || length > input.remaining()) {
            throw new IllegalArgumentException("invalid V1 Schedule binding " + name + " length");
        }
        return readFixed(input, Math.toIntExact(length));
    }
}
