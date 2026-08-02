package io.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.List;

/** Canonical Client Command envelope codec and NDL1 frame adapter. */
public final class CommandCodec {
    private CommandCodec() {
    }

    public static byte[] encodeEnvelope(final PreparedCommand command) {
        final byte[] client = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, command.commandId().bytes());
            CanonicalProtobuf.bytes(output, 3, command.delayMessageId().bytes());
            CanonicalProtobuf.uint32(output, 5, command.type().wireValue());
            CanonicalProtobuf.int64(output, 7, command.retryUntilEpochMs());
            CanonicalProtobuf.bytes(output, 8, command.canonicalBody());
            CanonicalProtobuf.bytes(output, 9, command.commandHash());
            CanonicalProtobuf.uint32(output, 10, 1);
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, client);
        });
    }

    /** Encodes a command after validating the currently supported V1 body branches. */
    public static byte[] encodeEnvelopeV1(final PreparedCommand command) {
        validateV1Body(command);
        return encodeEnvelope(command);
    }

    public static byte[] encodeFrame(final PreparedCommand command) {
        return ShardLogFrame.encodeClientCommand(encodeEnvelope(command));
    }

    /** Encodes a frame after validating the currently supported V1 body branches. */
    public static byte[] encodeFrameV1(final PreparedCommand command) {
        return ShardLogFrame.encodeClientCommand(encodeEnvelopeV1(command));
    }

    public static PreparedCommand decodeFrame(final byte[] frame) {
        final ShardLogFrame.Decoded decoded = ShardLogFrame.decode(frame);
        if (decoded.recordKind() != ShardLogFrame.CLIENT_COMMAND_KIND) {
            throw new IllegalArgumentException("frame is not a Client Command");
        }
        return decodeEnvelope(decoded.canonicalEnvelope());
    }

    public static PreparedCommand decodeEnvelope(final byte[] envelope) {
        final CanonicalProtobuf.Reader outer = new CanonicalProtobuf.Reader(envelope);
        final List<CanonicalProtobuf.Reader.Field> fields = readAll(outer);
        requireFieldCount(fields, 2);
        requireVarint(fields.get(0), 1, 1);
        final CanonicalProtobuf.Reader.Field clientField = fields.get(1);
        requireWire(clientField, 2, 2);

        final CanonicalProtobuf.Reader inner = new CanonicalProtobuf.Reader(clientField.rawValue());
        final List<CanonicalProtobuf.Reader.Field> command = readAll(inner);
        if (command.size() != 8) {
            throw new IllegalArgumentException("Client Command envelope fields are incomplete or unknown");
        }
        requireVarint(command.get(0), 1, 1);
        final CommandId commandId = new CommandId(requireBytes(command.get(1), 2));
        final DelayMessageId messageId = new DelayMessageId(requireBytes(command.get(2), 3));
        final int commandType = Math.toIntExact(requireVarint(command.get(3), 5));
        final long retryUntil = requireVarint(command.get(4), 7);
        final byte[] body = requireBytes(command.get(5), 8);
        final byte[] hash = requireBytes(command.get(6), 9);
        requireVarint(command.get(7), 10, 1);
        final CommandType type = switch (commandType) {
            case 1 -> CommandType.SCHEDULE;
            case 2 -> CommandType.PREPARE_LARGE_SCHEDULE;
            case 3 -> CommandType.COMMIT_LARGE_SCHEDULE;
            case 4 -> CommandType.CANCEL;
            case 5 -> CommandType.RESCHEDULE;
            default -> throw new IllegalArgumentException("unknown CommandType: " + commandType);
        };
        final ShardId shardId = commandId.routingId().shardId();
        if (!shardId.equals(messageId.routingId().shardId())) {
            throw new IllegalArgumentException("command and message route mismatch");
        }
        return new PreparedCommand(shardId, commandId, messageId, type, retryUntil, body, hash);
    }

    /** Decodes and validates the Registry-shaped Schedule/Prepare body branches. */
    public static PreparedCommand decodeEnvelopeV1(final byte[] envelope) {
        final PreparedCommand command = decodeEnvelope(envelope);
        validateV1Body(command);
        return command;
    }

    /** Decodes a frame and validates the Registry-shaped Schedule/Prepare body branches. */
    public static PreparedCommand decodeFrameV1(final byte[] frame) {
        final ShardLogFrame.Decoded decoded = ShardLogFrame.decode(frame);
        if (decoded.recordKind() != ShardLogFrame.CLIENT_COMMAND_KIND) {
            throw new IllegalArgumentException("frame is not a Client Command");
        }
        return decodeEnvelopeV1(decoded.canonicalEnvelope());
    }

    private static void validateV1Body(final PreparedCommand command) {
        switch (command.type()) {
            case SCHEDULE -> {
                final ScheduleCommandBodyV1 body = ScheduleCommandBodyV1.decode(command.canonicalBody());
                requireCommonBodyIdentity(command, body.delayMessageId(), body.retryUntilEpochMs());
            }
            case PREPARE_LARGE_SCHEDULE -> {
                final PrepareLargeScheduleBodyV1 body = PrepareLargeScheduleBodyV1.decode(command.canonicalBody());
                requireCommonBodyIdentity(command, body.delayMessageId(), body.retryUntilEpochMs());
            }
            default -> throw new IllegalArgumentException("V1 body codec is not implemented for " + command.type());
        }
    }

    private static void requireCommonBodyIdentity(final PreparedCommand command, final DelayMessageId bodyMessageId,
                                                  final long bodyRetryUntilEpochMs) {
        if (!command.delayMessageId().equals(bodyMessageId)
                || command.retryUntilEpochMs() != bodyRetryUntilEpochMs) {
            throw new IllegalArgumentException("Client body common fields do not match outer command");
        }
    }

    private static List<CanonicalProtobuf.Reader.Field> readAll(final CanonicalProtobuf.Reader reader) {
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return fields;
    }

    private static void requireFieldCount(final List<CanonicalProtobuf.Reader.Field> fields, final int expected) {
        if (fields.size() != expected) {
            throw new IllegalArgumentException("unexpected protobuf field count");
        }
    }

    private static void requireWire(final CanonicalProtobuf.Reader.Field field, final int number, final int wire) {
        if (field.number() != number || field.wireType() != wire) {
            throw new IllegalArgumentException("unexpected protobuf field " + field.number());
        }
    }

    private static void requireVarint(final CanonicalProtobuf.Reader.Field field, final int number, final long expected) {
        requireWire(field, number, 0);
        if (field.unsignedValue() != expected) {
            throw new IllegalArgumentException("unexpected value for protobuf field " + number);
        }
    }

    private static long requireVarint(final CanonicalProtobuf.Reader.Field field, final int number) {
        requireWire(field, number, 0);
        return field.unsignedValue();
    }

    private static byte[] requireBytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        requireWire(field, number, 2);
        return field.rawValue();
    }
}
