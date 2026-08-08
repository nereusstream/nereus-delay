package io.nereusstream.delay.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Canonical V1 payload for the durable ingress acknowledgement.  It proves
 * only that one prepared command was accepted by the ingress Broker; it does
 * not contain or imply a command-application result.
 */
public final class CommandQueuedReceiptV1 {
    public static final int RECEIPT_VERSION = 1;
    public static final int CAPABILITY_BITS = 0x001f;
    private static final int HASH_LENGTH = 32;
    private static final int ATTEMPT_ID_LENGTH = 16;
    private static final byte KAFKA_SOURCE_BRANCH = 1;
    private static final byte PULSAR_SOURCE_BRANCH = 2;

    private final PreparedCommandRef command;
    private final SourcePosition sourcePosition;
    private final SafeBrokerAck brokerAck;
    private final long receiptQueryUntilEpochMs;
    private final byte[] physicalEnqueueAttemptId;
    private final byte[] receiptPayloadDigest;

    private CommandQueuedReceiptV1(final PreparedCommandRef command, final SourcePosition sourcePosition,
                                   final SafeBrokerAck brokerAck, final long receiptQueryUntilEpochMs,
                                   final byte[] physicalEnqueueAttemptId, final byte[] receiptPayloadDigest) {
        this.command = Objects.requireNonNull(command, "command");
        this.sourcePosition = Objects.requireNonNull(sourcePosition, "sourcePosition");
        this.brokerAck = Objects.requireNonNull(brokerAck, "brokerAck");
        if (!command.shardId().equals(sourcePosition.shardId())) {
            throw new IllegalArgumentException("Prepared command and Source Position belong to different shards");
        }
        if (receiptQueryUntilEpochMs < sourcePosition.brokerPersistenceTimeEpochMs()) {
            throw new IllegalArgumentException("receipt query boundary precedes Broker persistence time");
        }
        requireNonZero(physicalEnqueueAttemptId, ATTEMPT_ID_LENGTH, "physicalEnqueueAttemptId");
        Bytes.requireLength(receiptPayloadDigest, HASH_LENGTH, "receiptPayloadDigest");
        this.receiptQueryUntilEpochMs = receiptQueryUntilEpochMs;
        this.physicalEnqueueAttemptId = Bytes.copy(physicalEnqueueAttemptId);
        this.receiptPayloadDigest = Bytes.copy(receiptPayloadDigest);
        validateSourceAndAck(sourcePosition, brokerAck);
    }

    /** Creates a payload and frame from an immutable command and exact Broker evidence. */
    public static CommandQueuedReceiptV1 create(final PreparedCommand preparedCommand,
                                                final SourcePosition sourcePosition,
                                                final SafeBrokerAck brokerAck,
                                                final long receiptQueryUntilEpochMs,
                                                final byte[] physicalEnqueueAttemptId) {
        final PreparedCommandRef command = PreparedCommandRef.from(preparedCommand);
        final byte[] fields = canonicalFields(command, sourcePosition, brokerAck, receiptQueryUntilEpochMs,
                physicalEnqueueAttemptId);
        return new CommandQueuedReceiptV1(command, sourcePosition, brokerAck, receiptQueryUntilEpochMs,
                physicalEnqueueAttemptId, Bytes.sha256(Bytes.utf8("nereus-delay-command-queued-receipt-v1\0"),
                        fields));
    }

    public static CommandQueuedReceiptV1 decodeFrame(final byte[] frame) {
        final ReceiptFrame.Decoded decoded = ReceiptFrame.decode(frame);
        if (decoded.kind() != ReceiptKind.COMMAND_QUEUED) {
            throw new IllegalArgumentException("receipt frame is not COMMAND_QUEUED");
        }
        return decodePayload(decoded.payload());
    }

    public static CommandQueuedReceiptV1 decodePayload(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "CommandQueuedReceiptV1");
        requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8});
        if (nonNegative(fields.get(0), 1) != RECEIPT_VERSION) {
            throw new IllegalArgumentException("unsupported CommandQueuedReceipt version");
        }
        final PreparedCommandRef command = PreparedCommandRef.decode(nested(fields.get(1), 2));
        final SourcePosition sourcePosition = decodeSourcePosition(nested(fields.get(2), 3));
        final SafeBrokerAck brokerAck = SafeBrokerAck.decode(nested(fields.get(3), 4));
        final long queryUntil = nonNegative(fields.get(4), 5);
        if (nonNegative(fields.get(5), 6) != CAPABILITY_BITS) {
            throw new IllegalArgumentException("CommandQueuedReceipt capability bits are not the V1 set");
        }
        final byte[] attemptId = fixed(fields.get(6), 7, ATTEMPT_ID_LENGTH);
        final byte[] digest = fixed(fields.get(7), 8, HASH_LENGTH);
        final byte[] canonical = canonicalFields(command, sourcePosition, brokerAck, queryUntil, attemptId);
        if (!Bytes.constantTimeEquals(digest,
                Bytes.sha256(Bytes.utf8("nereus-delay-command-queued-receipt-v1\0"), canonical))) {
            throw new IllegalArgumentException("CommandQueuedReceipt payload digest mismatch");
        }
        final CommandQueuedReceiptV1 result = new CommandQueuedReceiptV1(command, sourcePosition, brokerAck,
                queryUntil, attemptId, digest);
        if (!Arrays.equals(encoded, result.payload())) {
            throw new IllegalArgumentException("non-canonical CommandQueuedReceipt payload");
        }
        return result;
    }

    public PreparedCommandRef command() {
        return command;
    }

    public SourcePosition sourcePosition() {
        return sourcePosition;
    }

    public SafeBrokerAck brokerAck() {
        return brokerAck;
    }

    public long receiptQueryUntilEpochMs() {
        return receiptQueryUntilEpochMs;
    }

    public byte[] physicalEnqueueAttemptId() {
        return Bytes.copy(physicalEnqueueAttemptId);
    }

    public byte[] receiptPayloadDigest() {
        return Bytes.copy(receiptPayloadDigest);
    }

    public byte[] payload() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECEIPT_VERSION);
            CanonicalProtobuf.bytes(output, 2, command.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, encodeSourcePosition(sourcePosition));
            CanonicalProtobuf.bytes(output, 4, brokerAck.canonicalBytes());
            CanonicalProtobuf.int64(output, 5, receiptQueryUntilEpochMs);
            CanonicalProtobuf.uint32(output, 6, CAPABILITY_BITS);
            CanonicalProtobuf.bytes(output, 7, physicalEnqueueAttemptId);
            CanonicalProtobuf.bytes(output, 8, receiptPayloadDigest);
        });
    }

    public byte[] frame() {
        return ReceiptFrame.encode(ReceiptKind.COMMAND_QUEUED, payload());
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof CommandQueuedReceiptV1 that)) {
            return false;
        }
        return command.equals(that.command) && sourcePosition.equals(that.sourcePosition)
                && brokerAck.equals(that.brokerAck) && receiptQueryUntilEpochMs == that.receiptQueryUntilEpochMs
                && Arrays.equals(physicalEnqueueAttemptId, that.physicalEnqueueAttemptId)
                && Arrays.equals(receiptPayloadDigest, that.receiptPayloadDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(command, sourcePosition, brokerAck, receiptQueryUntilEpochMs,
                Arrays.hashCode(physicalEnqueueAttemptId), Arrays.hashCode(receiptPayloadDigest));
    }

    private static byte[] canonicalFields(final PreparedCommandRef command, final SourcePosition sourcePosition,
                                          final SafeBrokerAck brokerAck, final long queryUntil,
                                          final byte[] attemptId) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECEIPT_VERSION);
            CanonicalProtobuf.bytes(output, 2, command.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, encodeSourcePosition(sourcePosition));
            CanonicalProtobuf.bytes(output, 4, brokerAck.canonicalBytes());
            CanonicalProtobuf.int64(output, 5, queryUntil);
            CanonicalProtobuf.uint32(output, 6, CAPABILITY_BITS);
            CanonicalProtobuf.bytes(output, 7, attemptId);
        });
    }

    private static byte[] encodeSourcePosition(final SourcePosition position) {
        Objects.requireNonNull(position, "position");
        if (position instanceof KafkaSourcePosition kafka) {
            return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, KAFKA_SOURCE_BRANCH,
                    CanonicalProtobuf.message(kafkaOutput -> {
                        CanonicalProtobuf.bytes(kafkaOutput, 1, kafka.shardId().routeIncarnation().bytes());
                        CanonicalProtobuf.bytes(kafkaOutput, 2,
                                kafka.authenticatedClusterId().getBytes(StandardCharsets.UTF_8));
                        CanonicalProtobuf.bytes(kafkaOutput, 3, uuidBytes(kafka.nativeTopicUuid()));
                        CanonicalProtobuf.uint32Bits(kafkaOutput, 4, kafka.shardId().partition());
                        CanonicalProtobuf.uint64Bits(kafkaOutput, 5, kafka.offset());
                        if (kafka.leaderEpoch() != null) {
                            CanonicalProtobuf.uint32Bits(kafkaOutput, 6, kafka.leaderEpoch());
                        }
                        CanonicalProtobuf.int64(kafkaOutput, 7, kafka.brokerLogAppendTimeEpochMs());
                    })));
        }
        final PulsarSourcePosition pulsar = requirePulsar(position);
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, PULSAR_SOURCE_BRANCH,
                CanonicalProtobuf.message(pulsarOutput -> {
                    CanonicalProtobuf.bytes(pulsarOutput, 1, pulsar.shardId().routeIncarnation().bytes());
                    CanonicalProtobuf.bytes(pulsarOutput, 2, pulsar.brokerResourceIncarnation());
                    CanonicalProtobuf.bytes(pulsarOutput, 3,
                            utf8Nfc(pulsar.physicalTopic(), "physicalTopic").getBytes(StandardCharsets.UTF_8));
                    CanonicalProtobuf.uint32Bits(pulsarOutput, 4, pulsar.shardId().partition());
                    CanonicalProtobuf.uint64Bits(pulsarOutput, 5, pulsar.ledgerId());
                    CanonicalProtobuf.uint64Bits(pulsarOutput, 6, pulsar.entryId());
                    CanonicalProtobuf.uint32Bits(pulsarOutput, 7, pulsar.normalizedBatchIndex());
                    CanonicalProtobuf.uint32Bits(pulsarOutput, 8, pulsar.batchSize());
                    CanonicalProtobuf.uint32(pulsarOutput, 9, pulsar.entryKind().wireValue());
                    CanonicalProtobuf.int64(pulsarOutput, 10, pulsar.brokerEntryTimestampEpochMs());
                })));
    }

    private static SourcePosition decodeSourcePosition(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> outer = read(encoded, "SourcePositionV1");
        if (outer.size() != 1) {
            throw new IllegalArgumentException("SourcePositionV1 must select one branch");
        }
        final CanonicalProtobuf.Reader.Field branch = outer.get(0);
        final List<CanonicalProtobuf.Reader.Field> fields = read(nested(branch, branch.number()),
                "SourcePosition branch");
        if (branch.number() == KAFKA_SOURCE_BRANCH) {
            requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 7}, true);
            final byte[] route = fixed(fields.get(0), 1, RouteIncarnation.LENGTH);
            final String cluster = utf8(bytes(fields.get(1), 2), "authenticatedClusterId");
            final UUID topic = uuid(fixed(fields.get(2), 3, 16));
            final int partition = uint32Bits(fields.get(3), 4);
            final long offset = uint64Bits(fields.get(4), 5);
            final Integer leader = optionalVarint(fields, 6, "leaderEpoch");
            final long append = nonNegative(field(fields, 7), 7);
            return new KafkaSourcePosition(new ShardId(new RouteIncarnation(route), partition), cluster, topic,
                    offset, leader, append);
        }
        if (branch.number() == PULSAR_SOURCE_BRANCH) {
            requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
            final byte[] route = fixed(fields.get(0), 1, RouteIncarnation.LENGTH);
            final byte[] resource = fixed(fields.get(1), 2, 32);
            final String topic = utf8Nfc(utf8(bytes(fields.get(2), 3), "physicalTopic"), "physicalTopic");
            final int partition = uint32Bits(fields.get(3), 4);
            final long ledger = uint64Bits(fields.get(4), 5);
            final long entry = uint64Bits(fields.get(5), 6);
            final int batchIndex = uint32Bits(fields.get(6), 7);
            final int batchSize = uint32Bits(fields.get(7), 8);
            final PulsarSourcePosition.EntryKind entryKind = switch (uint32Int(fields.get(8), 9)) {
                case 1 -> PulsarSourcePosition.EntryKind.NON_BATCH;
                case 2 -> PulsarSourcePosition.EntryKind.BATCH;
                default -> throw new IllegalArgumentException("unknown Pulsar source entry kind");
            };
            final long timestamp = nonNegative(fields.get(9), 10);
            return new PulsarSourcePosition(new ShardId(new RouteIncarnation(route), partition), resource, topic,
                    ledger, entry, batchIndex, batchSize, entryKind, timestamp);
        }
        throw new IllegalArgumentException("unknown SourcePositionV1 branch: " + branch.number());
    }

    private static void validateSourceAndAck(final SourcePosition source, final SafeBrokerAck ack) {
        if (source instanceof KafkaSourcePosition kafka && ack instanceof KafkaQueuedAck kafkaAck) {
            if (!kafka.authenticatedClusterId().equals(kafkaAck.authenticatedClusterId())
                    || !kafka.nativeTopicUuid().equals(kafkaAck.nativeTopicUuid())
                    || kafka.shardId().partition() != kafkaAck.partition()
                    || kafka.offset() != kafkaAck.offset()
                    || !Objects.equals(kafka.leaderEpoch(), kafkaAck.leaderEpoch())
                    || kafka.brokerLogAppendTimeEpochMs() != kafkaAck.brokerLogAppendTimeEpochMs()) {
                throw new IllegalArgumentException("Kafka Source Position and SafeBrokerAck disagree");
            }
            return;
        }
        if (source instanceof PulsarSourcePosition pulsar && ack instanceof PulsarQueuedAck pulsarAck) {
            if (!Arrays.equals(pulsar.brokerResourceIncarnation(), pulsarAck.brokerResourceIncarnation())
                    || !pulsar.physicalTopic().equals(pulsarAck.physicalTopic())
                    || pulsar.shardId().partition() != pulsarAck.partition()
                    || pulsar.ledgerId() != pulsarAck.ledgerId() || pulsar.entryId() != pulsarAck.entryId()
                    || pulsar.normalizedBatchIndex() != pulsarAck.normalizedBatchIndex()
                    || pulsar.batchSize() != pulsarAck.batchSize()
                    || pulsar.brokerEntryTimestampEpochMs() != pulsarAck.brokerEntryTimestampEpochMs()) {
                throw new IllegalArgumentException("Pulsar Source Position and SafeBrokerAck disagree");
            }
            return;
        }
        throw new IllegalArgumentException("Source Position and SafeBrokerAck adapter branches disagree");
    }

    private static PulsarSourcePosition requirePulsar(final SourcePosition position) {
        if (!(position instanceof PulsarSourcePosition pulsar)) {
            throw new IllegalArgumentException("unsupported SourcePosition implementation");
        }
        return pulsar;
    }

    private static List<CanonicalProtobuf.Reader.Field> read(final byte[] encoded, final String name) {
        Objects.requireNonNull(encoded, name);
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException(name + " is empty");
        }
        return fields;
    }

    private static byte[] nested(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid nested protobuf field " + number);
        }
        return field.rawValue();
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        return nested(field, number);
    }

    private static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number, final int length) {
        final byte[] value = bytes(field, number);
        Bytes.requireLength(value, length, "protobuf field " + number);
        return value;
    }

    private static long nonNegative(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalArgumentException("invalid protobuf varint field " + number);
        }
        final long value = field.unsignedValue();
        if (value < 0) {
            throw new IllegalArgumentException("protobuf int64 field exceeds signed range " + number);
        }
        return value;
    }

    private static long uint64Bits(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalArgumentException("invalid protobuf uint64 field " + number);
        }
        return field.unsignedValue();
    }

    private static int uint32Int(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = nonNegative(field, number);
        if (value > 0xffff_ffffL || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("protobuf uint32 field exceeds local range " + number);
        }
        return (int) value;
    }

    private static int uint32Bits(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = nonNegative(field, number);
        if (value > 0xffff_ffffL) {
            throw new IllegalArgumentException("protobuf uint32 field exceeds unsigned range " + number);
        }
        return (int) value;
    }

    private static Integer optionalVarint(final List<CanonicalProtobuf.Reader.Field> fields, final int number,
                                          final String name) {
        for (CanonicalProtobuf.Reader.Field field : fields) {
            if (field.number() == number) {
                return uint32Bits(field, number);
            }
        }
        return null;
    }

    private static CanonicalProtobuf.Reader.Field field(final List<CanonicalProtobuf.Reader.Field> fields,
                                                        final int number) {
        for (CanonicalProtobuf.Reader.Field field : fields) {
            if (field.number() == number) {
                return field;
            }
        }
        throw new IllegalArgumentException("missing protobuf field " + number);
    }

    private static void requireNumbers(final List<CanonicalProtobuf.Reader.Field> fields, final int[] expected) {
        requireNumbers(fields, expected, false);
    }

    private static void requireNumbers(final List<CanonicalProtobuf.Reader.Field> fields, final int[] expected,
                                       final boolean allowOptionalLeaderEpoch) {
        if (allowOptionalLeaderEpoch) {
            if (fields.size() != expected.length && fields.size() != expected.length + 1) {
                throw new IllegalArgumentException("unexpected protobuf field count");
            }
            final int[] withOptional = fields.size() == expected.length
                    ? expected : new int[]{1, 2, 3, 4, 5, 6, 7};
            for (int index = 0; index < withOptional.length; index++) {
                if (fields.get(index).number() != withOptional[index]) {
                    throw new IllegalArgumentException("unexpected protobuf field order");
                }
            }
            return;
        }
        if (fields.size() != expected.length) {
            throw new IllegalArgumentException("unexpected protobuf field count");
        }
        for (int index = 0; index < expected.length; index++) {
            if (fields.get(index).number() != expected[index]) {
                throw new IllegalArgumentException("unexpected protobuf field order");
            }
        }
    }

    private static String utf8(final byte[] bytes, final String name) {
        final String value = new String(bytes, StandardCharsets.UTF_8);
        if (!Arrays.equals(value.getBytes(StandardCharsets.UTF_8), bytes) || value.indexOf('\0') >= 0
                || value.isBlank()) {
            throw new IllegalArgumentException("invalid UTF-8 " + name);
        }
        return value;
    }

    private static String utf8Nfc(final String value, final String name) {
        Objects.requireNonNull(value, name);
        final String decoded = new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        if (!decoded.equals(value) || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))
                || value.indexOf('\0') >= 0
                || value.isBlank()) {
            throw new IllegalArgumentException(name + " must be nonblank NFC UTF-8");
        }
        return value;
    }

    private static UUID uuid(final byte[] bytes) {
        Bytes.requireLength(bytes, 16, "uuid");
        final ByteBuffer input = ByteBuffer.wrap(bytes);
        return new UUID(input.getLong(), input.getLong());
    }

    private static byte[] uuidBytes(final UUID uuid) {
        return ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits())
                .array();
    }

    private static void requireNonZero(final byte[] value, final int expectedLength, final String name) {
        Bytes.requireLength(value, expectedLength, name);
        for (byte item : value) {
            if (item != 0) {
                return;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }

    /** The exact PreparedCommandRefV1 projection, without command body bytes. */
    public record PreparedCommandRef(ShardId shardId, CommandId commandId, DelayMessageId delayMessageId,
                                     CommandType commandType, ProtocolTuple protocolTuple, byte[] commandHash,
                                     long retryUntilEpochMs, byte[] frameSha256) {
        public PreparedCommandRef {
            Objects.requireNonNull(shardId, "shardId");
            Objects.requireNonNull(commandId, "commandId");
            Objects.requireNonNull(delayMessageId, "delayMessageId");
            Objects.requireNonNull(commandType, "commandType");
            Objects.requireNonNull(protocolTuple, "protocolTuple");
            if (!shardId.equals(commandId.routingId().shardId())
                    || !shardId.equals(delayMessageId.routingId().shardId())) {
                throw new IllegalArgumentException("PreparedCommandRef identities do not belong to shard");
            }
            Bytes.requireLength(commandHash, HASH_LENGTH, "commandHash");
            if (retryUntilEpochMs < 0) {
                throw new IllegalArgumentException("retryUntilEpochMs must be non-negative");
            }
            Bytes.requireLength(frameSha256, HASH_LENGTH, "frameSha256");
            commandHash = Bytes.copy(commandHash);
            frameSha256 = Bytes.copy(frameSha256);
        }

        /** Projects only a Registry-shaped command; compatibility bodies fail closed. */
        public static PreparedCommandRef from(final PreparedCommand command) {
            Objects.requireNonNull(command, "command");
            return new PreparedCommandRef(command.shardId(), command.commandId(), command.delayMessageId(),
                    command.type(), ProtocolTuple.managedCommandV1(), command.commandHash(),
                    command.retryUntilEpochMs(), Bytes.sha256(CommandCodec.encodeFrameV1(command)));
        }

        @Override
        public byte[] commandHash() {
            return Bytes.copy(commandHash);
        }

        @Override
        public byte[] frameSha256() {
            return Bytes.copy(frameSha256);
        }

        @Override
        public boolean equals(final Object other) {
            if (!(other instanceof PreparedCommandRef that)) {
                return false;
            }
            return shardId.equals(that.shardId) && commandId.equals(that.commandId)
                    && delayMessageId.equals(that.delayMessageId) && commandType == that.commandType
                    && protocolTuple.equals(that.protocolTuple) && Arrays.equals(commandHash, that.commandHash)
                    && retryUntilEpochMs == that.retryUntilEpochMs && Arrays.equals(frameSha256, that.frameSha256);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardId, commandId, delayMessageId, commandType, protocolTuple,
                    Arrays.hashCode(commandHash), retryUntilEpochMs, Arrays.hashCode(frameSha256));
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, shardId.routeIncarnation().bytes());
                CanonicalProtobuf.uint32Bits(output, 2, shardId.partition());
                CanonicalProtobuf.bytes(output, 3, commandId.bytes());
                CanonicalProtobuf.bytes(output, 4, delayMessageId.bytes());
                CanonicalProtobuf.uint32(output, 5, commandType.wireValue());
                CanonicalProtobuf.bytes(output, 6, protocolTuple.canonicalBytes());
                CanonicalProtobuf.bytes(output, 7, commandHash);
                CanonicalProtobuf.int64(output, 8, retryUntilEpochMs);
                CanonicalProtobuf.bytes(output, 9, frameSha256);
            });
        }

        static PreparedCommandRef decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "PreparedCommandRefV1");
            requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9});
            final ShardId shard = new ShardId(new RouteIncarnation(fixed(fields.get(0), 1, 16)),
                    uint32Bits(fields.get(1), 2));
            final CommandId commandId = new CommandId(fixed(fields.get(2), 3, CommandId.LENGTH));
            final DelayMessageId messageId = new DelayMessageId(fixed(fields.get(3), 4, DelayMessageId.LENGTH));
            final CommandType type = commandType(uint32Int(fields.get(4), 5));
            final ProtocolTuple tuple = ProtocolTuple.decode(nested(fields.get(5), 6));
            return new PreparedCommandRef(shard, commandId, messageId, type, tuple,
                    fixed(fields.get(6), 7, HASH_LENGTH), nonNegative(fields.get(7), 8),
                    fixed(fields.get(8), 9, HASH_LENGTH));
        }

        private static CommandType commandType(final int value) {
            for (CommandType type : CommandType.values()) {
                if (type.wireValue() == value) {
                    return type;
                }
            }
            throw new IllegalArgumentException("unknown CommandType: " + value);
        }
    }

    /** Exact ProtocolTupleV1 used by the managed Client Command branch. */
    public record ProtocolTuple(int framingVersion, int logEnvelopeVersion, int recordKind,
                                int envelopeVersion, int bodyVersion) {
        public ProtocolTuple {
            if (framingVersion != 1 || logEnvelopeVersion != 1 || recordKind != ShardLogFrame.CLIENT_COMMAND_KIND
                    || envelopeVersion != 1 || bodyVersion != 1) {
                throw new IllegalArgumentException("unsupported managed V1 ProtocolTuple");
            }
        }

        public static ProtocolTuple managedCommandV1() {
            return new ProtocolTuple(1, 1, ShardLogFrame.CLIENT_COMMAND_KIND, 1, 1);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.uint32(output, 1, framingVersion);
                CanonicalProtobuf.uint32(output, 2, logEnvelopeVersion);
                CanonicalProtobuf.uint32(output, 3, recordKind);
                CanonicalProtobuf.uint32(output, 4, envelopeVersion);
                CanonicalProtobuf.uint32(output, 5, bodyVersion);
            });
        }

        private static ProtocolTuple decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "ProtocolTupleV1");
            requireNumbers(fields, new int[]{1, 2, 3, 4, 5});
            return new ProtocolTuple(uint32Int(fields.get(0), 1), uint32Int(fields.get(1), 2),
                    uint32Int(fields.get(2), 3), uint32Int(fields.get(3), 4), uint32Int(fields.get(4), 5));
        }
    }

    /** Safe Broker acknowledgement projection with no open metadata map. */
    public sealed interface SafeBrokerAck permits KafkaQueuedAck, PulsarQueuedAck {
        byte[] canonicalBytes();

        static SafeBrokerAck decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "SafeBrokerAckV1");
            if (fields.size() != 1) {
                throw new IllegalArgumentException("SafeBrokerAckV1 must select one branch");
            }
            return switch (fields.get(0).number()) {
                case 1 -> KafkaQueuedAck.decode(nested(fields.get(0), 1));
                case 2 -> PulsarQueuedAck.decode(nested(fields.get(0), 2));
                default -> throw new IllegalArgumentException("unknown SafeBrokerAckV1 branch");
            };
        }
    }

    public record KafkaQueuedAck(String authenticatedClusterId, UUID nativeTopicUuid, int partition, long offset,
                                 Integer leaderEpoch, long brokerLogAppendTimeEpochMs, byte[] responseSha256)
            implements SafeBrokerAck {
        public KafkaQueuedAck {
            authenticatedClusterId = utf8Nfc(authenticatedClusterId, "authenticatedClusterId");
            Objects.requireNonNull(nativeTopicUuid, "nativeTopicUuid");
            if (brokerLogAppendTimeEpochMs < 0) {
                throw new IllegalArgumentException("invalid Kafka queued acknowledgement");
            }
            Bytes.requireLength(responseSha256, HASH_LENGTH, "responseSha256");
            responseSha256 = Bytes.copy(responseSha256);
        }

        @Override
        public byte[] responseSha256() {
            return Bytes.copy(responseSha256);
        }

        @Override
        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1,
                    CanonicalProtobuf.message(kafka -> {
                        CanonicalProtobuf.bytes(kafka, 1,
                                CanonicalProtobuf.message(resource -> {
                                    CanonicalProtobuf.bytes(resource, 1,
                                            authenticatedClusterId.getBytes(StandardCharsets.UTF_8));
                                    CanonicalProtobuf.bytes(resource, 2, uuidBytes(nativeTopicUuid));
                                }));
                        CanonicalProtobuf.uint32Bits(kafka, 2, partition);
                        CanonicalProtobuf.uint64Bits(kafka, 3, offset);
                        if (leaderEpoch != null) {
                            CanonicalProtobuf.uint32Bits(kafka, 4, leaderEpoch);
                        }
                        CanonicalProtobuf.int64(kafka, 5, brokerLogAppendTimeEpochMs);
                        CanonicalProtobuf.bytes(kafka, 6, responseSha256);
                    })));
        }

        private static KafkaQueuedAck decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "KafkaQueuedAckV1");
            if (fields.size() != 5 && fields.size() != 6) {
                throw new IllegalArgumentException("Kafka queued acknowledgement fields are incomplete");
            }
            if (fields.get(0).number() != 1 || fields.get(1).number() != 2 || fields.get(2).number() != 3) {
                throw new IllegalArgumentException("Kafka queued acknowledgement field order is invalid");
            }
            final List<CanonicalProtobuf.Reader.Field> resource = read(nested(fields.get(0), 1),
                    "KafkaResourceIdentityV1");
            requireNumbers(resource, new int[]{1, 2});
            final String cluster = utf8(bytes(resource.get(0), 1), "authenticatedClusterId");
            final UUID topic = uuid(fixed(resource.get(1), 2, 16));
            int index = 3;
            Integer leader = null;
            if (fields.get(index).number() == 4) {
                leader = uint32Bits(fields.get(index), 4);
                index++;
            }
            if (fields.get(index).number() != 5 || fields.size() != index + 2) {
                throw new IllegalArgumentException("Kafka queued acknowledgement optional fields are invalid");
            }
            return new KafkaQueuedAck(cluster, topic, uint32Bits(fields.get(1), 2), uint64Bits(fields.get(2), 3),
                    leader, nonNegative(fields.get(index), 5), fixed(fields.get(index + 1), 6, HASH_LENGTH));
        }

        @Override
        public boolean equals(final Object other) {
            if (!(other instanceof KafkaQueuedAck that)) {
                return false;
            }
            return authenticatedClusterId.equals(that.authenticatedClusterId)
                    && nativeTopicUuid.equals(that.nativeTopicUuid) && partition == that.partition
                    && offset == that.offset && Objects.equals(leaderEpoch, that.leaderEpoch)
                    && brokerLogAppendTimeEpochMs == that.brokerLogAppendTimeEpochMs
                    && Arrays.equals(responseSha256, that.responseSha256);
        }

        @Override
        public int hashCode() {
            return Objects.hash(authenticatedClusterId, nativeTopicUuid, partition, offset, leaderEpoch,
                    brokerLogAppendTimeEpochMs, Arrays.hashCode(responseSha256));
        }
    }

    public record PulsarQueuedAck(String authenticatedClusterId, byte[] brokerResourceIncarnation,
                                  String physicalTopic, long physicalTopicCreationTimestamp, int partition,
                                  long ledgerId, long entryId, int normalizedBatchIndex, int batchSize,
                                  long brokerEntryTimestampEpochMs, byte[] sendReceiptSha256)
            implements SafeBrokerAck {
        public PulsarQueuedAck {
            authenticatedClusterId = utf8Nfc(authenticatedClusterId, "authenticatedClusterId");
            Bytes.requireLength(brokerResourceIncarnation, 32, "brokerResourceIncarnation");
            physicalTopic = utf8Nfc(physicalTopic, "physicalTopic");
            if (batchSize == 0 || Integer.compareUnsigned(normalizedBatchIndex, batchSize) >= 0
                    || brokerEntryTimestampEpochMs < 0) {
                throw new IllegalArgumentException("invalid Pulsar queued acknowledgement");
            }
            Bytes.requireLength(sendReceiptSha256, HASH_LENGTH, "sendReceiptSha256");
            brokerResourceIncarnation = Bytes.copy(brokerResourceIncarnation);
            sendReceiptSha256 = Bytes.copy(sendReceiptSha256);
        }

        @Override
        public byte[] brokerResourceIncarnation() {
            return Bytes.copy(brokerResourceIncarnation);
        }

        @Override
        public byte[] sendReceiptSha256() {
            return Bytes.copy(sendReceiptSha256);
        }

        @Override
        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 2,
                    CanonicalProtobuf.message(pulsar -> {
                        CanonicalProtobuf.bytes(pulsar, 1,
                                CanonicalProtobuf.message(resource -> {
                                    CanonicalProtobuf.bytes(resource, 1,
                                            authenticatedClusterId.getBytes(StandardCharsets.UTF_8));
                                    CanonicalProtobuf.bytes(resource, 2, brokerResourceIncarnation);
                                    CanonicalProtobuf.bytes(resource, 3, physicalTopic.getBytes(StandardCharsets.UTF_8));
                                    CanonicalProtobuf.uint64Bits(resource, 4, physicalTopicCreationTimestamp);
                                }));
                        CanonicalProtobuf.uint32Bits(pulsar, 2, partition);
                        CanonicalProtobuf.uint64Bits(pulsar, 3, ledgerId);
                        CanonicalProtobuf.uint64Bits(pulsar, 4, entryId);
                        CanonicalProtobuf.uint32Bits(pulsar, 5, normalizedBatchIndex);
                        CanonicalProtobuf.uint32Bits(pulsar, 6, batchSize);
                        CanonicalProtobuf.int64(pulsar, 7, brokerEntryTimestampEpochMs);
                        CanonicalProtobuf.bytes(pulsar, 8, sendReceiptSha256);
                    })));
        }

        private static PulsarQueuedAck decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "PulsarQueuedAckV1");
            requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8});
            final List<CanonicalProtobuf.Reader.Field> resource = read(nested(fields.get(0), 1),
                    "PulsarResourceIdentityV1");
            requireNumbers(resource, new int[]{1, 2, 3, 4});
            return new PulsarQueuedAck(utf8(bytes(resource.get(0), 1), "authenticatedClusterId"),
                    fixedBytes(bytes(resource.get(1), 2), 32, "brokerResourceIncarnation"),
                    utf8Nfc(utf8(bytes(resource.get(2), 3), "physicalTopic"), "physicalTopic"),
                    uint64Bits(resource.get(3), 4), uint32Bits(fields.get(1), 2), uint64Bits(fields.get(2), 3),
                    uint64Bits(fields.get(3), 4), uint32Bits(fields.get(4), 5), uint32Bits(fields.get(5), 6),
                    nonNegative(fields.get(6), 7), fixed(fields.get(7), 8, HASH_LENGTH));
        }

        @Override
        public boolean equals(final Object other) {
            if (!(other instanceof PulsarQueuedAck that)) {
                return false;
            }
            return authenticatedClusterId.equals(that.authenticatedClusterId)
                    && Arrays.equals(brokerResourceIncarnation, that.brokerResourceIncarnation)
                    && physicalTopic.equals(that.physicalTopic)
                    && physicalTopicCreationTimestamp == that.physicalTopicCreationTimestamp
                    && partition == that.partition && ledgerId == that.ledgerId && entryId == that.entryId
                    && normalizedBatchIndex == that.normalizedBatchIndex && batchSize == that.batchSize
                    && brokerEntryTimestampEpochMs == that.brokerEntryTimestampEpochMs
                    && Arrays.equals(sendReceiptSha256, that.sendReceiptSha256);
        }

        @Override
        public int hashCode() {
            return Objects.hash(authenticatedClusterId, Arrays.hashCode(brokerResourceIncarnation), physicalTopic,
                    physicalTopicCreationTimestamp, partition, ledgerId, entryId, normalizedBatchIndex, batchSize,
                    brokerEntryTimestampEpochMs, Arrays.hashCode(sendReceiptSha256));
        }
    }

    private static byte[] fixedBytes(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return value;
    }
}
