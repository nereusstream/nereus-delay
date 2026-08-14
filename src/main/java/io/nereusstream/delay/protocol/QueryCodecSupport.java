package io.nereusstream.delay.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Shared strict helpers for the closed public query view codecs. */
final class QueryCodecSupport {
    private static final int KAFKA_SOURCE_BRANCH = 1;
    private static final int PULSAR_SOURCE_BRANCH = 2;

    private QueryCodecSupport() {
    }

    static List<CanonicalProtobuf.Reader.Field> read(final byte[] encoded, final String name) {
        return read(encoded, name, false);
    }

    static List<CanonicalProtobuf.Reader.Field> read(final byte[] encoded, final String name,
                                                     final boolean allowRepeatedFields) {
        Objects.requireNonNull(encoded, name);
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, allowRepeatedFields);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException(name + " is empty");
        }
        return fields;
    }

    static void requireNumbers(final List<CanonicalProtobuf.Reader.Field> fields, final int[] expected,
                               final String name) {
        if (fields.size() != expected.length) {
            throw new IllegalArgumentException(name + " has an unexpected field count");
        }
        for (int index = 0; index < expected.length; index++) {
            if (fields.get(index).number() != expected[index]) {
                throw new IllegalArgumentException(name + " has an unexpected field order");
            }
        }
    }

    static byte[] nested(final CanonicalProtobuf.Reader.Field field, final int number) {
        return bytes(field, number);
    }

    static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid protobuf bytes field " + number);
        }
        return field.rawValue();
    }

    static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number, final int length) {
        final byte[] value = bytes(field, number);
        Bytes.requireLength(value, length, "protobuf field " + number);
        return value;
    }

    static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalArgumentException("invalid protobuf varint field " + number);
        }
        return field.unsignedValue();
    }

    static int uint32(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = uint(field, number);
        if (value < 0 || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("protobuf uint32 field exceeds local range " + number);
        }
        return (int) value;
    }

    static int uint32Bits(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = uint(field, number);
        if (value < 0 || value > 0xffff_ffffL) {
            throw new IllegalArgumentException("protobuf uint32 field exceeds unsigned range " + number);
        }
        return (int) value;
    }

    /** Returns the complete raw bit pattern of a protobuf uint64 field. */
    static long uint64Bits(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalArgumentException("invalid protobuf uint64 field " + number);
        }
        return field.unsignedValue();
    }

    static boolean bool(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = uint(field, number);
        if (value < 0 || value > 1) {
            throw new IllegalArgumentException("protobuf bool field is not 0 or 1: " + number);
        }
        return value == 1;
    }

    static CanonicalProtobuf.Reader.Field field(final List<CanonicalProtobuf.Reader.Field> fields, final int number) {
        for (CanonicalProtobuf.Reader.Field field : fields) {
            if (field.number() == number) {
                return field;
            }
        }
        throw new IllegalArgumentException("missing protobuf field " + number);
    }

    static CanonicalProtobuf.Reader.Field optional(final List<CanonicalProtobuf.Reader.Field> fields,
                                                   final int number) {
        for (CanonicalProtobuf.Reader.Field field : fields) {
            if (field.number() == number) {
                return field;
            }
        }
        return null;
    }

    static void requireCanonical(final byte[] encoded, final byte[] canonical, final String name) {
        if (!Arrays.equals(encoded, canonical)) {
            throw new IllegalArgumentException("non-canonical " + name);
        }
    }

    static byte[] encodeSourcePosition(final SourcePosition position) {
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
        if (!(position instanceof PulsarSourcePosition pulsar)) {
            throw new IllegalArgumentException("unsupported SourcePosition implementation");
        }
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, PULSAR_SOURCE_BRANCH,
                CanonicalProtobuf.message(pulsarOutput -> {
                    CanonicalProtobuf.bytes(pulsarOutput, 1, pulsar.shardId().routeIncarnation().bytes());
                    CanonicalProtobuf.bytes(pulsarOutput, 2, pulsar.brokerResourceIncarnation());
                    CanonicalProtobuf.bytes(pulsarOutput, 3,
                            pulsar.physicalTopic().getBytes(StandardCharsets.UTF_8));
                    CanonicalProtobuf.uint32Bits(pulsarOutput, 4, pulsar.shardId().partition());
                    CanonicalProtobuf.uint64Bits(pulsarOutput, 5, pulsar.ledgerId());
                    CanonicalProtobuf.uint64Bits(pulsarOutput, 6, pulsar.entryId());
                    CanonicalProtobuf.uint32Bits(pulsarOutput, 7, pulsar.normalizedBatchIndex());
                    CanonicalProtobuf.uint32Bits(pulsarOutput, 8, pulsar.batchSize());
                    CanonicalProtobuf.uint32(pulsarOutput, 9, pulsar.entryKind().wireValue());
                    CanonicalProtobuf.int64(pulsarOutput, 10, pulsar.brokerEntryTimestampEpochMs());
                })));
    }

    static SourcePosition decodeSourcePosition(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> outer = read(encoded, "SourcePositionV1");
        if (outer.size() != 1) {
            throw new IllegalArgumentException("SourcePositionV1 must select one branch");
        }
        final CanonicalProtobuf.Reader.Field branch = outer.get(0);
        final List<CanonicalProtobuf.Reader.Field> fields = read(nested(branch, branch.number()),
                "SourcePosition branch");
        if (branch.number() == KAFKA_SOURCE_BRANCH) {
            if (fields.size() != 6 && fields.size() != 7) {
                throw new IllegalArgumentException("invalid Kafka SourcePositionV1 fields");
            }
            final byte[] route = fixed(fields.get(0), 1, RouteIncarnation.LENGTH);
            final String cluster = utf8(bytes(fields.get(1), 2), "authenticatedClusterId");
            final UUID topic = uuid(fixed(fields.get(2), 3, 16));
            final int partition = uint32Bits(fields.get(3), 4);
            final long offset = uint(fields.get(4), 5);
            final Integer leaderEpoch;
            if (fields.size() == 7) {
                leaderEpoch = uint32Bits(fields.get(5), 6);
                if (fields.get(6).number() != 7) {
                    throw new IllegalArgumentException("invalid Kafka SourcePositionV1 leader epoch order");
                }
            } else {
                leaderEpoch = null;
                if (fields.get(5).number() != 7) {
                    throw new IllegalArgumentException("invalid Kafka SourcePositionV1 append time");
                }
            }
            final long appendTime = uint(fields.get(fields.size() - 1), 7);
            return new KafkaSourcePosition(new ShardId(new RouteIncarnation(route), partition), cluster, topic,
                    offset, leaderEpoch, appendTime);
        }
        if (branch.number() == PULSAR_SOURCE_BRANCH) {
            requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, "Pulsar SourcePositionV1");
            final byte[] route = fixed(fields.get(0), 1, RouteIncarnation.LENGTH);
            final byte[] resource = fixed(fields.get(1), 2, 32);
            final String topic = utf8(bytes(fields.get(2), 3), "physicalTopic");
            final int partition = uint32Bits(fields.get(3), 4);
            final long ledger = uint(fields.get(4), 5);
            final long entry = uint(fields.get(5), 6);
            final int batchIndex = uint32Bits(fields.get(6), 7);
            final int batchSize = uint32Bits(fields.get(7), 8);
            final PulsarSourcePosition.EntryKind entryKind = switch (uint32Bits(fields.get(8), 9)) {
                case 1 -> PulsarSourcePosition.EntryKind.NON_BATCH;
                case 2 -> PulsarSourcePosition.EntryKind.BATCH;
                default -> throw new IllegalArgumentException("unknown Pulsar source entry kind");
            };
            final long timestamp = uint(fields.get(9), 10);
            return new PulsarSourcePosition(new ShardId(new RouteIncarnation(route), partition), resource, topic,
                    ledger, entry, batchIndex, batchSize, entryKind, timestamp);
        }
        throw new IllegalArgumentException("unknown SourcePositionV1 branch: " + branch.number());
    }

    private static String utf8(final byte[] value, final String name) {
        final String decoded = new String(value, StandardCharsets.UTF_8);
        if (!Arrays.equals(decoded.getBytes(StandardCharsets.UTF_8), value) || decoded.isBlank()) {
            throw new IllegalArgumentException("invalid UTF-8 field " + name);
        }
        return decoded;
    }

    private static byte[] uuidBytes(final UUID value) {
        return ByteBuffer.allocate(16).putLong(value.getMostSignificantBits()).putLong(value.getLeastSignificantBits())
                .array();
    }

    private static UUID uuid(final byte[] value) {
        return new UUID(Bytes.readU64be(value, 0), Bytes.readU64be(value, 8));
    }
}
