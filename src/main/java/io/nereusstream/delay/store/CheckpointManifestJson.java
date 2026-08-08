package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.EvidenceKindV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.PulsarSourcePosition;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourcePosition;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/** Strict decoder for the closed V1 checkpoint manifest JSON projection. */
final class CheckpointManifestJson {
    private static final String[] ROOT_KEYS = {
            "appliedShardLogPosition", "checkpointId", "controlStateDigest", "createdAt", "createdBy",
            "dbIdentity", "evidenceCursors", "files", "lineageGeneration", "manifestVersion",
            "parentCheckpoint", "recoveryLineageId", "referencedSemanticVersionsDigest",
            "restoredFromCheckpointId", "shardId", "shardMutationSequence", "sourceStoreIncarnation",
            "storeFormatVersion"
    };
    private static final String[] CREATED_BY_KEYS = {"deploymentId", "ownerEpoch", "workerRunId"};
    private static final String[] CREATED_AT_KEYS = {"earliestEpochMs", "latestEpochMs", "monotonicAnchorNs",
            "sampleSequence", "source", "sourceConfigGeneration", "sourceEvidenceSha256", "sourceId",
            "sourceKeyVersion", "sourceSignature"};
    private static final String[] FILE_KEYS = {"checksum", "etag", "length", "name", "objectKey", "objectVersion"};
    private static final String[] PARENT_KEYS = {"checkpointId", "manifestSha256"};
    private static final String[] SHARD_KEYS = {"partition", "routeIncarnation"};
    private static final String[] KAFKA_POSITION_KEYS = {"brokerLogAppendTime", "clusterId", "kind",
            "leaderEpoch", "offset", "partition", "routeIncarnation", "topicUuid"};
    private static final String[] PULSAR_POSITION_KEYS = {"batchIndex", "batchSize", "brokerEntryTimestamp",
            "entryId", "entryKind", "kind", "ledgerId", "partition", "physicalTopic", "resourceIncarnation",
            "routeIncarnation"};
    private static final String[] KAFKA_EVIDENCE_KEYS = {"destinationLaneId", "evidenceGeneration", "evidenceKind",
            "evidenceResourceIncarnation", "laneIncarnation", "lastObservedLsoExclusive",
            "maxBrokerPersistedAtThroughCursor", "nextOffsetExclusive", "physicalPartition", "topicUuid"};
    private static final String[] PULSAR_EVIDENCE_KEYS = {"batchIndex", "batchSize", "destinationLaneId", "entryId",
            "evidenceGeneration", "evidenceKind", "evidenceResourceIncarnation", "laneIncarnation", "ledgerId",
            "maxBrokerPersistedAtThroughCursor", "physicalPartition", "physicalTopic",
            "physicalTopicCreationTimestamp", "resourceToken"};

    private CheckpointManifestJson() {
    }

    static CheckpointManifest decode(final byte[] encoded, final CheckpointManifestLimits limits) {
        Objects.requireNonNull(encoded, "encoded");
        Objects.requireNonNull(limits, "limits");
        if (encoded.length > limits.maxManifestBytes()) {
            throw new IllegalArgumentException("manifest bytes exceed configured bound");
        }
        final String json = new String(encoded, StandardCharsets.UTF_8);
        if (!java.util.Arrays.equals(json.getBytes(StandardCharsets.UTF_8), encoded)) {
            throw new IllegalArgumentException("manifest is not valid UTF-8");
        }
        final Parser parser = new Parser(json, Math.max(limits.maxFiles(), limits.maxEvidenceCursors()));
        final Map<String, Object> root = object(parser.parse(), "manifest");
        parser.ensureEnd();
        keys(root, ROOT_KEYS, "manifest");

        final CheckpointManifest manifest = new CheckpointManifest(
                base64(root.get("checkpointId"), "checkpointId"),
                base64(root.get("recoveryLineageId"), "recoveryLineageId"),
                decimal(root.get("lineageGeneration"), "lineageGeneration"),
                decodeParent(root.get("parentCheckpoint")),
                nullableBase64(root.get("restoredFromCheckpointId"), "restoredFromCheckpointId"),
                decodeCreatedBy(root.get("createdBy")),
                decodeCreatedAt(root.get("createdAt")),
                decodeShard(root.get("shardId")),
                base64(root.get("dbIdentity"), "dbIdentity"),
                uuid(string(root.get("sourceStoreIncarnation"), "sourceStoreIncarnation")),
                number(root.get("storeFormatVersion"), "storeFormatVersion"),
                decimal(root.get("shardMutationSequence"), "shardMutationSequence"),
                decodeSourcePosition(root.get("appliedShardLogPosition")),
                hex(root.get("controlStateDigest"), "controlStateDigest"),
                hex(root.get("referencedSemanticVersionsDigest"), "referencedSemanticVersionsDigest"),
                decodeEvidenceCursors(root.get("evidenceCursors")),
                decodeFiles(root.get("files")));
        manifest.validateLimits(limits);

        if (number(root.get("manifestVersion"), "manifestVersion") != 1) {
            throw new IllegalArgumentException("unsupported manifest version");
        }
        if (!java.util.Arrays.equals(encoded, manifest.canonicalJsonBytes())) {
            throw new IllegalArgumentException("manifest JSON is not canonical V1 JSON");
        }
        return manifest;
    }

    private static CheckpointManifest.ParentCheckpoint decodeParent(final Object value) {
        if (value == null) {
            return null;
        }
        final Map<String, Object> parent = object(value, "parentCheckpoint");
        keys(parent, PARENT_KEYS, "parentCheckpoint");
        return new CheckpointManifest.ParentCheckpoint(base64(parent.get("checkpointId"), "parent checkpointId"),
                lowercaseHex(parent.get("manifestSha256"), "parent manifestSha256"));
    }

    private static CheckpointManifest.CreatedBy decodeCreatedBy(final Object value) {
        final Map<String, Object> fields = object(value, "createdBy");
        keys(fields, CREATED_BY_KEYS, "createdBy");
        return new CheckpointManifest.CreatedBy(base64(fields.get("deploymentId"), "deploymentId"),
                base64(fields.get("workerRunId"), "workerRunId"),
                unsignedDecimal(fields.get("ownerEpoch"), "ownerEpoch"));
    }

    private static CheckpointManifest.CreatedAt decodeCreatedAt(final Object value) {
        final Map<String, Object> fields = object(value, "createdAt");
        keys(fields, CREATED_AT_KEYS, "createdAt");
        final Object signature = fields.get("sourceSignature");
        return new CheckpointManifest.CreatedAt(
                decimal(fields.get("earliestEpochMs"), "earliestEpochMs"),
                decimal(fields.get("latestEpochMs"), "latestEpochMs"),
                string(fields.get("source"), "source"),
                base64(fields.get("sourceId"), "sourceId"),
                unsignedDecimal(fields.get("sourceConfigGeneration"), "sourceConfigGeneration"),
                unsignedDecimal(fields.get("sampleSequence"), "sampleSequence"),
                unsignedDecimal(fields.get("monotonicAnchorNs"), "monotonicAnchorNs"),
                hex(fields.get("sourceEvidenceSha256"), "sourceEvidenceSha256"),
                uint32Number(fields.get("sourceKeyVersion"), "sourceKeyVersion"),
                signature == null ? null : base64(signature, "sourceSignature"));
    }

    private static ShardId decodeShard(final Object value) {
        final Map<String, Object> fields = object(value, "shardId");
        keys(fields, SHARD_KEYS, "shardId");
        return new ShardId(RouteIncarnation.fromUuid(uuid(string(fields.get("routeIncarnation"),
                "shard routeIncarnation"))), uint32Number(fields.get("partition"), "shard partition"));
    }

    private static List<EvidenceCursorV1> decodeEvidenceCursors(final Object value) {
        if (!(value instanceof List<?> values)) {
            throw new IllegalArgumentException("manifest evidenceCursors must be an array");
        }
        final List<EvidenceCursorV1> result = new ArrayList<>(values.size());
        for (Object item : values) {
            final Map<String, Object> fields = object(item, "evidence cursor");
            final EvidenceKindV1 kind;
            try {
                kind = EvidenceKindV1.valueOf(string(fields.get("evidenceKind"), "evidenceKind"));
            } catch (IllegalArgumentException exception) {
                throw new IllegalArgumentException("unknown evidenceKind", exception);
            }
            if (kind == EvidenceKindV1.KAFKA_RECEIPT_CONTIGUOUS) {
                keys(fields, KAFKA_EVIDENCE_KEYS, "Kafka evidence cursor");
                result.add(EvidenceCursorV1.kafka(
                        base64(fields.get("destinationLaneId"), "destinationLaneId"),
                        base64(fields.get("laneIncarnation"), "laneIncarnation"),
                        uuidBytes(uuid(string(fields.get("topicUuid"), "topicUuid"))),
                        uint32Number(fields.get("physicalPartition"), "physicalPartition"),
                        unsignedDecimal(fields.get("evidenceGeneration"), "evidenceGeneration"),
                        decimal(fields.get("maxBrokerPersistedAtThroughCursor"),
                                "maxBrokerPersistedAtThroughCursor"),
                        unsignedDecimal(fields.get("nextOffsetExclusive"), "nextOffsetExclusive"),
                        unsignedDecimal(fields.get("lastObservedLsoExclusive"), "lastObservedLsoExclusive")));
            } else {
                keys(fields, PULSAR_EVIDENCE_KEYS, "Pulsar evidence cursor");
                result.add(EvidenceCursorV1.pulsar(
                        base64(fields.get("destinationLaneId"), "destinationLaneId"),
                        base64(fields.get("laneIncarnation"), "laneIncarnation"),
                        base64(fields.get("resourceToken"), "resourceToken"),
                        uint32Number(fields.get("physicalPartition"), "physicalPartition"),
                        unsignedDecimal(fields.get("evidenceGeneration"), "evidenceGeneration"),
                        decimal(fields.get("maxBrokerPersistedAtThroughCursor"),
                                "maxBrokerPersistedAtThroughCursor"),
                        string(fields.get("physicalTopic"), "physicalTopic"),
                        unsignedDecimal(fields.get("physicalTopicCreationTimestamp"),
                                "physicalTopicCreationTimestamp"),
                        unsignedDecimal(fields.get("ledgerId"), "ledgerId"),
                        unsignedDecimal(fields.get("entryId"), "entryId"),
                        uint32Number(fields.get("batchIndex"), "batchIndex"),
                        uint32Number(fields.get("batchSize"), "batchSize")));
            }
        }
        return result;
    }

    private static List<CheckpointManifest.FileEntry> decodeFiles(final Object value) {
        if (!(value instanceof List<?> values) || values.isEmpty()) {
            throw new IllegalArgumentException("manifest files must be a non-empty array");
        }
        final List<CheckpointManifest.FileEntry> result = new ArrayList<>(values.size());
        for (Object item : values) {
            final Map<String, Object> fields = object(item, "file");
            keys(fields, FILE_KEYS, "file");
            final Object etag = fields.get("etag");
            result.add(new CheckpointManifest.FileEntry(
                    string(fields.get("name"), "file name"), decimal(fields.get("length"), "file length"),
                    hex(fields.get("checksum"), "file checksum"), base64(fields.get("objectKey"), "objectKey"),
                    base64(fields.get("objectVersion"), "objectVersion"),
                    etag == null ? null : base64(etag, "etag")));
        }
        return result;
    }

    private static SourcePosition decodeSourcePosition(final Object value) {
        final Map<String, Object> fields = object(value, "appliedShardLogPosition");
        final String kind = string(fields.get("kind"), "source position kind");
        if ("KAFKA".equals(kind)) {
            keys(fields, KAFKA_POSITION_KEYS, "Kafka source position");
            final ShardId shard = new ShardId(RouteIncarnation.fromUuid(uuid(string(fields.get("routeIncarnation"),
                    "Kafka routeIncarnation"))), uint32Number(fields.get("partition"), "Kafka partition"));
            final Object leader = fields.get("leaderEpoch");
            return new KafkaSourcePosition(shard, utf8Base64(fields.get("clusterId"), "clusterId"),
                    uuid(string(fields.get("topicUuid"), "topicUuid")),
                    unsignedDecimal(fields.get("offset"), "offset"),
                    leader == null ? null : uint32Number(leader, "leaderEpoch"),
                    decimal(fields.get("brokerLogAppendTime"), "brokerLogAppendTime"));
        }
        if ("PULSAR".equals(kind)) {
            keys(fields, PULSAR_POSITION_KEYS, "Pulsar source position");
            final ShardId shard = new ShardId(RouteIncarnation.fromUuid(uuid(string(fields.get("routeIncarnation"),
                    "Pulsar routeIncarnation"))), uint32Number(fields.get("partition"), "Pulsar partition"));
            final PulsarSourcePosition.EntryKind entryKind;
            try {
                entryKind = PulsarSourcePosition.EntryKind.valueOf(string(fields.get("entryKind"), "entryKind"));
            } catch (IllegalArgumentException exception) {
                throw new IllegalArgumentException("unknown Pulsar entryKind", exception);
            }
            return new PulsarSourcePosition(shard, base64(fields.get("resourceIncarnation"), "resourceIncarnation"),
                    string(fields.get("physicalTopic"), "physicalTopic"),
                    unsignedDecimal(fields.get("ledgerId"), "ledgerId"),
                    unsignedDecimal(fields.get("entryId"), "entryId"),
                    uint32Number(fields.get("batchIndex"), "batchIndex"),
                    uint32Number(fields.get("batchSize"), "batchSize"),
                    entryKind, decimal(fields.get("brokerEntryTimestamp"), "brokerEntryTimestamp"));
        }
        throw new IllegalArgumentException("unknown source position kind: " + kind);
    }

    private static Map<String, Object> object(final Object value, final String name) {
        if (!(value instanceof Map<?, ?> raw)) {
            throw new IllegalArgumentException(name + " must be an object");
        }
        @SuppressWarnings("unchecked") final Map<String, Object> result = (Map<String, Object>) raw;
        return result;
    }

    private static void keys(final Map<String, Object> value, final String[] expected, final String name) {
        if (value.size() != expected.length || !List.of(expected).equals(new ArrayList<>(value.keySet()))) {
            throw new IllegalArgumentException(name + " has non-canonical fields");
        }
        final Set<String> unique = new HashSet<>(value.keySet());
        if (unique.size() != expected.length) {
            throw new IllegalArgumentException(name + " contains duplicate fields");
        }
    }

    private static String string(final Object value, final String name) {
        if (!(value instanceof String result)) {
            throw new IllegalArgumentException(name + " must be a JSON string");
        }
        return result;
    }

    private static int number(final Object value, final String name) {
        final long parsed = decimalNumber(value, name);
        if (parsed > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " exceeds local integer range");
        }
        return (int) parsed;
    }

    private static int uint32Number(final Object value, final String name) {
        if (!(value instanceof JsonNumber number) || !number.text().matches("0|[1-9][0-9]*")) {
            throw new IllegalArgumentException(name + " must be a canonical uint32 integer");
        }
        try {
            final long parsed = Long.parseLong(number.text());
            if (parsed > 0xffff_ffffL) {
                throw new IllegalArgumentException(name + " exceeds uint32 range");
            }
            return (int) parsed;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(name + " exceeds uint32 range", exception);
        }
    }

    private static long decimal(final Object value, final String name) {
        final String text = string(value, name);
        if (!text.matches("0|[1-9][0-9]*")) {
            throw new IllegalArgumentException(name + " must be a canonical unsigned decimal string");
        }
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(name + " exceeds the supported uint64 range", exception);
        }
    }

    private static long unsignedDecimal(final Object value, final String name) {
        final String text = string(value, name);
        if (!text.matches("0|[1-9][0-9]*")) {
            throw new IllegalArgumentException(name + " must be a canonical unsigned decimal string");
        }
        try {
            return Long.parseUnsignedLong(text);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(name + " exceeds the supported uint64 range", exception);
        }
    }

    private static long decimalNumber(final Object value, final String name) {
        if (!(value instanceof JsonNumber number) || !number.text().matches("0|[1-9][0-9]*")) {
            throw new IllegalArgumentException(name + " must be a canonical JSON integer");
        }
        try {
            return Long.parseLong(number.text());
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(name + " exceeds local integer range", exception);
        }
    }

    private static byte[] base64(final Object value, final String name) {
        final String text = string(value, name);
        final byte[] decoded;
        try {
            decoded = Base64.getUrlDecoder().decode(text);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException(name + " is not unpadded Base64url", exception);
        }
        if (!Base64.getUrlEncoder().withoutPadding().encodeToString(decoded).equals(text)) {
            throw new IllegalArgumentException(name + " is not canonical Base64url");
        }
        return decoded;
    }

    private static byte[] nullableBase64(final Object value, final String name) {
        return value == null ? null : base64(value, name);
    }

    private static byte[] hex(final Object value, final String name) {
        return Bytes.hexToBytes(lowercaseHex(value, name));
    }

    private static String lowercaseHex(final Object value, final String name) {
        final String text = string(value, name);
        if (text.isEmpty() || (text.length() & 1) != 0 || !text.matches("[0-9a-f]+")) {
            throw new IllegalArgumentException(name + " must be lowercase hexadecimal");
        }
        return text;
    }

    private static String utf8Base64(final Object value, final String name) {
        final byte[] bytes = base64(value, name);
        final String text = new String(bytes, StandardCharsets.UTF_8);
        if (!java.util.Arrays.equals(text.getBytes(StandardCharsets.UTF_8), bytes) || text.isBlank()) {
            throw new IllegalArgumentException(name + " is not valid nonblank UTF-8");
        }
        return text;
    }

    private static UUID uuid(final String value) {
        try {
            final UUID result = UUID.fromString(value);
            if (!result.toString().equals(value)) {
                throw new IllegalArgumentException("UUID is not canonical lowercase text");
            }
            return result;
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("invalid canonical UUID", exception);
        }
    }

    private static byte[] uuidBytes(final UUID value) {
        return ByteBuffer.allocate(16).putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits()).array();
    }

    private record JsonNumber(String text) {
    }

    private static final class Parser {
        private final String input;
        private final int maxArrayElements;
        private int index;

        private Parser(final String input, final int maxArrayElements) {
            this.input = input;
            this.maxArrayElements = maxArrayElements;
        }

        private Object parse() {
            skipWhitespace();
            final Object result = value();
            skipWhitespace();
            return result;
        }

        private Object value() {
            skipWhitespace();
            if (index >= input.length()) {
                throw error("unexpected end of JSON");
            }
            return switch (input.charAt(index)) {
                case '{' -> objectValue();
                case '[' -> arrayValue();
                case '"' -> stringValue();
                case 't' -> literal("true", Boolean.TRUE);
                case 'f' -> literal("false", Boolean.FALSE);
                case 'n' -> literal("null", null);
                default -> numberValue();
            };
        }

        private Map<String, Object> objectValue() {
            index++;
            final Map<String, Object> result = new LinkedHashMap<>();
            skipWhitespace();
            if (take('}')) {
                return result;
            }
            while (true) {
                skipWhitespace();
                if (index >= input.length() || input.charAt(index) != '"') {
                    throw error("object key must be a string");
                }
                final String key = stringValue();
                if (!take(':')) {
                    throw error("object key is not followed by colon");
                }
                if (result.containsKey(key)) {
                    throw error("duplicate object key");
                }
                result.put(key, value());
                skipWhitespace();
                if (take('}')) {
                    return result;
                }
                if (!take(',')) {
                    throw error("object entry is not followed by comma or close");
                }
            }
        }

        private List<Object> arrayValue() {
            index++;
            final List<Object> result = new ArrayList<>();
            skipWhitespace();
            if (take(']')) {
                return result;
            }
            while (true) {
                if (result.size() >= maxArrayElements) {
                    throw error("JSON array exceeds configured element bound");
                }
                result.add(value());
                skipWhitespace();
                if (take(']')) {
                    return result;
                }
                if (!take(',')) {
                    throw error("array value is not followed by comma or close");
                }
            }
        }

        private String stringValue() {
            if (!take('"')) {
                throw error("expected string");
            }
            final StringBuilder result = new StringBuilder();
            while (index < input.length()) {
                final char current = input.charAt(index++);
                if (current == '"') {
                    return result.toString();
                }
                if (current < 0x20) {
                    throw error("unescaped control character in string");
                }
                if (current != '\\') {
                    result.append(current);
                    continue;
                }
                if (index >= input.length()) {
                    throw error("unterminated string escape");
                }
                final char escaped = input.charAt(index++);
                switch (escaped) {
                    case '"', '\\', '/' -> result.append(escaped);
                    case 'b' -> result.append('\b');
                    case 'f' -> result.append('\f');
                    case 'n' -> result.append('\n');
                    case 'r' -> result.append('\r');
                    case 't' -> result.append('\t');
                    case 'u' -> result.append((char) hexDigit(input, 4));
                    default -> throw error("unknown string escape");
                }
            }
            throw error("unterminated string");
        }

        private JsonNumber numberValue() {
            final int start = index;
            if (take('-')) {
                // The type checker rejects negative values; consuming it here
                // keeps malformed JSON from being mistaken for a literal.
            }
            digits();
            if (take('.')) {
                digits();
            }
            if (index < input.length() && (input.charAt(index) == 'e' || input.charAt(index) == 'E')) {
                index++;
                if (index < input.length() && (input.charAt(index) == '+' || input.charAt(index) == '-')) {
                    index++;
                }
                digits();
            }
            return new JsonNumber(input.substring(start, index));
        }

        private void digits() {
            final int start = index;
            while (index < input.length() && Character.isDigit(input.charAt(index))) {
                index++;
            }
            if (start == index) {
                throw error("expected JSON number digits");
            }
        }

        private Object literal(final String literal, final Object value) {
            if (!input.startsWith(literal, index)) {
                throw error("invalid JSON literal");
            }
            index += literal.length();
            return value;
        }

        private int hexDigit(final String ignored, final int count) {
            if (index + count > input.length()) {
                throw error("short unicode escape");
            }
            int value = 0;
            for (int offset = 0; offset < count; offset++) {
                final int digit = Character.digit(input.charAt(index++), 16);
                if (digit < 0) {
                    throw error("invalid unicode escape");
                }
                value = (value << 4) | digit;
            }
            return value;
        }

        private boolean take(final char expected) {
            if (index < input.length() && input.charAt(index) == expected) {
                index++;
                return true;
            }
            return false;
        }

        private void skipWhitespace() {
            while (index < input.length()) {
                final char current = input.charAt(index);
                if (current != ' ' && current != '\t' && current != '\n' && current != '\r') {
                    return;
                }
                index++;
            }
        }

        private void ensureEnd() {
            skipWhitespace();
            if (index != input.length()) {
                throw error("trailing JSON bytes");
            }
        }

        private IllegalArgumentException error(final String message) {
            return new IllegalArgumentException(message + " at byte " + index);
        }
    }
}
