package io.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;

/**
 * Minimal parser for the immutable fields of the Registry canonical Lane
 * tuple.  The complete Profile semantic/target authority remains outside the
 * local value codec; this parser only proves that the two Profile slots in a
 * typed Lane projection are the exact bytes carried by the tuple.
 */
final class CanonicalLaneTupleV1 {
    private static final int TENANT_SCOPE_LENGTH = 32;
    private static final int HASH_LENGTH = 32;
    private static final int MAX_TUPLE_BYTES = 1 << 20;
    private static final int MAX_PROFILE_ID_BYTES = 256;
    private static final int KAFKA_ADAPTER = 1;
    private static final int PULSAR_ADAPTER = 2;
    private static final int KAFKA_RESOURCE = 1;
    private static final int PULSAR_RESOURCE = 2;
    private static final int ORDERED_LANE = 1;
    private static final int UNORDERED_LANE = 2;

    private CanonicalLaneTupleV1() {
    }

    static void requireProfileProjection(final byte[] encoded, final ProfileRefV1 destination,
                                         final ProfileRefV1 capability) {
        final Cursor cursor = new Cursor(encoded);
        cursor.fixed(TENANT_SCOPE_LENGTH, "tenantRoutingScope");
        final int adapter = cursor.u8("adapterKind");
        if (adapter != KAFKA_ADAPTER && adapter != PULSAR_ADAPTER) {
            throw new IllegalArgumentException("unknown Lane tuple adapter kind");
        }
        final byte[] cluster = cursor.lp32(256, "authenticatedTargetClusterId");
        requireCanonicalText(cluster, "authenticatedTargetClusterId");
        final int resourceKind = cursor.u8("brokerResourceKind");
        if ((adapter == KAFKA_ADAPTER && resourceKind != KAFKA_RESOURCE)
                || (adapter == PULSAR_ADAPTER && resourceKind != PULSAR_RESOURCE)) {
            throw new IllegalArgumentException("Lane tuple adapter/resource kind mismatch");
        }
        if (adapter == KAFKA_ADAPTER) {
            cursor.fixed(16, "kafkaNativeTopicUuid");
        } else {
            cursor.fixed(32, "pulsarResourceIncarnation");
            cursor.u64("pulsarPhysicalTopicCreationTimestamp");
        }
        final byte[] physicalTopic = cursor.lp32(1 << 20, "physicalTopicIdentity");
        if (adapter == KAFKA_ADAPTER) {
            Bytes.requireLength(physicalTopic, 16, "kafkaPhysicalTopicIdentity");
        } else {
            requireCanonicalText(physicalTopic, "pulsarPhysicalTopicIdentity");
        }
        cursor.u32("physicalPartition");

        final byte[] destinationId = cursor.lp32(MAX_PROFILE_ID_BYTES, "destinationProfileId");
        final long destinationVersion = cursor.u64("destinationProfileVersion");
        final byte[] destinationHash = cursor.fixed(HASH_LENGTH, "destinationProfileSemanticHash");
        final byte[] capabilityId = cursor.lp32(MAX_PROFILE_ID_BYTES, "capabilityProfileId");
        final long capabilityVersion = cursor.u64("capabilityProfileVersion");
        final byte[] capabilityHash = cursor.fixed(HASH_LENGTH, "capabilityProfileSemanticHash");

        final int laneKind = cursor.u8("orderingLaneKind");
        if (laneKind == ORDERED_LANE) {
            cursor.fixed(HASH_LENGTH, "orderingDomainHash");
        } else if (laneKind == UNORDERED_LANE) {
            cursor.u32("unorderedBucket");
        } else {
            throw new IllegalArgumentException("unknown Lane tuple ordering kind");
        }
        cursor.requireEnd();

        if (!Arrays.equals(destinationId, destination.profileId())
                || destinationVersion != destination.version()
                || !Bytes.constantTimeEquals(destinationHash, destination.semanticHash())) {
            throw new IllegalArgumentException("destination Profile does not project from canonical Lane tuple");
        }
        if (!Arrays.equals(capabilityId, capability.profileId())
                || capabilityVersion != capability.version()
                || !Bytes.constantTimeEquals(capabilityHash, capability.semanticHash())) {
            throw new IllegalArgumentException("capability Profile does not project from canonical Lane tuple");
        }
    }

    private static void requireCanonicalText(final byte[] value, final String name) {
        final String decoded = new String(value, StandardCharsets.UTF_8);
        if (value.length == 0 || decoded.isBlank()
                || !Arrays.equals(decoded.getBytes(StandardCharsets.UTF_8), value)
                || decoded.indexOf('\0') >= 0 || !decoded.equals(Normalizer.normalize(decoded, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be non-empty canonical UTF-8");
        }
    }

    private static final class Cursor {
        private final byte[] value;
        private int offset;

        private Cursor(final byte[] value) {
            if (value == null || value.length == 0 || value.length > MAX_TUPLE_BYTES) {
                throw new IllegalArgumentException("canonical Lane tuple is outside the V1 bound");
            }
            this.value = value;
        }

        private byte[] fixed(final int length, final String name) {
            if (length < 0 || offset > value.length - length) {
                throw new IllegalArgumentException("canonical Lane tuple is truncated at " + name);
            }
            final byte[] result = Arrays.copyOfRange(value, offset, offset + length);
            offset += length;
            return result;
        }

        private byte[] lp32(final int maximum, final String name) {
            final long length = u32(name + " length");
            if (length > maximum || length > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(name + " exceeds its V1 bound");
            }
            return fixed((int) length, name);
        }

        private int u8(final String name) {
            if (offset >= value.length) {
                throw new IllegalArgumentException("canonical Lane tuple is truncated at " + name);
            }
            return value[offset++] & 0xff;
        }

        private long u32(final String name) {
            if (offset > value.length - 4) {
                throw new IllegalArgumentException("canonical Lane tuple is truncated at " + name);
            }
            final long result = Bytes.readU32be(value, offset);
            offset += 4;
            return result;
        }

        private long u64(final String name) {
            if (offset > value.length - 8) {
                throw new IllegalArgumentException("canonical Lane tuple is truncated at " + name);
            }
            final long result = Bytes.readU64be(value, offset);
            offset += 8;
            return result;
        }

        private void requireEnd() {
            if (offset != value.length) {
                throw new IllegalArgumentException("canonical Lane tuple has trailing bytes");
            }
        }
    }
}
