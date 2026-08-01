package io.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Canonical sorted per-Lane quota usage map for the shard quota class. */
public final class LaneQuotaUsageMapV1 {
    private static final int VERSION = 1;
    private static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN =
            Bytes.utf8("nereus-delay-lane-quota-usage-map-v1\0");

    private final List<LaneQuotaUsageEntryV1> entries;
    private final byte[] mapDigest;

    public LaneQuotaUsageMapV1(final List<LaneQuotaUsageEntryV1> entries) {
        Objects.requireNonNull(entries, "entries");
        this.entries = sortedUnique(entries);
        this.mapDigest = Bytes.sha256(DIGEST_DOMAIN, fieldsOneAndTwo());
    }

    public List<LaneQuotaUsageEntryV1> entries() {
        return entries;
    }

    public byte[] mapDigest() {
        return Bytes.copy(mapDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneAndTwo());
            CanonicalProtobuf.bytes(output, 3, mapDigest);
        });
    }

    public static LaneQuotaUsageMapV1 decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() < 2 || fields.get(0).number() != 1 || fields.get(fields.size() - 1).number() != 3) {
            throw new IllegalArgumentException("invalid LaneQuotaUsageMapV1 field order");
        }
        if (QueryCodecSupport.uint32(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported LaneQuotaUsageMapV1 version");
        }
        final List<LaneQuotaUsageEntryV1> entries = new ArrayList<>();
        for (int index = 1; index < fields.size() - 1; index++) {
            if (fields.get(index).number() != 2) {
                throw new IllegalArgumentException("LaneQuotaUsageMapV1 entries must use field 2");
            }
            entries.add(LaneQuotaUsageEntryV1.decode(QueryCodecSupport.nested(fields.get(index), 2)));
        }
        final byte[] mapDigest = QueryCodecSupport.fixed(fields.get(fields.size() - 1), 3, HASH_LENGTH);
        final LaneQuotaUsageMapV1 result = new LaneQuotaUsageMapV1(entries);
        if (!Bytes.constantTimeEquals(mapDigest, result.mapDigest)) {
            throw new IllegalArgumentException("LaneQuotaUsageMapV1 digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "LaneQuotaUsageMapV1");
        return result;
    }

    private byte[] fieldsOneAndTwo() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            for (LaneQuotaUsageEntryV1 entry : entries) {
                CanonicalProtobuf.bytes(output, 2, entry.canonicalBytes());
            }
        });
    }

    private static List<LaneQuotaUsageEntryV1> sortedUnique(final List<LaneQuotaUsageEntryV1> values) {
        final List<LaneQuotaUsageEntryV1> result = new ArrayList<>(values.size());
        LaneQuotaUsageEntryV1 previous = null;
        for (LaneQuotaUsageEntryV1 value : values) {
            Objects.requireNonNull(value, "usage entry");
            if (previous != null && compare(previous, value) >= 0) {
                throw new IllegalArgumentException("LaneQuotaUsageMapV1 entries must be sorted and unique");
            }
            result.add(value);
            previous = value;
        }
        return Collections.unmodifiableList(result);
    }

    private static int compare(final LaneQuotaUsageEntryV1 left, final LaneQuotaUsageEntryV1 right) {
        int result = Arrays.compareUnsigned(left.laneId().bytes(), right.laneId().bytes());
        if (result != 0) {
            return result;
        }
        return Arrays.compareUnsigned(left.laneIncarnation(), right.laneIncarnation());
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof LaneQuotaUsageMapV1 that && entries.equals(that.entries)
                && Arrays.equals(mapDigest, that.mapDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entries, Arrays.hashCode(mapDigest));
    }
}
