package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Canonical sorted per-Lane quota usage map for the shard quota class. */
public final class LaneQuotaUsageMap {
    private static final int VERSION = 1;
    private static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-lane-quota-usage-map\0");

    private final List<LaneQuotaUsageEntry> entries;
    private final byte[] mapDigest;

    public LaneQuotaUsageMap(final List<LaneQuotaUsageEntry> entries) {
        Objects.requireNonNull(entries, "entries");
        this.entries = sortedUnique(entries);
        this.mapDigest = Bytes.sha256(DIGEST_DOMAIN, fieldsOneAndTwo());
    }

    public List<LaneQuotaUsageEntry> entries() {
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

    public static LaneQuotaUsageMap decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() < 2
                || fields.get(0).number() != 1
                || fields.get(fields.size() - 1).number() != 3) {
            throw new IllegalArgumentException("invalid LaneQuotaUsageMap field order");
        }
        if (QueryCodecSupport.uint32(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported LaneQuotaUsageMap version");
        }
        final List<LaneQuotaUsageEntry> entries = new ArrayList<>();
        for (int index = 1; index < fields.size() - 1; index++) {
            if (fields.get(index).number() != 2) {
                throw new IllegalArgumentException("LaneQuotaUsageMap entries must use field 2");
            }
            entries.add(LaneQuotaUsageEntry.decode(QueryCodecSupport.nested(fields.get(index), 2)));
        }
        final byte[] mapDigest = QueryCodecSupport.fixed(fields.get(fields.size() - 1), 3, HASH_LENGTH);
        final LaneQuotaUsageMap result = new LaneQuotaUsageMap(entries);
        if (!Bytes.constantTimeEquals(mapDigest, result.mapDigest)) {
            throw new IllegalArgumentException("LaneQuotaUsageMap digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "LaneQuotaUsageMap");
        return result;
    }

    private byte[] fieldsOneAndTwo() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            for (LaneQuotaUsageEntry entry : entries) {
                CanonicalProtobuf.bytes(output, 2, entry.canonicalBytes());
            }
        });
    }

    private static List<LaneQuotaUsageEntry> sortedUnique(final List<LaneQuotaUsageEntry> values) {
        final List<LaneQuotaUsageEntry> result = new ArrayList<>(values.size());
        LaneQuotaUsageEntry previous = null;
        for (LaneQuotaUsageEntry value : values) {
            Objects.requireNonNull(value, "usage entry");
            if (previous != null && compare(previous, value) >= 0) {
                throw new IllegalArgumentException("LaneQuotaUsageMap entries must be sorted and unique");
            }
            result.add(value);
            previous = value;
        }
        return Collections.unmodifiableList(result);
    }

    private static int compare(final LaneQuotaUsageEntry left, final LaneQuotaUsageEntry right) {
        int result =
                Arrays.compareUnsigned(left.laneId().bytes(), right.laneId().bytes());
        if (result != 0) {
            return result;
        }
        return Arrays.compareUnsigned(left.laneIncarnation(), right.laneIncarnation());
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof LaneQuotaUsageMap that
                && entries.equals(that.entries)
                && Arrays.equals(mapDigest, that.mapDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entries, Arrays.hashCode(mapDigest));
    }
}
