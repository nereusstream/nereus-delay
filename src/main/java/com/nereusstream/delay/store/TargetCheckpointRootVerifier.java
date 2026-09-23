package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaBookkeeping;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Read-only physical check of the format-2 root, bookkeeping and source frontier.
 *
 * <p>This checks only the fixed accounting anchor. It does not audit Target projections,
 * authenticate external controls, or make an image publishable or recoverable.</p>
 */
public final class TargetCheckpointRootVerifier {
    private static final int FIXED_VALUE_TYPE = 1;

    private TargetCheckpointRootVerifier() {}

    public record RootProof(
            StoreMetadata metadata,
            SourcePosition source,
            long mutationSequence,
            TargetQuotaIncarnation root,
            TargetQuotaBookkeeping bookkeeping,
            TargetQuotaAggregate aggregate) {
        public RootProof {
            Objects.requireNonNull(metadata, "metadata");
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(root, "root");
            Objects.requireNonNull(bookkeeping, "bookkeeping");
            Objects.requireNonNull(aggregate, "aggregate");
        }
    }

    /** Reads an immutable RocksDB image under finite physical limits without changing its markers. */
    public static RootProof validate(
            final Path image, final ShardId expectedShard, final CheckpointManifestLimits limits) {
        Objects.requireNonNull(image, "image");
        Objects.requireNonNull(expectedShard, "expectedShard");
        Objects.requireNonNull(limits, "limits");
        if (limits.maxFiles() == Integer.MAX_VALUE
                || limits.maxTotalFileBytes() == Long.MAX_VALUE
                || limits.maxIndividualFileBytes() == Long.MAX_VALUE
                || limits.maxPathBytes() == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Target checkpoint root validation requires finite physical limits");
        }
        final var files = CheckpointFileInventory.collect(image, limits);
        if (!Files.isRegularFile(image.resolve("CURRENT"), LinkOption.NOFOLLOW_LINKS)
                || files.stream()
                        .noneMatch(file -> file.name().startsWith("MANIFEST-")
                                && !file.name().contains("/"))) {
            throw new IllegalArgumentException("Target checkpoint is not a complete RocksDB image");
        }
        final List<byte[]> names;
        try (Options options = new Options()) {
            names = RocksDB.listColumnFamilies(options, image.toString());
        } catch (RocksDBException failure) {
            throw new IllegalArgumentException("cannot inspect Target checkpoint column families", failure);
        }
        final Set<String> expectedFamilies = new HashSet<>();
        expectedFamilies.add(new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8));
        for (ColumnFamily family : ColumnFamily.values()) {
            expectedFamilies.add(family.rocksName());
        }
        final Set<String> actualFamilies = new HashSet<>();
        for (byte[] name : names) {
            actualFamilies.add(new String(name, StandardCharsets.UTF_8));
        }
        if (actualFamilies.size() != names.size() || !actualFamilies.equals(expectedFamilies)) {
            throw new IllegalArgumentException("Target checkpoint column-family set differs from the Store");
        }
        final List<ColumnFamilyOptions> options = new ArrayList<>();
        final List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
        final List<ColumnFamilyHandle> handles = new ArrayList<>();
        try {
            for (byte[] name : names) {
                final var familyOptions = new ColumnFamilyOptions();
                options.add(familyOptions);
                descriptors.add(new ColumnFamilyDescriptor(name, familyOptions));
            }
            try (DBOptions dbOptions = new DBOptions().setCreateIfMissing(false).setCreateMissingColumnFamilies(false);
                    RocksDB db = RocksDB.openReadOnly(dbOptions, image.toString(), descriptors, handles)) {
                final int metaIndex = names.stream()
                        .map(name -> new String(name, StandardCharsets.UTF_8))
                        .toList()
                        .indexOf(ColumnFamily.META.rocksName());
                return readRoot(db, handles.get(metaIndex), expectedShard);
            } catch (RocksDBException failure) {
                throw new IllegalArgumentException("cannot open Target checkpoint read-only", failure);
            }
        } finally {
            for (ColumnFamilyHandle handle : handles) {
                handle.close();
            }
            for (ColumnFamilyOptions option : options) {
                option.close();
            }
        }
    }

    private static RootProof readRoot(final RocksDB db, final ColumnFamilyHandle meta, final ShardId shard) {
        try {
            final byte[] format = requiredFixed(db, meta, 1, "store format");
            if (format.length != Integer.BYTES || Bytes.readU32be(format, 0) != 2) {
                throw new IllegalArgumentException("Target checkpoint has another Store format");
            }
            final StoreMetadata metadata = StoreMetadata.decode(requiredFixed(db, meta, 2, "shard identity"));
            if (metadata.storeFormatVersion() != 2 || !metadata.shardId().equals(shard)) {
                throw new IllegalArgumentException("Target checkpoint identity belongs to another Shard or format");
            }
            final SourcePosition source = SourcePositionCodec.decode(requiredFixed(db, meta, 3, "source position"));
            final byte[] sequenceBytes = requiredFixed(db, meta, 5, "mutation sequence");
            if (sequenceBytes.length != Long.BYTES) {
                throw new IllegalArgumentException("Target checkpoint mutation sequence has another width");
            }
            final long sequence = Bytes.readU64be(sequenceBytes, 0);
            if (sequence == 0 || !source.shardId().equals(shard)) {
                throw new IllegalArgumentException("Target checkpoint source frontier belongs to another Shard");
            }
            final byte[] bookkeepingKey = TargetQuotaBookkeeping.keyFor(shard);
            final byte[] bookkeepingBytes =
                    requiredTarget(db, meta, bookkeepingKey, TargetQuotaBookkeeping.VALUE_TYPE, "bookkeeping root");
            final TargetQuotaBookkeeping bookkeeping = TargetQuotaBookkeeping.decode(bookkeepingBytes);
            if (!Arrays.equals(bookkeeping.key(), bookkeepingKey)
                    || bookkeeping.owner().kind() != TargetQuotaIdentity.Kind.SHARD
                    || !bookkeeping.owner().shard().equals(shard)) {
                throw new IllegalArgumentException("Target checkpoint bookkeeping belongs to another Shard");
            }
            final byte[] rootKey = bookkeeping.owner().key();
            rootKey[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
            final TargetQuotaIncarnation root = TargetQuotaIncarnation.decodeForStore(
                    rootKey,
                    requiredTarget(db, meta, rootKey, TargetQuotaIncarnation.VALUE_TYPE, "root incarnation"),
                    shard,
                    bookkeeping.tenantScope());
            if (!bookkeeping.owner().equals(root.identity())
                    || !Arrays.equals(
                            bookkeeping.accounting().canonicalBytes(),
                            root.accounting().canonicalBytes())) {
                throw new IllegalArgumentException("Target checkpoint root and bookkeeping disagree");
            }
            final byte[] aggregateKey = TargetQuotaAggregate.genesis(
                            shard, root.identity().accountingIncarnation())
                    .key();
            final TargetQuotaAggregate aggregate = TargetQuotaAggregate.decodeForStore(
                    aggregateKey,
                    requiredTarget(db, meta, aggregateKey, TargetQuotaAggregate.VALUE_TYPE, "aggregate"),
                    shard);
            if (!Arrays.equals(
                            aggregate.accountingIncarnation(), root.identity().accountingIncarnation())
                    || aggregate.mutation() == null
                    || aggregate.mutation().sequence() != sequence
                    || Long.compareUnsigned(bookkeeping.revision(), aggregate.revision()) > 0
                    || !Arrays.equals(aggregate.mutation().source().canonicalBytes(), source.canonicalBytes())) {
                throw new IllegalArgumentException("Target checkpoint aggregate and source frontier disagree");
            }
            root.latestMutation().requireAtOrBefore(aggregate.mutation());
            bookkeeping.mutation().requireAtOrBefore(aggregate.mutation());
            return new RootProof(metadata, source, sequence, root, bookkeeping, aggregate);
        } catch (RocksDBException failure) {
            throw new IllegalArgumentException("cannot read Target checkpoint root", failure);
        }
    }

    private static byte[] requiredFixed(
            final RocksDB db, final ColumnFamilyHandle meta, final int keyKind, final String name)
            throws RocksDBException {
        final byte[] encoded = db.get(meta, KeyCodec.metaFixed(keyKind));
        if (encoded == null) {
            throw new IllegalArgumentException("Target checkpoint is missing " + name);
        }
        return ValueEnvelope.decode(encoded, FIXED_VALUE_TYPE).payload();
    }

    private static byte[] requiredTarget(
            final RocksDB db, final ColumnFamilyHandle meta, final byte[] key, final int type, final String name)
            throws RocksDBException {
        final byte[] encoded = db.get(meta, key);
        if (encoded == null) {
            throw new IllegalArgumentException("Target checkpoint is missing " + name);
        }
        return TargetValueEnvelope.decode(encoded, type).payload();
    }
}
