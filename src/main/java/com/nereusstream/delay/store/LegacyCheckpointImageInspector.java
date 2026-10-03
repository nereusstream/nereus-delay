package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/** Read-only identity and byte-inventory check for one legacy format-1 checkpoint image. */
public final class LegacyCheckpointImageInspector {
    private LegacyCheckpointImageInspector() {}

    /**
     * Verifies the physical image against its already-authenticated manifest and exact Shard.
     * This does not inspect logical Message/Claim/Attempt relationships or authorize migration.
     */
    public static ImageProof inspect(
            final Path image,
            final ShardId expectedShard,
            final CheckpointManifest manifest,
            final CheckpointManifestLimits limits) {
        Objects.requireNonNull(image, "image");
        Objects.requireNonNull(expectedShard, "expectedShard");
        final CheckpointManifest exactManifest = Objects.requireNonNull(manifest, "manifest");
        final CheckpointManifestLimits exactLimits = Objects.requireNonNull(limits, "limits");
        TargetCheckpointRootVerifier.requireFinitePhysicalLimits(exactLimits);
        exactLimits.validateManifest(exactManifest);
        if (exactManifest.storeFormatVersion() != 1 || !expectedShard.equals(exactManifest.shardId())) {
            throw new IllegalArgumentException("legacy checkpoint manifest format or Shard identity mismatch");
        }

        final List<CheckpointFileInventory> files = CheckpointFileInventory.collect(image, exactLimits);
        requireFileInventoryMatches(files, exactManifest.files());
        final StoreMetadata metadata;
        final SourcePosition sourcePosition;
        final long mutationSequence;
        final byte[] checkpointId;
        try (Options listOptions = new Options()) {
            final List<byte[]> listedColumnFamilies = RocksDB.listColumnFamilies(listOptions, image.toString());
            requireExactColumnFamilies(listedColumnFamilies);
            final List<ColumnFamilyOptions> familyOptions = new ArrayList<>();
            final List<ColumnFamilyHandle> handles = new ArrayList<>();
            DBOptions dbOptions = null;
            RocksDB db = null;
            try {
                final List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
                for (byte[] name : listedColumnFamilies) {
                    final ColumnFamilyOptions options = new ColumnFamilyOptions();
                    familyOptions.add(options);
                    descriptors.add(new ColumnFamilyDescriptor(name, options));
                }
                dbOptions = new DBOptions().setCreateIfMissing(false).setCreateMissingColumnFamilies(false);
                db = RocksDB.openReadOnly(dbOptions, image.toString(), descriptors, handles);
                final Map<String, ColumnFamilyHandle> byName = indexHandles(listedColumnFamilies, handles);
                requireDefaultColumnFamilyEmpty(
                        db, byName.get(new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8)));
                final ColumnFamilyHandle meta = byName.get(ColumnFamily.META.rocksName());
                final int format = readU32(db, meta, ShardStore.META_STORE_FORMAT);
                metadata = StoreMetadata.decode(readMetadataValue(db, meta, ShardStore.META_SHARD_IDENTITY));
                if (format != 1 || metadata.storeFormatVersion() != 1 || !expectedShard.equals(metadata.shardId())) {
                    throw new IllegalArgumentException("legacy checkpoint image Store metadata mismatch");
                }
                sourcePosition = SourcePositionCodec.decode(
                        readMetadataValue(db, meta, ShardStore.META_APPLIED_SOURCE_POSITION));
                mutationSequence = readU64Bits(db, meta, ShardStore.META_MUTATION_SEQUENCE);
                checkpointId = readMetadataValue(db, meta, ShardStore.META_CHECKPOINT_ID);
            } catch (RocksDBException failure) {
                throw new IllegalArgumentException("cannot open legacy checkpoint image read-only", failure);
            } finally {
                if (db != null) {
                    db.close();
                }
                for (ColumnFamilyHandle handle : handles) {
                    handle.close();
                }
                for (ColumnFamilyOptions options : familyOptions) {
                    options.close();
                }
                if (dbOptions != null) {
                    dbOptions.close();
                }
            }
        } catch (RocksDBException failure) {
            throw new IllegalArgumentException("cannot enumerate legacy checkpoint column families", failure);
        }

        final List<CheckpointFileInventory> filesAfterRead = CheckpointFileInventory.collect(image, exactLimits);
        requireSameInventory(files, filesAfterRead);
        requireManifestIdentity(exactManifest, metadata, sourcePosition, mutationSequence, checkpointId);
        long totalBytes = 0;
        for (CheckpointFileInventory file : files) {
            totalBytes = Math.addExact(totalBytes, file.length());
        }
        return new ImageProof(metadata, checkpointId, sourcePosition, mutationSequence, totalBytes, files);
    }

    private static void requireExactColumnFamilies(final List<byte[]> names) {
        final Set<String> actual = new HashSet<>();
        for (byte[] name : names) {
            actual.add(new String(name, StandardCharsets.UTF_8));
        }
        final Set<String> expected = new HashSet<>();
        expected.add(new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8));
        for (ColumnFamily family : ColumnFamily.values()) {
            expected.add(family.rocksName());
        }
        if (actual.size() != names.size() || !actual.equals(expected)) {
            throw new IllegalArgumentException("legacy checkpoint column-family inventory mismatch");
        }
    }

    private static Map<String, ColumnFamilyHandle> indexHandles(
            final List<byte[]> names, final List<ColumnFamilyHandle> handles) {
        if (names.size() != handles.size()) {
            throw new IllegalArgumentException("RocksDB returned an incomplete column-family handle set");
        }
        final Map<String, ColumnFamilyHandle> result = new HashMap<>();
        for (int index = 0; index < names.size(); index++) {
            final String name = new String(names.get(index), StandardCharsets.UTF_8);
            if (result.put(name, handles.get(index)) != null) {
                throw new IllegalArgumentException("legacy checkpoint has duplicate column-family names");
            }
        }
        return result;
    }

    private static void requireDefaultColumnFamilyEmpty(final RocksDB db, final ColumnFamilyHandle defaultFamily)
            throws RocksDBException {
        try (RocksIterator iterator = db.newIterator(Objects.requireNonNull(defaultFamily, "defaultFamily"))) {
            iterator.seekToFirst();
            iterator.status();
            if (iterator.isValid()) {
                throw new IllegalArgumentException("legacy checkpoint default column family must remain empty");
            }
        }
    }

    private static int readU32(final RocksDB db, final ColumnFamilyHandle meta, final int key)
            throws RocksDBException {
        final byte[] value = readMetadataValue(db, meta, key);
        if (value.length != 4) {
            throw new IllegalArgumentException("legacy checkpoint fixed metadata has an invalid uint32 length");
        }
        return ByteBuffer.wrap(value).getInt();
    }

    private static long readU64Bits(final RocksDB db, final ColumnFamilyHandle meta, final int key)
            throws RocksDBException {
        final byte[] value = readMetadataValue(db, meta, key);
        if (value.length != 8) {
            throw new IllegalArgumentException("legacy checkpoint fixed metadata has an invalid uint64 length");
        }
        return ByteBuffer.wrap(value).getLong();
    }

    private static byte[] readMetadataValue(final RocksDB db, final ColumnFamilyHandle meta, final int key)
            throws RocksDBException {
        final byte[] encoded = db.get(meta, KeyCodec.metaFixed(key));
        if (encoded == null) {
            throw new IllegalArgumentException("legacy checkpoint is missing fixed metadata key " + key);
        }
        return ValueEnvelope.decode(encoded, ShardStore.META_FIXED_VALUE_TYPE).payload();
    }

    private static void requireFileInventoryMatches(
            final List<CheckpointFileInventory> actual, final List<CheckpointManifest.FileEntry> expected) {
        if (actual.size() != expected.size()) {
            throw new IllegalArgumentException("legacy checkpoint physical files differ from manifest");
        }
        final Map<String, CheckpointManifest.FileEntry> expectedByName = new HashMap<>();
        for (CheckpointManifest.FileEntry entry : expected) {
            expectedByName.put(entry.name(), entry);
        }
        for (CheckpointFileInventory file : actual) {
            final CheckpointManifest.FileEntry entry = expectedByName.remove(file.name());
            if (entry == null
                    || entry.length() != file.length()
                    || !Bytes.constantTimeEquals(entry.checksum(), file.checksum())) {
                throw new IllegalArgumentException("legacy checkpoint physical file differs from manifest");
            }
        }
        if (!expectedByName.isEmpty()) {
            throw new IllegalArgumentException("legacy checkpoint manifest contains absent physical files");
        }
    }

    private static void requireSameInventory(
            final List<CheckpointFileInventory> before, final List<CheckpointFileInventory> after) {
        if (before.size() != after.size()) {
            throw new IllegalArgumentException("legacy checkpoint changed during read-only inspection");
        }
        for (int index = 0; index < before.size(); index++) {
            final CheckpointFileInventory left = before.get(index);
            final CheckpointFileInventory right = after.get(index);
            if (!left.name().equals(right.name())
                    || left.length() != right.length()
                    || !Bytes.constantTimeEquals(left.checksum(), right.checksum())) {
                throw new IllegalArgumentException("legacy checkpoint changed during read-only inspection");
            }
        }
    }

    private static void requireManifestIdentity(
            final CheckpointManifest manifest,
            final StoreMetadata metadata,
            final SourcePosition sourcePosition,
            final long mutationSequence,
            final byte[] checkpointId) {
        if (!manifest.shardId().equals(metadata.shardId())
                || !Bytes.constantTimeEquals(manifest.dbIdentity(), metadata.dbIdentity())
                || !manifest.sourceStoreIncarnation().equals(metadata.storeIncarnationUuid())
                || !Bytes.constantTimeEquals(manifest.checkpointId(), checkpointId)
                || manifest.shardMutationSequence() != mutationSequence
                || !Arrays.equals(
                        manifest.appliedShardLogPosition().canonicalBytes(), sourcePosition.canonicalBytes())) {
            throw new IllegalArgumentException("legacy checkpoint Store, checkpoint or source cut identity mismatch");
        }
    }

    public record ImageProof(
            StoreMetadata metadata,
            byte[] checkpointId,
            SourcePosition appliedSourcePosition,
            long mutationSequence,
            long physicalBytes,
            List<CheckpointFileInventory> files) {
        public ImageProof {
            Objects.requireNonNull(metadata, "metadata");
            Bytes.requireLength(checkpointId, 16, "checkpointId");
            checkpointId = Bytes.copy(checkpointId);
            Objects.requireNonNull(appliedSourcePosition, "appliedSourcePosition");
            if (mutationSequence < 0 || physicalBytes < 0) {
                throw new IllegalArgumentException("legacy checkpoint proof contains a negative counter");
            }
            files = List.copyOf(Objects.requireNonNull(files, "files"));
        }

        @Override
        public byte[] checkpointId() {
            return Bytes.copy(checkpointId);
        }
    }
}
