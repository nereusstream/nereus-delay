package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.SourcePositionCodec;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Read-only validation of the complete shard-bound control snapshot inside a
 * physical checkpoint image. A directory without a RocksDB MANIFEST is
 * retained as a legacy embedded fixture seam; a recognized RocksDB image must
 * carry key 10 and is never allowed to publish a mismatched digest.
 */
final class CheckpointControlSnapshotVerifier {
    private static final int META_CONTROL_SNAPSHOT = 10;
    private static final int META_FIXED_VALUE_TYPE = 1;

    private CheckpointControlSnapshotVerifier() {
    }

    static void validate(final Path checkpointDirectory, final ShardId expectedShard,
                         final byte[] expectedDigest) {
        validate(checkpointDirectory, expectedShard, expectedDigest, null, null, null, null);
    }

    /**
     * Validates the physical identity of a checkpoint against its complete
     * manifest.  A manifest must describe the DB image that is actually being
     * uploaded; file checksums and the control snapshot alone are not enough
     * because an image can otherwise be paired with another Store Incarnation
     * or DB identity.
     */
    static void validate(final Path checkpointDirectory, final CheckpointManifest manifest) {
        Objects.requireNonNull(manifest, "manifest");
        validate(checkpointDirectory, manifest.shardId(), manifest.controlStateDigest(),
                manifest.dbIdentity(), manifest.sourceStoreIncarnation(), manifest.storeFormatVersion(), manifest);
    }

    private static void validate(final Path checkpointDirectory, final ShardId expectedShard,
                                 final byte[] expectedDigest, final byte[] expectedDbIdentity,
                                 final UUID expectedStoreIncarnation, final Integer expectedStoreFormat,
                                 final CheckpointManifest expectedManifest) {
        Objects.requireNonNull(checkpointDirectory, "checkpointDirectory");
        Objects.requireNonNull(expectedShard, "expectedShard");
        Bytes.requireLength(expectedDigest, 32, "expectedDigest");
        if (!hasManifestFile(checkpointDirectory)) {
            return;
        }
        final List<byte[]> columnFamilies;
        try (Options options = new Options()) {
            columnFamilies = RocksDB.listColumnFamilies(options, checkpointDirectory.toString());
        } catch (RocksDBException failure) {
            throw new IllegalArgumentException("cannot inspect checkpoint RocksDB column families", failure);
        }
        int metaIndex = -1;
        for (int index = 0; index < columnFamilies.size(); index++) {
            if (java.util.Arrays.equals(columnFamilies.get(index), Bytes.utf8(ColumnFamily.META.rocksName()))) {
                metaIndex = index;
                break;
            }
        }
        if (metaIndex < 0) {
            throw new IllegalArgumentException("checkpoint RocksDB is missing meta_cf");
        }

        final List<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>();
        final List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
        final List<ColumnFamilyHandle> handles = new ArrayList<>();
        try {
            for (byte[] name : columnFamilies) {
                final ColumnFamilyOptions options = new ColumnFamilyOptions();
                columnFamilyOptions.add(options);
                descriptors.add(new ColumnFamilyDescriptor(name, options));
            }
            try (DBOptions dbOptions = new DBOptions()
                    .setCreateIfMissing(false)
                    .setCreateMissingColumnFamilies(false);
                 RocksDB db = RocksDB.openReadOnly(dbOptions, checkpointDirectory.toString(), descriptors, handles)) {
                final ColumnFamilyHandle meta = handles.get(metaIndex);
                final byte[] formatEncoded = db.get(meta, KeyCodec.metaFixed(1));
                if (formatEncoded == null) {
                    throw new IllegalArgumentException("checkpoint RocksDB is missing store format");
                }
                final byte[] format = ValueEnvelope.decode(formatEncoded, META_FIXED_VALUE_TYPE).payload();
                if (format.length != Integer.BYTES || Bytes.readU32be(format, 0) != 1) {
                    throw new IllegalArgumentException("checkpoint RocksDB has an unsupported store format");
                }
                final byte[] identityEncoded = db.get(meta, KeyCodec.metaFixed(2));
                if (identityEncoded == null) {
                    throw new IllegalArgumentException("checkpoint RocksDB is missing shard identity");
                }
                final StoreMetadata metadata;
                try {
                    metadata = StoreMetadata.decode(
                            ValueEnvelope.decode(identityEncoded, META_FIXED_VALUE_TYPE).payload());
                } catch (IllegalArgumentException malformed) {
                    throw new IllegalArgumentException("checkpoint RocksDB has malformed shard identity", malformed);
                }
                if (!expectedShard.equals(metadata.shardId())) {
                    throw new IllegalArgumentException("checkpoint store identity belongs to another shard");
                }
                if (expectedStoreFormat != null && metadata.storeFormatVersion() != expectedStoreFormat) {
                    throw new IllegalArgumentException("checkpoint store format does not match manifest");
                }
                if (expectedDbIdentity != null
                        && !Bytes.constantTimeEquals(metadata.dbIdentity(), expectedDbIdentity)) {
                    throw new IllegalArgumentException("checkpoint DB identity does not match manifest");
                }
                if (expectedStoreIncarnation != null
                        && !expectedStoreIncarnation.equals(metadata.storeIncarnationUuid())) {
                    throw new IllegalArgumentException("checkpoint Store Incarnation does not match manifest");
                }
                final byte[] encoded = db.get(meta, KeyCodec.metaFixed(META_CONTROL_SNAPSHOT));
                if (encoded == null) {
                    throw new IllegalArgumentException("checkpoint RocksDB is missing control snapshot");
                }
                final byte[] payload = ValueEnvelope.decode(encoded, META_FIXED_VALUE_TYPE).payload();
                final CompatibleControlSnapshotV1 snapshot = CompatibleControlSnapshotV1.decode(payload);
                if (!expectedShard.equals(snapshot.shard().shardId())) {
                    throw new IllegalArgumentException("checkpoint control snapshot belongs to another shard");
                }
                if (!Bytes.constantTimeEquals(snapshot.snapshotDigest(), expectedDigest)) {
                    throw new IllegalArgumentException("checkpoint control snapshot digest does not match manifest");
                }
                if (expectedManifest != null) {
                    final byte[] checkpointId = requiredPayload(db, meta, 7, "checkpoint identity");
                    if (!Bytes.constantTimeEquals(checkpointId, expectedManifest.checkpointId())) {
                        throw new IllegalArgumentException("checkpoint identity does not match manifest");
                    }
                    final byte[] sourceBytes = requiredPayload(db, meta, 3, "applied source position");
                    final SourcePosition sourcePosition;
                    try {
                        sourcePosition = SourcePositionCodec.decode(sourceBytes);
                    } catch (IllegalArgumentException malformed) {
                        throw new IllegalArgumentException("checkpoint source position is malformed", malformed);
                    }
                    if (!Bytes.constantTimeEquals(sourcePosition.canonicalBytes(),
                            expectedManifest.appliedShardLogPosition().canonicalBytes())) {
                        throw new IllegalArgumentException("checkpoint source position does not match manifest");
                    }
                    final byte[] mutationBytes = requiredPayload(db, meta, 5, "mutation sequence");
                    if (mutationBytes.length != Long.BYTES
                            || Bytes.readU64be(mutationBytes, 0) != expectedManifest.shardMutationSequence()) {
                        throw new IllegalArgumentException("checkpoint mutation sequence does not match manifest");
                    }
                    final byte[] cursorBytes = requiredPayload(db, meta, 6, "evidence cursors");
                    if (!StoreRuntimeMetadata.decodeEvidenceCursors(cursorBytes)
                            .equals(expectedManifest.evidenceCursors())) {
                        throw new IllegalArgumentException("checkpoint evidence cursors do not match manifest");
                    }
                }
            } catch (RocksDBException failure) {
                throw new IllegalArgumentException("cannot open checkpoint RocksDB read-only", failure);
            }
        } finally {
            Throwable cleanupFailure = null;
            for (ColumnFamilyHandle handle : handles) {
                try {
                    handle.close();
                } catch (RuntimeException | Error failure) {
                    cleanupFailure = append(cleanupFailure, failure);
                }
            }
            for (ColumnFamilyOptions options : columnFamilyOptions) {
                try {
                    options.close();
                } catch (RuntimeException | Error failure) {
                    cleanupFailure = append(cleanupFailure, failure);
                }
            }
            if (cleanupFailure != null) {
                throwUnchecked(cleanupFailure);
            }
        }
    }

    private static byte[] requiredPayload(final RocksDB db, final ColumnFamilyHandle meta,
                                          final int keyKind, final String description) throws RocksDBException {
        final byte[] encoded = db.get(meta, KeyCodec.metaFixed(keyKind));
        if (encoded == null) {
            throw new IllegalArgumentException("checkpoint RocksDB is missing " + description);
        }
        return ValueEnvelope.decode(encoded, META_FIXED_VALUE_TYPE).payload();
    }

    private static boolean hasManifestFile(final Path checkpointDirectory) {
        if (Files.isSymbolicLink(checkpointDirectory)
                || !Files.isDirectory(checkpointDirectory, LinkOption.NOFOLLOW_LINKS)) {
            return false;
        }
        try (var paths = Files.list(checkpointDirectory)) {
            return paths.anyMatch(path -> !Files.isSymbolicLink(path)
                    && Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                    && path.getFileName().toString().startsWith("MANIFEST-"));
        } catch (IOException failure) {
            throw new IllegalArgumentException("cannot inspect checkpoint manifest files", failure);
        }
    }

    private static Throwable append(final Throwable current, final Throwable next) {
        if (current == null) {
            return next;
        }
        current.addSuppressed(next);
        return current;
    }

    private static void throwUnchecked(final Throwable failure) {
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error errorFailure) {
            throw errorFailure;
        }
        throw new IllegalStateException("unexpected checked teardown failure", failure);
    }
}
