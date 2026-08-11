package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import io.nereusstream.delay.protocol.ShardId;
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

/**
 * Read-only validation of the shard-bound control snapshot inside a physical
 * checkpoint image.  A directory without a RocksDB MANIFEST is retained as a
 * legacy embedded fixture seam; a recognized RocksDB image is never allowed
 * to publish a mismatched key-10 digest.
 */
final class CheckpointControlSnapshotVerifier {
    private static final int META_CONTROL_SNAPSHOT = 10;
    private static final int META_FIXED_VALUE_TYPE = 1;

    private CheckpointControlSnapshotVerifier() {
    }

    static void validateIfPresent(final Path checkpointDirectory, final ShardId expectedShard,
                                  final byte[] expectedDigest) {
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
                final byte[] encoded = db.get(handles.get(metaIndex), KeyCodec.metaFixed(META_CONTROL_SNAPSHOT));
                if (encoded == null) {
                    return;
                }
                final byte[] payload = ValueEnvelope.decode(encoded, META_FIXED_VALUE_TYPE).payload();
                final CompatibleControlSnapshotV1 snapshot = CompatibleControlSnapshotV1.decode(payload);
                if (!expectedShard.equals(snapshot.shard().shardId())) {
                    throw new IllegalArgumentException("checkpoint control snapshot belongs to another shard");
                }
                if (!Bytes.constantTimeEquals(snapshot.snapshotDigest(), expectedDigest)) {
                    throw new IllegalArgumentException("checkpoint control snapshot digest does not match manifest");
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
