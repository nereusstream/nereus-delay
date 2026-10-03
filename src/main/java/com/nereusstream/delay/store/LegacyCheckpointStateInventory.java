package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.runtime.MessageRecord;
import com.nereusstream.delay.runtime.MessageStatus;
import com.nereusstream.delay.runtime.RetiredMessageIdentityRecord;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/** Bounded, read-only record inventory for one already-identified legacy format-1 checkpoint. */
public final class LegacyCheckpointStateInventory {
    private static final int MESSAGE_KEY_TAG = 1;
    private static final int MESSAGE_KEY_FORMAT = 1;
    private static final int MESSAGE_VALUE_TYPE = 1;

    private LegacyCheckpointStateInventory() {}

    /**
     * Inventories every registered Column Family and decodes the old Message status projection.
     * This deliberately does not audit cross-record relationships, replay source bytes, or decide
     * whether the image can be converted or activated.
     */
    public static Inventory inspect(
            final Path image,
            final ShardId expectedShard,
            final CheckpointManifest manifest,
            final CheckpointManifestLimits fileLimits,
            final ReadLimits readLimits) {
        Objects.requireNonNull(readLimits, "readLimits");
        final LegacyCheckpointImageInspector.ImageProof before =
                LegacyCheckpointImageInspector.inspect(image, expectedShard, manifest, fileLimits);
        final Inventory inventory = scan(image, before, readLimits);
        // Re-validate the manifest-bound image after the logical read to detect concurrent changes.
        final LegacyCheckpointImageInspector.ImageProof after =
                LegacyCheckpointImageInspector.inspect(image, expectedShard, manifest, fileLimits);
        if (!samePhysicalProof(before, after)) {
            throw new IllegalArgumentException("legacy checkpoint changed during state inventory");
        }
        return inventory;
    }

    private static Inventory scan(
            final Path image,
            final LegacyCheckpointImageInspector.ImageProof proof,
            final ReadLimits limits) {
        RocksDB.loadLibrary();
        final BoundedReadBudget budget = new BoundedReadBudget(
                limits.maxRecords(), limits.maxBytes(), limits.maxElapsedNanos(), System::nanoTime);
        final EnumMap<ColumnFamily, Long> recordsByFamily = new EnumMap<>(ColumnFamily.class);
        for (ColumnFamily family : ColumnFamily.values()) {
            recordsByFamily.put(family, 0L);
        }
        final TreeMap<String, Long> keyKinds = new TreeMap<>();
        final EnumMap<MessageStatus, Long> messageStatuses = new EnumMap<>(MessageStatus.class);
        for (MessageStatus status : MessageStatus.values()) {
            messageStatuses.put(status, 0L);
        }
        long retiredMessageIdentities = 0;
        long totalBytes = 0;
        try (Options listOptions = new Options()) {
            final List<byte[]> familyNames = RocksDB.listColumnFamilies(listOptions, image.toString());
            LegacyCheckpointImageInspector.requireExactColumnFamilies(familyNames);
            final List<ColumnFamilyOptions> familyOptions = new ArrayList<>();
            final List<ColumnFamilyHandle> handles = new ArrayList<>();
            DBOptions dbOptions = null;
            RocksDB db = null;
            try {
                final List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
                for (byte[] name : familyNames) {
                    final ColumnFamilyOptions options = new ColumnFamilyOptions();
                    familyOptions.add(options);
                    descriptors.add(new ColumnFamilyDescriptor(name, options));
                }
                dbOptions = new DBOptions().setCreateIfMissing(false).setCreateMissingColumnFamilies(false);
                db = RocksDB.openReadOnly(dbOptions, image.toString(), descriptors, handles);
                final Map<String, ColumnFamilyHandle> byName =
                        LegacyCheckpointImageInspector.indexHandles(familyNames, handles);
                for (ColumnFamily family : ColumnFamily.values()) {
                    final ColumnFamilyHandle handle = byName.get(family.rocksName());
                    try (RocksIterator iterator = db.newIterator(
                            Objects.requireNonNull(handle, "columnFamilyHandle"))) {
                        iterator.seekToFirst();
                        while (iterator.isValid()) {
                            if (!budget.beforeRead()) {
                                throw budget.incomplete();
                            }
                            final byte[] key = iterator.key();
                            final byte[] value = iterator.value();
                            if (!budget.tryCharge(key.length, value.length)) {
                                throw budget.incomplete();
                            }
                            recordsByFamily.compute(family, (ignored, count) -> Math.addExact(count, 1));
                            totalBytes = Math.addExact(totalBytes, (long) key.length + value.length);
                            final String keyKind = keyKind(family, key);
                            keyKinds.merge(keyKind, 1L, Math::addExact);
                            if (family == ColumnFamily.ID && isMessageKey(key)) {
                                final DelayMessageId messageId = messageId(key);
                                if (!messageId.routingId().shardId().equals(proof.metadata().shardId())) {
                                    throw new IllegalArgumentException("legacy Message key belongs to another Shard");
                                }
                                final byte[] payload = ValueEnvelope.decode(value, MESSAGE_VALUE_TYPE).payload();
                                if (RetiredMessageIdentityRecord.isEncoded(payload)) {
                                    final RetiredMessageIdentityRecord retired =
                                            RetiredMessageIdentityRecord.decode(payload);
                                    if (!messageId.equals(retired.messageId())) {
                                        throw new IllegalArgumentException(
                                                "legacy retired Message value identity differs from its key");
                                    }
                                    retiredMessageIdentities = Math.addExact(retiredMessageIdentities, 1);
                                } else {
                                    final MessageRecord message = MessageRecord.decode(payload);
                                    if (!SourcePositionCodec.decode(message.scheduleSourcePosition())
                                            .shardId()
                                            .equals(proof.metadata().shardId())) {
                                        throw new IllegalArgumentException(
                                                "legacy Message source position belongs to another Shard");
                                    }
                                    messageStatuses.compute(
                                            message.status(), (ignored, count) -> Math.addExact(count, 1));
                                }
                            }
                            iterator.next();
                        }
                        iterator.status();
                    }
                }
                if (!budget.beforeTimedWork()) {
                    throw budget.incomplete();
                }
            } catch (RocksDBException failure) {
                throw new IllegalArgumentException("cannot inventory legacy checkpoint records read-only", failure);
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
        return new Inventory(
                proof,
                recordsByFamily,
                keyKinds,
                messageStatuses,
                retiredMessageIdentities,
                budget.actualRecords(),
                totalBytes,
                budget.chargedBytes());
    }

    private static boolean isMessageKey(final byte[] key) {
        if (key.length < 2
                || Byte.toUnsignedInt(key[0]) != MESSAGE_KEY_TAG
                || Byte.toUnsignedInt(key[1]) != MESSAGE_KEY_FORMAT) {
            return false;
        }
        if (key.length != 2 + DelayMessageId.LENGTH) {
            throw new IllegalArgumentException("legacy Message key has an invalid length");
        }
        messageId(key);
        return true;
    }

    private static DelayMessageId messageId(final byte[] key) {
        return new DelayMessageId(java.util.Arrays.copyOfRange(key, 2, key.length));
    }

    private static String keyKind(final ColumnFamily family, final byte[] key) {
        final String tag = key.length == 0 ? "none" : String.format("%02x", Byte.toUnsignedInt(key[0]));
        final String format = key.length < 2 ? "none" : String.format("%02x", Byte.toUnsignedInt(key[1]));
        return family.rocksName() + ":" + tag + ":" + format;
    }

    private static boolean samePhysicalProof(
            final LegacyCheckpointImageInspector.ImageProof left,
            final LegacyCheckpointImageInspector.ImageProof right) {
        if (!left.metadata().shardId().equals(right.metadata().shardId())
                || !Bytes.constantTimeEquals(left.metadata().dbIdentity(), right.metadata().dbIdentity())
                || !left.metadata().storeIncarnationUuid().equals(right.metadata().storeIncarnationUuid())
                || !Bytes.constantTimeEquals(left.checkpointId(), right.checkpointId())
                || !Bytes.constantTimeEquals(
                        left.appliedSourcePosition().canonicalBytes(), right.appliedSourcePosition().canonicalBytes())
                || left.mutationSequence() != right.mutationSequence()
                || left.physicalBytes() != right.physicalBytes()
                || left.files().size() != right.files().size()) {
            return false;
        }
        for (int index = 0; index < left.files().size(); index++) {
            final CheckpointFileInventory leftFile = left.files().get(index);
            final CheckpointFileInventory rightFile = right.files().get(index);
            if (!leftFile.name().equals(rightFile.name())
                    || leftFile.length() != rightFile.length()
                    || !Bytes.constantTimeEquals(leftFile.checksum(), rightFile.checksum())) {
                return false;
            }
        }
        return true;
    }

    public record ReadLimits(int maxRecords, long maxBytes, long maxElapsedNanos) {
        public ReadLimits {
            if (maxRecords <= 0 || maxBytes <= 0 || maxElapsedNanos <= 0) {
                throw new IllegalArgumentException("legacy checkpoint read limits must be finite and positive");
            }
        }
    }

    public record Inventory(
            LegacyCheckpointImageInspector.ImageProof physicalProof,
            Map<ColumnFamily, Long> recordsByFamily,
            Map<String, Long> keyKinds,
            Map<MessageStatus, Long> messageStatuses,
            long retiredMessageIdentities,
            long scannedRecords,
            long scannedBytes,
            long chargedBytes) {
        public Inventory {
            Objects.requireNonNull(physicalProof, "physicalProof");
            final EnumMap<ColumnFamily, Long> familyCopy = new EnumMap<>(ColumnFamily.class);
            familyCopy.putAll(Objects.requireNonNull(recordsByFamily, "recordsByFamily"));
            recordsByFamily = Collections.unmodifiableMap(familyCopy);
            keyKinds = Collections.unmodifiableMap(new TreeMap<>(Objects.requireNonNull(keyKinds, "keyKinds")));
            final EnumMap<MessageStatus, Long> statusCopy = new EnumMap<>(MessageStatus.class);
            statusCopy.putAll(Objects.requireNonNull(messageStatuses, "messageStatuses"));
            messageStatuses = Collections.unmodifiableMap(statusCopy);
            if (retiredMessageIdentities < 0 || scannedRecords < 0 || scannedBytes < 0 || chargedBytes < 0) {
                throw new IllegalArgumentException("legacy checkpoint inventory has a negative count");
            }
        }
    }
}
