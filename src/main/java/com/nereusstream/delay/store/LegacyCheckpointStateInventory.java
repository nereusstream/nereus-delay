package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.runtime.MessageRecord;
import com.nereusstream.delay.runtime.MessageStatus;
import com.nereusstream.delay.runtime.RetiredMessageIdentityRecord;
import com.nereusstream.delay.runtime.TimelineEntry;
import com.nereusstream.delay.runtime.TimelineWorkRef;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
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
        final EnumMap<MessageDisposition, Long> messageDispositions = new EnumMap<>(MessageDisposition.class);
        for (MessageDisposition disposition : MessageDisposition.values()) {
            messageDispositions.put(disposition, 0L);
        }
        long retiredMessageIdentities = 0;
        long scannedRecords = 0;
        long totalBytes = 0;
        final List<Conflict> conflicts = new ArrayList<>();
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
                final ColumnFamilyHandle timeline = byName.get(ColumnFamily.TIMELINE.rocksName());
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
                            scannedRecords = Math.addExact(scannedRecords, 1);
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
                                    final SourcePosition schedulePosition =
                                            SourcePositionCodec.decode(message.scheduleSourcePosition());
                                    if (!schedulePosition.shardId().equals(proof.metadata().shardId())) {
                                        throw new IllegalArgumentException(
                                                "legacy Message source position belongs to another Shard");
                                    }
                                    messageStatuses.compute(
                                            message.status(), (ignored, count) -> Math.addExact(count, 1));
                                    final MessageDisposition disposition = dispositionFor(message.status());
                                    messageDispositions.compute(
                                            disposition, (ignored, count) -> Math.addExact(count, 1));
                                    final byte[] oldMessageKey = KeyCodec.idMessage(messageId);
                                    if (message.status() == MessageStatus.SCHEDULED) {
                                        auditScheduledIndexes(
                                                db,
                                                timeline,
                                                messageId,
                                                message,
                                                schedulePosition,
                                                budget,
                                                conflicts);
                                    } else {
                                        final ConflictReason blocker = blockerFor(message.status());
                                        if (blocker != null) {
                                            conflicts.add(conflict(oldMessageKey, schedulePosition, blocker));
                                        }
                                    }
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
                messageDispositions,
                retiredMessageIdentities,
                scannedRecords,
                totalBytes,
                budget.chargedBytes(),
                conflicts);
    }

    private static void auditScheduledIndexes(
            final RocksDB db,
            final ColumnFamilyHandle timeline,
            final DelayMessageId messageId,
            final MessageRecord message,
            final SourcePosition schedulePosition,
            final BoundedReadBudget budget,
            final List<Conflict> conflicts)
            throws RocksDBException {
        final TimelineWorkRef work = message.runtimeIndex().timeline();
        final byte[] messageKey = KeyCodec.idMessage(messageId);
        if (work == null) {
            conflicts.add(conflict(
                    messageKey,
                    schedulePosition,
                    ConflictReason.SCHEDULED_WORK_REFERENCE_MISSING));
            return;
        }
        final boolean ordered = message.orderingMode() == OrderingMode.DELIVERY_TIME_FIFO;
        if (work.orderedHeadBlocking() != ordered
                || work.retryEligibilityAtEpochMs() != message.retryEligibilityAtEpochMs()
                || work.actionAtEpochMs() > message.deliverAtEpochMs()) {
            conflicts.add(conflict(
                    messageKey,
                    schedulePosition,
                    ConflictReason.SCHEDULED_WORK_FIELDS_DISAGREE));
            return;
        }
        final long eligibleAt = ordered
                ? message.deliverAtEpochMs()
                : Math.max(work.actionAtEpochMs(), work.retryEligibilityAtEpochMs());
        final byte[] expectedTimelineKey = ordered
                ? KeyCodec.timelineOrdered(
                        message.laneId(),
                        eligibleAt,
                        schedulePosition.sourceOrderToken(),
                        messageId,
                        message.generation())
                : KeyCodec.timelineDue(
                        message.laneId(),
                        eligibleAt,
                        schedulePosition.sourceOrderToken(),
                        messageId,
                        message.generation());
        if (!Arrays.equals(expectedTimelineKey, work.encodedTimelineKey())) {
            conflicts.add(conflict(
                    messageKey,
                    schedulePosition,
                    ConflictReason.SCHEDULED_WORK_KEY_DISAGREES_WITH_MESSAGE));
            return;
        }
        final byte[] timelineValue = readPoint(db, timeline, expectedTimelineKey, budget);
        if (timelineValue == null) {
            conflicts.add(conflict(messageKey, schedulePosition, ConflictReason.DUE_INDEX_MISSING));
        } else if (!matchesTimelineValue(timelineValue, messageId, message, work)) {
            conflicts.add(conflict(messageKey, schedulePosition, ConflictReason.DUE_INDEX_VALUE_MISMATCH));
        }
        final byte[] expiryKey = KeyCodec.timelineExpiry(
                message.expireAtEpochMs(), message.laneId(), messageId, message.generation());
        final byte[] expiryValue = readPoint(db, timeline, expiryKey, budget);
        if (expiryValue == null) {
            conflicts.add(conflict(messageKey, schedulePosition, ConflictReason.EXPIRY_INDEX_MISSING));
        } else if (!matchesTimelineValue(expiryValue, messageId, message, work)) {
            conflicts.add(conflict(messageKey, schedulePosition, ConflictReason.EXPIRY_INDEX_VALUE_MISMATCH));
        }
    }

    private static byte[] readPoint(
            final RocksDB db, final ColumnFamilyHandle family, final byte[] key, final BoundedReadBudget budget)
            throws RocksDBException {
        if (!budget.beforeRead()) {
            throw budget.incomplete();
        }
        final byte[] value = db.get(family, key);
        if (!budget.tryCharge(key.length, value == null ? 0 : value.length)) {
            throw budget.incomplete();
        }
        return value;
    }

    private static boolean matchesTimelineValue(
            final byte[] encoded,
            final DelayMessageId messageId,
            final MessageRecord message,
            final TimelineWorkRef expectedWork) {
        try {
            final byte[] payload = ValueEnvelope.decode(encoded, 1).payload();
            if (isLegacyTimelineEntry(payload)) {
                final TimelineEntry entry = TimelineEntry.decode(payload);
                return entry.messageId().equals(messageId) && entry.generation() == message.generation();
            }
            final TimelineWorkRef work = TimelineWorkRef.decode(payload);
            return Arrays.equals(work.canonicalBytes(), expectedWork.canonicalBytes());
        } catch (IllegalArgumentException malformed) {
            return false;
        }
    }

    private static boolean isLegacyTimelineEntry(final byte[] encoded) {
        return encoded.length >= Integer.BYTES
                && java.nio.ByteBuffer.wrap(encoded, 0, Integer.BYTES).getInt() == 1;
    }

    private static Conflict conflict(
            final byte[] messageKey,
            final SourcePosition sourcePosition,
            final ConflictReason reason) {
        return new Conflict(Bytes.sha256(messageKey), "MESSAGE", sourcePosition.canonicalBytes(), reason);
    }

    public static MessageDisposition dispositionFor(final MessageStatus status) {
        return switch (Objects.requireNonNull(status, "status")) {
            case SCHEDULED -> MessageDisposition.CANDIDATE_PENDING_FULL_AUDIT;
            case CLAIMED -> MessageDisposition.BLOCKED_PENDING_CLAIM_RECONCILIATION;
            case PUBLISHING, UNCERTAIN -> MessageDisposition.PRESERVE_OLD_SEND_RECOVERY;
            case HANDED_OFF -> MessageDisposition.PRESERVE_BROKER_RESPONSIBILITY;
            case CANCELED, SUPERSEDED, PUBLISHED, EXPIRED, DEAD_LETTER ->
                    MessageDisposition.PRESERVE_TERMINAL_AND_REFERENCES;
        };
    }

    private static ConflictReason blockerFor(final MessageStatus status) {
        return switch (status) {
            case SCHEDULED -> null;
            case CLAIMED -> ConflictReason.CLAIM_REQUIRES_SOURCE_CUT_RECONCILIATION;
            case PUBLISHING, UNCERTAIN -> ConflictReason.UNRESOLVED_SEND_REQUIRES_OLD_RECOVERY;
            case HANDED_OFF -> ConflictReason.HANDOFF_REQUIRES_OLD_BROKER_RESPONSIBILITY;
            case CANCELED, SUPERSEDED, PUBLISHED, EXPIRED, DEAD_LETTER ->
                    ConflictReason.TERMINAL_REQUIRES_FLOOR_AND_REFERENCE_CLOSURE;
        };
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
            Map<MessageDisposition, Long> messageDispositions,
            long retiredMessageIdentities,
            long scannedRecords,
            long scannedBytes,
            long chargedBytes,
            List<Conflict> conflicts) {
        public Inventory {
            Objects.requireNonNull(physicalProof, "physicalProof");
            final EnumMap<ColumnFamily, Long> familyCopy = new EnumMap<>(ColumnFamily.class);
            familyCopy.putAll(Objects.requireNonNull(recordsByFamily, "recordsByFamily"));
            recordsByFamily = Collections.unmodifiableMap(familyCopy);
            keyKinds = Collections.unmodifiableMap(new TreeMap<>(Objects.requireNonNull(keyKinds, "keyKinds")));
            final EnumMap<MessageStatus, Long> statusCopy = new EnumMap<>(MessageStatus.class);
            statusCopy.putAll(Objects.requireNonNull(messageStatuses, "messageStatuses"));
            messageStatuses = Collections.unmodifiableMap(statusCopy);
            final EnumMap<MessageDisposition, Long> dispositionCopy = new EnumMap<>(MessageDisposition.class);
            dispositionCopy.putAll(Objects.requireNonNull(messageDispositions, "messageDispositions"));
            messageDispositions = Collections.unmodifiableMap(dispositionCopy);
            conflicts = List.copyOf(Objects.requireNonNull(conflicts, "conflicts"));
            if (retiredMessageIdentities < 0 || scannedRecords < 0 || scannedBytes < 0 || chargedBytes < 0) {
                throw new IllegalArgumentException("legacy checkpoint inventory has a negative count");
            }
        }
    }

    public enum MessageDisposition {
        CANDIDATE_PENDING_FULL_AUDIT,
        BLOCKED_PENDING_CLAIM_RECONCILIATION,
        PRESERVE_OLD_SEND_RECOVERY,
        PRESERVE_BROKER_RESPONSIBILITY,
        PRESERVE_TERMINAL_AND_REFERENCES
    }

    public enum ConflictReason {
        SCHEDULED_WORK_REFERENCE_MISSING,
        SCHEDULED_WORK_FIELDS_DISAGREE,
        SCHEDULED_WORK_KEY_DISAGREES_WITH_MESSAGE,
        DUE_INDEX_MISSING,
        DUE_INDEX_VALUE_MISMATCH,
        EXPIRY_INDEX_MISSING,
        EXPIRY_INDEX_VALUE_MISMATCH,
        CLAIM_REQUIRES_SOURCE_CUT_RECONCILIATION,
        UNRESOLVED_SEND_REQUIRES_OLD_RECOVERY,
        HANDOFF_REQUIRES_OLD_BROKER_RESPONSIBILITY,
        TERMINAL_REQUIRES_FLOOR_AND_REFERENCE_CLOSURE
    }

    public record Conflict(
            byte[] oldKeyDigest, String recordKind, byte[] sourcePosition, ConflictReason reason) {
        public Conflict {
            Bytes.requireLength(oldKeyDigest, 32, "oldKeyDigest");
            oldKeyDigest = Bytes.copy(oldKeyDigest);
            if (!"MESSAGE".equals(Objects.requireNonNull(recordKind, "recordKind"))) {
                throw new IllegalArgumentException("legacy index conflict record kind is not registered");
            }
            sourcePosition = SourcePositionCodec.decode(sourcePosition).canonicalBytes();
            Objects.requireNonNull(reason, "reason");
        }

        @Override
        public byte[] oldKeyDigest() {
            return Bytes.copy(oldKeyDigest);
        }

        @Override
        public byte[] sourcePosition() {
            return Bytes.copy(sourcePosition);
        }
    }
}
