package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaBookkeeping;
import com.nereusstream.delay.protocol.TargetQuotaCounter;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaTotal;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 * Read-only physical check of the format-2 root, bookkeeping and source frontier.
 *
 * <p>The optional quota and business-ledger scans establish only local projection consistency.
 * This does not authenticate the manifest control digest or make an image publishable or recoverable.</p>
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

    /** Maximum decoded accounting records and aggregate key/value bytes for a recovery-only scan. */
    public record QuotaAuditLimits(int maxRecords, long maxKeyValueBytes) {
        public QuotaAuditLimits {
            if (maxRecords <= 0
                    || maxRecords == Integer.MAX_VALUE
                    || maxKeyValueBytes <= 0
                    || maxKeyValueBytes == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Target quota audit requires finite positive limits");
            }
        }
    }

    /** Finite full-scan and dependency point-read limits for a recovery-only business ledger fold. */
    public record LedgerAuditLimits(int maxRecords, long maxKeyValueBytes, int maxPointReads, long maxPointReadBytes) {
        public LedgerAuditLimits {
            if (maxRecords <= 0
                    || maxRecords == Integer.MAX_VALUE
                    || maxKeyValueBytes <= 0
                    || maxKeyValueBytes == Long.MAX_VALUE
                    || maxPointReads <= 0
                    || maxPointReads == Integer.MAX_VALUE
                    || maxPointReadBytes <= 0
                    || maxPointReadBytes == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Target ledger audit requires finite positive limits");
            }
        }
    }

    /** Reads an immutable RocksDB image under finite physical limits without changing its markers. */
    public static RootProof validate(
            final Path image, final ShardId expectedShard, final CheckpointManifestLimits limits) {
        return validateImage(image, expectedShard, limits, null, null, null);
    }

    /** Audits the bounded quota projection graph; independent work ledgers still require a separate fold. */
    public static RootProof auditQuotaProjections(
            final Path image,
            final ShardId expectedShard,
            final CheckpointManifestLimits physicalLimits,
            final QuotaAuditLimits quotaLimits) {
        return validateImage(image, expectedShard, physicalLimits, null, Objects.requireNonNull(quotaLimits), null);
    }

    /** Rebuilds quota usage from bounded actual business records after the quota projection audit. */
    public static RootProof auditIndependentLedger(
            final Path image,
            final ShardId expectedShard,
            final CheckpointManifestLimits physicalLimits,
            final QuotaAuditLimits quotaLimits,
            final LedgerAuditLimits ledgerLimits) {
        return validateImage(
                image,
                expectedShard,
                physicalLimits,
                null,
                Objects.requireNonNull(quotaLimits, "quotaLimits"),
                Objects.requireNonNull(ledgerLimits, "ledgerLimits"));
    }

    /**
     * Binds the physical files and fixed image identity to a format-2 manifest. The manifest's
     * control-state and semantic-version digests still require separate authenticated validation.
     */
    public static RootProof validateManifestImageIdentity(
            final Path image, final CheckpointManifest manifest, final CheckpointManifestLimits limits) {
        requireFormat2Manifest(manifest, limits);
        return validateImage(image, manifest.shardId(), limits, manifest, null, null);
    }

    /** Binds Manifest identity and both local audits in one immutable image read. */
    public static RootProof auditManifestImageLedger(
            final Path image,
            final CheckpointManifest manifest,
            final CheckpointManifestLimits physicalLimits,
            final QuotaAuditLimits quotaLimits,
            final LedgerAuditLimits ledgerLimits) {
        requireFormat2Manifest(manifest, physicalLimits);
        return validateImage(
                image,
                manifest.shardId(),
                physicalLimits,
                manifest,
                Objects.requireNonNull(quotaLimits, "quotaLimits"),
                Objects.requireNonNull(ledgerLimits, "ledgerLimits"));
    }

    private static void requireFormat2Manifest(
            final CheckpointManifest manifest, final CheckpointManifestLimits limits) {
        Objects.requireNonNull(manifest, "manifest");
        if (manifest.storeFormatVersion() != 2) {
            throw new IllegalArgumentException("Target image binding requires a format-2 manifest");
        }
        manifest.validateLimits(Objects.requireNonNull(limits, "limits"));
    }

    private static RootProof validateImage(
            final Path image,
            final ShardId expectedShard,
            final CheckpointManifestLimits limits,
            final CheckpointManifest manifest,
            final QuotaAuditLimits quotaLimits,
            final LedgerAuditLimits ledgerLimits) {
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
        if (manifest != null) {
            if (files.size() != manifest.files().size()) {
                throw new IllegalArgumentException("Target image file count does not match manifest");
            }
            for (int index = 0; index < files.size(); index++) {
                final var actual = files.get(index);
                final var declared = manifest.files().get(index);
                if (!actual.name().equals(declared.name())
                        || actual.length() != declared.length()
                        || !Bytes.constantTimeEquals(actual.checksum(), declared.checksum())) {
                    throw new IllegalArgumentException("Target image file inventory does not match manifest");
                }
            }
        }
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
        final List<String> familyNames = names.stream()
                .map(name -> new String(name, StandardCharsets.UTF_8))
                .toList();
        final Set<String> expectedFamilies = new HashSet<>();
        expectedFamilies.add(new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8));
        for (ColumnFamily family : ColumnFamily.values()) {
            expectedFamilies.add(family.rocksName());
        }
        final Set<String> actualFamilies = new HashSet<>();
        actualFamilies.addAll(familyNames);
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
                final int metaIndex = familyNames.indexOf(ColumnFamily.META.rocksName());
                final ColumnFamilyHandle meta = handles.get(metaIndex);
                final RootProof proof = readRoot(db, meta, expectedShard);
                if (manifest != null) {
                    requireManifestImageIdentity(db, meta, proof, manifest);
                }
                if (quotaLimits != null) {
                    auditQuotaProjections(db, meta, proof, quotaLimits);
                }
                if (ledgerLimits != null) {
                    final Map<ColumnFamily, ColumnFamilyHandle> familyHandles = new EnumMap<>(ColumnFamily.class);
                    for (ColumnFamily family : ColumnFamily.values()) {
                        familyHandles.put(family, handles.get(familyNames.indexOf(family.rocksName())));
                    }
                    final ColumnFamilyHandle defaultHandle = handles.get(
                            familyNames.indexOf(new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8)));
                    TargetCheckpointLedgerAudit.audit(db, familyHandles, defaultHandle, proof, ledgerLimits);
                }
                return proof;
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

    private static void auditQuotaProjections(
            final RocksDB db, final ColumnFamilyHandle meta, final RootProof proof, final QuotaAuditLimits limits)
            throws RocksDBException {
        final var budget = new QuotaAuditBudget(limits);
        final ShardId shard = proof.metadata().shardId();
        final byte[] tenant = proof.bookkeeping().tenantScope();
        final List<TargetQuotaCounter> counters = scanQuota(
                db,
                meta,
                TargetKeyCodec.QUOTA_COUNTER_TAG,
                TargetQuotaCounter.VALUE_TYPE,
                budget,
                (key, payload) -> TargetQuotaCounter.decodeForStore(key, payload, shard));
        final List<TargetQuotaTotal> totals = scanQuota(
                db,
                meta,
                TargetKeyCodec.QUOTA_TOTAL_TAG,
                TargetQuotaTotal.VALUE_TYPE,
                budget,
                (key, payload) -> TargetQuotaTotal.decodeForStore(key, payload, shard, tenant));
        final List<TargetQuotaGrantActivation> grants = scanQuota(
                db,
                meta,
                TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG,
                TargetQuotaGrantActivation.VALUE_TYPE,
                budget,
                (key, payload) -> TargetQuotaGrantActivation.decodeForStore(key, payload, shard, tenant));
        proof.bookkeeping().auditInventory(proof.aggregate(), counters, totals, grants);
        final Map<TargetQuotaIdentity, TargetQuotaCounter> byIdentity = new HashMap<>();
        final Map<TargetPartitionId, TargetQuotaUsage> targetUsage = new HashMap<>();
        TargetQuotaUsage shardUsage = TargetQuotaUsage.empty();
        for (TargetQuotaCounter counter : counters) {
            proof.aggregate().requireCounter(counter);
            if (byIdentity.put(counter.identity(), counter) != null) {
                throw new IllegalStateException("duplicate Target quota counter identity");
            }
            if (!counter.identity().kind().isMirror()) {
                shardUsage = shardUsage.add(counter.usage());
                if (counter.identity().kind().hasTarget()) {
                    targetUsage.merge(counter.identity().target(), counter.usage(), TargetQuotaUsage::add);
                }
            }
        }
        if (!shardUsage.equals(proof.aggregate().usage())) {
            throw new IllegalStateException("Target quota aggregate differs from primary counters");
        }
        for (TargetQuotaCounter counter : counters) {
            if (counter.identity().kind().isMirror()) {
                if (!byIdentity.containsKey(counter.identity().primary())) {
                    throw new IllegalStateException("Target quota tenant mirror has no primary counter");
                }
                continue;
            }
            final TargetQuotaIdentity primary = counter.identity();
            final TargetQuotaIdentity mirror = new TargetQuotaIdentity(
                    primary.kind().hasTarget()
                            ? TargetQuotaIdentity.Kind.TENANT_TARGET
                            : TargetQuotaIdentity.Kind.TENANT_SHARD,
                    shard,
                    primary.accountingIncarnation(),
                    primary.target(),
                    tenant);
            final TargetQuotaCounter actualMirror = byIdentity.get(mirror);
            if (actualMirror == null
                    || !counter.usage().resources().equals(actualMirror.usage().resources())) {
                throw new IllegalStateException("Target quota primary and tenant mirror disagree");
            }
        }
        final Map<TargetPartitionId, TargetQuotaTotal> byTarget = new HashMap<>();
        for (TargetQuotaTotal total : totals) {
            total.requireAggregate(proof.aggregate());
            if (byTarget.put(total.scope().target(), total) != null
                    || !total.usage()
                            .equals(targetUsage.getOrDefault(total.scope().target(), TargetQuotaUsage.empty()))) {
                throw new IllegalStateException("Target quota total differs from primary counters");
            }
        }
        for (TargetQuotaCounter counter : counters) {
            if (counter.identity().kind() == TargetQuotaIdentity.Kind.TARGET) {
                final TargetQuotaTotal total = byTarget.get(counter.identity().target());
                if (total == null) {
                    throw new IllegalStateException("Target quota primary counter has no total");
                }
                total.requireCounter(counter);
            }
        }
        if (!byTarget.keySet().containsAll(targetUsage.keySet())) {
            throw new IllegalStateException("Target quota primary counter has no total");
        }
        for (TargetQuotaGrantActivation grant : grants) {
            grant.mutation().requireAtOrBefore(proof.aggregate().mutation());
        }
    }

    private static <T> List<T> scanQuota(
            final RocksDB db,
            final ColumnFamilyHandle meta,
            final int tag,
            final int valueType,
            final QuotaAuditBudget budget,
            final BiFunction<byte[], byte[], T> decoder)
            throws RocksDBException {
        final List<T> records = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator(meta)) {
            iterator.seek(new byte[] {(byte) tag});
            while (iterator.isValid()) {
                final byte[] key = iterator.key();
                if (key.length == 0 || Byte.toUnsignedInt(key[0]) != tag) {
                    break;
                }
                final byte[] value = iterator.value();
                budget.charge(key.length, value.length);
                if (key.length < 2 || Byte.toUnsignedInt(key[1]) != TargetKeyCodec.KEY_FORMAT) {
                    throw new IllegalArgumentException("Target quota projection has another key format");
                }
                records.add(decoder.apply(
                        key, TargetValueEnvelope.decode(value, valueType).payload()));
                iterator.next();
            }
            iterator.status();
        }
        return records;
    }

    private static final class QuotaAuditBudget {
        private final QuotaAuditLimits limits;
        private int records;
        private long bytes;

        private QuotaAuditBudget(final QuotaAuditLimits limits) {
            this.limits = limits;
        }

        private void charge(final int keyBytes, final int valueBytes) {
            final long nextBytes = (long) keyBytes + valueBytes;
            if (records >= limits.maxRecords() || nextBytes > limits.maxKeyValueBytes() - bytes) {
                throw new IllegalArgumentException("Target quota audit budget exceeded");
            }
            records++;
            bytes += nextBytes;
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

    private static void requireManifestImageIdentity(
            final RocksDB db, final ColumnFamilyHandle meta, final RootProof proof, final CheckpointManifest manifest)
            throws RocksDBException {
        if (!Bytes.constantTimeEquals(proof.metadata().dbIdentity(), manifest.dbIdentity())
                || !proof.metadata().storeIncarnationUuid().equals(manifest.sourceStoreIncarnation())
                || !Bytes.constantTimeEquals(proof.root().recoveryLineage(), manifest.recoveryLineageId())
                || proof.mutationSequence() != manifest.shardMutationSequence()
                || !Bytes.constantTimeEquals(
                        proof.source().canonicalBytes(),
                        manifest.appliedShardLogPosition().canonicalBytes())) {
            throw new IllegalArgumentException("Target image Store/source identity does not match manifest");
        }
        final byte[] checkpointId = requiredFixed(db, meta, 7, "checkpoint identity");
        if (!Bytes.constantTimeEquals(checkpointId, manifest.checkpointId())) {
            throw new IllegalArgumentException("Target image checkpoint identity does not match manifest");
        }
        final byte[] ownerEpoch = requiredFixed(db, meta, 8, "opened Owner epoch");
        if (ownerEpoch.length != Long.BYTES
                || Bytes.readU64be(ownerEpoch, 0) != manifest.createdBy().ownerEpoch()) {
            throw new IllegalArgumentException("Target image Owner epoch does not match manifest");
        }
        final byte[] cursorBytes = requiredFixed(db, meta, 6, "evidence cursors");
        if (!StoreRuntimeMetadata.decodeEvidenceCursors(cursorBytes).equals(manifest.evidenceCursors())) {
            throw new IllegalArgumentException("Target image evidence cursors do not match manifest");
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
