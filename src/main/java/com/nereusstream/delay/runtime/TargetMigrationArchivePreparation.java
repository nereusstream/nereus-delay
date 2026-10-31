package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.ownership.SourceReplayRecord;
import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Paid, finite preparation of the old logical namespace; only the Source archive writer materializes it. */
public final class TargetMigrationArchivePreparation {
    private static final byte[] PACKET_DOMAIN = Bytes.utf8("nereus-delay-migration-archive-packet-v1\0");
    private static final byte[] RANGE_DOMAIN = Bytes.utf8("nereus-delay-migration-archive-range-v1\0");
    private static final byte[] ARCHIVE_DOMAIN = Bytes.utf8("nereus-delay-migration-logical-first-archive-v1\0");
    private static final int LEGACY_PACKET_SCHEMA = 1;
    private static final int FULL_SOURCE_PACKET_SCHEMA = 2;
    private TargetMigrationArchivePreparation() {}

    /** Original private Compatibility opening, including its actual Source admission and child-loan protection. */
    public interface Held extends AutoCloseable {
        TargetMigrationRetirementStore.AppliedIntent intent();
        ShardStore.LegacyCheckpointReplayCopy shadow();
        ShardStore target();
        LegacyMigrationBundle.Sealed sealed();
        LegacyMigrationBundle.Held original();
        LegacyMigrationBundle.ImmutableObjects objects();
        Limits limits();
        BoundedReadBudget reads();
        TargetMigrationArchiveClosedContract.Materials closedMaterials();
        void requireCurrent();
        void requireLocalCurrent();
        long retainedBytes();
        @Override void close();
    }

    public record Limits(int recordsPerTurn, int maximumLogicalRows, int maximumPacketBytes,
            long maximumRetainedBytes, LegacyMigrationBundle.Limits companions) {
        public Limits {
            Objects.requireNonNull(companions);
            if (recordsPerTurn <= 0 || recordsPerTurn == Integer.MAX_VALUE || maximumLogicalRows <= 0
                    || maximumLogicalRows == Integer.MAX_VALUE || maximumPacketBytes <= 0
                    || maximumPacketBytes == Integer.MAX_VALUE
                    || maximumRetainedBytes <= 0 || maximumRetainedBytes == Long.MAX_VALUE)
                throw new IllegalArgumentException("archive preparation requires its actual finite cumulative envelope");
        }
    }

    public static Cursor begin(Held held, Limits limits, BoundedReadBudget cumulative) {
        Objects.requireNonNull(held); Objects.requireNonNull(limits); Objects.requireNonNull(cumulative);
        held.requireCurrent();
        if (limits.maximumRetainedBytes() > held.retainedBytes()
                || cumulative.maxBytes() == Long.MAX_VALUE || cumulative.maxRecords() == Integer.MAX_VALUE)
            throw new IllegalArgumentException("archive requires the original paid cumulative read/retained allocation");
        return new Cursor(held, limits, cumulative);
    }

    /** One fixed companion chunk or one live range page per turn, all at the same two protected cuts. */
    public static final class Cursor {
        private final Held held;
        private final Limits limits;
        private final BoundedReadBudget cumulative;
        private final ShardStore.ReadView oldView, targetView;
        private final TargetQuotaMutation cut;
        private final TargetMigrationPlan plan;
        private final LegacyMigrationBundle.Companion baseline;
        private final byte[] baselineManifest;
        private final NavigableMap<String, Raw> original = new TreeMap<>(), shadowRows = new TreeMap<>(), prefixRows = new TreeMap<>();
        private final List<TargetMigrationPrefixRecord> prefixes = new ArrayList<>();
        private final Map<Long, TargetQuotaMutation> steps = new HashMap<>();
        private final List<TargetMigrationOrigin> origins = new ArrayList<>();
        private final Map<String, LegacyCheckpointStateInventory.RawEntry> originalReferences = new HashMap<>();
        private byte[] lower, sourceDigest = Bytes.sha256(PACKET_DOMAIN);
        private int phase, baselineChunk, prefixIndex, prefixChunk;
        private long retained;
        private LegacyMigrationBundle.Companion prefixCompanion;
        private byte[] prefixManifest, prefixHeader;
        private List<byte[]> prefixHeaderRecords;
        private boolean complete;
        private Cursor(Held held, Limits limits, BoundedReadBudget cumulative) {
            this.held = held; this.limits = limits; this.cumulative = cumulative;
            plan = held.intent().migration().plan();
            TargetMigrationStore.requireIdentity(held.target().metadata(), plan);
            if (!Arrays.equals(held.shadow().store().metadata().storeIncarnation(), plan.oldStore())
                    || !Arrays.equals(held.shadow().store().metadata().dbIdentity(), plan.oldDbIdentity()))
                throw new IllegalArgumentException("archive changed its original protected shadow");
            final var target = held.target().readWithBudget(cumulative, () -> {
                held.requireLocalCurrent();
                final var root = Objects.requireNonNull(TargetMigrationStore.decodeRoot(held.target().get(ColumnFamily.META,
                        TargetMigrationRoot.keyFor(plan.scope().shard()))));
                if (!root.equals(held.intent().migration())) throw new IllegalStateException("archive changed actual Source A");
                final var aggregate = TargetQuotaAggregate.decode(TargetValueEnvelope.decode(held.target().get(ColumnFamily.META,
                        TargetQuotaAggregate.genesis(plan.scope().shard(), root.owner().identity().accountingIncarnation()).key()),
                        TargetQuotaAggregate.VALUE_TYPE).payload());
                final TargetRecordAccounting.View view = nativeView(held.target());
                final var retirement = Objects.requireNonNull(TargetMigrationRetirementStore.selected(view, List.of(), root, aggregate.mutation()));
                if (retirement.retired() || !retirement.intentAt().equals(held.intent().intentAt()))
                    throw new IllegalStateException("archive requires its actual unconfirmed retirement intent");
                return TargetMigrationStore.actualFrontier(held.target(), root);
            });
            cut = target.value(); targetView = target.view();
            final var old = held.shadow().store().readWithBudget(cumulative, () -> {
                if (!Arrays.equals(held.shadow().store().appliedShardLogPosition().canonicalBytes(), cut.source().canonicalBytes())
                        || held.shadow().store().shardMutationSequence() != cut.sequence())
                    throw new IllegalStateException("archive must capture a closed original shadow/Target Source step");
                final byte[] progress = held.shadow().store().get(ColumnFamily.META, TargetMigrationShadowProgress.keyFor(plan.scope().shard()));
                if (progress != null) TargetMigrationShadowProgress.decode(TargetValueEnvelope.decode(progress,
                        TargetMigrationShadowProgress.VALUE_TYPE).payload()).requirePlan(plan);
                return Boolean.TRUE;
            });
            oldView = old.view();
            baseline = LegacyMigrationBundle.readCompanionIndex(held.sealed().custody().manifest(), LegacyMigrationBundle.Section.CUSTODY,
                    held.sealed().material(), held.original(), limits.companions(), held.objects());
            baselineManifest = exact(baseline.manifest());
        }
        public boolean complete() { return complete; }
        public TargetQuotaMutation shadowCut() { return cut; }
        public long retainedBytes() { return retained; }
        public void advance() {
            if (complete) return;
            currentCuts();
            if (!cumulative.beforeTimedWork()) throw cumulative.incomplete();
            if (phase == 0) readBaseline();
            else if (phase == 1) readMeta();
            else if (phase == 2) readPrefix();
            else if (phase == 3) readShadow();
            else if (phase == 4) readPositions();
            else finish();
            currentCuts();
        }
        private void readBaseline() {
            if (baselineChunk == baseline.chunks().size()) { phase++; lower = new byte[] {(byte) TargetKeyCodec.MIGRATION_ROOT_TAG, 1}; return; }
            final var chunk = baseline.chunks().get(baselineChunk);
            final var records = LegacyMigrationBundle.readCompanionChunk(baseline, baselineChunk++, held.original(), limits.companions(), held.objects());
            final byte[] chunkBytes = chunkBytes(baseline, chunk, records);
            chargeObject(chunk.object());
            for (int index = 0; index < records.size(); index++) {
                final long ordinal = chunk.firstOrdinal() + index;
                if (ordinal >= held.sealed().material().view().entries().size()) continue;
                final byte[] record = records.get(index);
                final var raw = oldRaw(record, 1);
                if (raw.ordinal() != ordinal || !Arrays.equals(record,
                        Bytes.concat(Bytes.u8(1), held.sealed().material().view().entries().get(Math.toIntExact(ordinal)).canonicalBytes())))
                    throw new IllegalStateException("C archive inventory changed a complete original raw row");
                originalReferences.put(referenceKey(chunk.object(), index), raw);
                retain(Math.addExact(Math.multiplyExact(2L, raw.key().length + (long) raw.value().length), 512));
                if (raw.family() != ColumnFamily.DEDUPE) continue;
                final byte[] proof = compactProof(baseline.manifest(), baselineManifest, chunk.object(), chunkBytes, ordinal);
                add(original, raw.key(), new Raw(raw.key(), raw.value(), raw.key()[0] == 3
                        ? TargetMigrationArchiveEntry.Origin.PHYSICAL_ONLY : TargetMigrationArchiveEntry.Origin.COMPACT_BASELINE,
                        null, plan.baseline(), proof));
            }
        }
        private void readMeta() {
            final var page = page(held.target(), targetView, ColumnFamily.META, lower,
                    new byte[] {(byte) TargetKeyCodec.MIGRATION_RETIREMENT_TAG, 1}, row -> {
                final var value = TargetValueEnvelope.decodeAny(row.value());
                sourceDigest = Bytes.sha256(PACKET_DOMAIN, sourceDigest, row.key(), row.value());
                retain(Math.addExact(Math.multiplyExact(4L, row.value().length), row.key().length + 512L));
                if (value.valueType() == TargetMigrationOrigin.VALUE_TYPE) {
                    final var origin = TargetMigrationOrigin.decode(value.payload()); origin.requirePlan(plan); origins.add(origin);
                } else if (value.valueType() == TargetMigrationPrefixRecord.VALUE_TYPE) {
                    final var prefix = TargetMigrationPrefixRecord.decode(value.payload()); prefix.requirePlan(plan);
                    if (Long.compareUnsigned(prefix.at().sequence(), cut.sequence()) > 0) throw new IllegalStateException("prefix exceeds archive cut");
                    prefixes.add(prefix); step(prefix.at());
                }
            });
            lower = page.next(); if (page.end()) { phase++; lower = null; prefixes.sort((a,b) -> Long.compareUnsigned(a.at().sequence(), b.at().sequence())); }
        }
        private void readPrefix() {
            if (prefixIndex == prefixes.size()) { phase++; lower = new byte[] {1, 1}; return; }
            final var prefix = prefixes.get(prefixIndex);
            if (prefixCompanion == null) {
                prefixCompanion = LegacyMigrationBundle.readCompanionIndex(prefix.sourceRecord().object().resource(), LegacyMigrationBundle.Section.CUSTODY,
                        held.sealed().material(), held.original(), limits.companions(), held.objects());
                prefixManifest = exact(prefixCompanion.manifest()); prefixChunk = 0; prefixHeader = null;
            }
            if (prefixChunk == prefixCompanion.chunks().size()) {
                prefixIndex++; prefixCompanion = null; return;
            }
            final var chunk = prefixCompanion.chunks().get(prefixChunk);
            final var records = LegacyMigrationBundle.readCompanionChunk(prefixCompanion, prefixChunk++, held.original(), limits.companions(), held.objects());
            final byte[] encodedChunk = chunkBytes(prefixCompanion, chunk, records); chargeObject(chunk.object());
            if (chunk.firstOrdinal() == 0) {
                prefixHeader = records.getFirst(); prefixHeaderRecords = records;
                requirePrefixHeader(prefix, prefixHeader);
            }
            for (int index = 0; index < records.size(); index++) {
                final byte[] record = records.get(index);
                if (record.length == 0 || record[0] != 2) continue;
                final var raw = oldRaw(record, 2);
                originalReferences.put(referenceKey(chunk.object(), index), raw);
                retain(Math.addExact(Math.multiplyExact(2L, raw.key().length + (long) raw.value().length), 512));
                if (raw.family() != ColumnFamily.DEDUPE || original.containsKey(Bytes.hex(raw.key()))) continue;
                final SourcePosition first = rawSource(raw.key(), raw.value());
                if (first == null || !Arrays.equals(first.canonicalBytes(), prefix.at().source().canonicalBytes())) continue;
                final var headerChunk = prefixCompanion.chunks().getFirst();
                final byte[] proof = prefixProof(prefix, prefixCompanion.manifest(), prefixManifest, chunk.object(), encodedChunk,
                        chunk.firstOrdinal() + index, headerChunk.object(), chunkBytes(prefixCompanion, headerChunk, prefixHeaderRecords));
                add(prefixRows, raw.key(), new Raw(raw.key(), raw.value(), raw.key()[0] == 3
                        ? TargetMigrationArchiveEntry.Origin.PHYSICAL_ONLY : TargetMigrationArchiveEntry.Origin.LEGACY_SOURCE_FIRST,
                        raw.key()[0] == 3 ? null : first, prefix.at(), proof));
            }
        }
        private void readShadow() {
            final var page = page(held.shadow().store(), oldView, ColumnFamily.DEDUPE, lower, new byte[] {6, 1}, row -> {
                final String key = Bytes.hex(row.key()); final var base = original.get(key);
                final var raw = base == null ? prefixRows.get(key) : base;
                if (raw == null || !Arrays.equals(raw.value(), row.value()))
                    throw new IllegalStateException("shadow logical record lacks its exact C or actual prefix/raw provenance");
                add(shadowRows, row.key(), raw);
            });
            lower = page.next(); if (page.end()) { phase++; lower = new byte[] {(byte) TargetKeyCodec.RESULT_COMMAND_TAG, 1}; }
        }
        private void readPositions() {
            final var page = page(held.target(), targetView, ColumnFamily.DEDUPE, lower,
                    new byte[] {(byte) (TargetKeyCodec.RESULT_POSITION_TAG + 1), 1}, row -> {
                final var value = TargetValueEnvelope.decode(row.value(), TargetResultRecord.VALUE_TYPE);
                final var record = TargetResultRecord.decode(value.payload()); record.requireStored(row.key(), value.valueType(), value.payload());
                if (record.kind().position() && Long.compareUnsigned(record.mutation().sequence(), plan.baseline().sequence()) > 0) step(record.mutation());
                sourceDigest = Bytes.sha256(PACKET_DOMAIN, sourceDigest, row.key(), row.value());
            });
            lower = page.next(); if (page.end()) phase++;
        }
        private void finish() {
            for (var row : original.entrySet()) if (!shadowRows.containsKey(row.getKey()))
                throw new IllegalStateException("protected shadow dropped an original C logical identity without an accepted replacement");
            for (var origin : origins) {
                final var raw = Objects.requireNonNull(originalReferences.get(referenceKey(origin.oldRecord().object().resource(), origin.oldRecord().ordinal())),
                        "migration origin's complete original record");
                if (raw.family() != origin.oldFamily() || !Arrays.equals(Bytes.sha256(raw.key()), origin.oldKeyDigest())
                        || !Arrays.equals(Bytes.sha256(raw.value()), origin.oldValueDigest()))
                    throw new IllegalStateException("archive fold changed a Native66 original raw/fee owner association");
            }
            TargetQuotaMutation prior = plan.baseline();
            for (int index = 0; !prior.equals(cut) && index <= steps.size(); index++) {
                final var next = Objects.requireNonNull(steps.get(TargetQuotaMutation.increment(prior.sequence())), "complete archive Source successor");
                next.requireAfter(prior); prior = next;
            }
            if (!prior.equals(cut)) throw new IllegalStateException("archive did not reach the exact actual shadow cut");
            for (var raw : shadowRows.values()) if (raw.key()[0] == 2) {
                final byte[] firstKey = Bytes.concat(new byte[] {1, 1}, Arrays.copyOfRange(raw.key(), 2, raw.key().length));
                final var first = Objects.requireNonNull(shadowRows.get(Bytes.hex(firstKey)), "original query Command first");
                final var a = CommandDedupeRecord.decode(TargetValueEnvelope.decodeAny(first.value()).payload()).result();
                final var b = CommandResult.decode(TargetValueEnvelope.decodeAny(raw.value()).payload());
                if (!a.equals(b)) throw new IllegalStateException("original query result differs from compact logical first");
            }
            complete = true;
        }
        public TransferMaterial prepareBatch(TargetMigrationArchiveState prior) {
            if (!complete) throw new IllegalStateException("archive full scan/fold has not reached every exact range EOF");
            held.requireCurrent();
            if (prior != null && (!prior.shadowCut().equals(cut) || prior.taken()))
                throw new IllegalStateException("archive COPY changed the original frozen base cut");
            final var all = List.copyOf(shadowRows.values());
            final long copied = prior == null ? 0 : prior.coverage().totalRows();
            if (copied > all.size()) throw new IllegalStateException("archive predecessor coverage exceeds complete original inventory");
            final int from = Math.toIntExact(copied); int to = from; long bytes = 0, wireBytes = 0;
            while (to < all.size() && to - from < TargetMigrationArchiveRequest.MAX_ROWS) {
                final long size = all.get(to).encoded().length + 64L;
                final long nextWire = Math.addExact(wireBytes, all.get(to).key().length + (long) all.get(to).value().length
                        + 2L * TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES + TargetMigrationRecordReference.MAX_CANONICAL_BYTES + 128);
                final long frame = Math.addExact(nextWire, coverage(all, to + 1).canonicalBytes().length
                        + 2L * TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES + 4096);
                if (size > limits.maximumPacketBytes() / 2L) throw new IllegalStateException("one complete archive row/provenance exceeds prepaid packet capacity");
                if ((bytes + size > limits.maximumPacketBytes() / 2L || frame > TargetMigrationArchiveRequest.MAX_CANONICAL_BYTES / 2L) && to > from) break;
                if (frame > TargetMigrationArchiveRequest.MAX_CANONICAL_BYTES / 2L)
                    throw new IllegalStateException("one archive row exceeds the complete Source request envelope");
                bytes = Math.addExact(bytes, size); wireBytes = nextWire; to++;
            }
            final var beforeCoverage = coverage(all, from);
            if (prior != null && !beforeCoverage.equals(prior.coverage()))
                throw new IllegalStateException("archive actual predecessor differs from independently reconstructed range/count/raw fold");
            return new TransferMaterial(held, prior, cut, prior == null ? cut : prior.coveredThrough(), coverage(all, to),
                    all.subList(from, to), sourceDigest, prior == null ? 0 : prior.tailRows(), prior == null ? 0 : prior.tailRawBytes(), limits.maximumPacketBytes(), null);
        }
        private void step(TargetQuotaMutation at) {
            if (Long.compareUnsigned(at.sequence(), cut.sequence()) > 0) throw new IllegalStateException("archive Source step exceeds cut");
            final var before = steps.putIfAbsent(at.sequence(), at);
            if (before != null && !before.equals(at)) throw new IllegalStateException("archive prefix/position disagree at one actual Source sequence");
        }
        private void add(Map<String, Raw> rows, byte[] key, Raw raw) {
            if (key.length < 3 || key[1] != 1 || key[0] < 1 || key[0] > 5) throw new IllegalStateException("unknown original DEDUPE conflict domain");
            final var prior = rows.putIfAbsent(Bytes.hex(key), raw);
            if (prior != null && !Arrays.equals(prior.value(), raw.value())) throw new IllegalStateException("original logical first changed immutable bytes");
            if (prior == null) retain(Math.addExact(Math.multiplyExact(4L, raw.encoded().length), 512));
            if (rows.size() > limits.maximumLogicalRows())
                throw new IllegalStateException("archive complete material exceeds original cumulative retained allocation");
        }
        private void retain(long bytes) {
            retained = Math.addExact(retained, bytes);
            if (retained > limits.maximumRetainedBytes() || retained > held.retainedBytes())
                throw new IllegalStateException("archive material/codec copies exceed original retained allocation");
        }
        private byte[] exact(CheckpointResource resource) {
            chargeObject(resource);
            final byte[] bytes = held.objects().readExact(resource, limits.companions().maxChunkBytes());
            new TargetMigrationObjectReference(resource).requireContent(bytes); return bytes;
        }
        private void chargeObject(CheckpointResource resource) {
            if (!cumulative.beforeRead() || !cumulative.tryCharge(resource.canonicalBytes().length, Math.toIntExact(resource.manifestLength())))
                throw cumulative.incomplete();
        }
        private void currentCuts() {
            held.requireCurrent();
            held.shadow().store().withReadView(oldView, () -> null);
            held.target().withReadView(targetView, () -> null);
        }
        private Page page(ShardStore store, ShardStore.ReadView view, ColumnFamily family, byte[] start, byte[] end,
                java.util.function.Consumer<ShardStore.KeyValue> consumer) {
            return store.withReadView(view, () -> store.readWithBudget(cumulative, () -> {
                final byte[][] last = {null};
                final var result = store.visitResult(family, start, end, limits.recordsPerTurn(), cumulative, (row, shared) -> {
                    if (last[0] != null && Arrays.compareUnsigned(last[0], row.key()) >= 0) throw new IllegalStateException("archive range page repeated/regressed a key");
                    consumer.accept(row); last[0] = row.key(); return true;
                });
                if (result.stop() == ShardStore.VisitStop.INCOMPLETE) throw cumulative.incomplete();
                return new Page(last[0] == null ? start : Bytes.concat(last[0], new byte[] {0}), result.stop() == ShardStore.VisitStop.RANGE_END);
            }).value());
        }
    }
    private record Page(byte[] next, boolean end) {}

    public record Packet(TargetMigrationPlan plan, TargetMigrationRetirementRecord retirement,
            TargetQuotaMutation shadowCut, TargetQuotaMutation coveredThrough, byte[] priorArchiveRoot,
            TargetMigrationArchiveCoverage coverage, byte[] sourceCoverageDigest,
            List<TargetMigrationArchiveEntry> entries, long tailRows, long tailRawBytes,
            TargetMigrationObjectReference priorInventory, byte[] closedContract, int schemaVersion) {
        public Packet {
            priorArchiveRoot = Bytes.copy(priorArchiveRoot); sourceCoverageDigest = Bytes.copy(sourceCoverageDigest);
            entries = List.copyOf(entries);
            closedContract = closedContract == null ? null : Bytes.copy(closedContract);
        }
        @Override public byte[] priorArchiveRoot() { return Bytes.copy(priorArchiveRoot); }
        @Override public byte[] sourceCoverageDigest() { return Bytes.copy(sourceCoverageDigest); }
        @Override public byte[] closedContract() { return closedContract == null ? null : Bytes.copy(closedContract); }
    }

    /** Pure closed packet decoding, including the complete C/prefix/prospective Source provenance, never trust pins. */
    public static Packet decodePacket(byte[] bytes, TargetMigrationObjectReference actualPacket) {
        actualPacket.requireContent(bytes);
        final var f = QueryCodecSupport.read(bytes, "complete archive packet", true);
        final int version = QueryCodecSupport.uint32(f.getFirst(), 1);
        if (f.size() < 10 || f.size() > TargetMigrationArchiveRequest.MAX_ROWS + 12
                || version != LEGACY_PACKET_SCHEMA && version != FULL_SOURCE_PACKET_SCHEMA)
            throw new IllegalArgumentException("archive packet shape exceeds its closed schema");
        final var plan = TargetMigrationPlan.decode(QueryCodecSupport.bytes(f.get(1), 2));
        final var retirement = TargetMigrationRetirementRecord.decode(QueryCodecSupport.bytes(f.get(2), 3));
        final var shadowCut = TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(3), 4));
        final var through = TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(4), 5));
        final byte[] prior = QueryCodecSupport.fixed(f.get(5), 6, 32);
        final var coverage = TargetMigrationArchiveCoverage.decode(QueryCodecSupport.bytes(f.get(6), 7));
        final byte[] source = QueryCodecSupport.fixed(f.get(7), 8, 32);
        final var entries = new ArrayList<TargetMigrationArchiveEntry>();
        int index = 8;
        while (index < f.size() && f.get(index).number() == 9) {
            final var raw = decodeRaw(QueryCodecSupport.bytes(f.get(index), 9));
            final var entry = new TargetMigrationArchiveEntry(raw.key(), raw.value(), raw.origin(), raw.first(), raw.provenance(),
                    new TargetMigrationRecordReference(actualPacket, entries.size()));
            TargetMigrationArchiveSemantics.requireEntry(entry, plan);
            requireRawProof(raw, plan, version == FULL_SOURCE_PACKET_SCHEMA);
            if (!entries.isEmpty() && Arrays.compareUnsigned(entries.getLast().originalKey(), entry.originalKey()) >= 0)
                throw new IllegalArgumentException("archive packet repeats or reorders an original identity");
            entries.add(entry);
            index++;
        }
        if (index + 2 > f.size()) throw new IllegalArgumentException("archive packet omitted cumulative tail counters");
        final long tailRows = QueryCodecSupport.uint(f.get(index++), 10);
        final long tailBytes = QueryCodecSupport.uint(f.get(index++), 11);
        final var priorInventory = index < f.size() && f.get(index).number() == 12
                ? TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.get(index++), 12)) : null;
        final byte[] closed = index < f.size() && f.get(index).number() == 13 ? QueryCodecSupport.bytes(f.get(index++), 13) : null;
        if (index != f.size()) throw new IllegalArgumentException("archive packet has unknown fields");
        shadowCut.requireAtOrBefore(through);
        if (!Arrays.equals(retirement.intention().planDigest(), plan.digest()) || !retirement.intention().scope().equals(plan.scope()))
            throw new IllegalArgumentException("archive packet changed its exact original intent/plan");
        return new Packet(plan, retirement, shadowCut, through, prior, coverage, source, entries, tailRows, tailBytes, priorInventory, closed, version);
    }

    public static void requireArchivePacketProvenance(TargetMigrationRoot root, TargetMigrationRetirementRecord retirement,
            byte[] packetBytes, TargetMigrationObjectReference actualPacket, List<TargetMigrationArchiveEntry> entries) {
        final var packet = decodePacket(packetBytes, actualPacket);
        retirement.requireRoot(root);
        if (!packet.plan().equals(root.plan()) || !packet.retirement().intentAt().equals(retirement.intentAt())
                || !packet.entries().equals(entries)) throw new IllegalStateException("archive packet changed authenticated original Root/I/records");
        if (packet.closedContract() != null)
            TargetMigrationArchiveClosedContract.requireRawMaterials(packet.closedContract(), root.plan(), retirement, packet.coveredThrough());
    }

    private static Raw decodeRaw(byte[] encoded) {
        final var f = QueryCodecSupport.read(encoded, "complete archive original row");
        final boolean first = f.size() == 6;
        QueryCodecSupport.requireNumbers(f, first ? new int[] {1,2,3,4,5,6} : new int[] {1,2,3,5,6}, "archive original row");
        final int kind = QueryCodecSupport.uint32(f.get(2), 3);
        if (kind < 1 || kind > TargetMigrationArchiveEntry.Origin.values().length) throw new IllegalArgumentException("unknown archive provenance kind");
        int i = 3; final var originalFirst = first ? TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(i++), 4)) : null;
        final var raw = new Raw(QueryCodecSupport.bytes(f.get(0), 1), QueryCodecSupport.bytes(f.get(1), 2),
                TargetMigrationArchiveEntry.Origin.values()[kind - 1], originalFirst,
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(i++), 5)), QueryCodecSupport.bytes(f.get(i), 6));
        if (!Arrays.equals(encoded, raw.encoded())) throw new IllegalArgumentException("noncanonical archive original row");
        return raw;
    }

    private static TargetMigrationArchiveCoverage coverage(List<Raw> rows, int through) {
        final var ranges = new ArrayList<TargetMigrationArchiveCoverage.Range>();
        for (int namespace = 1; namespace <= 5; namespace++) {
            long count = 0, bytes = 0; byte[] last = null;
            byte[] root = Bytes.sha256(RANGE_DOMAIN, Bytes.u8(namespace)); boolean remaining = false;
            for (int index = 0; index < rows.size(); index++) {
                final var row = rows.get(index); if (row.key()[0] != namespace) continue;
                if (index >= through) { remaining = true; continue; }
                count = Math.incrementExact(count); bytes = Math.addExact(bytes, Math.addExact(row.key().length, row.value().length));
                root = Bytes.sha256(RANGE_DOMAIN, root, row.key(), row.value()); last = row.key();
            }
            ranges.add(new TargetMigrationArchiveCoverage.Range(namespace, new byte[] {(byte) namespace, 1},
                    new byte[] {(byte) (namespace + 1), 1}, last, count, bytes, root, !remaining));
        }
        return new TargetMigrationArchiveCoverage(ranges);
    }

    private static TargetRecordAccounting.View nativeView(ShardStore store) {
        return new TargetRecordAccounting.View() {
            @Override public ShardId shardId() { return store.shardId(); }
            @Override public byte[] projected(ColumnFamily family, byte[] key, List<TargetStoreBackend.Edit> overlay) {
                if (!overlay.isEmpty()) throw new IllegalArgumentException("archive capture has no hypothetical overlay");
                return store.get(family, key);
            }
        };
    }

    private static byte[] chunkBytes(LegacyMigrationBundle.Companion companion, LegacyMigrationBundle.Chunk chunk, List<byte[]> records) {
        final var parts = new ArrayList<byte[]>();
        parts.add(Bytes.u32be(1)); parts.add(Bytes.u8(companion.section().ordinal() + 1)); parts.add(Bytes.u64be(chunk.firstOrdinal()));
        for (var record : records) parts.add(Bytes.lp32(record));
        final byte[] result = Bytes.concat(parts.toArray(byte[][]::new));
        new TargetMigrationObjectReference(chunk.object()).requireContent(result); return result;
    }
    private static String referenceKey(CheckpointResource object, long ordinal) { return Bytes.hex(Bytes.sha256(object.canonicalBytes())) + ":" + ordinal; }
    private static LegacyCheckpointStateInventory.RawEntry oldRaw(byte[] record, int kind) {
        final var b = ByteBuffer.wrap(record);
        if (b.remaining() < 10 || Byte.toUnsignedInt(b.get()) != kind) throw new IllegalArgumentException("wrong complete original record kind");
        final long ordinal = b.getLong(); final int family = Byte.toUnsignedInt(b.get());
        if (family >= ColumnFamily.values().length) throw new IllegalArgumentException("unknown original column family");
        final byte[] key = lp(b), value = lp(b);
        if (b.hasRemaining()) throw new IllegalArgumentException("original record has trailing bytes");
        return new LegacyCheckpointStateInventory.RawEntry(ordinal, ColumnFamily.values()[family], key, value);
    }
    private static byte[] lp(ByteBuffer b) {
        if (b.remaining() < 4) throw new IllegalArgumentException("truncated original length");
        final int n = b.getInt(); if (n < 0 || n > b.remaining()) throw new IllegalArgumentException("truncated original material");
        final byte[] result = new byte[n]; b.get(result); return result;
    }
    private static SourcePosition rawSource(byte[] key, byte[] raw) {
        final var value = TargetValueEnvelope.decodeAny(raw);
        return switch (key[0]) {
            case 1 -> SourcePositionCodec.decode(CommandDedupeRecord.decode(value.payload()).result().appliedSourcePosition());
            case 2 -> SourcePositionCodec.decode(CommandResult.decode(value.payload()).appliedSourcePosition());
            case 3 -> TargetSourcePosition.decode(Arrays.copyOfRange(key, 2, key.length));
            case 5 -> SourcePositionCodec.decode(SystemMutationResult.decode(value.payload()).appliedSourcePosition());
            default -> null;
        };
    }

    private static byte[] compactProof(CheckpointResource manifest, byte[] manifestBytes, CheckpointResource chunk, byte[] bytes, long ordinal) {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1); CanonicalProtobuf.bytes(out, 2, manifest.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, manifestBytes); CanonicalProtobuf.bytes(out, 4, chunk.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, bytes); CanonicalProtobuf.uint64(out, 6, ordinal);
        });
    }
    private static byte[] prefixProof(TargetMigrationPrefixRecord prefix, CheckpointResource manifest, byte[] manifestBytes,
            CheckpointResource chunk, byte[] bytes, long ordinal, CheckpointResource headerChunk, byte[] headerBytes) {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 2); CanonicalProtobuf.bytes(out, 2, manifest.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, manifestBytes); CanonicalProtobuf.bytes(out, 4, chunk.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, bytes); CanonicalProtobuf.uint64(out, 6, ordinal);
            CanonicalProtobuf.bytes(out, 7, prefix.canonicalBytes()); CanonicalProtobuf.bytes(out, 8, headerChunk.canonicalBytes());
            CanonicalProtobuf.bytes(out, 9, headerBytes);
        });
    }

    private record Header(SourcePosition prior, SourcePosition actual, byte[] frame, byte[] result) {}
    private static Header header(byte[] raw) {
        final var b = ByteBuffer.wrap(raw); if (b.remaining() < 1 || b.get() != 1) throw new IllegalArgumentException("missing original Source header");
        final var prior = TargetSourcePosition.decode(lp(b)); final var source = TargetSourcePosition.decode(lp(b));
        if (b.remaining() < 8) throw new IllegalArgumentException("missing actual old sequence"); b.getLong();
        final byte[] frame = lp(b), result = lp(b); if (b.hasRemaining()) throw new IllegalArgumentException("trailing Source header");
        return new Header(prior, source, frame, result);
    }
    private static void requirePrefixHeader(TargetMigrationPrefixRecord prefix, byte[] raw) {
        requirePrefixHeader(prefix, raw, true);
    }
    private static void requirePrefixHeader(TargetMigrationPrefixRecord prefix, byte[] raw, boolean fullSource) {
        final var h = header(raw);
        if (!Arrays.equals(h.actual().canonicalBytes(), prefix.at().source().canonicalBytes())
                || !Arrays.equals(h.prior().canonicalBytes(), prefix.prior().source().canonicalBytes()))
            throw new IllegalStateException("archive original prefix changed physical Source predecessor");
        if (prefix.recordKind() == ProtocolTuple.CLIENT_COMMAND) {
            requireSourceFrame(h.frame(), prefix.at(), fullSource);
            final var command = CommandCodec.decodeFrame(h.frame());
            if (!Arrays.equals(command.commandId().bytes(), prefix.originalIdentity())
                    || !Arrays.equals(command.commandHash(), prefix.originalSemanticHash())
                    || !Arrays.equals(command.canonicalFrameDigest(), prefix.at().mutationDigest())
                    || !Arrays.equals(Bytes.sha256(h.frame()), prefix.rawFrameDigest()))
                throw new IllegalStateException("archive original Command prefix changed full frame/hash/protocol");
            CommandResult.decode(h.result());
        } else {
            final var input = SystemMutation.decodeTargetEnvelope(h.frame());
            requireSourceFrame(input.encodeFrame(), prefix.at(), fullSource);
            if (!Arrays.equals(input.systemMutationId(), prefix.originalIdentity())
                    || !Arrays.equals(input.mutationHash(), prefix.originalSemanticHash())
                    || !Arrays.equals(Bytes.sha256(input.canonicalEnvelope()), prefix.at().mutationDigest())
                    || !Arrays.equals(Bytes.sha256(input.encodeFrame()), prefix.rawFrameDigest()))
                throw new IllegalStateException("archive original System prefix changed full frame/hash/protocol");
            SystemMutationResult.decode(h.result());
        }
    }

    private static void requireRawProof(Raw row, TargetMigrationPlan plan) {
        requireRawProof(row, plan, true);
    }
    private static void requireRawProof(Raw row, TargetMigrationPlan plan, boolean fullSource) {
        final var f = QueryCodecSupport.read(row.proof(), "archive complete original proof");
        final int kind = QueryCodecSupport.uint32(f.getFirst(), 1);
        if (kind == 3 || kind == 5) {
            if (kind == 5 && !fullSource) throw new IllegalArgumentException("legacy archive packet cannot carry full Source proof");
            requireProspectiveProof(row, f, kind == 5); return;
        }
        if (kind == 4) {
            QueryCodecSupport.requireNumbers(f, new int[] {1,2,3,4,5,6,7}, "actual legacy Source header proof");
            final var prefix = TargetMigrationPrefixRecord.decode(QueryCodecSupport.bytes(f.get(6), 7)); prefix.requirePlan(plan);
            final var manifest = CheckpointResource.decode(QueryCodecSupport.bytes(f.get(1), 2));
            if (!prefix.sourceRecord().object().resource().equals(manifest) || !prefix.at().equals(row.provenance())
                    || prefix.sourceRecord().ordinal() != QueryCodecSupport.uint(f.get(5), 6))
                throw new IllegalStateException("archive legacy tail changed its exact Source header reference");
            final byte[] h = protectedRecord(manifest, QueryCodecSupport.bytes(f.get(2), 3),
                    CheckpointResource.decode(QueryCodecSupport.bytes(f.get(3), 4)), QueryCodecSupport.bytes(f.get(4), 5),
                    QueryCodecSupport.uint(f.get(5), 6), plan);
            requirePrefixHeader(prefix, h, fullSource); requirePrefixRow(row, prefix, h); return;
        }
        QueryCodecSupport.requireNumbers(f, kind == 1 ? new int[] {1,2,3,4,5,6} : new int[] {1,2,3,4,5,6,7,8,9}, "archive original proof");
        if (kind != 1 && kind != 2) throw new IllegalArgumentException("unknown archive original proof");
        final var manifest = CheckpointResource.decode(QueryCodecSupport.bytes(f.get(1), 2));
        final byte[] manifestBytes = QueryCodecSupport.bytes(f.get(2), 3);
        final var chunk = CheckpointResource.decode(QueryCodecSupport.bytes(f.get(3), 4));
        final byte[] chunkBytes = QueryCodecSupport.bytes(f.get(4), 5);
        final long ordinal = QueryCodecSupport.uint(f.get(5), 6);
        final byte[] raw = protectedRecord(manifest, manifestBytes, chunk, chunkBytes, ordinal, plan);
        final var original = oldRaw(raw, kind == 1 ? 1 : 2);
        if (original.family() != ColumnFamily.DEDUPE || !Arrays.equals(original.key(), row.key()) || !Arrays.equals(original.value(), row.value()))
            throw new IllegalStateException("archive original fixed chunk differs from the complete copied row");
        if (kind == 1) {
            if (!manifest.equals(plan.custody().resource()) || !row.provenance().equals(plan.baseline()) || row.first() != null)
                throw new IllegalStateException("compact archive invented a first or changed its original C manifest");
        } else {
            final var prefix = TargetMigrationPrefixRecord.decode(QueryCodecSupport.bytes(f.get(6), 7)); prefix.requirePlan(plan);
            if (!prefix.sourceRecord().object().resource().equals(manifest) || !row.provenance().equals(prefix.at()))
                throw new IllegalStateException("archive prefix original record changed protected Source provenance");
            final byte[] header = protectedRecord(manifest, manifestBytes,
                    CheckpointResource.decode(QueryCodecSupport.bytes(f.get(7), 8)), QueryCodecSupport.bytes(f.get(8), 9), prefix.sourceRecord().ordinal(), plan);
            requirePrefixHeader(prefix, header, fullSource); requirePrefixRow(row, prefix, header);
        }
    }

    private static byte[] protectedRecord(CheckpointResource manifest, byte[] encodedManifest, CheckpointResource chunk,
            byte[] encodedChunk, long ordinal, TargetMigrationPlan plan) {
        new TargetMigrationObjectReference(manifest).requireContent(encodedManifest);
        new TargetMigrationObjectReference(chunk).requireContent(encodedChunk);
        final var f = QueryCodecSupport.read(encodedManifest, "fixed original companion", true);
        if (f.size() < 5 || QueryCodecSupport.uint32(f.get(0), 1) != 1
                || QueryCodecSupport.uint32(f.get(1), 2) != LegacyMigrationBundle.Section.CUSTODY.ordinal() + 1
                || !Arrays.equals(QueryCodecSupport.fixed(f.get(2), 3, 32), plan.oldManifest().resource().manifestSha256())
                || !Arrays.equals(QueryCodecSupport.bytes(f.get(3), 4), plan.baseline().canonicalBytes()))
            throw new IllegalArgumentException("original companion changed exact C/manifest domain");
        long first = -1; int count = -1;
        for (int i = 5; i < f.size(); i++) {
            final var b = ByteBuffer.wrap(QueryCodecSupport.bytes(f.get(i), 6));
            final long start = b.getLong(); final int n = b.getInt(); final byte[] object = lp(b);
            if (b.hasRemaining()) throw new IllegalArgumentException("trailing fixed chunk identity");
            if (Arrays.equals(object, chunk.canonicalBytes())) { first = start; count = n; }
        }
        if (first < 0 || count <= 0 || ordinal < first || ordinal - first >= count)
            throw new IllegalArgumentException("original raw record is outside the exact manifest/chunk inventory");
        final var b = ByteBuffer.wrap(encodedChunk);
        if (b.remaining() < 13 || b.getInt() != 1 || Byte.toUnsignedInt(b.get()) != LegacyMigrationBundle.Section.CUSTODY.ordinal() + 1 || b.getLong() != first)
            throw new IllegalArgumentException("original chunk header changed");
        byte[] selected = null;
        for (int index = 0; index < count; index++) { final byte[] record = lp(b); if (first + index == ordinal) selected = record; }
        if (b.hasRemaining()) throw new IllegalArgumentException("original chunk has unaccounted bytes");
        return Objects.requireNonNull(selected);
    }

    private static void requirePrefixRow(Raw row, TargetMigrationPrefixRecord prefix, byte[] sourceHeader) {
        final var h = header(sourceHeader);
        if (row.key()[0] == 3) {
            if (!Arrays.equals(Arrays.copyOfRange(row.key(), 2, row.key().length), h.actual().canonicalBytes())
                    || !Arrays.equals(TargetValueEnvelope.decode(row.value(), 3).payload(), prefix.originalIdentity()))
                throw new IllegalStateException("archive physical prefix changed actual original Source identity");
            return;
        }
        if (prefix.recordKind() == ProtocolTuple.CLIENT_COMMAND) {
            final var command = CommandCodec.decodeFrame(h.frame()); final var result = CommandResult.decode(h.result());
            if (row.key()[0] == 1) {
                final var first = CommandDedupeRecord.decode(TargetValueEnvelope.decode(row.value(), 1).payload());
                if (!Arrays.equals(row.key(), KeyCodec.dedupeCommand(command.commandId())) || !first.protocolTuple().equals(command.protocolTuple())
                        || !Arrays.equals(first.commandHash(), command.commandHash()) || !first.result().equals(result))
                    throw new IllegalStateException("archive original Command first changed its exact outcome");
            } else if (row.key()[0] != 2 || !Arrays.equals(row.key(), KeyCodec.dedupeResult(command.commandId()))
                    || !CommandResult.decode(TargetValueEnvelope.decode(row.value(), 2).payload()).equals(result))
                throw new IllegalStateException("archive original query result changed its exact Command outcome");
        } else {
            final var input = SystemMutation.decodeTargetEnvelope(h.frame());
            final var result = SystemMutationResult.decode(TargetValueEnvelope.decode(row.value(), SystemMutationResult.VALUE_TYPE).payload());
            if (row.key()[0] != 5 || !Arrays.equals(row.key(), KeyCodec.dedupeSystemMutation(input.systemMutationId()))
                    || !Arrays.equals(result.encode(), h.result()) || !Arrays.equals(result.mutationHash(), input.mutationHash())
                    || result.mutationType() != input.type() || result.retryUntilEpochMs() != input.retryUntilEpochMs()
                    || !Arrays.equals(result.authorIdentity(), input.authorIdentity()))
                throw new IllegalStateException("archive original System first changed its exact immutable outcome");
        }
    }

    private record Raw(byte[] key, byte[] value, TargetMigrationArchiveEntry.Origin origin,
            SourcePosition first, TargetQuotaMutation provenance, byte[] proof) {
        private Raw {
            key = Bytes.copy(key); value = Bytes.copy(value); proof = Bytes.copy(proof);
        }
        byte[] encoded() { return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, key); CanonicalProtobuf.bytes(out, 2, value);
            CanonicalProtobuf.uint32(out, 3, origin.ordinal() + 1);
            if (first != null) CanonicalProtobuf.bytes(out, 4, first.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, provenance.canonicalBytes()); CanonicalProtobuf.bytes(out, 6, proof);
        }); }
    }

    /** Complete immutable packet generated by the private scanner, before actual new-owner put/version-GET. */
    public static final class TransferMaterial {
        private final Held held;
        private final TargetMigrationArchiveState prior;
        private final TargetQuotaMutation shadowCut, coveredThrough;
        private final TargetMigrationArchiveCoverage coverage;
        private final List<Raw> rows;
        private final byte[] packet, sourceDigest, priorRoot;
        private final long tailRows, tailBytes;
        private final TargetMigrationArchiveClosedContract.Qualification closed;
        private SourceReplayEntry actualTail;
        private TransferMaterial(Held held, TargetMigrationArchiveState prior, TargetQuotaMutation shadowCut,
                TargetQuotaMutation coveredThrough, TargetMigrationArchiveCoverage coverage, List<Raw> rows,
                byte[] sourceDigest, long tailRows, long tailBytes, int maximumBytes,
                TargetMigrationArchiveClosedContract.Qualification closed) {
            this.held = held; this.prior = prior; this.shadowCut = shadowCut; this.coveredThrough = coveredThrough;
            this.coverage = coverage; this.rows = List.copyOf(rows); this.sourceDigest = Bytes.copy(sourceDigest);
            this.tailRows = tailRows; this.tailBytes = tailBytes;
            this.closed = closed;
            priorRoot = prior == null ? TargetMigrationArchiveRequest.genesisRoot(intent().intention().digest()) : prior.archiveRoot();
            packet = CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, FULL_SOURCE_PACKET_SCHEMA); CanonicalProtobuf.bytes(out, 2, plan().canonicalBytes());
                CanonicalProtobuf.bytes(out, 3, intent().record().canonicalBytes());
                CanonicalProtobuf.bytes(out, 4, shadowCut.canonicalBytes()); CanonicalProtobuf.bytes(out, 5, coveredThrough.canonicalBytes());
                CanonicalProtobuf.bytes(out, 6, priorRoot); CanonicalProtobuf.bytes(out, 7, coverage.canonicalBytes());
                CanonicalProtobuf.bytes(out, 8, sourceDigest);
                for (var row : rows) CanonicalProtobuf.bytes(out, 9, row.encoded());
                CanonicalProtobuf.uint64(out, 10, tailRows); CanonicalProtobuf.uint64(out, 11, tailBytes);
                if (prior != null && prior.inventory() != null) CanonicalProtobuf.bytes(out, 12, prior.inventory().canonicalBytes());
                if (closed != null) CanonicalProtobuf.bytes(out, 13, closed.rawMaterials());
            });
            if (rows.size() > TargetMigrationArchiveRequest.MAX_ROWS || packet.length > maximumBytes
                    || Math.multiplyExact(4L, packet.length) > held.retainedBytes())
                throw new IllegalStateException("archive fixed packet exceeds its original paid byte/copy allocation");
        }
        public TargetMigrationRetirementStore.AppliedIntent intent() { return held.intent(); }
        public TargetMigrationPlan plan() { return intent().migration().plan(); }
        public TargetQuotaMutation shadowCut() { return shadowCut; }
        public TargetQuotaMutation coveredThrough() { return coveredThrough; }
        public TargetMigrationArchiveCoverage coverage() { return coverage; }
        public byte[] packetBytes() { return Bytes.copy(packet); }
        public byte[] sourceCoverageDigest() { return Bytes.copy(sourceDigest); }
        public byte[] priorArchiveRoot() { return Bytes.copy(priorRoot); }
        public TargetMigrationObjectReference priorInventory() { return prior == null ? null : prior.inventory(); }
        public long recordCount() { return rows.size(); }
        public long tailRows() { return tailRows; }
        public long tailRawBytes() { return tailBytes; }
        /** Incremental packet/coverage/list bytes; Raw rows and the original hold belong to the scanner. */
        public long retainedBytes() {
            return Math.addExact(packet.length, Math.addExact(coverage.canonicalBytes().length, 256L + 8L * rows.size()));
        }
        public void requireCurrent() { held.requireCurrent(); }
        void requireLocalCurrent() { held.requireLocalCurrent(); }
        public List<TargetMigrationArchiveEntry> entries(TargetMigrationObjectReference actualPacket) {
            actualPacket.requireContent(packet);
            final var result = new ArrayList<TargetMigrationArchiveEntry>();
            for (int ordinal = 0; ordinal < rows.size(); ordinal++) {
                final var row = rows.get(ordinal);
                final var entry = new TargetMigrationArchiveEntry(row.key(), row.value(), row.origin(), row.first(),
                        row.provenance(), new TargetMigrationRecordReference(actualPacket, ordinal));
                TargetMigrationArchiveSemantics.requireEntry(entry, plan()); result.add(entry);
            }
            return List.copyOf(result);
        }
        public byte[] archiveRoot(TargetMigrationObjectReference actualPacket) {
            byte[] result = priorRoot;
            for (var entry : entries(actualPacket)) result = Bytes.sha256(ARCHIVE_DOMAIN, result, entry.digest());
            return Bytes.copy(result);
        }
        public PreparedBatch received(TargetMigrationArchiveReceived received) {
            requireCurrent(); received.requireMaterial(this);
            final var request = request(new TargetMigrationArchiveRequest.CopyBatch(
                    prior == null ? 1 : TargetQuotaMutation.increment(prior.generation()), shadowCut, coveredThrough,
                    entries(received.packet()), coverage, received.inventory()));
            return new PreparedBatch(this, received, request);
        }
        private TargetMigrationArchiveRequest request(TargetMigrationArchiveRequest.Action action) {
            return new TargetMigrationArchiveRequest(plan().scope(), plan().newLineage(), plan().migrationId(), plan().digest(),
                    intent().migration().activatedAt(), intent().intention().digest(), intent().intentAt(),
                    prior == null ? null : prior.digest(), priorRoot, action);
        }
    }

    public static final class PreparedBatch {
        private final TransferMaterial material;
        private final TargetMigrationArchiveReceived received;
        private final TargetMigrationArchiveRequest request;
        private PreparedBatch(TransferMaterial material, TargetMigrationArchiveReceived received, TargetMigrationArchiveRequest request) {
            this.material = material; this.received = received; this.request = request;
        }
        public TargetMigrationArchiveRequest request() { return request; }
        public long retainedBytes() {
            long bytes = material.retainedBytes() + 256;
            for (var entry : request.copyBatch().rows()) bytes = Math.addExact(bytes,
                    entry.originalKey().length + (long) entry.rawValue().length + 128);
            return bytes;
        }
        public void requireCurrent() { material.requireLocalCurrent(); received.requireLocalCurrent(); }
        public void requireCopy(TargetMigrationRoot root, TargetMigrationRetirementRecord retirement,
                TargetMigrationArchiveState priorState, TargetMigrationArchiveRequest exact, SourcePosition source) {
            material.requireLocalCurrent(); received.requireLocalCurrent(); exact.requireRetirement(root, retirement);
            if (!request.equals(exact) || !Objects.equals(material.prior, priorState)
                    || source.compareTo(exact.coveredThrough().source()) <= 0)
                throw new IllegalStateException("COPY changed private complete scan/received material or exact predecessor");
        }
    }

    public static final class PreparedTakeover {
        private final Held held;
        private final TargetMigrationArchiveReceived received;
        private final TargetMigrationArchiveState prior;
        private final TargetMigrationArchiveRequest request;
        private final TargetMigrationArchiveClosedContract.Qualification closed;
        private PreparedTakeover(Held held, TargetMigrationArchiveReceived received, TargetMigrationArchiveState prior,
                TargetMigrationArchiveRequest request, TargetMigrationArchiveClosedContract.Qualification closed) {
            this.held = held; this.received = received; this.prior = prior; this.request = request;
            this.closed = closed;
        }
        public TargetMigrationArchiveRequest request() { return request; }
        public long retainedBytes() { return Math.addExact(request.canonicalBytes().length, 256L); }
        public void requireCurrent() { held.requireLocalCurrent(); received.requireLocalCurrent(); }
        public void requireTakeover(TargetMigrationRoot root, TargetMigrationRetirementRecord retirement,
                TargetMigrationArchiveState actual, TargetMigrationArchiveRequest exact, SourcePosition source) {
            held.requireLocalCurrent(); received.requireLocalCurrent(); exact.requireRetirement(root, retirement);
            closed.requireState(root, retirement, actual);
            if (!prior.equals(actual) || !request.equals(exact) || !exact.coverage().complete()
                    || !exact.coveredThrough().equals(actual.coveredThrough())
                    || source.compareTo(exact.coveredThrough().source()) <= 0)
                throw new IllegalStateException("TAKEOVER changed its complete actual base/tail/receiver predecessor");
        }
    }

    public static final class PreparedTail {
        private final Held held;
        private final ShardStore.ReadView targetView;
        private final SourceReplayEntry actual;
        private final TargetQuotaMutation at;
        private final List<TargetMigrationArchiveEntry> entries;
        private final TargetMigrationArchiveRow.SourceBranch branch;
        private final TargetMigrationArchiveReceived received;
        private List<TargetStoreBackend.Edit> expectedBusiness = List.of();
        private PreparedTail(Held held, ShardStore.ReadView targetView, SourceReplayEntry actual, TargetQuotaMutation at,
                List<TargetMigrationArchiveEntry> entries, TargetMigrationArchiveRow.SourceBranch branch,
                TargetMigrationArchiveReceived received) {
            this.held = held; this.targetView = targetView; this.actual = actual; this.at = at;
            this.entries = List.copyOf(entries); this.branch = branch; this.received = received;
        }
        private void requireBusiness(List<TargetStoreBackend.Edit> business) {
            if (!sameEdits(causeEdits(expectedBusiness), causeEdits(business)))
                throw new IllegalStateException("archive tail changed its actual prospective Source first/prefix facts");
        }
        public List<TargetMigrationArchiveEntry> entries() { return entries; }
        public SourceReplayEntry actual() { return actual; }
        public TargetMigrationObjectReference inventory() { return received.inventory(); }
        public TargetMigrationArchiveRow.SourceBranch branch() { return branch; }
        public void requireCurrent() { held.requireLocalCurrent(); received.requireLocalCurrent(); }
        public void requireTail(TargetStoreBackend.Reader reader, TargetMigrationRoot root,
                TargetMigrationRetirementRecord retirement, SourceReplayEntry original, TargetQuotaMutation mutation) {
            held.requireLocalCurrent(); received.requireLocalCurrent();
            held.target().requirePayloadSummaryReadView(targetView);
            retirement.requireRoot(root);
            if (!root.plan().equals(held.intent().migration().plan()) || !retirement.intentAt().equals(held.intent().intentAt())
                    || !at.equals(mutation) || !Arrays.equals(frame(actual), frame(original))
                    || !Arrays.equals(actual.position().canonicalBytes(), original.position().canonicalBytes())
                    || !Arrays.equals(reader.metadata().storeIncarnation(), root.plan().newStore()))
                throw new IllegalStateException("archive tail changed its actual original frame/Native view/Source batch");
        }
    }

    /** Minted only inside the real Native planner; it carries exact projected facts, never a caller's completed claim. */
    public static final class TailPreparationRequired extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final transient TargetMigrationRoot root;
        private final transient TargetMigrationRetirementRecord retirement;
        private final transient TargetMigrationArchiveState state;
        private final transient ShardStore.ReadView view;
        private final transient TargetQuotaMutation at;
        private final transient List<TargetStoreBackend.Edit> business;
        private final long closedFence;
        private final transient Map<String, byte[]> evidence;
        @java.io.Serial
        private void writeObject(java.io.ObjectOutputStream ignored) throws java.io.IOException {
            throw new java.io.NotSerializableException(TailPreparationRequired.class.getName());
        }
        @java.io.Serial
        private void readObject(java.io.ObjectInputStream ignored) throws java.io.IOException, ClassNotFoundException {
            throw new java.io.InvalidObjectException("archive tail preparation requires its live Native planner capability");
        }
        private TailPreparationRequired(TargetMigrationRoot root, TargetMigrationRetirementRecord retirement,
                TargetMigrationArchiveState state, ShardStore.ReadView view, TargetQuotaMutation at,
                List<TargetStoreBackend.Edit> business, long closedFence, Map<String, byte[]> evidence) {
            super("actual Source archive tail requires its original paid owned-I/O receiver preparation");
            this.root = root; this.retirement = retirement; this.state = state; this.view = view;
            this.at = at; this.business = List.copyOf(business); this.closedFence = closedFence; this.evidence = Map.copyOf(evidence);
        }
        public TargetMigrationRoot root() { return root; }
        public TargetMigrationRetirementRecord retirement() { return retirement; }
        public TargetMigrationArchiveState prior() { return state; }
        public TargetQuotaMutation at() { return at; }
        public long retainedBytes() {
            long bytes = root.canonicalBytes().length + retirement.canonicalBytes().length + state.canonicalBytes().length;
            for (var edit : business) bytes = Math.addExact(bytes, edit.key().length
                    + (edit.before() == null ? 0 : edit.before().length) + (edit.after() == null ? 0 : edit.after().length));
            return Math.multiplyExact(4L, bytes);
        }
        public TransferMaterial material(Held held, SourceReplayEntry actual) {
            held.requireCurrent(); held.target().withReadView(view, () -> null);
            if (!held.intent().migration().equals(root) || !held.intent().intentAt().equals(retirement.intentAt())
                    || !Arrays.equals(actual.position().canonicalBytes(), at.source().canonicalBytes())
                    || !Arrays.equals(sourceDigest(actual), at.mutationDigest()))
                throw new IllegalStateException("archive tail receiver changed original Source/cut/root/intent");
            final var rows = new TreeMap<String, Raw>();
            for (var edit : business) {
                if (edit.family() != ColumnFamily.DEDUPE || edit.before() != null || edit.after() == null) continue;
                final var value = TargetValueEnvelope.decodeAny(edit.after());
                if (value.valueType() != TargetResultRecord.VALUE_TYPE) continue;
                final var record = TargetResultRecord.decode(value.payload()); record.requireStored(edit.key(), value.valueType(), value.payload());
                if (!record.mutation().equals(at)) continue;
                final byte[] key = TargetMigrationArchiveSemantics.originalKey(record);
                final byte[] proof = prospectiveProof(actual, evidence, closedFence);
                final var raw = new Raw(key, edit.after(), record.kind().position() ? TargetMigrationArchiveEntry.Origin.PHYSICAL_ONLY
                        : TargetMigrationArchiveEntry.Origin.TARGET_SOURCE_FIRST, record.kind().position() ? null : at.source(), at, proof);
                requireRawProof(raw, root.plan()); rows.put(Bytes.hex(key), raw);
            }
            // Legacy-prefix facts are already in the original immutable after-view objects. Only its physical
            // Source row and newly born logical firsts are copied; original baseline firsts await their COPY page.
            for (var edit : business) if (edit.family() == ColumnFamily.META && edit.after() != null
                    && edit.key().length >= 2 && Byte.toUnsignedInt(edit.key()[0]) == TargetKeyCodec.MIGRATION_PREFIX_TAG) {
                final var prefix = TargetMigrationPrefixRecord.decode(TargetValueEnvelope.decode(edit.after(), TargetMigrationPrefixRecord.VALUE_TYPE).payload());
                prefix.requirePlan(root.plan());
                if (!prefix.at().equals(at)) continue;
                readLegacyTail(held, actual, prefix, rows);
            }
            long rawBytes = 0; for (var row : rows.values()) rawBytes = Math.addExact(rawBytes, row.key().length + (long) row.value().length);
            final byte[] digest = Bytes.sha256(PACKET_DOMAIN, state.digest(), frame(actual), at.canonicalBytes());
            final var material = new TransferMaterial(held, state, state.shadowCut(), at, state.coverage(), List.copyOf(rows.values()), digest,
                    Math.addExact(state.tailRows(), rows.size()), Math.addExact(state.tailRawBytes(), rawBytes), held.limits().maximumPacketBytes(), null);
            material.actualTail = actual; return material;
        }
        public PreparedTail received(TransferMaterial material, TargetMigrationArchiveReceived received) {
            received.requireMaterial(material);
            if (material.prior != state || !material.coveredThrough().equals(at)) throw new IllegalArgumentException("another original archive tail material");
            final SourceReplayEntry actual = Objects.requireNonNull(material.actualTail, "actual original Source tail");
            final var proof = new PreparedTail(material.held, view, actual, at, material.entries(received.packet()),
                    business.stream().anyMatch(e -> e.family() == ColumnFamily.META && e.key().length >= 2
                            && Byte.toUnsignedInt(e.key()[0]) == TargetKeyCodec.MIGRATION_PREFIX_TAG)
                            ? TargetMigrationArchiveRow.SourceBranch.LEGACY_PREFIX : TargetMigrationArchiveRow.SourceBranch.TARGET_FIRST, received);
            proof.expectedBusiness = business; return proof;
        }
    }

    public static void requireTail(TargetStoreBackend.Reader reader, List<TargetStoreBackend.Edit> inputBusiness, TargetQuotaMutation stamp) {
        final byte[] rootRaw = reader.get(ColumnFamily.META, TargetMigrationRoot.keyFor(reader.shardId()));
        if (rootRaw == null) return;
        final var root = TargetMigrationStore.decodeRoot(rootRaw); if (!root.activated()) return;
        final byte[] raw = reader.get(ColumnFamily.META, TargetMigrationArchiveState.headKey(root.plan().migrationId()));
        if (raw == null) return;
        final var state = TargetMigrationArchiveState.decode(TargetValueEnvelope.decode(raw, TargetMigrationArchiveState.VALUE_TYPE).payload());
        if (state.taken()) return;
        if (isManagement(inputBusiness, stamp)) return;
        final var retirement = Objects.requireNonNull(TargetMigrationRetirementStore.selected(reader, List.of(), root, reader.aggregate().mutation()));
        state.requireRetirement(root, retirement);
        final var proof = reader.archiveTailProof();
        if (proof == null) {
            final Map<String, byte[]> evidence = new HashMap<>();
            for (var edit : inputBusiness) if (edit.family() == ColumnFamily.DEDUPE && edit.after() != null) {
                evidence.put(Bytes.hex(edit.key()), edit.after());
                final var value = TargetValueEnvelope.decodeAny(edit.after());
                if (value.valueType() != TargetResultRecord.VALUE_TYPE) continue;
                final var record = TargetResultRecord.decode(value.payload());
                if (!record.kind().position() || record.kind() == TargetResultRecord.Kind.POSITION_COMMAND_EXPIRED) continue;
                final byte[] firstKey = Bytes.concat(new byte[] {(byte) (record.kind() == TargetResultRecord.Kind.POSITION_SYSTEM
                        ? TargetKeyCodec.RESULT_SYSTEM_TAG : TargetKeyCodec.RESULT_COMMAND_TAG), 1}, record.logicalId());
                final byte[] first = reader.projected(ColumnFamily.DEDUPE, firstKey, inputBusiness);
                if (first != null) evidence.put(Bytes.hex(firstKey), first);
            }
            throw new TailPreparationRequired(root, retirement, state, reader.archiveReadView(), stamp, inputBusiness,
                    reader.closedIngressDeadlineThrough(), evidence);
        }
        proof.requireBusiness(inputBusiness); proof.requireTail(reader, root, retirement, proof.actual(), stamp);
    }

    private static boolean isManagement(List<TargetStoreBackend.Edit> edits, TargetQuotaMutation stamp) {
        for (var edit : edits) if (edit.family() == ColumnFamily.META && edit.after() != null && edit.key().length > 34
                && Byte.toUnsignedInt(edit.key()[0]) == TargetKeyCodec.MIGRATION_RETIREMENT_TAG) {
            final var value = TargetValueEnvelope.decodeAny(edit.after());
            if (value.valueType() == TargetMigrationArchiveState.VALUE_TYPE) {
                final var state = TargetMigrationArchiveState.decode(value.payload());
                if (state.history() && state.managementAt().equals(stamp)) return true;
            }
        }
        return false;
    }

    private static boolean sameEdits(List<TargetStoreBackend.Edit> a, List<TargetStoreBackend.Edit> b) {
        if (a.size() != b.size()) return false;
        for (int i = 0; i < a.size(); i++) if (a.get(i).family() != b.get(i).family()
                || !Arrays.equals(a.get(i).key(), b.get(i).key()) || !Arrays.equals(a.get(i).before(), b.get(i).before())
                || !Arrays.equals(a.get(i).after(), b.get(i).after())) return false;
        return true;
    }
    private static List<TargetStoreBackend.Edit> causeEdits(List<TargetStoreBackend.Edit> edits) {
        return edits.stream().filter(edit -> edit.family() == ColumnFamily.DEDUPE
                || edit.family() == ColumnFamily.META && edit.key().length >= 2
                    && Byte.toUnsignedInt(edit.key()[0]) == TargetKeyCodec.MIGRATION_PREFIX_TAG).toList();
    }

    private static void readLegacyTail(Held held, SourceReplayEntry actual, TargetMigrationPrefixRecord prefix, Map<String, Raw> rows) {
        final var limits = held.limits().companions(); final var budget = held.reads();
        final var manifest = prefix.sourceRecord().object().resource(); charge(budget, manifest);
        final var companion = LegacyMigrationBundle.readCompanionIndex(manifest, LegacyMigrationBundle.Section.CUSTODY,
                held.sealed().material(), held.original(), limits, held.objects());
        charge(budget, manifest); final byte[] manifestBytes = held.objects().readExact(manifest, limits.maxChunkBytes());
        new TargetMigrationObjectReference(manifest).requireContent(manifestBytes);
        int chunkIndex = 0;
        while (chunkIndex < companion.chunks().size() && prefix.sourceRecord().ordinal() >= companion.chunks().get(chunkIndex).firstOrdinal()
                + companion.chunks().get(chunkIndex).records()) chunkIndex++;
        if (chunkIndex == companion.chunks().size()) throw new IllegalStateException("prefix source header is outside its fixed inventory");
        final var chunk = companion.chunks().get(chunkIndex); charge(budget, chunk.object());
        final var records = LegacyMigrationBundle.readCompanionChunk(companion, chunkIndex, held.original(), limits, held.objects());
        final byte[] chunkBytes = chunkBytes(companion, chunk, records);
        final byte[] headerBytes = records.get(Math.toIntExact(prefix.sourceRecord().ordinal() - chunk.firstOrdinal()));
        requirePrefixHeader(prefix, headerBytes);
        final var h = header(headerBytes);
        if (!Arrays.equals(sourceDigest(actual), prefix.at().mutationDigest())
                || !Arrays.equals(actual.position().canonicalBytes(), h.actual().canonicalBytes()))
            throw new IllegalStateException("legacy tail changed the actual original Source frame");
        final var keys = new ArrayList<byte[]>();
        if (actual instanceof SourceReplayRecord command) {
            keys.add(KeyCodec.dedupeCommand(command.command().commandId())); keys.add(KeyCodec.dedupeResult(command.command().commandId()));
        } else keys.add(KeyCodec.dedupeSystemMutation(((SourceReplayMutation) actual).mutation().systemMutationId()));
        keys.add(KeyCodec.dedupePosition(actual.position().canonicalBytes()));
        final byte[] proof = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 4); CanonicalProtobuf.bytes(out, 2, manifest.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, manifestBytes); CanonicalProtobuf.bytes(out, 4, chunk.object().canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, chunkBytes); CanonicalProtobuf.uint64(out, 6, prefix.sourceRecord().ordinal());
            CanonicalProtobuf.bytes(out, 7, prefix.canonicalBytes());
        });
        held.shadow().store().readWithBudget(budget, () -> {
            if (!Arrays.equals(held.shadow().store().appliedShardLogPosition().canonicalBytes(), actual.position().canonicalBytes()))
                throw new IllegalStateException("legacy archive tail is not the actual old applier after-view");
            for (var key : keys) {
                final byte[] value = held.shadow().store().get(ColumnFamily.DEDUPE, key);
                if (value == null) continue;
                final var first = rawSource(key, value);
                if (first == null || !Arrays.equals(first.canonicalBytes(), actual.position().canonicalBytes())) continue;
                final var raw = new Raw(key, value, key[0] == 3 ? TargetMigrationArchiveEntry.Origin.PHYSICAL_ONLY
                        : TargetMigrationArchiveEntry.Origin.LEGACY_SOURCE_FIRST, key[0] == 3 ? null : first, prefix.at(), proof);
                requirePrefixRow(raw, prefix, headerBytes); rows.put(Bytes.hex(key), raw);
            }
            return Boolean.TRUE;
        });
        held.requireCurrent();
    }
    private static void charge(BoundedReadBudget budget, CheckpointResource resource) {
        if (!budget.beforeRead() || !budget.tryCharge(resource.canonicalBytes().length, Math.toIntExact(resource.manifestLength())))
            throw budget.incomplete();
    }
    private static byte[] sourceDigest(SourceReplayEntry actual) {
        return actual instanceof SourceReplayRecord command ? command.command().canonicalFrameDigest()
                : Bytes.sha256(((SourceReplayMutation) actual).mutation().canonicalEnvelope());
    }
    private static byte[] prospectiveProof(SourceReplayEntry actual, Map<String, byte[]> evidence, long fence) {
        final byte[] firstKey = actual instanceof SourceReplayRecord command
                ? Bytes.concat(new byte[] {(byte) TargetKeyCodec.RESULT_COMMAND_TAG, 1}, command.command().commandId().bytes())
                : Bytes.concat(new byte[] {(byte) TargetKeyCodec.RESULT_SYSTEM_TAG, 1}, ((SourceReplayMutation) actual).mutation().systemMutationId());
        final byte[] first = evidence.get(Bytes.hex(firstKey));
        final byte[] positionKey = Bytes.concat(new byte[] {(byte) TargetKeyCodec.RESULT_POSITION_TAG, 1}, actual.position().canonicalBytes());
        final byte[] position = Objects.requireNonNull(evidence.get(Bytes.hex(positionKey)), "actual prospective Source POSITION");
        final byte[] actualFrame = frame(actual);
        final var actualAt = TargetResultRecord.decode(TargetValueEnvelope.decode(position, TargetResultRecord.VALUE_TYPE).payload()).mutation();
        TargetMigrationArchiveSourceFrame.requireFrame(actualFrame, actualAt);
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 5); CanonicalProtobuf.bytes(out, 2, actualFrame);
            CanonicalProtobuf.bytes(out, 3, first == null ? new byte[0] : first); CanonicalProtobuf.bytes(out, 4, position);
            CanonicalProtobuf.bytes(out, 5, Bytes.u64beBits(fence));
        });
    }
    private static void requireProspectiveProof(Raw row, List<CanonicalProtobuf.Reader.Field> f, boolean fullSource) {
        QueryCodecSupport.requireNumbers(f, new int[] {1,2,3,4,5}, "prospective original Target Source");
        final byte[] frame = QueryCodecSupport.bytes(f.get(1), 2);
        requireSourceFrame(frame, row.provenance(), fullSource);
        final var wire = ShardLogFrame.decode(frame);
        final var position = TargetResultRecord.decode(TargetValueEnvelope.decode(QueryCodecSupport.bytes(f.get(3), 4), TargetResultRecord.VALUE_TYPE).payload());
        if (!position.kind().position() || !position.mutation().equals(row.provenance()))
            throw new IllegalStateException("archive tail changed its actual prospective physical Source");
        final byte[] firstRaw = QueryCodecSupport.bytes(f.get(2), 3);
        final var first = firstRaw.length == 0 ? null : TargetResultRecord.decode(TargetValueEnvelope.decode(firstRaw, TargetResultRecord.VALUE_TYPE).payload());
        final var selected = TargetResultRecord.decode(TargetValueEnvelope.decode(row.value(), TargetResultRecord.VALUE_TYPE).payload());
        final byte[] digest;
        if (wire.recordKind() == ProtocolTuple.CLIENT_COMMAND) {
            final var command = CommandCodec.decodeFrame(frame); digest = command.canonicalFrameDigest();
            if (!Arrays.equals(position.logicalId(), command.commandId().bytes())) throw new IllegalStateException("archive Target tail changed actual Command identity");
            if (position.kind() == TargetResultRecord.Kind.POSITION_COMMAND_EXPIRED) position.requireExpiredCommand(command);
            else position.requireFirst(Objects.requireNonNull(first));
            if (!selected.kind().position()) {
                if (first == null || first.kind() != TargetResultRecord.Kind.COMMAND || !first.mutation().equals(row.provenance()))
                    throw new IllegalStateException("archive Target first was not born in this actual Source batch");
                final var commandFirst = CommandDedupeRecord.decode(first.typedPayload());
                if (!commandFirst.protocolTuple().equals(command.protocolTuple()) || !Arrays.equals(commandFirst.commandHash(), command.commandHash()))
                    throw new IllegalStateException("archive Target first changed full original Command protocol/hash");
                if (selected.kind() == TargetResultRecord.Kind.RESULT) selected.requireFirst(first);
                else if (!Arrays.equals(row.value(), firstRaw)) throw new IllegalStateException("archive Target Command first changed raw bytes");
            }
        } else {
            final var input = SystemMutation.decodeTargetFrame(frame); digest = Bytes.sha256(input.canonicalEnvelope());
            position.requireFirst(Objects.requireNonNull(first));
            if (!Arrays.equals(position.logicalId(), input.systemMutationId()) || first.kind() != TargetResultRecord.Kind.SYSTEM)
                throw new IllegalStateException("archive Target System tail changed actual identity");
            final var result = SystemMutationResult.decode(first.typedPayload());
            if (!Arrays.equals(result.mutationId(), input.systemMutationId()) || !Arrays.equals(result.mutationHash(), input.mutationHash())
                    || result.mutationType() != input.type() || result.retryUntilEpochMs() != input.retryUntilEpochMs()
                    || !Arrays.equals(result.authorIdentity(), input.authorIdentity()))
                throw new IllegalStateException("archive Target System first changed original author/type/hash/retry");
            if (!selected.kind().position() && (!first.mutation().equals(row.provenance()) || !Arrays.equals(row.value(), firstRaw)))
                throw new IllegalStateException("archive Target System first changed original raw Source fact");
        }
        if (!Arrays.equals(digest, row.provenance().mutationDigest())
                || selected.kind().position() && !Arrays.equals(row.value(), QueryCodecSupport.bytes(f.get(3), 4)))
            throw new IllegalStateException("archive Source frame differs from the exact prospective Native mutation");
    }

    public static TransferMaterial finalReceiveMaterial(Held held, TargetMigrationArchiveState actual) {
        held.requireCurrent();
        if (!actual.coverage().complete() || actual.taken()) throw new IllegalStateException("archive base is not ready for exact receiver sealing");
        final var read = held.target().readWithBudget(held.reads(), () -> {
            held.requireLocalCurrent();
            final var selected = TargetMigrationArchiveState.decode(TargetValueEnvelope.decode(Objects.requireNonNull(
                    held.target().get(ColumnFamily.META, TargetMigrationArchiveState.headKey(held.intent().migration().plan().migrationId()))),
                    TargetMigrationArchiveState.VALUE_TYPE).payload());
            if (!selected.equals(actual) || !Arrays.equals(held.target().appliedShardLogPosition().canonicalBytes(), actual.coveredThrough().source().canonicalBytes())
                    || held.target().shardMutationSequence() != actual.coveredThrough().sequence())
                throw new IllegalStateException("receiver final seal changed the complete actual Native Source frontier");
            return selected;
        });
        held.target().withReadView(read.view(), () -> null);
        final var closed = TargetMigrationArchiveClosedContract.qualify(held.closedMaterials(), held.intent().migration(), held.intent().record(), actual);
        return new TransferMaterial(held, actual, actual.shadowCut(), actual.coveredThrough(), actual.coverage(), List.of(),
                Bytes.sha256(PACKET_DOMAIN, actual.digest(), actual.coveredThrough().canonicalBytes(), closed.digest()), actual.tailRows(),
                actual.tailRawBytes(), held.limits().maximumPacketBytes(), closed);
    }

    public static PreparedTakeover takeover(Held held, TargetMigrationArchiveState actual,
            TargetMigrationArchiveReceived received) {
        held.requireCurrent(); received.requireCurrent();
        if (actual.taken() || !actual.coverage().complete()) throw new IllegalStateException("archive base copy is not complete");
        final var intent = held.intent(); final var plan = intent.migration().plan();
        final var closed = TargetMigrationArchiveClosedContract.qualify(held.closedMaterials(), intent.migration(), intent.record(), actual);
        final var receipt = received.receipt();
        if (!Arrays.equals(receipt.archiveRoot(), actual.archiveRoot()) || !receipt.coverage().equals(actual.coverage())
                || !receipt.coveredThrough().equals(actual.coveredThrough()) || !receipt.shadowCut().equals(actual.shadowCut()))
            throw new IllegalStateException("new owner has not actually received the complete current archive tail");
        if (!Arrays.equals(receipt.sourceCoverageDigest(), Bytes.sha256(PACKET_DOMAIN, actual.digest(), actual.coveredThrough().canonicalBytes(), closed.digest())))
            throw new IllegalStateException("actual new-owner receipt lacks this complete original closed-contract qualification");
        final var request = new TargetMigrationArchiveRequest(plan.scope(), plan.newLineage(), plan.migrationId(), plan.digest(),
                intent.migration().activatedAt(), intent.intention().digest(), intent.intentAt(), actual.digest(), actual.archiveRoot(),
                new TargetMigrationArchiveRequest.Takeover(TargetQuotaMutation.increment(actual.generation()), actual.shadowCut(),
                        actual.coveredThrough(), actual.coverage(), receipt));
        received.requireTakeoverMaterial(intent.migration(), intent.record(), request);
        return new PreparedTakeover(held, received, actual, request, closed);
    }

    public static PreparedTakeover takeover(Held held, TargetMigrationArchiveState actual,
            TargetMigrationArchiveReceived received, SourcePosition actualSource) {
        final var result = takeover(held, actual, received);
        if (actualSource.compareTo(actual.coveredThrough().source()) <= 0)
            throw new IllegalArgumentException("TAKEOVER Source does not follow complete received frontier");
        return result;
    }

    private static byte[] frame(SourceReplayEntry entry) {
        return entry instanceof SourceReplayRecord command ? CommandCodec.encodeFrame(command.command())
                : ((SourceReplayMutation) entry).mutation().encodeFrame();
    }

    private static void requireSourceFrame(byte[] frame, TargetQuotaMutation at, boolean fullSource) {
        if (!fullSource && frame.length > TargetMigrationArchiveRequest.MAX_CANONICAL_BYTES)
            throw new IllegalArgumentException("legacy archive Source proof exceeds its original frame bound");
        TargetMigrationArchiveSourceFrame.requireFrame(frame, at);
    }
}
