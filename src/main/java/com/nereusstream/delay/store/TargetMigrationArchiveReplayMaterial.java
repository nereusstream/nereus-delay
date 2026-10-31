package com.nereusstream.delay.store;

import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.ownership.SourceReplayOutcome;
import com.nereusstream.delay.ownership.SourceReplayRecord;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetMigrationArchiveEntry;
import com.nereusstream.delay.protocol.TargetMigrationArchivePhysicalRecord;
import com.nereusstream.delay.protocol.TargetMigrationArchiveRequest;
import com.nereusstream.delay.protocol.TargetMigrationArchiveRow;
import com.nereusstream.delay.protocol.TargetMigrationArchiveSourceFrame;
import com.nereusstream.delay.protocol.TargetMigrationArchiveState;
import com.nereusstream.delay.protocol.TargetMigrationObjectReference;
import com.nereusstream.delay.protocol.TargetMigrationRecordReference;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.runtime.TargetMigrationArchiveClosedContract;
import com.nereusstream.delay.runtime.TargetMigrationArchiveStore;
import java.util.Arrays;
import java.util.Objects;

/** Actual post-T raw physical occurrence and custody append, before its ordinary Source Native commit. */
public final class TargetMigrationArchiveReplayMaterial implements TargetMigrationArchiveStore.ReplayInputs {
    private final TargetMigrationArchiveHeld.RuntimeHold owner;
    private final TargetMigrationArchiveStore.AppliedTakeover takeover;
    private final TargetQuotaMutation before;
    private final byte[] priorDigest, frameDigest, logicalDigest, result;
    private final SourcePosition source;
    private final long frozenFence;
    private final TargetMigrationObjectReference packet, inventory;
    private final byte[] closedProof;

    private TargetMigrationArchiveReplayMaterial(
            TargetMigrationArchiveHeld.RuntimeHold owner,
            TargetMigrationArchiveStore.AppliedTakeover takeover,
            TargetQuotaMutation before,
            byte[] priorDigest,
            byte[] frameDigest,
            byte[] logicalDigest,
            byte[] result,
            SourcePosition source,
            long frozenFence,
            TargetMigrationObjectReference packet,
            TargetMigrationObjectReference inventory) {
        this(
                owner,
                takeover,
                before,
                priorDigest,
                frameDigest,
                logicalDigest,
                result,
                source,
                frozenFence,
                packet,
                inventory,
                null);
    }

    private TargetMigrationArchiveReplayMaterial(
            TargetMigrationArchiveHeld.RuntimeHold owner,
            TargetMigrationArchiveStore.AppliedTakeover takeover,
            TargetQuotaMutation before,
            byte[] priorDigest,
            byte[] frameDigest,
            byte[] logicalDigest,
            byte[] result,
            SourcePosition source,
            long frozenFence,
            TargetMigrationObjectReference packet,
            TargetMigrationObjectReference inventory,
            byte[] closedProof) {
        this.owner = owner;
        this.takeover = takeover;
        this.before = before;
        this.priorDigest = Bytes.copy(priorDigest);
        this.frameDigest = Bytes.copy(frameDigest);
        this.logicalDigest = logicalDigest == null ? null : Bytes.copy(logicalDigest);
        this.result = Bytes.copy(result);
        this.source = source;
        this.frozenFence = frozenFence;
        this.packet = packet;
        this.inventory = inventory;
        this.closedProof = closedProof == null ? null : Bytes.copy(closedProof);
    }

    static TargetMigrationArchiveReplayMaterial prepare(
            TargetMigrationArchiveHeld.RuntimeHold owner,
            TargetMigrationArchiveStore.AppliedTakeover takeover,
            TargetMigrationArchiveState prior,
            TargetQuotaMutation before,
            SourceReplayEntry actual,
            TargetMigrationArchiveRow logical,
            SourceReplayOutcome outcome,
            long frozenFence) {
        owner.requireCurrent();
        takeover.requireCurrent();
        logical.requirePlan(takeover.migration().plan());
        if (!prior.taken()
                || prior.inventory() == null
                || !prior.intentAt().equals(takeover.retirement().intentAt())
                || !before.source().sameSourceIdentity(actual.position())
                || actual.position().compareTo(before.source()) <= 0
                || !outcome.position().equals(actual.position())) {
            throw new IllegalStateException("physical archive material changed actual T/head/predecessor/Source");
        }
        final var custody = owner.custody();
        final byte[] frame = frame(actual);
        final byte[] digest = sourceDigest(actual);
        final byte[] result = outcome.isCommand()
                ? outcome.commandResult().encode()
                : outcome.systemMutationResult().encode();
        final var stamp =
                new TargetQuotaMutation(TargetQuotaMutation.increment(before.sequence()), actual.position(), digest);
        TargetMigrationArchiveSourceFrame.requireFrame(frame, stamp);
        final var physical = new TargetMigrationArchivePhysicalRecord(
                outcome.isCommand() ? ProtocolTuple.CLIENT_COMMAND : ProtocolTuple.SYSTEM_MUTATION,
                logical.entry().originalKey(),
                logical.digest(),
                result,
                frozenFence);
        final byte[] raw = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 2);
            CanonicalProtobuf.bytes(out, 2, takeover.migration().plan().digest());
            CanonicalProtobuf.bytes(out, 3, takeover.retirement().intention().digest());
            CanonicalProtobuf.bytes(out, 4, takeover.retirement().intentAt().canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, takeover.state().digest());
            CanonicalProtobuf.bytes(out, 6, takeover.takeoverAt().canonicalBytes());
            CanonicalProtobuf.bytes(out, 7, before.canonicalBytes());
            CanonicalProtobuf.bytes(out, 8, actual.position().canonicalBytes());
            CanonicalProtobuf.bytes(out, 9, frame);
            CanonicalProtobuf.bytes(out, 10, logical.digest());
            CanonicalProtobuf.bytes(out, 11, physical.canonicalBytes());
            CanonicalProtobuf.bytes(out, 12, prior.inventory().canonicalBytes());
            CanonicalProtobuf.bytes(out, 13, prior.archiveRoot());
        });
        if (raw.length > custody.limits.maximumPacketBytes()) {
            throw new IllegalStateException(
                    "complete physical replay material exceeds its actual prepaid object window");
        }
        custody.owner.requireOriginalReplay(raw, takeover, logical, actual, outcome, frozenFence);
        final var previous = TargetMigrationArchiveInventory.decode(
                owner.readExact(prior.inventory(), custody.limits.maximumReceiptBytes()));
        if (!Arrays.equals(previous.archiveRoot(), prior.archiveRoot())
                || !previous.owner().equals(custody.owner.owner())
                || !previous.allocation().equals(custody.owner.allocation())) {
            throw new IllegalStateException("physical replay lost its actual prior receiving custody");
        }
        final int batches = Math.incrementExact(previous.packets());
        final long reserved = Math.addExact(
                previous.reservedBytes(), Math.addExact(raw.length, custody.limits.maximumReceiptBytes()));
        if (batches > custody.limits.maximumBatches() || reserved > custody.limits.maximumRawBytes()) {
            throw new IllegalStateException("physical replay exceeds its persistent original receiver allocation");
        }
        custody.owner.requireCapacity(
                takeover.migration(), takeover.retirement(), reserved, Math.multiplyExact(batches, 2));
        final String prefix =
                "archive/" + Bytes.hex(takeover.retirement().intention().digest()) + "/";
        final var packet = new TargetMigrationObjectReference(owner.putExact(
                prefix + "physical/" + Bytes.hex(Bytes.sha256(raw)) + ".pb", raw, custody.limits.maximumPacketBytes()));
        final var entry = new TargetMigrationArchiveEntry(
                KeyCodec.dedupePosition(actual.position().canonicalBytes()),
                TargetValueEnvelope.encode(TargetMigrationArchiveRow.VALUE_TYPE, physical.canonicalBytes()),
                TargetMigrationArchiveEntry.Origin.ARCHIVE_PHYSICAL,
                null,
                stamp,
                new TargetMigrationRecordReference(packet, 0));
        final byte[] archiveRoot = TargetMigrationArchiveRequest.appendRoot(prior.archiveRoot(), entry);
        final var next = new TargetMigrationArchiveInventory(
                takeover.retirement().intention().digest(),
                takeover.retirement().intentAt(),
                prior.shadowCut(),
                stamp,
                custody.owner.owner(),
                custody.owner.allocation(),
                prior.archiveRoot(),
                prior.inventory(),
                packet,
                archiveRoot,
                prior.coverage(),
                Bytes.sha256(previous.sourceCoverageDigest(), digest, result),
                1,
                batches,
                reserved,
                custody.owner.originalObligation(),
                custody.owner.retention(),
                Math.incrementExact(prior.tailRows()),
                Math.addExact(prior.tailRawBytes(), entry.rawValue().length));
        final byte[] encoded = next.canonicalBytes();
        if (encoded.length > custody.limits.maximumReceiptBytes()) {
            throw new IllegalStateException("full physical receiver evidence exceeds its prepaid immutable object");
        }
        final var inventory = new TargetMigrationObjectReference(owner.putExact(
                prefix + "inventory/"
                        + Bytes.hex(Bytes.sha256(
                                Bytes.sha256(raw),
                                archiveRoot,
                                prior.inventory().canonicalBytes())) + ".pb",
                encoded,
                custody.limits.maximumReceiptBytes()));
        owner.requireCurrent();
        return new TargetMigrationArchiveReplayMaterial(
                owner,
                takeover,
                before,
                prior.digest(),
                digest,
                logical.digest(),
                result,
                actual.position(),
                frozenFence,
                packet,
                inventory);
    }

    static TargetMigrationArchiveReplayMaterial prepareClosed(
            TargetMigrationArchiveHeld.RuntimeHold owner,
            TargetMigrationArchiveStore.AppliedTakeover takeover,
            TargetMigrationArchiveState prior,
            TargetMigrationArchiveClosedContract.Decision decision) {
        owner.requireCurrent();
        takeover.requireCurrent();
        Objects.requireNonNull(decision).requireCurrent();
        final var actual = decision.actual();
        final var before = decision.before();
        final var outcome = decision.outcome();
        if (!prior.taken()
                || prior.inventory() == null
                || !prior.intentAt().equals(takeover.retirement().intentAt())
                || !before.source().sameSourceIdentity(actual.position())
                || actual.position().compareTo(before.source()) <= 0) {
            throw new IllegalStateException("closed archive material changed actual T/head/predecessor/Source");
        }
        final var custody = owner.custody();
        final byte[] frame = frame(actual);
        final byte[] digest = sourceDigest(actual);
        final byte[] result = outcome.isCommand()
                ? outcome.commandResult().encode()
                : outcome.systemMutationResult().encode();
        final byte[] proof = decision.proofBytes();
        TargetMigrationArchiveSourceFrame.requireFrame(frame, decision.at());
        final byte[] raw = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 2);
            CanonicalProtobuf.bytes(out, 2, takeover.migration().plan().digest());
            CanonicalProtobuf.bytes(out, 3, takeover.retirement().intention().digest());
            CanonicalProtobuf.bytes(out, 4, takeover.retirement().intentAt().canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, takeover.state().digest());
            CanonicalProtobuf.bytes(out, 6, takeover.takeoverAt().canonicalBytes());
            CanonicalProtobuf.bytes(out, 7, before.canonicalBytes());
            CanonicalProtobuf.bytes(out, 8, actual.position().canonicalBytes());
            CanonicalProtobuf.bytes(out, 9, frame);
            CanonicalProtobuf.bytes(out, 11, result);
            CanonicalProtobuf.bytes(out, 12, prior.inventory().canonicalBytes());
            CanonicalProtobuf.bytes(out, 13, prior.archiveRoot());
            CanonicalProtobuf.bytes(out, 14, proof);
        });
        if (raw.length > custody.limits.maximumPacketBytes()) {
            throw new IllegalStateException("complete closed-contract raw material exceeds its actual prepaid packet");
        }
        custody.owner.requireOriginalPacket(raw, java.util.List.of(), takeover.migration(), takeover.retirement());
        final var previous = TargetMigrationArchiveInventory.decode(
                owner.readExact(prior.inventory(), custody.limits.maximumReceiptBytes()));
        if (!Arrays.equals(previous.archiveRoot(), prior.archiveRoot())
                || !previous.owner().equals(custody.owner.owner())
                || !previous.allocation().equals(custody.owner.allocation())) {
            throw new IllegalStateException("closed source lost its actual prior receiving custody");
        }
        final int batches = Math.incrementExact(previous.packets());
        final long reserved = Math.addExact(
                previous.reservedBytes(), Math.addExact(raw.length, custody.limits.maximumReceiptBytes()));
        if (batches > custody.limits.maximumBatches() || reserved > custody.limits.maximumRawBytes()) {
            throw new IllegalStateException("closed packet exceeds its persistent original receiver allocation");
        }
        custody.owner.requireCapacity(
                takeover.migration(), takeover.retirement(), reserved, Math.multiplyExact(batches, 2));
        final String prefix =
                "archive/" + Bytes.hex(takeover.retirement().intention().digest()) + "/";
        final var packet = new TargetMigrationObjectReference(owner.putExact(
                prefix + "physical/" + Bytes.hex(Bytes.sha256(raw)) + ".pb", raw, custody.limits.maximumPacketBytes()));
        final var entries = TargetMigrationArchiveStore.closedEntries(
                takeover, prior, decision, new TargetMigrationRecordReference(packet, 0));
        byte[] archiveRoot = prior.archiveRoot();
        long bytes = 0;
        for (var entry : entries) {
            archiveRoot = TargetMigrationArchiveRequest.appendRoot(archiveRoot, entry);
            bytes = Math.addExact(bytes, entry.rawValueLength());
        }
        final var next = new TargetMigrationArchiveInventory(
                takeover.retirement().intention().digest(),
                takeover.retirement().intentAt(),
                prior.shadowCut(),
                decision.at(),
                custody.owner.owner(),
                custody.owner.allocation(),
                prior.archiveRoot(),
                prior.inventory(),
                packet,
                archiveRoot,
                prior.coverage(),
                Bytes.sha256(previous.sourceCoverageDigest(), digest, result, proof),
                entries.size(),
                batches,
                reserved,
                custody.owner.originalObligation(),
                custody.owner.retention(),
                Math.addExact(prior.tailRows(), entries.size()),
                Math.addExact(prior.tailRawBytes(), bytes));
        final byte[] encoded = next.canonicalBytes();
        if (encoded.length > custody.limits.maximumReceiptBytes()) {
            throw new IllegalStateException("complete closed receiver evidence exceeds its immutable reservation");
        }
        final var inventory = new TargetMigrationObjectReference(owner.putExact(
                prefix + "inventory/"
                        + Bytes.hex(Bytes.sha256(
                                Bytes.sha256(raw),
                                archiveRoot,
                                prior.inventory().canonicalBytes())) + ".pb",
                encoded,
                custody.limits.maximumReceiptBytes()));
        decision.requireCurrent();
        owner.requireCurrent();
        return new TargetMigrationArchiveReplayMaterial(
                owner,
                takeover,
                before,
                prior.digest(),
                digest,
                null,
                result,
                actual.position(),
                decision.fence(),
                packet,
                inventory,
                proof);
    }

    @Override
    public TargetMigrationRecordReference sourceRecord(SourceReplayEntry actual) {
        requireEntry(actual);
        return new TargetMigrationRecordReference(packet, 0);
    }

    @Override
    public TargetMigrationObjectReference inventory() {
        requireCurrent();
        return inventory;
    }

    @Override
    public void requireFence(long actualFrozenFence) {
        requireCurrent();
        if (actualFrozenFence != frozenFence) {
            throw new IllegalStateException("physical replay changed its actually captured original Source fence");
        }
    }

    @Override
    public void requirePredecessor(TargetMigrationArchiveState actualHead, TargetQuotaMutation actualBefore) {
        requireCurrent();
        if (!before.equals(actualBefore) || !Arrays.equals(priorDigest, actualHead.digest())) {
            throw new IllegalStateException("physical replay changed its exact original Native head/cut");
        }
    }

    @Override
    public void requireReplay(
            TargetMigrationArchiveStore.AppliedTakeover actual,
            TargetMigrationArchiveRow logical,
            SourceReplayEntry entry,
            SourceReplayOutcome outcome) {
        if (closedProof != null) {
            throw new IllegalStateException("closed-contract material cannot stand for an existing logical first");
        }
        requireEntry(entry);
        final byte[] encoded = outcome.isCommand()
                ? outcome.commandResult().encode()
                : outcome.systemMutationResult().encode();
        if (!actual.takeoverAt().equals(takeover.takeoverAt())
                || !actual.request().equals(takeover.request())
                || !Arrays.equals(logicalDigest, logical.digest())
                || !Arrays.equals(result, encoded)) {
            throw new IllegalStateException("physical replay changed its complete actual T/old first/result");
        }
    }

    public void requireClosed(
            TargetMigrationArchiveClosedContract.Decision decision,
            SourceReplayEntry actual,
            SourceReplayOutcome outcome) {
        requireEntry(actual);
        Objects.requireNonNull(decision).requireCurrent();
        final byte[] encoded = outcome.isCommand()
                ? outcome.commandResult().encode()
                : outcome.systemMutationResult().encode();
        if (closedProof == null
                || !Arrays.equals(closedProof, decision.proofBytes())
                || !before.equals(decision.before())
                || frozenFence != decision.fence()
                || !Arrays.equals(result, encoded)) {
            throw new IllegalStateException(
                    "closed material changed its private full contract/Source/predecessor/fence/outcome");
        }
    }

    public void requireClosed(
            TargetMigrationArchiveStore.AppliedTakeover actual,
            TargetMigrationArchiveClosedContract.Decision decision,
            SourceReplayEntry entry,
            SourceReplayOutcome outcome) {
        if (!actual.takeoverAt().equals(takeover.takeoverAt())
                || !actual.request().equals(takeover.request())) {
            throw new IllegalStateException("closed material changed actual Source T");
        }
        requireClosed(decision, entry, outcome);
    }

    private void requireEntry(SourceReplayEntry actual) {
        requireCurrent();
        if (!source.equals(actual.position()) || !Arrays.equals(frameDigest, sourceDigest(actual))) {
            throw new IllegalStateException("physical replay changed its original complete Source occurrence");
        }
    }

    @Override
    public void requireCurrent() {
        owner.requireLocalCurrent();
    }

    static void requirePacket(
            byte[] raw,
            TargetMigrationArchiveRow row,
            TargetCheckpointDependencies dependencies,
            TargetMigrationArchiveCustody.NewOwner owner) {
        row.entry().originalRecord().object().requireContent(raw);
        final var f = QueryCodecSupport.read(raw, "actual archived physical Source packet");
        final boolean closed = f.getLast().number() == 14;
        QueryCodecSupport.requireNumbers(
                f,
                closed
                        ? new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14}
                        : new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
                "archived physical Source packet");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != 2
                || row.entry().originalRecord().ordinal() != 0) {
            throw new IllegalArgumentException("unknown archived physical packet codec/ordinal");
        }
        final var root = Objects.requireNonNull(dependencies.migrationRoot());
        final var retirement = Objects.requireNonNull(dependencies.migrationRetirement());
        final var source = TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(7), 8));
        final byte[] frame = QueryCodecSupport.bytes(f.get(8), 9);
        final int frameLimit = row.schemaVersion() == TargetMigrationArchiveRow.V1_SCHEMA_VERSION
                ? TargetMigrationArchiveRequest.MAX_CANONICAL_BYTES
                : TargetMigrationArchiveSourceFrame.MAX_FRAME_BYTES;
        if (frame.length > frameLimit) {
            throw new IllegalStateException("archive packet Source frame exceeds its actual Native reader schema");
        }
        TargetMigrationArchiveSourceFrame.requireFrame(frame, row.materializedAt());
        final var before = TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(6), 7));
        final var step =
                Objects.requireNonNull(dependencies.migrationArchiveState(TargetMigrationArchiveState.historyKey(
                        root.plan().migrationId(),
                        TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(5), 6))
                                .sequence())));
        if (!step.taken()
                || !Arrays.equals(
                        QueryCodecSupport.fixed(f.get(1), 2, 32), root.plan().digest())
                || !Arrays.equals(
                        QueryCodecSupport.fixed(f.get(2), 3, 32),
                        retirement.intention().digest())
                || !TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(3), 4))
                        .equals(retirement.intentAt())
                || !Arrays.equals(QueryCodecSupport.fixed(f.get(4), 5, 32), step.digest())
                || !source.equals(row.materializedAt().source())
                || row.materializedAt().sequence() != TargetQuotaMutation.increment(before.sequence())
                || !Arrays.equals(frame, row.sourceFrame())) {
            throw new IllegalStateException(
                    "archived physical raw content changed actual I/T/Source/first/Native stamp");
        }
        if (closed) {
            TargetMigrationArchiveClosedContract.requireClosedProof(
                    QueryCodecSupport.bytes(f.getLast(), 14), root, retirement, row);
            final byte[] expected = QueryCodecSupport.bytes(f.get(9), 11);
            final byte[] actual;
            if (row.entry().namespace() == 3) {
                actual = TargetMigrationArchivePhysicalRecord.decode(
                                TargetValueEnvelope.decode(row.entry().rawValue(), TargetMigrationArchiveRow.VALUE_TYPE)
                                        .payload())
                        .outcome();
            } else if (row.entry().namespace() == 1) {
                actual = Objects.requireNonNull(
                                com.nereusstream.delay.runtime.TargetMigrationArchiveSemantics.decode(row.entry())
                                        .command(),
                                "closed archive Command has no actual original result")
                        .encode();
            } else if (row.entry().namespace() == 2) {
                actual = com.nereusstream.delay.runtime.CommandResult.decode(
                                ValueEnvelope.decode(row.entry().rawValue(), 2).payload())
                        .encode();
            } else if (row.entry().namespace() == 5) {
                actual = com.nereusstream.delay.runtime.SystemMutationResult.decode(ValueEnvelope.decode(
                                        row.entry().rawValue(),
                                        com.nereusstream.delay.runtime.SystemMutationResult.VALUE_TYPE)
                                .payload())
                        .encode();
            } else {
                throw new IllegalStateException("closed Source packet changed its original logical/physical namespace");
            }
            if (!Arrays.equals(expected, actual)) {
                throw new IllegalStateException("closed raw packet differs from its actual immutable Source result");
            }
            if (row.entry().namespace() == 3) {
                TargetMigrationArchiveStore.requirePhysical(dependencies, row);
            }
            owner.requireOriginalPacket(raw, java.util.List.of(row.entry()), root, retirement);
            return;
        }
        final var physical = TargetMigrationArchivePhysicalRecord.decode(QueryCodecSupport.bytes(f.get(10), 11));
        if (!Arrays.equals(
                physical.canonicalBytes(),
                TargetValueEnvelope.decode(row.entry().rawValue(), TargetMigrationArchiveRow.VALUE_TYPE)
                        .payload())) {
            throw new IllegalStateException("archived physical packet differs from its actual immutable Native bytes");
        }
        final var logical = Objects.requireNonNull(dependencies.migrationArchive(physical.firstOriginalKey()));
        if (!Arrays.equals(logical.digest(), QueryCodecSupport.fixed(f.get(9), 10, 32))) {
            throw new IllegalStateException("archived physical packet changed original logical first");
        }
        com.nereusstream.delay.runtime.TargetMigrationArchiveStore.requirePhysical(dependencies, row);
        owner.requireOriginalPacket(raw, java.util.List.of(row.entry()), root, retirement);
    }

    public long retainedBytes() {
        return 128L
                + priorDigest.length
                + frameDigest.length
                + (logicalDigest == null ? 0 : logicalDigest.length)
                + result.length
                + source.canonicalBytes().length
                + packet.canonicalBytes().length
                + inventory.canonicalBytes().length
                + (closedProof == null ? 0 : closedProof.length);
    }

    static byte[] frame(SourceReplayEntry actual) {
        final byte[] frame = actual instanceof SourceReplayRecord command
                ? CommandCodec.encodeFrame(command.command())
                : ((SourceReplayMutation) actual).mutation().encodeFrame();
        if (frame.length > TargetMigrationArchiveSourceFrame.MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "actual complete Source frame exceeds its registered Source codec bound");
        }
        return frame;
    }

    static byte[] sourceDigest(SourceReplayEntry actual) {
        return actual instanceof SourceReplayRecord command
                ? command.command().canonicalFrameDigest()
                : Bytes.sha256(((SourceReplayMutation) actual).mutation().canonicalEnvelope());
    }
}
