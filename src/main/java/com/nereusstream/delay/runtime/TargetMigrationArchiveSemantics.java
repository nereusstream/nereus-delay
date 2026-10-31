package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.*;
import java.util.Arrays;
import java.util.Objects;

/** Original logical results remain distinct from archive materialization and physical-position evidence. */
public final class TargetMigrationArchiveSemantics {
    private TargetMigrationArchiveSemantics() {}

    public record Decoded(ProtocolTuple commandTuple, byte[] semanticHash, CommandResult command,
            SystemMutationResult system, byte[] physicalIdentity, SourcePosition physicalPosition) {
        public Decoded {
            semanticHash = semanticHash == null ? null : Bytes.copy(semanticHash);
            physicalIdentity = physicalIdentity == null ? null : Bytes.copy(physicalIdentity);
        }
        @Override public byte[] semanticHash() { return semanticHash == null ? null : Bytes.copy(semanticHash); }
        @Override public byte[] physicalIdentity() { return physicalIdentity == null ? null : Bytes.copy(physicalIdentity); }
    }

    public static void requireEntry(TargetMigrationArchiveEntry entry, TargetMigrationPlan plan) {
        Objects.requireNonNull(entry); Objects.requireNonNull(plan);
        plan.baseline().requireAtOrBefore(entry.provenanceCut());
        if (!entry.provenanceCut().source().sameSourceIdentity(plan.baseline().source()))
            throw new IllegalArgumentException("archive changed its original Source resource");
        final var value = decode(entry);
        final SourcePosition original = value.command() != null
                ? SourcePositionCodec.decode(value.command().appliedSourcePosition())
                : value.system() != null ? SourcePositionCodec.decode(value.system().appliedSourcePosition()) : value.physicalPosition();
        if (original != null) {
            TargetSourcePosition.requireBounded(original);
            if (!original.sameSourceIdentity(plan.baseline().source()) || original.compareTo(entry.provenanceCut().source()) > 0
                    || entry.originalFirst() != null && !Arrays.equals(original.canonicalBytes(), entry.originalFirst().canonicalBytes()))
                throw new IllegalArgumentException("archive original result exceeds or changes its authenticated provenance");
        }
    }

    public static Decoded decode(TargetMigrationArchiveEntry entry) {
        final byte[] key = entry.originalKey();
        final var envelope = TargetValueEnvelope.decodeAny(entry.rawValue());
        final int namespace = entry.namespace();
        if (namespace == 3 && entry.origin() == TargetMigrationArchiveEntry.Origin.ARCHIVE_PHYSICAL) {
            if (envelope.valueType() != TargetMigrationArchiveRow.VALUE_TYPE)
                throw new IllegalArgumentException("archive replay physical evidence changed its dedicated Native format");
            final var physical = TargetMigrationArchivePhysicalRecord.decode(envelope.payload());
            final byte[] first = physical.firstOriginalKey();
            if (first == null) return new Decoded(null, null, null, null, null,
                    TargetSourcePosition.decode(Arrays.copyOfRange(key, 2, key.length)));
            if (first.length != (physical.recordKind() == ProtocolTuple.CLIENT_COMMAND ? CommandId.LENGTH + 2 : 34)
                    || first[1] != 1) throw new IllegalArgumentException("archive physical evidence changed original first key");
            return new Decoded(null, null, null, null, Arrays.copyOfRange(first, 2, first.length),
                    TargetSourcePosition.decode(Arrays.copyOfRange(key, 2, key.length)));
        }
        if (envelope.valueType() == TargetResultRecord.VALUE_TYPE) {
            final var record = TargetResultRecord.decode(envelope.payload());
            record.requireStored(record.key(), envelope.valueType(), envelope.payload());
            if (!Arrays.equals(originalKey(record), key))
                throw new IllegalArgumentException("archive Native result changed its original logical namespace/key");
            record.mutation().requireAtOrBefore(entry.provenanceCut());
            if (namespace != 3 && entry.origin() != TargetMigrationArchiveEntry.Origin.TARGET_SOURCE_FIRST)
                throw new IllegalArgumentException("Target first lacks its explicit original Target provenance");
            return switch (record.kind()) {
                case COMMAND -> {
                    final var command = CommandDedupeRecord.decode(record.typedPayload());
                    yield new Decoded(command.protocolTuple(), command.commandHash(), command.result(), null, null, null);
                }
                case RESULT -> new Decoded(null, null, CommandResult.decode(record.typedPayload()), null, null, null);
                case SYSTEM -> {
                    final var system = SystemMutationResult.decode(record.typedPayload());
                    yield new Decoded(null, system.mutationHash(), null, system, null, null);
                }
                case POSITION_COMMAND, POSITION_SYSTEM, POSITION_COMMAND_EXPIRED ->
                    new Decoded(null, null, null, null, record.logicalId(), record.mutation().source());
            };
        }
        if (namespace == 1) {
            if (envelope.valueType() != 1 || key.length != 2 + CommandId.LENGTH)
                throw new IllegalArgumentException("original Command archive has another key/value format");
            final var command = CommandDedupeRecord.decode(envelope.payload());
            final var id = new CommandId(Arrays.copyOfRange(key, 2, key.length));
            if (!id.routingId().shardId().equals(entry.provenanceCut().source().shardId()))
                throw new IllegalArgumentException("original Command archive belongs to another Shard");
            return new Decoded(command.protocolTuple(), command.commandHash(), command.result(), null, null, null);
        }
        if (namespace == 2) {
            if (envelope.valueType() != 2 || key.length != 2 + CommandId.LENGTH)
                throw new IllegalArgumentException("original query result archive changed format");
            return new Decoded(null, null, CommandResult.decode(envelope.payload()), null, null, null);
        }
        if (namespace == 3) {
            if (envelope.valueType() != 3 || envelope.payload().length != CommandId.LENGTH && envelope.payload().length != 32)
                throw new IllegalArgumentException("original POSITION archive changed its closed identity union");
            final var position = TargetSourcePosition.decode(Arrays.copyOfRange(key, 2, key.length));
            return new Decoded(null, null, null, null, envelope.payload(), position);
        }
        if (namespace == 5) {
            if (envelope.valueType() != SystemMutationResult.VALUE_TYPE || key.length != 34)
                throw new IllegalArgumentException("original System first archive changed format");
            final var result = SystemMutationResult.decode(envelope.payload());
            if (!Arrays.equals(result.encode(), envelope.payload())
                    || !Arrays.equals(result.mutationId(), Arrays.copyOfRange(key, 2, key.length)))
                throw new IllegalArgumentException("original System first archive changed exact identity/result bytes");
            return new Decoded(null, result.mutationHash(), null, result, null, null);
        }
        if (namespace != 4 || entry.origin() != TargetMigrationArchiveEntry.Origin.COMPACT_BASELINE)
            throw new IllegalArgumentException("unrecognized original logical archive namespace");
        return new Decoded(null, null, null, null, null, null);
    }

    public static byte[] originalKey(TargetResultRecord record) {
        return switch (record.kind()) {
            case COMMAND -> Bytes.concat(new byte[] {1, 1}, record.logicalId());
            case RESULT -> Bytes.concat(new byte[] {2, 1}, record.logicalId());
            case SYSTEM -> Bytes.concat(new byte[] {5, 1}, record.logicalId());
            case POSITION_COMMAND, POSITION_SYSTEM, POSITION_COMMAND_EXPIRED -> KeyCodec.dedupePosition(record.mutation().source().canonicalBytes());
        };
    }

    public static void requireEquivalent(TargetMigrationArchiveEntry existing, TargetMigrationArchiveEntry incoming) {
        if (!Arrays.equals(existing.originalKey(), incoming.originalKey()))
            throw new IllegalArgumentException("archive comparison split an original logical identity");
        final var a = decode(existing); final var b = decode(incoming);
        if (!Objects.equals(a.commandTuple(), b.commandTuple()) || !Arrays.equals(a.semanticHash(), b.semanticHash())
                || !Objects.equals(a.command(), b.command())
                || !Arrays.equals(a.system() == null ? null : a.system().encode(), b.system() == null ? null : b.system().encode())
                || !Arrays.equals(a.physicalIdentity(), b.physicalIdentity())
                || !Arrays.equals(a.physicalPosition() == null ? null : a.physicalPosition().canonicalBytes(),
                    b.physicalPosition() == null ? null : b.physicalPosition().canonicalBytes())
                || existing.namespace() == 4 && !Arrays.equals(existing.rawValue(), incoming.rawValue()))
            throw new IllegalStateException("archive encountered conflicting original first/decision/protocol evidence");
    }

    public static CommandResult commandOutcome(TargetMigrationArchiveEntry entry, PreparedCommand command, SourcePosition actual) {
        return commandOutcome(entry, command, actual, IngressFenceState.OPEN);
    }

    public static CommandResult commandOutcome(TargetMigrationArchiveEntry entry, PreparedCommand command,
            SourcePosition actual, long closedIngressDeadlineThrough) {
        if (entry.namespace() != 1 || !Arrays.equals(entry.originalKey(), KeyCodec.dedupeCommand(command.commandId())))
            throw new IllegalArgumentException("logical Command replay requires its exact original Command first");
        final var first = decode(entry);
        final boolean expired = actual.brokerPersistenceTimeEpochMs() > command.retryUntilEpochMs()
                || closedIngressDeadlineThrough >= command.retryUntilEpochMs();
        if (expired || !command.protocolTuple().equals(first.commandTuple())
                || !Arrays.equals(command.commandHash(), first.semanticHash()))
            return new CommandResult(ApplyStatus.REJECTED,
                    expired ? StableCode.COMMAND_RETRY_WINDOW_EXPIRED : StableCode.COMMAND_ID_CONFLICT,
                    -1, 0, null, actual.canonicalBytes());
        return Objects.requireNonNull(first.command(), "original logical Command result");
    }

    public static SystemMutationResult systemOutcome(TargetMigrationArchiveEntry entry, SystemMutation input, SourcePosition actual) {
        return systemOutcome(entry, input, actual, IngressFenceState.OPEN);
    }

    public static SystemMutationResult systemOutcome(TargetMigrationArchiveEntry entry, SystemMutation input,
            SourcePosition actual, long closedIngressDeadlineThrough) {
        if (entry.namespace() != 5 || !Arrays.equals(entry.originalKey(), KeyCodec.dedupeSystemMutation(input.systemMutationId())))
            throw new IllegalArgumentException("logical System replay requires its exact original System first");
        final var first = Objects.requireNonNull(decode(entry).system());
        if (first.mutationType() != input.type() || first.retryUntilEpochMs() != input.retryUntilEpochMs()
                || !Arrays.equals(first.mutationHash(), input.mutationHash())
                || !Arrays.equals(first.authorIdentity(), input.authorIdentity()))
            throw new IllegalStateException("original System identity was reused with different immutable fields");
        if (actual.brokerPersistenceTimeEpochMs() > input.retryUntilEpochMs()
                || closedIngressDeadlineThrough >= input.retryUntilEpochMs())
            return SystemMutationResult.from(input, ApplyStatus.REJECTED, StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED, actual.canonicalBytes());
        return first;
    }
}
