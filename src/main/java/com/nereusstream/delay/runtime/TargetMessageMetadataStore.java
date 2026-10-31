package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Guarded metadata projection. No payload or historical binding payload escapes this read. */
public final class TargetMessageMetadataStore {
    public record Snapshot(
            TargetMessageLocator locator,
            long stateVersion,
            GenerationAggregateState state,
            StableCode terminalCode,
            PayloadAvailability payloadAvailability,
            boolean possibleDestinationDuplicate,
            SourcePosition source) {
        public Snapshot {
            Objects.requireNonNull(locator);
            Objects.requireNonNull(state);
            Objects.requireNonNull(payloadAvailability);
            TargetSourcePosition.requireBounded(source);
            if (stateVersion == 0 || !locator.messageId().routingId().shardId().equals(source.shardId())) {
                throw new IllegalArgumentException("invalid Target metadata cut");
            }
        }
    }

    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final byte[] lineage;

    public TargetMessageMetadataStore(TargetStoreBackend backend, TargetQuotaScope scope, byte[] lineage) {
        this.backend = Objects.requireNonNull(backend);
        this.scope = Objects.requireNonNull(scope);
        this.lineage = Bytes.copy(lineage);
        Bytes.requireLength(lineage, 16, "metadata lineage");
        if (scope.target() != null) {
            throw new IllegalArgumentException("metadata query requires its original Source scope");
        }
    }

    public Optional<Snapshot> read(
            BoundedReadBudget budget, DelayMessageId id, TargetStoreBackend.ReadAuthority authority) {
        Objects.requireNonNull(id);
        if (!id.routingId().shardId().equals(scope.shard())) {
            throw new IllegalArgumentException("metadata query crossed Source Shard");
        }
        return backend.guardedRead(budget, reader -> read(reader, id), authority);
    }

    private Optional<Snapshot> read(TargetStoreBackend.Reader reader, DelayMessageId id) {
        final byte[] key = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, TargetKeyCodec.KEY_FORMAT}, id.bytes());
        final byte[] raw = reader.get(ColumnFamily.META, key);
        final byte[] primary = reader.get(ColumnFamily.ID, TargetKeyCodec.message(id));
        if (raw == null) {
            if (primary != null) {
                throw new IllegalStateException("Target metadata primary has no frozen payload owner");
            }
            return Optional.empty();
        }
        final var owner = TargetQuotaPayloadOwner.decodeForStore(
                key,
                TargetValueEnvelope.decode(raw, TargetQuotaPayloadOwner.VALUE_TYPE)
                        .payload(),
                scope.shard(),
                scope.tenantScope());
        if (!Arrays.equals(owner.recoveryLineage(), lineage)) {
            throw new IllegalStateException("metadata owner lineage changed");
        }
        owner.mutation().requireAtOrBefore(reader.aggregate().mutation());
        if (owner.phase() == TargetQuotaPayloadOwner.Phase.RELEASED) {
            final var root = new TargetQuotaIdentity(
                    TargetQuotaIdentity.Kind.SHARD,
                    scope.shard(),
                    reader.aggregate().accountingIncarnation(),
                    null,
                    null);
            if (owner.kind() == TargetQuotaPayloadOwner.Kind.OBJECT && owner.committedPayload() == null) {
                TargetRetirementSourceHistoryStore.requirePayloadReclaim(
                        reader,
                        List.of(),
                        scope,
                        lineage,
                        root,
                        owner,
                        reader.aggregate().mutation());
                throw new IllegalStateException("query requires the original reservation metadata path");
            }
            final var operation = TargetRetirementSourceHistoryStore.releasedPayloadOperation(
                    reader,
                    List.of(),
                    scope,
                    lineage,
                    root,
                    owner,
                    reader.aggregate().mutation());
            final var terminal = terminal(reader, operation.locator());
            return Optional.of(snapshot(
                    terminal.locator(),
                    terminal.stateVersion(),
                    terminal.runtime(),
                    terminal.terminalCode(),
                    PayloadAvailability.PAYLOAD_RECLAIMED,
                    reader.source()));
        }
        if (primary == null) {
            if (owner.kind() == TargetQuotaPayloadOwner.Kind.OBJECT) {
                throw new IllegalStateException("query requires the original reservation metadata path");
            }
            throw new IllegalStateException("retained inline metadata lost its primary before a Source release");
        }
        final var message = TargetMessageRecord.decodeForStore(
                TargetKeyCodec.message(id),
                TargetValueEnvelope.decode(primary, TargetMessageRecord.VALUE_TYPE)
                        .payload(),
                scope.shard());
        owner.requireMessagePayload(message);
        final StableCode code;
        if (message.runtime().terminal()) {
            final var terminal = terminal(reader, message.locator());
            terminal.requireOwner(owner);
            if (terminal.stateVersion() != message.stateVersion()
                    || !Arrays.equals(
                            terminal.runtime().canonicalBytes(),
                            message.runtime().canonicalBytes())) {
                throw new IllegalStateException("Target terminal metadata differs from current primary");
            }
            code = terminal.terminalCode();
        } else {
            code = null;
        }
        return Optional.of(snapshot(
                message.locator(),
                message.stateVersion(),
                message.runtime(),
                code,
                owner.kind() == TargetQuotaPayloadOwner.Kind.INLINE
                        ? PayloadAvailability.INLINE_RETAINED
                        : PayloadAvailability.OBJECT_RETAINED,
                reader.source()));
    }

    private TargetTerminalGenerationRecord terminal(TargetStoreBackend.Reader reader, TargetMessageLocator locator) {
        final byte[] key = TargetTerminalGenerationRecord.key(locator), raw = reader.get(ColumnFamily.TERMINAL, key);
        if (raw == null) {
            throw new IllegalStateException("Target query lost its terminal metadata");
        }
        final var terminal = TargetTerminalGenerationRecord.decode(
                TargetValueEnvelope.decode(raw, TargetTerminalGenerationRecord.VALUE_TYPE)
                        .payload());
        if (!Arrays.equals(terminal.key(), key) || !Arrays.equals(terminal.recoveryLineage(), lineage)) {
            throw new IllegalStateException("Target terminal query changed key/lineage");
        }
        terminal.mutation().requireAtOrBefore(reader.aggregate().mutation());
        return terminal;
    }

    private static Snapshot snapshot(
            TargetMessageLocator locator,
            long version,
            TargetGenerationRuntimeIndex runtime,
            StableCode code,
            PayloadAvailability availability,
            SourcePosition source) {
        return new Snapshot(
                locator,
                version,
                runtime.aggregateState(),
                code,
                availability,
                runtime.possibleDestinationDuplicate(),
                source);
    }
}
