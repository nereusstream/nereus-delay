package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SystemMutation;
import java.util.Objects;

/**
 * The only authority allowed to assign a Source Position to a prepared
 * System Mutation. A production implementation is backed by the assigned
 * Kafka/Pulsar Command Topic partition; this interface deliberately does not
 * expose a local position allocator or a RocksDB apply shortcut.
 */
@FunctionalInterface
public interface ShardLogMutationAppender {
    AppendOutcome append(SystemMutation mutation);

    enum AppendDisposition {
        PERSISTED,
        DEFINITIVELY_NOT_PERSISTED,
        UNKNOWN
    }

    record AppendOutcome(
            AppendDisposition disposition,
            SourcePosition sourcePosition,
            Long sourceConnectionGeneration,
            byte[] guardAttestationDigest) {
        public AppendOutcome {
            Objects.requireNonNull(disposition, "disposition");
            if (sourceConnectionGeneration != null && sourceConnectionGeneration <= 0) {
                throw new IllegalArgumentException("sourceConnectionGeneration must be positive");
            }
            guardAttestationDigest = guardAttestationDigest == null ? null : Bytes.copy(guardAttestationDigest);
            if ((sourceConnectionGeneration == null) != (guardAttestationDigest == null)) {
                throw new IllegalArgumentException("source connection proof must be complete");
            }
            if (disposition == AppendDisposition.PERSISTED) {
                Objects.requireNonNull(sourcePosition, "persisted Source Position");
            } else if (sourcePosition != null) {
                throw new IllegalArgumentException("non-persisted append cannot carry a Source Position");
            } else if (sourceConnectionGeneration != null) {
                throw new IllegalArgumentException("non-persisted append cannot carry source connection proof");
            }
        }

        @Override
        public byte[] guardAttestationDigest() {
            return guardAttestationDigest == null ? null : Bytes.copy(guardAttestationDigest);
        }

        public static AppendOutcome persisted(final SourcePosition position) {
            return persisted(position, null, null);
        }

        public static AppendOutcome persisted(
                final SourcePosition position,
                final Long sourceConnectionGeneration,
                final byte[] guardAttestationDigest) {
            return new AppendOutcome(
                    AppendDisposition.PERSISTED,
                    Objects.requireNonNull(position, "position"),
                    sourceConnectionGeneration,
                    guardAttestationDigest);
        }

        public static AppendOutcome definitelyNotPersisted() {
            return new AppendOutcome(AppendDisposition.DEFINITIVELY_NOT_PERSISTED, null, null, null);
        }

        public static AppendOutcome unknown() {
            return new AppendOutcome(AppendDisposition.UNKNOWN, null, null, null);
        }
    }
}
