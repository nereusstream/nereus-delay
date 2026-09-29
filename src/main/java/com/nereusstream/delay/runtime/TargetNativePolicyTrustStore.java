package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.semantic.TargetNativePolicyTrust;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/** Guarded point-read view of source-applied Native authority history for Claim and Admission checks. */
public final class TargetNativePolicyTrustStore implements TargetNativePolicyTrust {
    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final int maximumReadRecords;
    private final long maximumReadBytes;
    private final long maximumReadElapsedNanos;
    private final LongSupplier monotonicClock;
    private final TargetStoreBackend.ReadAuthority reads;

    public TargetNativePolicyTrustStore(
            final TargetStoreBackend backend,
            final TargetQuotaScope scope,
            final byte[] lineage,
            final int maximumReadRecords,
            final long maximumReadBytes,
            final long maximumReadElapsedNanos,
            final LongSupplier monotonicClock,
            final TargetStoreBackend.ReadAuthority reads) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.scope = Objects.requireNonNull(scope, "scope");
        com.nereusstream.delay.protocol.Bytes.requireLength(lineage, 16, "recoveryLineage");
        if (scope.target() != null
                || java.util.Arrays.equals(lineage, new byte[16])
                || maximumReadRecords <= 0
                || maximumReadBytes <= 0
                || maximumReadElapsedNanos <= 0) {
            throw new IllegalArgumentException("Native trust requires bounded Shard Store reads");
        }
        this.lineage = com.nereusstream.delay.protocol.Bytes.copy(lineage);
        this.maximumReadRecords = maximumReadRecords;
        this.maximumReadBytes = maximumReadBytes;
        this.maximumReadElapsedNanos = maximumReadElapsedNanos;
        this.monotonicClock = Objects.requireNonNull(monotonicClock, "monotonicClock");
        this.reads = Objects.requireNonNull(reads, "reads");
    }

    @Override
    public Optional<PublisherPermission> publisher(
            final byte[] scopeDigest, final int keyGeneration, final SourcePosition asOf) {
        return read(reader -> {
            TargetNativePolicyStoreAuthority.requireQueryVisible(reader, asOf);
            return view(reader).publisher(scopeDigest, keyGeneration, asOf);
        });
    }

    @Override
    public Optional<Activation> activation(final byte[] scopeDigest, final long generation) {
        return read(reader -> view(reader).activation(scopeDigest, generation));
    }

    @Override
    public Optional<MemberApproval> member(
            final byte[] grantDigest, final byte[] scopeDigest, final SourcePosition asOf) {
        return read(reader -> {
            TargetNativePolicyStoreAuthority.requireQueryVisible(reader, asOf);
            return view(reader).member(grantDigest, scopeDigest, asOf);
        });
    }

    /** Activation and issuer history are resolved from one guarded Store view. */
    @Override
    public Activation requireTrusted(
            final TargetNativePolicyScope exactScope,
            final com.nereusstream.delay.protocol.TargetNativePolicySnapshot snapshot,
            final SourcePosition at) {
        return read(reader -> {
            TargetNativePolicyStoreAuthority.requireQueryVisible(reader, at);
            return view(reader).requireTrusted(exactScope, snapshot, at);
        });
    }

    private <T> T read(final java.util.function.Function<TargetStoreBackend.Reader, T> operation) {
        return backend.guardedRead(
                new BoundedReadBudget(maximumReadRecords, maximumReadBytes, maximumReadElapsedNanos, monotonicClock),
                operation,
                reads);
    }

    private TargetNativePolicyTrust view(final TargetStoreBackend.Reader reader) {
        return TargetNativePolicyStoreAuthority.inView(reader, scope, lineage);
    }
}
