package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.store.RenewableS3CompatibleCheckpointObjectStoreAdapter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Exact Prepared/frame custody in the original immutable migration companion namespace. Never a second Source log. */
public final class TargetMigrationPreparedCustody implements TargetMigrationSourceExposure.PreparedCustody {
    /** Real original owner/credential/storage/retention/cost loan. This interface supplies no signing algorithm. */
    public interface Backing extends CheckpointInputFuture.Cleanup, AutoCloseable {
        RenewableS3CompatibleCheckpointObjectStoreAdapter objects();
        byte[] lineage();
        byte[] checkpointId();
        int maximumObjectBytes();
        long retainedBytes();
        /** Authenticates exact original Source, manager purpose, prepaid frame/copies and protected lifetime. */
        void requireBound(TargetMigrationSourceExposure.PreparedRequest request);
        /**
         * Independent original conditional-write/no-delete/read-after-write ownership and original exposure history.
         * An already exposed/UNKNOWN frame whose retained object is unavailable must fail, never sign a replacement.
         */
        void requireMayCreate(TargetMigrationSourceExposure.PreparedRequest request);
        /**
         * Actual original physical-capacity loan covering GET accumulators/response copies, version GET,
         * immutable object/getter copies, decoding/canonical scratch and retained Prepared objects.
         * An encoded object limit does not certify JVM/heap capacity.
         */
        default void requireReadAndCodecCapacity(TargetMigrationSourceExposure.PreparedRequest request) {
            throw new IllegalStateException("original Prepared provider/codec physical capacity is unqualified");
        }
        void requireCurrent();
        void requireLocalCurrent();
        boolean ioExited();
        void requestStop();
        @Override void close();
    }
    private static final byte[] FRAME = Bytes.utf8("NDIP3-F2-PREPARED-FRAME-1\0");
    private final Backing backing;
    private final TargetMigrationSourceExposure.PreparedRequest request;
    private TargetMigrationControlMutationFactory.Prepared exact;
    private TargetMigrationObjectReference object;
    private long retainedPreparedBytes;
    private CheckpointInputFuture.Cleanup operationFailure;
    private CheckpointInputFuture.Cleanup responseFailure;
    private CompletableFuture<Void> cleanupAttempt;
    private volatile boolean closing;

    private TargetMigrationPreparedCustody(Backing backing, TargetMigrationSourceExposure.PreparedRequest request) {
        this.backing = backing; this.request = request;
    }

    static TargetMigrationPreparedCustody retainOrRecover(TargetMigrationSourceExposure.ManagementHandle source,
            TargetMigrationSourceExposure.PreparedRequest request,
            Supplier<TargetMigrationControlMutationFactory.Prepared> signer, Runnable current) {
        if (!(request.opening() instanceof TargetMigrationMaintenance.Opening live))
            throw new IllegalArgumentException("exact live custody requires its privately minted actual maintenance acquisition");
        final Backing backing = Objects.requireNonNull(source.maintenancePreparedBacking(request), "original prepaid Prepared object backing");
        final var held = new TargetMigrationPreparedCustody(backing, request);
        // A late provider response or partially failed credential/HTTP operation remains owned before first GET/PUT.
        live.retainCleanup(held);
        try {
            current.run(); backing.requireBound(request); backing.requireCurrent();
            final long remaining = Math.subtractExact(live.remainingPreparedBytes(), backing.retainedBytes());
            final int maximum = Math.toIntExact(Math.min(Math.min(backing.maximumObjectBytes(),
                    live.configuration().maximumBytesPerTurn()), remaining));
            if (maximum < 1024 || maximum == Integer.MAX_VALUE || backing.retainedBytes() < 0)
                throw new IllegalArgumentException("Prepared custody requires the original finite object/input envelope");
            backing.requireReadAndCodecCapacity(request);
            final String prefix = "prepared-" + Bytes.hex(request.candidateId());
            final var existing = backing.objects().lookupMigrationObject(backing.lineage(), backing.checkpointId(), prefix + ".frame", maximum);
            if (existing.isPresent()) { held.restore(existing.orElseThrow().resource(), existing.orElseThrow().bytes()); current.run(); return held; }
            backing.requireMayCreate(request); current.run();
            // This in-memory candidate is unregistered and has never entered a Source SDK. Only the immutable
            // fixed-key conditional-PUT winner, independently GET/read back below, may become the exact frame.
            // A crash before persistence therefore leaves no unsigned claim that can permanently block recovery.
            final var signed = Objects.requireNonNull(signer.get(), "complete original Prepared and signed SDK frame");
            final byte[] bytes = encode(request, signed);
            if (bytes.length > maximum) throw new Unavailable("complete Prepared/frame exceeds original immutable-object envelope", held);
            try { backing.objects().putMigrationObject(backing.lineage(), backing.checkpointId(), prefix + ".frame", bytes, maximum); }
            catch (RuntimeException uncertain) {
                held.retainFailure(uncertain);
                final var observed = backing.objects().lookupMigrationObject(backing.lineage(), backing.checkpointId(), prefix + ".frame", maximum);
                if (observed.isEmpty()) throw uncertain;
                held.restore(observed.orElseThrow().resource(), observed.orElseThrow().bytes());
                current.run(); return held;
            }
            final var durable = backing.objects().lookupMigrationObject(backing.lineage(), backing.checkpointId(), prefix + ".frame", maximum).orElseThrow();
            held.restore(durable.resource(), durable.bytes()); current.run(); return held;
        } catch (RuntimeException | Error failure) {
            if (failure instanceof Unavailable retained) throw retained;
            held.retainFailure(failure);
            throw new Unavailable("original exact Prepared custody is unconfirmed; no Source SDK exposure is permitted", failure, held);
        }
    }

    private void retainFailure(Throwable failure) {
        Throwable actual = failure;
        while (actual instanceof java.util.concurrent.CompletionException && actual.getCause() != null && actual.getCause() != actual)
            actual = actual.getCause();
        if (actual instanceof CheckpointInputFuture.Cleanup pending) {
            if (operationFailure == null) operationFailure = pending;
            else if (operationFailure != pending) responseFailure = pending;
        }
    }

    private void restore(CheckpointResource resource, byte[] bytes) {
        new TargetMigrationObjectReference(resource).requireContent(bytes);
        final var restored = decodeExact(bytes, request.request()); exact = restored.exact();
        retainedPreparedBytes = Math.addExact(restored.controlBytes(), restored.sdkFrameBytes());
        object = new TargetMigrationObjectReference(resource);
    }
    /** Canonical byte decoding only. The independent startup Source owner still authenticates keys/actor/registration/cost/retention. */
    public static TargetMigrationControlMutationFactory.Prepared restoreExact(byte[] bytes, TargetMigrationRequest request) {
        return decodeExact(bytes, request).exact();
    }
    private record Restored(TargetMigrationControlMutationFactory.Prepared exact, int controlBytes, int sdkFrameBytes) {}
    private static Restored decodeExact(byte[] bytes, TargetMigrationRequest request) {
        Objects.requireNonNull(bytes); Objects.requireNonNull(request);
        final var buffer = ByteBuffer.wrap(bytes); final byte[] magic = new byte[FRAME.length]; buffer.get(magic);
        if (!Arrays.equals(magic, FRAME)) throw new IllegalStateException("unknown Prepared companion schema");
        final byte[] candidate = new byte[32]; buffer.get(candidate);
        final byte[] original = take(buffer, bytes.length);
        final byte[] control = take(buffer, bytes.length);
        final byte[] sdkFrame = take(buffer, bytes.length);
        if (buffer.hasRemaining() || !Arrays.equals(candidate, TargetMigrationSourceExposure.PreparedRequest.candidateId(request))
                || !Arrays.equals(original, request.canonicalBytes()))
            throw new IllegalStateException("original Prepared candidate cannot change its request, deadline or resources");
        final var restored = TargetMigrationControlMutationFactory.Prepared.restore(PreparedControlOperation.decode(control), SystemMutation.decodeTargetFrame(sdkFrame));
        if (!Arrays.equals(restored.mutation().encodeFrame(), sdkFrame)
                || !TargetMigrationControlBody.decode(restored.mutation().canonicalBody()).request().equals(request)
                || !Arrays.equals(bytes, encode(request, restored)))
            throw new IllegalStateException("Prepared archive changed complete original signed bytes");
        return new Restored(restored, control.length, sdkFrame.length);
    }
    private static byte[] take(ByteBuffer buffer, int maximum) {
        if (buffer.remaining() < 4) throw new IllegalArgumentException("truncated original Prepared archive");
        final int size = buffer.getInt();
        if (size < 1 || size > maximum || size > buffer.remaining()) throw new IllegalArgumentException("invalid bounded original Prepared field");
        final byte[] value = new byte[size]; buffer.get(value); return value;
    }
    private static byte[] encode(TargetMigrationSourceExposure.PreparedRequest request, TargetMigrationControlMutationFactory.Prepared exact) {
        return encode(request.request(), exact);
    }
    private static byte[] encode(TargetMigrationRequest request, TargetMigrationControlMutationFactory.Prepared exact) {
        return Bytes.concat(FRAME, TargetMigrationSourceExposure.PreparedRequest.candidateId(request), Bytes.lp32(request.canonicalBytes()),
                Bytes.lp32(exact.control().canonicalBytes()), Bytes.lp32(exact.mutation().encodeFrame()));
    }
    @Override public TargetMigrationControlMutationFactory.Prepared exact() { requireCurrent(); return Objects.requireNonNull(exact); }
    public TargetMigrationObjectReference object() { requireCurrent(); return Objects.requireNonNull(object); }
    @Override public void requireBound(TargetMigrationSourceExposure.PreparedRequest actual) {
        if (!Arrays.equals(actual.candidateId(), request.candidateId()) || !actual.request().equals(request.request()))
            throw new IllegalArgumentException("Prepared custody changed its original candidate");
        backing.requireBound(request);
    }
    @Override public void requireCurrent() {
        if (closing) throw new IllegalStateException("Prepared local loan is closing");
        request.opening().requireLocalCurrent(); backing.requireLocalCurrent();
    }
    @Override public long retainedBytes() {
        return Math.addExact(backing.retainedBytes(), retainedPreparedBytes);
    }
    @Override public void observedApplied(SystemMutationResult first) {
        if (exact == null || first.applyStatus() != ApplyStatus.APPLIED || first.stableCode() != StableCode.OK
                || !Arrays.equals(first.mutationId(), exact.mutation().systemMutationId())
                || !Arrays.equals(first.mutationHash(), exact.mutation().mutationHash()))
            throw new IllegalArgumentException("Prepared loan observation changed original actual first");
    }
    @Override public void requestStop() { closing = true; backing.requestStop(); }
    @Override public void close() { closing = true; backing.close(); }
    @Override public boolean ioExited() { return backing.ioExited() && (operationFailure == null || operationFailure.cleanupComplete())
            && (responseFailure == null || responseFailure.cleanupComplete()); }
    @Override public boolean cleanupComplete() { return closing && backing.cleanupComplete()
            && (operationFailure == null || operationFailure.cleanupComplete()) && (responseFailure == null || responseFailure.cleanupComplete())
            && (cleanupAttempt == null || cleanupAttempt.isDone()); }
    @Override public synchronized CompletableFuture<Void> retryCleanup() {
        if (cleanupAttempt != null && !cleanupAttempt.isDone()) return cleanupAttempt.copy();
        requestStop(); backing.close();
        cleanupAttempt = CompletableFuture.allOf(backing.retryCleanup(),
                operationFailure == null ? CompletableFuture.completedFuture(null) : operationFailure.retryCleanup(),
                responseFailure == null ? CompletableFuture.completedFuture(null) : responseFailure.retryCleanup());
        return cleanupAttempt.copy();
    }

    public static final class Unavailable extends IllegalStateException implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient TargetMigrationPreparedCustody original;
        private Unavailable(String message, TargetMigrationPreparedCustody original) { this(message, null, original); }
        private Unavailable(String message, Throwable failure, TargetMigrationPreparedCustody original) { super(message, failure); this.original = original; }
        @Override public boolean cleanupComplete() { return original.cleanupComplete(); }
        @Override public CompletableFuture<Void> retryCleanup() { original.requestStop(); original.close(); return original.retryCleanup(); }
    }
}
