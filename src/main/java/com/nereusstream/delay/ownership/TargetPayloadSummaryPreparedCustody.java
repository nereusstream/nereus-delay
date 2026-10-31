package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointResource;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TargetPayloadSummaryInitializationBody;
import com.nereusstream.delay.store.RenewableS3CompatibleCheckpointObjectStoreAdapter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Fixed-key conditional-PUT winner for one summary Prepared/full frame. It is never a second Source log. */
public final class TargetPayloadSummaryPreparedCustody implements CheckpointInputFuture.Cleanup, AutoCloseable {
    /** Actual original storage/cost/retention loan. Closing local handles never deletes the persistent archive. */
    public interface Backing extends CheckpointInputFuture.Cleanup, AutoCloseable {
        RenewableS3CompatibleCheckpointObjectStoreAdapter objects();

        byte[] lineage();

        byte[] checkpointId();

        int maximumObjectBytes();

        long retainedBytes();
        /** Pure local binding to the same already acquired original storage/retention/payment loan. */
        void requireBound(TargetPayloadSummaryInitializationMaintenance.PreparedRequest request);
        /** Original no-delete/conditional-write/exposure-history authority; missing UNKNOWN archive is not absence. */
        void requireMayCreate(TargetPayloadSummaryInitializationMaintenance.PreparedRequest request);

        void requireCurrent();

        void requireLocalCurrent();

        boolean ioExited();

        @Override
        void close();
    }

    private static final byte[] MAGIC = Bytes.utf8("NDIP3-PAYLOAD-SUMMARY-PREPARED-1\0");
    private final Backing backing;
    private final TargetPayloadSummaryInitializationMaintenance.PreparedRequest request;
    private TargetPayloadSummaryControlMutationFactory.Prepared exact;
    private CheckpointResource resource;
    private final List<CheckpointInputFuture.Cleanup> failures = new ArrayList<>();
    private boolean closing;

    private TargetPayloadSummaryPreparedCustody(
            Backing backing, TargetPayloadSummaryInitializationMaintenance.PreparedRequest request) {
        this.backing = backing;
        this.request = request;
    }

    static TargetPayloadSummaryPreparedCustody retainOrRecover(
            TargetPayloadSummaryInitializationMaintenance.ManagementHandle source,
            TargetPayloadSummaryInitializationMaintenance.PreparedRequest request,
            Supplier<TargetPayloadSummaryControlMutationFactory.Prepared> signer) {
        request.opening().requireCurrent();
        final var backing =
                Objects.requireNonNull(source.preparedBacking(request), "original paid summary archive backing");
        final var held = new TargetPayloadSummaryPreparedCustody(backing, request);
        request.opening().retainCleanup(held);
        try {
            backing.requireBound(request);
            backing.requireCurrent();
            final int maximum = backing.maximumObjectBytes();
            if (maximum < 1
                    || maximum == Integer.MAX_VALUE
                    || maximum > request.opening().configuration().maximumArchiveBytes()
                    || backing.retainedBytes() < 0
                    || Math.addExact(backing.retainedBytes(), maximum)
                            > request.opening().remainingRetainedBytes()) {
                throw new IllegalArgumentException(
                        "summary archive exceeds its original finite storage/input reservation");
            }
            final String name = "payload-summary-" + Bytes.hex(request.candidateId()) + ".frame";
            final var existing =
                    backing.objects().lookupMigrationObject(backing.lineage(), backing.checkpointId(), name, maximum);
            if (existing.isPresent()) {
                held.restore(
                        existing.orElseThrow().resource(),
                        existing.orElseThrow().bytes());
                return held;
            }
            backing.requireMayCreate(request);
            request.opening().requireCurrent();
            final var signed = Objects.requireNonNull(signer.get(), "original platform signed summary request");
            final byte[] encoded = encode(request, signed);
            if (encoded.length > maximum) {
                throw new IllegalArgumentException("complete summary Prepared/frame exceeds actual archive bound");
            }
            try {
                backing.objects().putMigrationObject(backing.lineage(), backing.checkpointId(), name, encoded, maximum);
            } catch (RuntimeException uncertain) {
                held.retainFailure(uncertain);
                final var observed = backing.objects()
                        .lookupMigrationObject(backing.lineage(), backing.checkpointId(), name, maximum);
                if (observed.isEmpty()) {
                    throw uncertain;
                }
                held.restore(
                        observed.orElseThrow().resource(),
                        observed.orElseThrow().bytes());
                return held;
            }
            final var durable = backing.objects()
                    .lookupMigrationObject(backing.lineage(), backing.checkpointId(), name, maximum)
                    .orElseThrow();
            held.restore(durable.resource(), durable.bytes());
            return held;
        } catch (RuntimeException | Error failure) {
            held.retainFailure(failure);
            throw new Unavailable(
                    "original summary Prepared custody is unconfirmed; Source SDK exposure remains forbidden",
                    failure,
                    held);
        }
    }

    private void restore(CheckpointResource actual, byte[] bytes) {
        if (bytes.length != actual.manifestLength() || !Arrays.equals(Bytes.sha256(bytes), actual.manifestSha256())) {
            throw new IllegalStateException("summary archive changed exact immutable bytes");
        }
        final var input = ByteBuffer.wrap(bytes);
        if (input.remaining() < MAGIC.length + 32) {
            throw new IllegalArgumentException("truncated summary Prepared archive");
        }
        final byte[] magic = new byte[MAGIC.length];
        input.get(magic);
        final byte[] candidate = new byte[32];
        input.get(candidate);
        final byte[] original = take(input, backing.maximumObjectBytes());
        final byte[] control = take(input, backing.maximumObjectBytes());
        final byte[] frame = take(input, backing.maximumObjectBytes());
        if (!Arrays.equals(magic, MAGIC)
                || !Arrays.equals(candidate, request.candidateId())
                || input.hasRemaining()
                || !Arrays.equals(original, request.request().canonicalBytes())) {
            throw new IllegalStateException("summary archive changed candidate, request or schema");
        }
        final var winner = TargetPayloadSummaryControlMutationFactory.Prepared.restore(
                PreparedControlOperation.decode(control), SystemMutation.decodeTargetFrame(frame));
        if (!TargetPayloadSummaryInitializationBody.decode(winner.mutation().canonicalBody())
                        .request()
                        .equals(request.request())
                || !Arrays.equals(frame, winner.mutation().encodeFrame())
                || !Arrays.equals(bytes, encode(request, winner))) {
            throw new IllegalStateException("summary archive changed complete exact signed bytes");
        }
        exact = winner;
        resource = actual;
        requireCurrent();
    }

    private static byte[] take(ByteBuffer input, int maximum) {
        if (input.remaining() < 4) {
            throw new IllegalArgumentException("truncated summary archive field");
        }
        final int length = input.getInt();
        if (length < 1 || length > maximum || length > input.remaining()) {
            throw new IllegalArgumentException("summary archive field exceeds its original bound");
        }
        final byte[] value = new byte[length];
        input.get(value);
        return value;
    }

    private static byte[] encode(
            TargetPayloadSummaryInitializationMaintenance.PreparedRequest request,
            TargetPayloadSummaryControlMutationFactory.Prepared exact) {
        return Bytes.concat(
                MAGIC,
                request.candidateId(),
                Bytes.lp32(request.request().canonicalBytes()),
                Bytes.lp32(exact.control().canonicalBytes()),
                Bytes.lp32(exact.mutation().encodeFrame()));
    }

    public TargetPayloadSummaryControlMutationFactory.Prepared exact() {
        requireCurrent();
        return Objects.requireNonNull(exact);
    }

    public CheckpointResource resource() {
        requireCurrent();
        return Objects.requireNonNull(resource);
    }

    public void requireCurrent() {
        if (closing) {
            throw new IllegalStateException("summary Prepared local loan is closing");
        }
        request.opening().requireLocalCurrent();
        backing.requireLocalCurrent();
        backing.requireBound(request);
    }

    public long retainedBytes() {
        return Math.addExact(
                backing.retainedBytes(),
                exact == null
                        ? 0
                        : Math.addExact(
                                exact.control().canonicalBytes().length,
                                exact.mutation().encodeFrame().length));
    }

    private void retainFailure(Throwable failure) {
        Throwable actual = failure;
        while (actual instanceof java.util.concurrent.CompletionException
                && actual.getCause() != null
                && actual.getCause() != actual) {
            actual = actual.getCause();
        }
        if (actual instanceof CheckpointInputFuture.Cleanup cleanup && !failures.contains(cleanup)) {
            failures.add(cleanup);
        }
    }

    @Override
    public void close() {
        closing = true;
        backing.close();
    }

    @Override
    public boolean cleanupComplete() {
        return closing
                && backing.ioExited()
                && backing.cleanupComplete()
                && failures.stream().allMatch(CheckpointInputFuture.Cleanup::cleanupComplete);
    }

    @Override
    public CompletableFuture<Void> retryCleanup() {
        close();
        final var pending = new ArrayList<CompletableFuture<Void>>();
        pending.add(backing.retryCleanup());
        for (var failure : failures) {
            pending.add(failure.retryCleanup());
        }
        return CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new));
    }

    public static final class Unavailable extends IllegalStateException implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient TargetPayloadSummaryPreparedCustody original;

        private Unavailable(String message, Throwable failure, TargetPayloadSummaryPreparedCustody original) {
            super(message, failure);
            this.original = original;
        }

        @Override
        public boolean cleanupComplete() {
            return original.cleanupComplete();
        }

        @Override
        public CompletableFuture<Void> retryCleanup() {
            return original.retryCleanup();
        }
    }
}
