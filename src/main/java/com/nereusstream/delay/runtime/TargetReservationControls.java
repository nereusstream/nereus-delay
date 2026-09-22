package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.IngressFenceState;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/** Source-ordered reservation overlay. Closure captures the fence at the first applicable Close marker. */
public final class TargetReservationControls {
    private TargetReservationControls() {}

    /**
     * Resolve the earliest applicable accepted Close for this exact binding through reader.source().
     * Empty means proven absence across Target, binding/domain and retained legacy control scopes.
     * The historical snapshot must remain protected through the surrounding read guard or native commit.
     * A current closed boolean, membership withdrawal or Profile deprecation is not sufficient evidence.
     */
    @FunctionalInterface
    public interface Authority {
        Optional<Closure> firstClosure(TargetStoreBackend.Reader reader, TargetScheduleBinding binding);
    }

    /** A source-pinned authority response, not a caller-supplied control command or a persisted marker format. */
    public record Closure(
            TargetPartitionId target,
            byte[] bindingDigest,
            byte[] recoveryLineage,
            SourcePosition source,
            long fenceAtClose) {
        public Closure {
            Objects.requireNonNull(target, "target");
            Bytes.requireLength(bindingDigest, 32, "bindingDigest");
            Bytes.requireLength(recoveryLineage, 16, "recoveryLineage");
            TargetSourcePosition.requireBounded(source);
            if (fenceAtClose < IngressFenceState.OPEN
                    || Arrays.equals(bindingDigest, new byte[32])
                    || Arrays.equals(recoveryLineage, new byte[16])) {
                throw new IllegalArgumentException("closure requires assigned identity and a valid historical fence");
            }
            bindingDigest = Bytes.copy(bindingDigest);
            recoveryLineage = Bytes.copy(recoveryLineage);
        }

        @Override
        public byte[] bindingDigest() {
            return Bytes.copy(bindingDigest);
        }

        @Override
        public byte[] recoveryLineage() {
            return Bytes.copy(recoveryLineage);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Closure that
                    && target.equals(that.target)
                    && source.equals(that.source)
                    && fenceAtClose == that.fenceAtClose
                    && Arrays.equals(bindingDigest, that.bindingDigest)
                    && Arrays.equals(recoveryLineage, that.recoveryLineage);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    target, source, fenceAtClose, Arrays.hashCode(bindingDigest), Arrays.hashCode(recoveryLineage));
        }

        public byte[] digest() {
            return Bytes.sha256(
                    Bytes.utf8("nereus-delay-target-reservation-closure\0"),
                    target.bytes(),
                    bindingDigest,
                    recoveryLineage,
                    source.canonicalBytes(),
                    Bytes.u64beBits(fenceAtClose));
        }
    }

    public record Decision(PayloadReservationStatus status, long watermark, Optional<Closure> closure) {
        public Decision {
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(closure, "closure");
            if (watermark < IngressFenceState.OPEN) {
                throw new IllegalArgumentException("invalid reservation decision watermark");
            }
        }
    }

    public static Decision resolve(
            TargetStoreBackend.Reader reader,
            TargetReservationRecord reservation,
            TargetScheduleBinding binding,
            TargetQueueState queue,
            Authority authority) {
        Objects.requireNonNull(authority, "authority");
        reservation.requireBinding(binding);
        binding.requireQueueProjection(queue);
        final long watermark = reader.closedIngressDeadlineThrough();
        if (reservation.status() != PayloadReservationStatus.RESERVED) {
            return new Decision(reservation.status(), watermark, Optional.empty());
        }
        reader.requireWithinElapsedBudget();
        final Optional<Closure> closure;
        try {
            closure = Objects.requireNonNull(authority.firstClosure(reader, binding), "reservation closure decision");
        } catch (ReadIncompleteException external) {
            throw new IllegalStateException("reservation closure authority did not complete", external);
        }
        reader.requireWithinElapsedBudget();
        if (closure.isEmpty()) {
            if (queue.admissionState() == TargetQueueState.AdmissionState.CLOSED) {
                throw new IllegalStateException("closed Target lacks its source-ordered reservation closure evidence");
            }
            return new Decision(reservation.effectiveStatus(watermark), watermark, closure);
        }
        final var closed = closure.orElseThrow();
        if (!closed.target().equals(binding.target())
                || !Arrays.equals(closed.bindingDigest(), binding.digest())
                || !Arrays.equals(closed.recoveryLineage(), reservation.recoveryLineage())
                || !closed.source().shardId().equals(reader.shardId())
                || closed.source().compareTo(reservation.prepareAnchor().source()) <= 0
                || reader.source() == null
                || closed.source().compareTo(reader.source()) > 0
                || closed.fenceAtClose() > watermark
                || (closed.source().compareTo(reader.source()) == 0
                        && (closed.fenceAtClose() != watermark
                                || !Arrays.equals(
                                        closed.source().canonicalBytes(),
                                        reader.source().canonicalBytes())))) {
            throw new IllegalStateException("reservation closure identity/source/fence differs from the current view");
        }
        return new Decision(
                reservation.expiryEpochMs() <= closed.fenceAtClose()
                        ? PayloadReservationStatus.EXPIRED
                        : PayloadReservationStatus.ABANDONED,
                watermark,
                closure);
    }
}
