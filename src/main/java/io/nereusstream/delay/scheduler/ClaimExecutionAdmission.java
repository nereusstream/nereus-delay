package io.nereusstream.delay.scheduler;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.ShardId;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Process-local logical message/byte permits retained by reversible Claims.
 *
 * <p>The pool enforces Lane, Shard and Worker ceilings together and protects
 * the configured minimum of every other READY Lane.  It is not durable quota
 * authority: an Owner recovery revokes/requeues old Claims before opening the
 * new Claim gate, then rebuilds this pool from zero.  A granted reservation is
 * retained from the Claim WriteBatch until Admission consumes the Claim or an
 * exact Claim revoke releases it.</p>
 */
public final class ClaimExecutionAdmission {
    private final long workerMaxMessages;
    private final long workerMaxBytes;
    private final Map<ShardId, ShardState> shards = new HashMap<>();
    private final Map<LaneKey, LaneState> lanes = new HashMap<>();
    private final Map<ClaimIdentity, Reservation> reservations = new HashMap<>();
    private long workerActiveMessages;
    private long workerActiveBytes;
    private long nextReservationId = 1;

    public ClaimExecutionAdmission(final long maximumWorkerMessages,
                                   final long maximumWorkerBytes) {
        if (maximumWorkerMessages <= 0 || maximumWorkerBytes <= 0) {
            throw new IllegalArgumentException("Worker Claim limits must be positive");
        }
        workerMaxMessages = maximumWorkerMessages;
        workerMaxBytes = maximumWorkerBytes;
    }

    /** Registers one owned Shard's hard Claim envelope before any Lane opens. */
    public synchronized void registerShard(final ShardSpec specification) {
        final ShardSpec spec = Objects.requireNonNull(specification, "specification");
        if (shards.putIfAbsent(spec.shardId(), new ShardState(spec)) != null) {
            throw new IllegalArgumentException("Shard Claim envelope is already registered");
        }
    }

    /** Registers one exact Lane incarnation while it is closed to Claim admission. */
    public synchronized void registerLane(final LaneSpec specification) {
        final LaneSpec spec = Objects.requireNonNull(specification, "specification");
        if (!shards.containsKey(spec.shardId())) {
            throw new IllegalArgumentException("Shard Claim envelope is not registered");
        }
        final LaneKey key = new LaneKey(spec.shardId(), spec.laneId());
        if (lanes.putIfAbsent(key, new LaneState(spec)) != null) {
            throw new IllegalArgumentException("Lane Claim envelope is already registered");
        }
    }

    /** Opens one Lane only when all hard envelopes can protect its READY minima. */
    public synchronized void openReady(final ShardId shardId,
                                       final DestinationLaneId laneId,
                                       final byte[] laneIncarnation) {
        final LaneState lane = lane(shardId, laneId);
        requireIncarnation(lane, laneIncarnation);
        if (lane.ready) {
            return;
        }
        final long workerMessages = protectedWorkerMessages(lane, true);
        final long workerBytes = protectedWorkerBytes(lane, true);
        if (!fits(workerActiveMessages, 0, workerMessages, workerMaxMessages)
                || !fits(workerActiveBytes, 0, workerBytes, workerMaxBytes)) {
            throw new IllegalStateException("Worker cannot protect Lane Claim minimum");
        }
        final ShardState shard = shard(shardId);
        final long shardMessages = protectedShardMessages(lane, true);
        final long shardBytes = protectedShardBytes(lane, true);
        if (!fits(shard.activeMessages, 0, shardMessages, shard.maxMessages)
                || !fits(shard.activeBytes, 0, shardBytes, shard.maxBytes)) {
            throw new IllegalStateException("Shard cannot protect Lane Claim minimum");
        }
        lane.ready = true;
    }

    /** Stops future Claim permits without releasing any already retained reservation. */
    public synchronized void closeReady(final ShardId shardId,
                                        final DestinationLaneId laneId,
                                        final byte[] laneIncarnation) {
        final LaneState lane = lane(shardId, laneId);
        requireIncarnation(lane, laneIncarnation);
        lane.ready = false;
    }

    /**
     * Attempts to reserve one exact Message Generation before its Claim
     * WriteBatch. Rejection is side-effect free.
     */
    public synchronized AdmissionDecision tryAcquire(final ShardId shardId,
                                                     final DestinationLaneId laneId,
                                                     final byte[] laneIncarnation,
                                                     final DelayMessageId messageId,
                                                     final long generation,
                                                     final long accountedBytes) {
        final ShardId requestedShard = Objects.requireNonNull(shardId, "shardId");
        final DestinationLaneId requestedLane = Objects.requireNonNull(laneId, "laneId");
        final DelayMessageId requestedMessage = Objects.requireNonNull(messageId, "messageId");
        if (!requestedMessage.routingId().shardId().equals(requestedShard)) {
            throw new IllegalArgumentException("Claim Message belongs to another Shard");
        }
        if (generation < 0 || generation > 0xffff_ffffL) {
            throw new IllegalArgumentException("Claim generation is outside uint32 range");
        }
        if (accountedBytes <= 0) {
            throw new IllegalArgumentException("Claim byte charge must be positive");
        }
        final LaneState lane = lanes.get(new LaneKey(requestedShard, requestedLane));
        if (lane == null) {
            return AdmissionDecision.rejected(Rejection.LANE_NOT_REGISTERED);
        }
        if (!Arrays.equals(lane.laneIncarnation, laneIncarnation)) {
            return AdmissionDecision.rejected(Rejection.LANE_IDENTITY_MISMATCH);
        }
        if (!lane.ready) {
            return AdmissionDecision.rejected(Rejection.LANE_NOT_READY);
        }
        final ClaimIdentity identity = new ClaimIdentity(requestedMessage, generation);
        if (reservations.containsKey(identity)) {
            return AdmissionDecision.rejected(Rejection.MESSAGE_GENERATION_ALREADY_RESERVED);
        }
        if (!fits(lane.activeMessages, 1, 0, lane.maxMessages)
                || !fits(lane.activeBytes, accountedBytes, 0, lane.maxBytes)) {
            return AdmissionDecision.rejected(Rejection.LANE_CAPACITY);
        }
        final ShardState shard = shard(requestedShard);
        if (!fits(shard.activeMessages, 1, protectedShardMessages(lane, false), shard.maxMessages)
                || !fits(shard.activeBytes, accountedBytes, protectedShardBytes(lane, false), shard.maxBytes)) {
            return AdmissionDecision.rejected(Rejection.SHARD_CAPACITY);
        }
        if (!fits(workerActiveMessages, 1, protectedWorkerMessages(lane, false), workerMaxMessages)
                || !fits(workerActiveBytes, accountedBytes, protectedWorkerBytes(lane, false), workerMaxBytes)) {
            return AdmissionDecision.rejected(Rejection.WORKER_CAPACITY);
        }

        final long reservationId = nextReservationId;
        nextReservationId = Math.addExact(nextReservationId, 1);
        final Reservation reservation = new Reservation(this, reservationId, identity, requestedShard,
                requestedLane, lane.laneIncarnation, accountedBytes);
        lane.activeMessages = Math.addExact(lane.activeMessages, 1);
        lane.activeBytes = Math.addExact(lane.activeBytes, accountedBytes);
        shard.activeMessages = Math.addExact(shard.activeMessages, 1);
        shard.activeBytes = Math.addExact(shard.activeBytes, accountedBytes);
        workerActiveMessages = Math.addExact(workerActiveMessages, 1);
        workerActiveBytes = Math.addExact(workerActiveBytes, accountedBytes);
        reservations.put(identity, reservation);
        return AdmissionDecision.granted(reservation);
    }

    /** Returns the current exact Lane accounting projection. */
    public synchronized LaneSnapshot laneSnapshot(final ShardId shardId,
                                                  final DestinationLaneId laneId) {
        final LaneState lane = lane(shardId, laneId);
        return new LaneSnapshot(lane.shardId, lane.laneId, lane.laneIncarnation, lane.ready,
                lane.activeMessages, lane.activeBytes, lane.minimumReadyMessages, lane.minimumReadyBytes,
                lane.maxMessages, lane.maxBytes);
    }

    /** Returns one owned Shard's current logical Claim accounting. */
    public synchronized ShardSnapshot shardSnapshot(final ShardId shardId) {
        final ShardState shard = shard(shardId);
        return new ShardSnapshot(shard.shardId, shard.activeMessages, shard.activeBytes,
                protectedShardMessages(null, shard.shardId), protectedShardBytes(null, shard.shardId),
                shard.maxMessages, shard.maxBytes);
    }

    /** Returns the Worker aggregate and currently protected READY minima. */
    public synchronized WorkerSnapshot workerSnapshot() {
        return new WorkerSnapshot(workerActiveMessages, workerActiveBytes,
                protectedWorkerMessages(null, false), protectedWorkerBytes(null, false),
                workerMaxMessages, workerMaxBytes);
    }

    /** Fails closed unless the reservation was created by this exact Worker permit pool. */
    public void requireOwnedReservation(final Reservation reservation) {
        final Reservation requested = Objects.requireNonNull(reservation, "reservation");
        if (requested.owner != this) {
            throw new IllegalArgumentException("Claim reservation belongs to another admission pool");
        }
    }

    private boolean release(final Reservation reservation) {
        synchronized (this) {
            if (reservation.state == ReservationState.RELEASED) {
                return false;
            }
            if (reservations.get(reservation.identity) != reservation) {
                throw new IllegalStateException("Claim reservation identity is not active");
            }
            final LaneState lane = lane(reservation.shardId, reservation.laneId);
            requireIncarnation(lane, reservation.laneIncarnation);
            final ShardState shard = shard(reservation.shardId);
            if (lane.activeMessages <= 0 || lane.activeBytes < reservation.accountedBytes
                    || shard.activeMessages <= 0 || shard.activeBytes < reservation.accountedBytes
                    || workerActiveMessages <= 0 || workerActiveBytes < reservation.accountedBytes) {
                throw new IllegalStateException("Claim permit accounting underflow");
            }
            lane.activeMessages--;
            lane.activeBytes -= reservation.accountedBytes;
            shard.activeMessages--;
            shard.activeBytes -= reservation.accountedBytes;
            workerActiveMessages--;
            workerActiveBytes -= reservation.accountedBytes;
            reservations.remove(reservation.identity);
            reservation.state = ReservationState.RELEASED;
            return true;
        }
    }

    private long protectedWorkerMessages(final LaneState excluded, final boolean includeExcludedIfClosed) {
        return protectedWorker(excluded, includeExcludedIfClosed, true);
    }

    private long protectedWorkerBytes(final LaneState excluded, final boolean includeExcludedIfClosed) {
        return protectedWorker(excluded, includeExcludedIfClosed, false);
    }

    private long protectedWorker(final LaneState excluded, final boolean includeExcludedIfClosed,
                                 final boolean messages) {
        long total = 0;
        for (LaneState lane : lanes.values()) {
            if (lane == excluded || !lane.ready) {
                continue;
            }
            total = Math.addExact(total, messages ? lane.minimumReadyMessages : lane.minimumReadyBytes);
        }
        if (excluded != null && includeExcludedIfClosed && !excluded.ready) {
            total = Math.addExact(total,
                    messages ? excluded.minimumReadyMessages : excluded.minimumReadyBytes);
        }
        return total;
    }

    private long protectedShardMessages(final LaneState excluded, final boolean includeExcludedIfClosed) {
        return protectedShard(excluded, includeExcludedIfClosed, true);
    }

    private long protectedShardBytes(final LaneState excluded, final boolean includeExcludedIfClosed) {
        return protectedShard(excluded, includeExcludedIfClosed, false);
    }

    private long protectedShard(final LaneState excluded, final boolean includeExcludedIfClosed,
                                final boolean messages) {
        if (excluded == null) {
            throw new IllegalArgumentException("excluded Lane is required for Shard protection");
        }
        return protectedShard(excluded, excluded.shardId, includeExcludedIfClosed, messages);
    }

    private long protectedShardMessages(final LaneState excluded, final ShardId shardId) {
        return protectedShard(excluded, shardId, false, true);
    }

    private long protectedShardBytes(final LaneState excluded, final ShardId shardId) {
        return protectedShard(excluded, shardId, false, false);
    }

    private long protectedShard(final LaneState excluded, final ShardId shardId,
                                final boolean includeExcludedIfClosed, final boolean messages) {
        long total = 0;
        for (LaneState lane : lanes.values()) {
            if (!lane.shardId.equals(shardId) || lane == excluded || !lane.ready) {
                continue;
            }
            total = Math.addExact(total, messages ? lane.minimumReadyMessages : lane.minimumReadyBytes);
        }
        if (excluded != null && includeExcludedIfClosed && !excluded.ready) {
            total = Math.addExact(total,
                    messages ? excluded.minimumReadyMessages : excluded.minimumReadyBytes);
        }
        return total;
    }

    private static boolean fits(final long retained, final long candidate,
                                final long protectedMinimum, final long maximum) {
        try {
            return Math.addExact(Math.addExact(retained, candidate), protectedMinimum) <= maximum;
        } catch (ArithmeticException overflow) {
            return false;
        }
    }

    private ShardState shard(final ShardId shardId) {
        final ShardState result = shards.get(Objects.requireNonNull(shardId, "shardId"));
        if (result == null) {
            throw new IllegalArgumentException("Shard Claim envelope is not registered");
        }
        return result;
    }

    private LaneState lane(final ShardId shardId, final DestinationLaneId laneId) {
        final LaneState result = lanes.get(new LaneKey(Objects.requireNonNull(shardId, "shardId"),
                Objects.requireNonNull(laneId, "laneId")));
        if (result == null) {
            throw new IllegalArgumentException("Lane Claim envelope is not registered");
        }
        return result;
    }

    private static void requireIncarnation(final LaneState lane, final byte[] laneIncarnation) {
        Bytes.requireLength(laneIncarnation, 16, "laneIncarnation");
        if (!Arrays.equals(lane.laneIncarnation, laneIncarnation)) {
            throw new IllegalArgumentException("Lane Claim incarnation mismatch");
        }
    }

    public enum Rejection {
        LANE_NOT_REGISTERED,
        LANE_IDENTITY_MISMATCH,
        LANE_NOT_READY,
        MESSAGE_GENERATION_ALREADY_RESERVED,
        LANE_CAPACITY,
        SHARD_CAPACITY,
        WORKER_CAPACITY
    }

    public enum ReservationState {
        ACTIVE,
        RELEASED
    }

    public record ShardSpec(ShardId shardId, long maxMessages, long maxBytes) {
        public ShardSpec {
            Objects.requireNonNull(shardId, "shardId");
            if (maxMessages <= 0 || maxBytes <= 0) {
                throw new IllegalArgumentException("Shard Claim limits must be positive");
            }
        }
    }

    public record LaneSpec(ShardId shardId, DestinationLaneId laneId, byte[] laneIncarnation,
                           long minimumReadyMessages, long minimumReadyBytes,
                           long maxMessages, long maxBytes) {
        public LaneSpec {
            Objects.requireNonNull(shardId, "shardId");
            Objects.requireNonNull(laneId, "laneId");
            Bytes.requireLength(laneIncarnation, 16, "laneIncarnation");
            if (minimumReadyMessages < 0 || minimumReadyBytes < 0
                    || maxMessages <= 0 || maxBytes <= 0
                    || minimumReadyMessages > maxMessages || minimumReadyBytes > maxBytes) {
                throw new IllegalArgumentException("invalid Lane Claim limits");
            }
            laneIncarnation = Bytes.copy(laneIncarnation);
        }

        @Override
        public byte[] laneIncarnation() {
            return Bytes.copy(laneIncarnation);
        }
    }

    public record AdmissionDecision(Reservation reservation, Rejection rejection) {
        public AdmissionDecision {
            if ((reservation == null) == (rejection == null)) {
                throw new IllegalArgumentException("Claim admission decision must be granted or rejected");
            }
        }

        private static AdmissionDecision granted(final Reservation reservation) {
            return new AdmissionDecision(Objects.requireNonNull(reservation, "reservation"), null);
        }

        private static AdmissionDecision rejected(final Rejection rejection) {
            return new AdmissionDecision(null, Objects.requireNonNull(rejection, "rejection"));
        }

        public boolean granted() {
            return reservation != null;
        }
    }

    public final class Reservation implements AutoCloseable {
        private final ClaimExecutionAdmission owner;
        private final long id;
        private final ClaimIdentity identity;
        private final ShardId shardId;
        private final DestinationLaneId laneId;
        private final byte[] laneIncarnation;
        private final long accountedBytes;
        private ReservationState state = ReservationState.ACTIVE;

        private Reservation(final ClaimExecutionAdmission owner, final long id,
                            final ClaimIdentity identity, final ShardId shardId,
                            final DestinationLaneId laneId, final byte[] laneIncarnation,
                            final long accountedBytes) {
            this.owner = owner;
            this.id = id;
            this.identity = identity;
            this.shardId = shardId;
            this.laneId = laneId;
            this.laneIncarnation = Bytes.copy(laneIncarnation);
            this.accountedBytes = accountedBytes;
        }

        public long id() {
            return id;
        }

        public DelayMessageId messageId() {
            return identity.messageId();
        }

        public long generation() {
            return identity.generation();
        }

        public ShardId shardId() {
            return shardId;
        }

        public DestinationLaneId laneId() {
            return laneId;
        }

        public byte[] laneIncarnation() {
            return Bytes.copy(laneIncarnation);
        }

        public long accountedBytes() {
            return accountedBytes;
        }

        public synchronized ReservationState state() {
            synchronized (owner) {
                return state;
            }
        }

        public boolean release() {
            return owner.release(this);
        }

        @Override
        public void close() {
            release();
        }
    }

    public record LaneSnapshot(ShardId shardId, DestinationLaneId laneId, byte[] laneIncarnation,
                               boolean ready, long activeMessages, long activeBytes,
                               long minimumReadyMessages, long minimumReadyBytes,
                               long maxMessages, long maxBytes) {
        public LaneSnapshot {
            laneIncarnation = Bytes.copy(laneIncarnation);
        }

        @Override
        public byte[] laneIncarnation() {
            return Bytes.copy(laneIncarnation);
        }
    }

    public record ShardSnapshot(ShardId shardId, long activeMessages, long activeBytes,
                                long protectedReadyMessages, long protectedReadyBytes,
                                long maxMessages, long maxBytes) {
    }

    public record WorkerSnapshot(long activeMessages, long activeBytes,
                                 long protectedReadyMessages, long protectedReadyBytes,
                                 long maxMessages, long maxBytes) {
    }

    private record ClaimIdentity(DelayMessageId messageId, long generation) {
        private ClaimIdentity {
            Objects.requireNonNull(messageId, "messageId");
        }
    }

    private record LaneKey(ShardId shardId, DestinationLaneId laneId) {
        private LaneKey {
            Objects.requireNonNull(shardId, "shardId");
            Objects.requireNonNull(laneId, "laneId");
        }
    }

    private static final class ShardState {
        private final ShardId shardId;
        private final long maxMessages;
        private final long maxBytes;
        private long activeMessages;
        private long activeBytes;

        private ShardState(final ShardSpec specification) {
            shardId = specification.shardId();
            maxMessages = specification.maxMessages();
            maxBytes = specification.maxBytes();
        }
    }

    private static final class LaneState {
        private final ShardId shardId;
        private final DestinationLaneId laneId;
        private final byte[] laneIncarnation;
        private final long minimumReadyMessages;
        private final long minimumReadyBytes;
        private final long maxMessages;
        private final long maxBytes;
        private boolean ready;
        private long activeMessages;
        private long activeBytes;

        private LaneState(final LaneSpec specification) {
            shardId = specification.shardId();
            laneId = specification.laneId();
            laneIncarnation = specification.laneIncarnation();
            minimumReadyMessages = specification.minimumReadyMessages();
            minimumReadyBytes = specification.minimumReadyBytes();
            maxMessages = specification.maxMessages();
            maxBytes = specification.maxBytes();
        }
    }
}
