package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Bounded local admission for physical target requests.
 *
 * <p>This is deliberately a resource gate, not a publish outcome authority.
 * A granted reservation only permits an adapter call to start.  The caller
 * must keep the reservation until the physical operation completes; a
 * callback timeout may mark it {@link ReservationState#ZOMBIE}, but may not
 * release its request/byte charge early.</p>
 */
public final class DestinationPhysicalAdmission {
    private final long workerMaxRequests;
    private final long workerMaxBytes;
    private final Map<String, ClusterState> clusters = new HashMap<>();
    private final Map<DestinationLaneId, LaneState> lanes = new HashMap<>();
    private long workerActiveRequests;
    private long workerActiveBytes;
    private long nextReservationId = 1;

    public DestinationPhysicalAdmission(final long workerMaxRequests, final long workerMaxBytes) {
        if (workerMaxRequests <= 0 || workerMaxBytes <= 0) {
            throw new IllegalArgumentException("worker physical limits must be positive");
        }
        this.workerMaxRequests = workerMaxRequests;
        this.workerMaxBytes = workerMaxBytes;
    }

    /** Registers the hard target-cluster envelope before any Lane is opened. */
    public synchronized void registerTargetCluster(final String targetClusterId,
                                                    final long maxRequests,
                                                    final long maxBytes) {
        final String cluster = requireClusterId(targetClusterId);
        if (maxRequests <= 0 || maxBytes <= 0) {
            throw new IllegalArgumentException("target-cluster physical limits must be positive");
        }
        if (clusters.putIfAbsent(cluster, new ClusterState(maxRequests, maxBytes)) != null) {
            throw new IllegalArgumentException("target cluster is already registered");
        }
    }

    /** Registers a Lane while it is still closed for physical admission. */
    public synchronized void registerLane(final LaneSpec specification) {
        Objects.requireNonNull(specification, "specification");
        if (!clusters.containsKey(specification.targetClusterId())) {
            throw new IllegalArgumentException("target cluster is not registered");
        }
        if (lanes.putIfAbsent(specification.laneId(), new LaneState(specification)) != null) {
            throw new IllegalArgumentException("Lane is already registered");
        }
    }

    /** Opens the Lane only if its committed READY minimum can be protected. */
    public synchronized void openReady(final DestinationLaneId laneId) {
        final LaneState lane = lane(laneId);
        if (lane.ready) {
            return;
        }
        ensureReadyMinimumFits(lane);
        lane.ready = true;
    }

    /** Removes the Lane from future admission and minimum protection. */
    public synchronized void closeReady(final DestinationLaneId laneId) {
        lane(laneId).ready = false;
    }

    /**
     * Unregisters a Lane after its exact physical channel generation has been
     * fenced and all physical reservations have quiesced.
     *
     * <p>This is only an in-process resource-registry operation.  It does not
     * authorize a logical Lane retirement, release an Oxia grant, or replace
     * the source-ordered terminal guard.  The incarnation check prevents a
     * stale teardown callback from removing a newer registration.</p>
     */
    public synchronized void unregisterLane(final DestinationLaneId laneId,
                                             final byte[] laneIncarnation) {
        final LaneState lane = lane(laneId);
        Bytes.requireLength(laneIncarnation, 16, "laneIncarnation");
        if (!Arrays.equals(lane.laneIncarnation, laneIncarnation)) {
            throw new IllegalArgumentException("Lane identity mismatch");
        }
        if (lane.ready) {
            throw new IllegalStateException("cannot unregister a READY Lane");
        }
        if (lane.activeRequests != 0 || lane.activeBytes != 0
                || lane.zombieRequests != 0 || lane.zombieBytes != 0) {
            throw new IllegalStateException("cannot unregister a Lane with physical charges");
        }
        lanes.remove(laneId);
    }

    /**
     * Attempts to reserve one physical request and its exact adapter byte
     * charge.  Rejection is explicit so the caller can turn it into a Lane
     * runtime block rather than a business-level message failure.
     */
    public synchronized AdmissionDecision tryAcquire(final DestinationLaneId laneId,
                                                      final byte[] laneIncarnation,
                                                      final long physicalBytes) {
        final LaneState lane = lanes.get(Objects.requireNonNull(laneId, "laneId"));
        if (lane == null) {
            return AdmissionDecision.rejected(Rejection.LANE_NOT_REGISTERED);
        }
        if (!Arrays.equals(lane.laneIncarnation, laneIncarnation)) {
            return AdmissionDecision.rejected(Rejection.LANE_IDENTITY_MISMATCH);
        }
        if (physicalBytes < 0) {
            throw new IllegalArgumentException("physical byte charge must be non-negative");
        }
        if (!lane.ready) {
            return AdmissionDecision.rejected(Rejection.LANE_NOT_READY);
        }
        if (lane.blocked || lane.zombieRequests >= lane.maxZombieRequests
                || lane.zombieBytes >= lane.maxZombieBytes) {
            lane.blocked = true;
            return AdmissionDecision.rejected(Rejection.ZOMBIE_CAPACITY);
        }
        // V1 reserves the vector in which every currently outstanding
        // request becomes a zombie.  Checking only the already-marked zombie
        // bucket would admit a request that can never fit that worst-case
        // vector; a later callback timeout would then strand it as an
        // in-flight charge that cannot be marked zombie.  Active charges are
        // retained until completion, so they are part of the potential
        // zombie envelope even before a timeout is observed.
        if (lane.activeRequests >= lane.maxZombieRequests
                || physicalBytes > lane.maxZombieBytes - lane.activeBytes) {
            return AdmissionDecision.rejected(Rejection.ZOMBIE_CAPACITY);
        }
        if (lane.activeRequests >= lane.maxRequests
                || physicalBytes > lane.maxBytes - lane.activeBytes) {
            return AdmissionDecision.rejected(Rejection.LANE_CAPACITY);
        }

        final long otherReadyRequests = readyMinimumRequests(lane, false);
        final long otherReadyBytes = readyMinimumBytes(lane, false);
        if (!fits(workerActiveRequests, 1, otherReadyRequests, workerMaxRequests)
                || !fits(workerActiveBytes, physicalBytes, otherReadyBytes, workerMaxBytes)) {
            return AdmissionDecision.rejected(Rejection.WORKER_CAPACITY);
        }
        final ClusterState cluster = clusters.get(lane.targetClusterId);
        final long otherClusterReadyRequests = readyMinimumRequests(lane, true);
        final long otherClusterReadyBytes = readyMinimumBytes(lane, true);
        if (!fits(cluster.activeRequests, 1, otherClusterReadyRequests, cluster.maxRequests)
                || !fits(cluster.activeBytes, physicalBytes, otherClusterReadyBytes, cluster.maxBytes)) {
            return AdmissionDecision.rejected(Rejection.TARGET_CLUSTER_CAPACITY);
        }

        final long reservationId = nextReservationId;
        nextReservationId = Math.addExact(nextReservationId, 1);
        final Reservation reservation = new Reservation(this, reservationId, lane.laneId,
                lane.laneIncarnation, lane.targetClusterId, physicalBytes);
        lane.activeRequests = Math.addExact(lane.activeRequests, 1);
        lane.activeBytes = Math.addExact(lane.activeBytes, physicalBytes);
        cluster.activeRequests = Math.addExact(cluster.activeRequests, 1);
        cluster.activeBytes = Math.addExact(cluster.activeBytes, physicalBytes);
        workerActiveRequests = Math.addExact(workerActiveRequests, 1);
        workerActiveBytes = Math.addExact(workerActiveBytes, physicalBytes);
        return AdmissionDecision.granted(reservation);
    }

    /** Clears a zombie-capacity block only after the caller has rechecked the physical state. */
    public synchronized void clearZombieBlock(final DestinationLaneId laneId) {
        final LaneState lane = lane(laneId);
        if (lane.zombieRequests >= lane.maxZombieRequests || lane.zombieBytes >= lane.maxZombieBytes) {
            throw new IllegalStateException("zombie capacity is still exhausted");
        }
        lane.blocked = false;
    }

    public synchronized LaneSnapshot laneSnapshot(final DestinationLaneId laneId) {
        final LaneState lane = lane(laneId);
        return new LaneSnapshot(lane.laneId, lane.laneIncarnation, lane.targetClusterId, lane.ready,
                lane.blocked, lane.activeRequests, lane.activeBytes, lane.zombieRequests, lane.zombieBytes,
                lane.maxRequests, lane.maxBytes, lane.maxZombieRequests, lane.maxZombieBytes,
                lane.minimumReadyRequests, lane.minimumReadyBytes);
    }

    public synchronized WorkerSnapshot workerSnapshot() {
        return new WorkerSnapshot(workerActiveRequests, workerActiveBytes,
                readyMinimumRequests(null, false), readyMinimumBytes(null, false),
                workerMaxRequests, workerMaxBytes);
    }

    public synchronized ClusterSnapshot clusterSnapshot(final String targetClusterId) {
        final ClusterState cluster = clusters.get(requireClusterId(targetClusterId));
        if (cluster == null) {
            throw new IllegalArgumentException("target cluster is not registered");
        }
        return new ClusterSnapshot(targetClusterId, cluster.activeRequests, cluster.activeBytes,
                readyMinimumRequests(null, true, targetClusterId),
                readyMinimumBytes(null, true, targetClusterId), cluster.maxRequests, cluster.maxBytes);
    }

    private void ensureReadyMinimumFits(final LaneState candidate) {
        final long workerMinimumRequests = readyMinimumRequests(candidate, false);
        final long workerMinimumBytes = readyMinimumBytes(candidate, false);
        if (!fits(workerActiveRequests, 0, workerMinimumRequests, workerMaxRequests)
                || !fits(workerActiveBytes, 0, workerMinimumBytes, workerMaxBytes)) {
            throw new IllegalStateException("worker cannot protect Lane READY minimum");
        }
        final ClusterState cluster = clusters.get(candidate.targetClusterId);
        final long clusterMinimumRequests = readyMinimumRequests(candidate, true);
        final long clusterMinimumBytes = readyMinimumBytes(candidate, true);
        if (!fits(cluster.activeRequests, 0, clusterMinimumRequests, cluster.maxRequests)
                || !fits(cluster.activeBytes, 0, clusterMinimumBytes, cluster.maxBytes)) {
            throw new IllegalStateException("target cluster cannot protect Lane READY minimum");
        }
    }

    private long readyMinimumRequests(final LaneState excluded, final boolean sameCluster) {
        return readyMinimumRequests(excluded, sameCluster, null);
    }

    private long readyMinimumRequests(final LaneState excluded, final boolean sameCluster,
                                      final String targetClusterId) {
        long total = 0;
        for (LaneState lane : lanes.values()) {
            if (lane == excluded || !lane.ready) {
                continue;
            }
            if (sameCluster && targetClusterId != null && !lane.targetClusterId.equals(targetClusterId)) {
                continue;
            }
            if (sameCluster && targetClusterId == null && excluded != null
                    && !lane.targetClusterId.equals(excluded.targetClusterId)) {
                continue;
            }
            total = Math.addExact(total, lane.minimumReadyRequests);
        }
        if (excluded != null && !excluded.ready) {
            total = Math.addExact(total, excluded.minimumReadyRequests);
        }
        return total;
    }

    private long readyMinimumBytes(final LaneState excluded, final boolean sameCluster) {
        return readyMinimumBytes(excluded, sameCluster, null);
    }

    private long readyMinimumBytes(final LaneState excluded, final boolean sameCluster,
                                   final String targetClusterId) {
        long total = 0;
        for (LaneState lane : lanes.values()) {
            if (lane == excluded || !lane.ready) {
                continue;
            }
            if (sameCluster && targetClusterId != null && !lane.targetClusterId.equals(targetClusterId)) {
                continue;
            }
            if (sameCluster && targetClusterId == null && excluded != null
                    && !lane.targetClusterId.equals(excluded.targetClusterId)) {
                continue;
            }
            total = Math.addExact(total, lane.minimumReadyBytes);
        }
        if (excluded != null && !excluded.ready) {
            total = Math.addExact(total, excluded.minimumReadyBytes);
        }
        return total;
    }

    private static boolean fits(final long retained, final long candidate, final long protectedMinimum,
                                final long maximum) {
        try {
            return Math.addExact(Math.addExact(retained, candidate), protectedMinimum) <= maximum;
        } catch (ArithmeticException overflow) {
            return false;
        }
    }

    private LaneState lane(final DestinationLaneId laneId) {
        final LaneState result = lanes.get(Objects.requireNonNull(laneId, "laneId"));
        if (result == null) {
            throw new IllegalArgumentException("Lane is not registered");
        }
        return result;
    }

    private static String requireClusterId(final String value) {
        Objects.requireNonNull(value, "targetClusterId");
        final String decoded = new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        if (!decoded.equals(value) || value.isBlank() || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException("targetClusterId must be nonblank NFC UTF-8");
        }
        return value;
    }

    private boolean markZombie(final Reservation reservation) {
        synchronized (this) {
            if (reservation.state == ReservationState.RELEASED) {
                return false;
            }
            if (reservation.state == ReservationState.ZOMBIE) {
                return true;
            }
            final LaneState lane = lane(reservation.laneId);
            if (lane.zombieRequests >= lane.maxZombieRequests
                    || reservation.physicalBytes > lane.maxZombieBytes - lane.zombieBytes) {
                lane.blocked = true;
                return false;
            }
            lane.zombieRequests = Math.addExact(lane.zombieRequests, 1);
            lane.zombieBytes = Math.addExact(lane.zombieBytes, reservation.physicalBytes);
            reservation.state = ReservationState.ZOMBIE;
            return true;
        }
    }

    private boolean release(final Reservation reservation) {
        synchronized (this) {
            if (reservation.state == ReservationState.RELEASED) {
                return false;
            }
            final LaneState lane = lane(reservation.laneId);
            final ClusterState cluster = clusters.get(reservation.targetClusterId);
            if (lane.activeRequests <= 0 || lane.activeBytes < reservation.physicalBytes
                    || cluster.activeRequests <= 0 || cluster.activeBytes < reservation.physicalBytes
                    || workerActiveRequests <= 0 || workerActiveBytes < reservation.physicalBytes) {
                throw new IllegalStateException("physical admission accounting underflow");
            }
            if (reservation.state == ReservationState.ZOMBIE
                    && (lane.zombieRequests <= 0 || lane.zombieBytes < reservation.physicalBytes)) {
                throw new IllegalStateException("zombie admission accounting underflow");
            }
            lane.activeRequests--;
            lane.activeBytes -= reservation.physicalBytes;
            cluster.activeRequests--;
            cluster.activeBytes -= reservation.physicalBytes;
            workerActiveRequests--;
            workerActiveBytes -= reservation.physicalBytes;
            if (reservation.state == ReservationState.ZOMBIE) {
                lane.zombieRequests--;
                lane.zombieBytes -= reservation.physicalBytes;
            }
            reservation.state = ReservationState.RELEASED;
            return true;
        }
    }

    public enum Rejection {
        LANE_NOT_REGISTERED,
        LANE_IDENTITY_MISMATCH,
        LANE_NOT_READY,
        ZOMBIE_CAPACITY,
        LANE_CAPACITY,
        WORKER_CAPACITY,
        TARGET_CLUSTER_CAPACITY
    }

    public enum ReservationState {
        IN_FLIGHT,
        ZOMBIE,
        RELEASED
    }

    public record TargetClusterSpec(String targetClusterId, long maxRequests, long maxBytes) {
        public TargetClusterSpec {
            requireClusterId(targetClusterId);
            if (maxRequests <= 0 || maxBytes <= 0) {
                throw new IllegalArgumentException("target-cluster physical limits must be positive");
            }
        }
    }

    public record LaneSpec(DestinationLaneId laneId, byte[] laneIncarnation, String targetClusterId,
                           long minimumReadyRequests, long minimumReadyBytes, long maxRequests, long maxBytes,
                           long maxZombieRequests, long maxZombieBytes) {
        public LaneSpec {
            Objects.requireNonNull(laneId, "laneId");
            Bytes.requireLength(laneIncarnation, 16, "laneIncarnation");
            requireClusterId(targetClusterId);
            if (minimumReadyRequests < 0 || minimumReadyBytes < 0 || maxRequests <= 0 || maxBytes <= 0
                    || maxZombieRequests <= 0 || maxZombieBytes <= 0
                    || minimumReadyRequests > maxRequests || minimumReadyBytes > maxBytes
                    || maxZombieRequests > maxRequests || maxZombieBytes > maxBytes) {
                throw new IllegalArgumentException("invalid Lane physical limits");
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
                throw new IllegalArgumentException("admission decision must be granted or rejected");
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
        private final DestinationPhysicalAdmission owner;
        private final long id;
        private final DestinationLaneId laneId;
        private final byte[] laneIncarnation;
        private final String targetClusterId;
        private final long physicalBytes;
        private ReservationState state = ReservationState.IN_FLIGHT;

        private Reservation(final DestinationPhysicalAdmission owner, final long id,
                             final DestinationLaneId laneId, final byte[] laneIncarnation,
                             final String targetClusterId, final long physicalBytes) {
            this.owner = owner;
            this.id = id;
            this.laneId = laneId;
            this.laneIncarnation = Bytes.copy(laneIncarnation);
            this.targetClusterId = targetClusterId;
            this.physicalBytes = physicalBytes;
        }

        public long id() {
            return id;
        }

        public DestinationLaneId laneId() {
            return laneId;
        }

        public byte[] laneIncarnation() {
            return Bytes.copy(laneIncarnation);
        }

        public String targetClusterId() {
            return targetClusterId;
        }

        public long physicalBytes() {
            return physicalBytes;
        }

        public synchronized ReservationState state() {
            synchronized (owner) {
                return state;
            }
        }

        /** Keeps the physical charge after a logical callback deadline. */
        public boolean markZombie() {
            return owner.markZombie(this);
        }

        /** Releases the charge only after physical completion or certified cancellation. */
        public boolean release() {
            return owner.release(this);
        }

        @Override
        public void close() {
            release();
        }
    }

    public record LaneSnapshot(DestinationLaneId laneId, byte[] laneIncarnation, String targetClusterId,
                               boolean ready, boolean blocked, long activeRequests, long activeBytes,
                               long zombieRequests, long zombieBytes, long maxRequests, long maxBytes,
                               long maxZombieRequests, long maxZombieBytes,
                               long minimumReadyRequests, long minimumReadyBytes) {
        public LaneSnapshot {
            laneIncarnation = Bytes.copy(laneIncarnation);
        }

        @Override
        public byte[] laneIncarnation() {
            return Bytes.copy(laneIncarnation);
        }
    }

    public record WorkerSnapshot(long activeRequests, long activeBytes, long protectedReadyRequests,
                                 long protectedReadyBytes, long maxRequests, long maxBytes) {
    }

    public record ClusterSnapshot(String targetClusterId, long activeRequests, long activeBytes,
                                  long protectedReadyRequests, long protectedReadyBytes,
                                  long maxRequests, long maxBytes) {
    }

    private static final class ClusterState {
        private final long maxRequests;
        private final long maxBytes;
        private long activeRequests;
        private long activeBytes;

        private ClusterState(final long maxRequests, final long maxBytes) {
            this.maxRequests = maxRequests;
            this.maxBytes = maxBytes;
        }
    }

    private static final class LaneState {
        private final DestinationLaneId laneId;
        private final byte[] laneIncarnation;
        private final String targetClusterId;
        private final long minimumReadyRequests;
        private final long minimumReadyBytes;
        private final long maxRequests;
        private final long maxBytes;
        private final long maxZombieRequests;
        private final long maxZombieBytes;
        private boolean ready;
        private boolean blocked;
        private long activeRequests;
        private long activeBytes;
        private long zombieRequests;
        private long zombieBytes;

        private LaneState(final LaneSpec specification) {
            laneId = specification.laneId();
            laneIncarnation = specification.laneIncarnation();
            targetClusterId = specification.targetClusterId();
            minimumReadyRequests = specification.minimumReadyRequests();
            minimumReadyBytes = specification.minimumReadyBytes();
            maxRequests = specification.maxRequests();
            maxBytes = specification.maxBytes();
            maxZombieRequests = specification.maxZombieRequests();
            maxZombieBytes = specification.maxZombieBytes();
        }
    }
}
