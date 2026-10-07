package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
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
 * A granted reservation only permits an adapter call to start. The caller
 * must keep the reservation until the physical operation completes; a
 * callback timeout may mark it {@link ReservationState#ZOMBIE}, but may not
 * release its request/byte charge early.</p>
 */
public final class DestinationPhysicalAdmission {
    private final long workerMaxRequests;
    private final long workerMaxBytes;
    private final Map<String, ClusterState> clusters = new HashMap<>();
    private final Map<DestinationLaneId, ChannelState> lanes = new HashMap<>();
    private final Map<String, ChannelState> targetChannels = new HashMap<>();
    private long workerActiveRequests;
    private long workerActiveBytes;
    private long nextReservationId = 1;
    private WorkClassExecutionRegistry workClassExecutionRegistry;

    public DestinationPhysicalAdmission(final long workerMaxRequests, final long workerMaxBytes) {
        if (workerMaxRequests <= 0 || workerMaxBytes <= 0) {
            throw new IllegalArgumentException("worker physical limits must be positive");
        }
        this.workerMaxRequests = workerMaxRequests;
        this.workerMaxBytes = workerMaxBytes;
    }

    /** Prevents one physical admission pool from multiplying queue capacity through multiple registries. */
    synchronized void bindWorkClassExecutionRegistry(final WorkClassExecutionRegistry registry) {
        final WorkClassExecutionRegistry requested = Objects.requireNonNull(registry, "registry");
        if (workClassExecutionRegistry != null && workClassExecutionRegistry != requested) {
            throw new IllegalArgumentException(
                    "destination physical admission is already bound to another work-class registry");
        }
        requested.bindWorkerSingleton(WorkClassExecutionRegistry.WorkerSingleton.DESTINATION_PHYSICAL_ADMISSION, this);
        workClassExecutionRegistry = requested;
    }

    /** Registers the hard target-cluster envelope before any Lane is opened. */
    public synchronized void registerTargetCluster(
            final String targetClusterId, final long maxRequests, final long maxBytes) {
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
        if (lanes.putIfAbsent(specification.laneId(), new ChannelState(specification)) != null) {
            throw new IllegalArgumentException("Lane is already registered");
        }
    }

    /** Registers one full immutable source-Shard/Target/channel generation, initially closed. */
    public synchronized void registerTargetChannel(final TargetChannelSpec specification) {
        Objects.requireNonNull(specification, "specification");
        if (!clusters.containsKey(specification.clusterId())) {
            throw new IllegalArgumentException("Target channel cluster is not registered");
        }
        if (targetChannels.putIfAbsent(targetKey(specification.channel()), new ChannelState(specification)) != null) {
            throw new IllegalArgumentException("Target channel is already registered");
        }
    }

    public synchronized void openTargetReady(final TargetChannelIdentity channel) {
        final var state = targetChannel(channel);
        if (!state.ready) {
            ensureReadyMinimumFits(state);
            state.ready = true;
        }
    }

    public synchronized void closeTargetReady(final TargetChannelIdentity channel) {
        targetChannel(channel).ready = false;
    }

    /** Stops this Source Shard's new/queued Target calls; physical completion and client teardown remain external. */
    public synchronized void closeTargetSourceShard(final ShardId shard) {
        Objects.requireNonNull(shard, "shard");
        for (var state : targetChannels.values()) {
            if (state.targetChannel.context().sourceShard().equals(shard)) {
                state.ready = false;
            }
        }
    }

    /** A closed generation remains registered until every physical/zombie charge has actually ended. */
    public synchronized void unregisterTargetChannel(final TargetChannelIdentity channel) {
        final var state = targetChannel(channel);
        if (state.ready || state.activeRequests != 0 || state.activeBytes != 0
                || state.zombieRequests != 0 || state.zombieBytes != 0) {
            throw new IllegalStateException("Target channel is READY or has outstanding physical charges");
        }
        targetChannels.remove(targetKey(channel));
    }

    public synchronized AdmissionDecision tryAcquireTarget(
            final TargetChannelIdentity channel, final long physicalBytes) {
        final var state = targetChannels.get(targetKey(channel));
        return state == null ? AdmissionDecision.rejected(Rejection.TARGET_CHANNEL_NOT_REGISTERED)
                : tryAcquire(state, physicalBytes);
    }

    public synchronized void clearTargetZombieBlock(final TargetChannelIdentity channel) {
        final var state = targetChannel(channel);
        if (state.zombieRequests >= state.maxZombieRequests || state.zombieBytes >= state.maxZombieBytes) {
            throw new IllegalStateException("Target channel zombie capacity is still exhausted");
        }
        state.blocked = false;
    }

    public synchronized TargetChannelSnapshot targetChannelSnapshot(final TargetChannelIdentity channel) {
        final var state = targetChannel(channel);
        return new TargetChannelSnapshot(channel, state.ready, state.blocked,
                state.activeRequests, state.activeBytes, state.zombieRequests, state.zombieBytes);
    }

    private ChannelState targetChannel(final TargetChannelIdentity channel) {
        final var state = targetChannels.get(targetKey(channel));
        if (state == null) {
            throw new IllegalArgumentException("Target channel is not registered");
        }
        return state;
    }

    private static String targetKey(final TargetChannelIdentity channel) {
        return Bytes.hex(Objects.requireNonNull(channel, "channel").encodedKey());
    }

    /** Opens the Lane only if its committed READY minimum can be protected. */
    public synchronized void openReady(final DestinationLaneId laneId) {
        final ChannelState lane = lane(laneId);
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
     * <p>This is only an in-process resource-registry operation. It does not
     * authorize a logical Lane retirement, release an Oxia grant, or replace
     * the source-ordered terminal guard. The incarnation check prevents a
     * stale teardown callback from removing a newer registration.</p>
     */
    public synchronized void unregisterLane(final DestinationLaneId laneId, final byte[] laneIncarnation) {
        final ChannelState lane = lane(laneId);
        Bytes.requireLength(laneIncarnation, 16, "laneIncarnation");
        if (!Arrays.equals(lane.laneIncarnation, laneIncarnation)) {
            throw new IllegalArgumentException("Lane identity mismatch");
        }
        if (lane.ready) {
            throw new IllegalStateException("cannot unregister a READY Lane");
        }
        if (lane.activeRequests != 0 || lane.activeBytes != 0 || lane.zombieRequests != 0 || lane.zombieBytes != 0) {
            throw new IllegalStateException("cannot unregister a Lane with physical charges");
        }
        lanes.remove(laneId);
    }

    /**
     * Attempts to reserve one physical request and its exact adapter byte
     * charge. Rejection is explicit so the caller can turn it into a Lane
     * runtime block rather than a business-level message failure.
     */
    public synchronized AdmissionDecision tryAcquire(
            final DestinationLaneId laneId, final byte[] laneIncarnation, final long physicalBytes) {
        final ChannelState lane = lanes.get(Objects.requireNonNull(laneId, "laneId"));
        if (lane == null) {
            return AdmissionDecision.rejected(Rejection.LANE_NOT_REGISTERED);
        }
        if (!Arrays.equals(lane.laneIncarnation, laneIncarnation)) {
            return AdmissionDecision.rejected(Rejection.LANE_IDENTITY_MISMATCH);
        }
        return tryAcquire(lane, physicalBytes);
    }

    private AdmissionDecision tryAcquire(final ChannelState lane, final long physicalBytes) {
        if (physicalBytes < 0) {
            throw new IllegalArgumentException("physical byte charge must be non-negative");
        }
        if (!lane.ready) {
            return AdmissionDecision.rejected(lane.targetChannel == null
                    ? Rejection.LANE_NOT_READY : Rejection.TARGET_CHANNEL_NOT_READY);
        }
        if (lane.blocked || lane.zombieRequests >= lane.maxZombieRequests || lane.zombieBytes >= lane.maxZombieBytes) {
            lane.blocked = true;
            return AdmissionDecision.rejected(Rejection.ZOMBIE_CAPACITY);
        }
        // reserves the vector in which every currently outstanding
        // request becomes a zombie. Checking only the already-marked zombie
        // bucket would admit a request that can never fit that worst-case
        // vector; a later callback timeout would then strand it as an
        // in-flight charge that cannot be marked zombie. Active charges are
        // retained until completion, so they are part of the potential
        // zombie envelope even before a timeout is observed.
        if (lane.activeRequests >= lane.maxZombieRequests || physicalBytes > lane.maxZombieBytes - lane.activeBytes) {
            return AdmissionDecision.rejected(Rejection.ZOMBIE_CAPACITY);
        }
        if (lane.activeRequests >= lane.maxRequests || physicalBytes > lane.maxBytes - lane.activeBytes) {
            return AdmissionDecision.rejected(lane.targetChannel == null
                    ? Rejection.LANE_CAPACITY : Rejection.TARGET_CHANNEL_CAPACITY);
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
        final Reservation reservation = new Reservation(
                this, reservationId, lane, physicalBytes);
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
        final ChannelState lane = lane(laneId);
        if (lane.zombieRequests >= lane.maxZombieRequests || lane.zombieBytes >= lane.maxZombieBytes) {
            throw new IllegalStateException("zombie capacity is still exhausted");
        }
        lane.blocked = false;
    }

    public synchronized LaneSnapshot laneSnapshot(final DestinationLaneId laneId) {
        final ChannelState lane = lane(laneId);
        return new LaneSnapshot(
                lane.laneId,
                lane.laneIncarnation,
                lane.targetClusterId,
                lane.ready,
                lane.blocked,
                lane.activeRequests,
                lane.activeBytes,
                lane.zombieRequests,
                lane.zombieBytes,
                lane.maxRequests,
                lane.maxBytes,
                lane.maxZombieRequests,
                lane.maxZombieBytes,
                lane.minimumReadyRequests,
                lane.minimumReadyBytes);
    }

    public synchronized WorkerSnapshot workerSnapshot() {
        return new WorkerSnapshot(
                workerActiveRequests,
                workerActiveBytes,
                readyMinimumRequests(null, false),
                readyMinimumBytes(null, false),
                workerMaxRequests,
                workerMaxBytes);
    }

    public synchronized ClusterSnapshot clusterSnapshot(final String targetClusterId) {
        final ClusterState cluster = clusters.get(requireClusterId(targetClusterId));
        if (cluster == null) {
            throw new IllegalArgumentException("target cluster is not registered");
        }
        return new ClusterSnapshot(
                targetClusterId,
                cluster.activeRequests,
                cluster.activeBytes,
                readyMinimumRequests(null, true, targetClusterId),
                readyMinimumBytes(null, true, targetClusterId),
                cluster.maxRequests,
                cluster.maxBytes);
    }

    private void ensureReadyMinimumFits(final ChannelState candidate) {
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

    private long readyMinimumRequests(final ChannelState excluded, final boolean sameCluster) {
        return readyMinimumRequests(excluded, sameCluster, null);
    }

    private long readyMinimumRequests(
            final ChannelState excluded, final boolean sameCluster, final String targetClusterId) {
        long total = 0;
        for (ChannelState lane : lanes.values()) {
            if (lane == excluded || !lane.ready) {
                continue;
            }
            if (sameCluster && targetClusterId != null && !lane.targetClusterId.equals(targetClusterId)) {
                continue;
            }
            if (sameCluster
                    && targetClusterId == null
                    && excluded != null
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

    private long readyMinimumBytes(final ChannelState excluded, final boolean sameCluster) {
        return readyMinimumBytes(excluded, sameCluster, null);
    }

    private long readyMinimumBytes(
            final ChannelState excluded, final boolean sameCluster, final String targetClusterId) {
        long total = 0;
        for (ChannelState lane : lanes.values()) {
            if (lane == excluded || !lane.ready) {
                continue;
            }
            if (sameCluster && targetClusterId != null && !lane.targetClusterId.equals(targetClusterId)) {
                continue;
            }
            if (sameCluster
                    && targetClusterId == null
                    && excluded != null
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

    private static boolean fits(
            final long retained, final long candidate, final long protectedMinimum, final long maximum) {
        try {
            return Math.addExact(Math.addExact(retained, candidate), protectedMinimum) <= maximum;
        } catch (ArithmeticException overflow) {
            return false;
        }
    }

    private ChannelState lane(final DestinationLaneId laneId) {
        final ChannelState result = lanes.get(Objects.requireNonNull(laneId, "laneId"));
        if (result == null) {
            throw new IllegalArgumentException("Lane is not registered");
        }
        return result;
    }

    private static String requireClusterId(final String value) {
        Objects.requireNonNull(value, "targetClusterId");
        final String decoded = new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        if (!decoded.equals(value)
                || value.isBlank()
                || value.indexOf('\0') >= 0
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
            final ChannelState lane = reservation.channel;
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
            final ChannelState lane = reservation.channel;
            final ClusterState cluster = clusters.get(reservation.targetClusterId);
            if (lane.activeRequests <= 0
                    || lane.activeBytes < reservation.physicalBytes
                    || cluster.activeRequests <= 0
                    || cluster.activeBytes < reservation.physicalBytes
                    || workerActiveRequests <= 0
                    || workerActiveBytes < reservation.physicalBytes) {
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
        TARGET_CLUSTER_CAPACITY,
        TARGET_CHANNEL_NOT_REGISTERED,
        TARGET_CHANNEL_NOT_READY,
        TARGET_CHANNEL_CAPACITY
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

    /** Finite per-generation limits; callers must also enforce activated Target/domain/slot/resource counts. */
    public record TargetChannelSpec(
            TargetChannelIdentity channel, CanonicalTargetPartition physical,
            long maxRequests, long maxBytes, long maxZombieRequests, long maxZombieBytes) {
        public TargetChannelSpec {
            Objects.requireNonNull(channel, "channel");
            Objects.requireNonNull(physical, "physical");
            if (!channel.context().target().equals(physical.id())
                    || channel.context().kind() == ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT
                            && physical.resource().kind() != BrokerResourceIdentity.Kind.KAFKA
                    || channel.context().kind() == ChannelKind.PULSAR_DEDUP_PRODUCER
                            && physical.resource().kind() != BrokerResourceIdentity.Kind.PULSAR
                    || maxRequests <= 0 || maxBytes <= 0 || maxZombieRequests <= 0 || maxZombieBytes <= 0
                    || maxZombieRequests > maxRequests || maxZombieBytes > maxBytes) {
                throw new IllegalArgumentException("invalid Target channel physical identity/limits");
            }
        }

        private String clusterId() {
            return physical.resource().kind() == BrokerResourceIdentity.Kind.KAFKA
                    ? physical.resource().kafka().authenticatedClusterId()
                    : physical.resource().pulsar().authenticatedClusterId();
        }
    }

    public record TargetChannelSnapshot(
            TargetChannelIdentity channel, boolean ready, boolean blocked,
            long activeRequests, long activeBytes, long zombieRequests, long zombieBytes) {}

    public record LaneSpec(
            DestinationLaneId laneId,
            byte[] laneIncarnation,
            String targetClusterId,
            long minimumReadyRequests,
            long minimumReadyBytes,
            long maxRequests,
            long maxBytes,
            long maxZombieRequests,
            long maxZombieBytes) {
        public LaneSpec {
            Objects.requireNonNull(laneId, "laneId");
            Bytes.requireLength(laneIncarnation, 16, "laneIncarnation");
            requireClusterId(targetClusterId);
            if (minimumReadyRequests < 0
                    || minimumReadyBytes < 0
                    || maxRequests <= 0
                    || maxBytes <= 0
                    || maxZombieRequests <= 0
                    || maxZombieBytes <= 0
                    || minimumReadyRequests > maxRequests
                    || minimumReadyBytes > maxBytes
                    || maxZombieRequests > maxRequests
                    || maxZombieBytes > maxBytes) {
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
        private final ChannelState channel;
        private final DestinationLaneId laneId;
        private final byte[] laneIncarnation;
        private final String targetClusterId;
        private final long physicalBytes;
        private ReservationState state = ReservationState.IN_FLIGHT;

        private Reservation(
                final DestinationPhysicalAdmission owner,
                final long id,
                final ChannelState channel,
                final long physicalBytes) {
            this.owner = owner;
            this.id = id;
            this.channel = channel;
            this.laneId = channel.laneId;
            this.laneIncarnation = channel.laneIncarnation == null ? null : Bytes.copy(channel.laneIncarnation);
            this.targetClusterId = channel.targetClusterId;
            this.physicalBytes = physicalBytes;
        }

        public long id() {
            return id;
        }

        public DestinationLaneId laneId() {
            if (laneId == null) {
                throw new IllegalStateException("Target reservation has no Lane identity");
            }
            return laneId;
        }

        public byte[] laneIncarnation() {
            if (laneIncarnation == null) {
                throw new IllegalStateException("Target reservation has no Lane incarnation");
            }
            return Bytes.copy(laneIncarnation);
        }

        public TargetChannelIdentity targetChannel() {
            return channel.targetChannel;
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

    public record LaneSnapshot(
            DestinationLaneId laneId,
            byte[] laneIncarnation,
            String targetClusterId,
            boolean ready,
            boolean blocked,
            long activeRequests,
            long activeBytes,
            long zombieRequests,
            long zombieBytes,
            long maxRequests,
            long maxBytes,
            long maxZombieRequests,
            long maxZombieBytes,
            long minimumReadyRequests,
            long minimumReadyBytes) {
        public LaneSnapshot {
            laneIncarnation = Bytes.copy(laneIncarnation);
        }

        @Override
        public byte[] laneIncarnation() {
            return Bytes.copy(laneIncarnation);
        }
    }

    public record WorkerSnapshot(
            long activeRequests,
            long activeBytes,
            long protectedReadyRequests,
            long protectedReadyBytes,
            long maxRequests,
            long maxBytes) {}

    public record ClusterSnapshot(
            String targetClusterId,
            long activeRequests,
            long activeBytes,
            long protectedReadyRequests,
            long protectedReadyBytes,
            long maxRequests,
            long maxBytes) {}

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

    private static final class ChannelState {
        private final TargetChannelIdentity targetChannel;
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

        private ChannelState(final LaneSpec specification) {
            targetChannel = null;
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

        private ChannelState(final TargetChannelSpec specification) {
            targetChannel = specification.channel();
            laneId = null;
            laneIncarnation = null;
            targetClusterId = specification.clusterId();
            // Target fairness and ordinary protection belong to the Worker Target ring/work-class graph;
            // a Profile, source Shard or channel slot never creates another Lane READY minimum.
            minimumReadyRequests = 0;
            minimumReadyBytes = 0;
            maxRequests = specification.maxRequests();
            maxBytes = specification.maxBytes();
            maxZombieRequests = specification.maxZombieRequests();
            maxZombieBytes = specification.maxZombieBytes();
        }
    }
}
