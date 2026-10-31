package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CommandType;
import com.nereusstream.delay.protocol.KafkaIngressRouteResource;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PulsarIngressRouteResource;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.RouteSnapshot;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.semantic.TrustedClock;
import com.nereusstream.delay.submission.ManagedSourceIngressAdmission;
import com.nereusstream.delay.submission.SubmissionPlanException;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Operational backpressure from fresh actual Source observations. Does not grant distributed append reservations
 * or replace physical/retention capacity authority; the mandatory protection validates those caller-held obligations.
 */
public final class TargetSourceBacklogAdmission implements ManagedSourceIngressAdmission {
    public record Limits(long records, long bytes, long maximumAgeMillis, long maximumObservationAgeNanos) {
        public Limits {
            if (records <= 0
                    || records == Long.MAX_VALUE
                    || bytes <= 0
                    || bytes == Long.MAX_VALUE
                    || maximumAgeMillis <= 0
                    || maximumAgeMillis == Long.MAX_VALUE
                    || maximumObservationAgeNanos <= 0
                    || maximumObservationAgeNanos == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Source backlog growth limits must be explicit and finite");
            }
        }
    }

    private final Limits limits;
    private final Function<ShardId, TargetSourceBacklog.Snapshot> snapshots;
    private final TrustedClock trustedClock;
    private final LongSupplier monotonicClock;
    private final BiConsumer<TargetSourceBacklog.Snapshot, Integer> protection;

    public TargetSourceBacklogAdmission(
            final Limits limits,
            final Function<ShardId, TargetSourceBacklog.Snapshot> snapshots,
            final TrustedClock trustedClock,
            final LongSupplier monotonicClock,
            final BiConsumer<TargetSourceBacklog.Snapshot, Integer> protection) {
        this.limits = Objects.requireNonNull(limits, "limits");
        this.snapshots = Objects.requireNonNull(snapshots, "snapshots");
        this.trustedClock = Objects.requireNonNull(trustedClock, "trustedClock");
        this.monotonicClock = Objects.requireNonNull(monotonicClock, "monotonicClock");
        this.protection = Objects.requireNonNull(protection, "protection");
    }

    @Override
    public void requireAllowed(final PreparedCommand command, final RouteSnapshot route, final int frameBytes) {
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(route, "route");
        // Cancel retains the existing bounded control admission and the same Source log; it never bypasses apply order.
        if (command.type() == CommandType.CANCEL) {
            return;
        }
        try {
            final var snapshot = Objects.requireNonNull(snapshots.apply(command.shardId()), "Source backlog snapshot");
            snapshot.cut().requireCurrent();
            requireRoute(command, route, snapshot);
            protection.accept(snapshot, frameBytes);
            snapshot.cut().requireCurrent();
            final long age = monotonicClock.getAsLong() - snapshot.observedAtNanos();
            if (age < 0
                    || age >= limits.maximumObservationAgeNanos()
                    || frameBytes <= 0
                    || snapshot.records() >= limits.records()
                    || frameBytes > limits.bytes()
                    || snapshot.bytes() > limits.bytes() - frameBytes) {
                throw new IllegalStateException("Source backlog growth/freshness boundary reached");
            }
            if (snapshot.oldestBrokerPersistenceTimeEpochMs() != null) {
                final long now = trustedClock.nowEpochMs();
                final long persisted = snapshot.oldestBrokerPersistenceTimeEpochMs();
                if (now < persisted || now - persisted >= limits.maximumAgeMillis()) {
                    throw new IllegalStateException("Source backlog oldest-age boundary reached or clock is unproven");
                }
            }
            snapshot.cut().requireCurrent();
        } catch (RuntimeException unavailable) {
            throw new SubmissionPlanException(StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED, unavailable);
        }
    }

    private static void requireRoute(
            final PreparedCommand command, final RouteSnapshot route, final TargetSourceBacklog.Snapshot snapshot) {
        final var applied = snapshot.cut().applied();
        boolean same = command.shardId().equals(applied.shardId());
        if (applied instanceof KafkaSourcePosition kafka
                && route.ingress() instanceof KafkaIngressRouteResource resource) {
            same &= kafka.authenticatedClusterId().equals(resource.authenticatedClusterId())
                    && kafka.nativeTopicUuid().equals(resource.nativeTopicUuid());
        } else if (applied instanceof PulsarSourcePosition pulsar
                && route.ingress() instanceof PulsarIngressRouteResource resource) {
            final var partition = resource.partition(command.shardId().partition());
            same &= pulsar.physicalTopic().equals(partition.physicalTopic())
                    && Bytes.constantTimeEquals(pulsar.brokerResourceIncarnation(), partition.resourceIncarnation());
        } else {
            same = false;
        }
        if (!same) {
            throw new IllegalStateException("Source backlog snapshot belongs to another resolved resource");
        }
    }
}
