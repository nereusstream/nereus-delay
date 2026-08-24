package com.nereusstream.delay.submission;

import com.nereusstream.delay.adapter.KafkaIngressResource;
import com.nereusstream.delay.adapter.KafkaProduceRequest;
import com.nereusstream.delay.adapter.PulsarIngressResource;
import com.nereusstream.delay.adapter.PulsarNativeSendRequest;
import com.nereusstream.delay.adapter.PulsarSendRequest;
import com.nereusstream.delay.adapter.PulsarTargetResource;
import com.nereusstream.delay.protocol.AdapterKindV1;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.IngressCredentialBindingRefV1;
import com.nereusstream.delay.protocol.KafkaIngressRouteResourceV1;
import com.nereusstream.delay.protocol.NativeCapabilitySnapshotV1;
import com.nereusstream.delay.protocol.NativePreparedDeliveryV1;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedSubmissionV1;
import com.nereusstream.delay.protocol.PulsarIngressRouteResourceV1;
import com.nereusstream.delay.protocol.PulsarPhysicalPartitionIdentityV1;
import com.nereusstream.delay.protocol.RouteSnapshotV1;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.route.RouteSnapshotProvider;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.TrustedClock;
import com.nereusstream.delay.transport.CredentialBindingKey;
import com.nereusstream.delay.transport.Digest32;
import com.nereusstream.delay.transport.KafkaCommandTransportKey;
import com.nereusstream.delay.transport.PulsarCommandTransportKey;
import java.util.Arrays;
import java.util.Objects;

/** Exact prepared-submission resolver with no active-route reselection. */
public final class RouteBoundSubmissionTransportPlanResolver implements SubmissionTransportPlanResolver {
    private final RouteSnapshotProvider routes;
    private final TrustedClock trustedClock;

    public RouteBoundSubmissionTransportPlanResolver(
            final RouteSnapshotProvider routes, final TrustedClock trustedClock) {
        this.routes = Objects.requireNonNull(routes, "routes");
        this.trustedClock = Objects.requireNonNull(trustedClock, "trustedClock");
    }

    @Override
    public SubmissionTransportPlan resolve(
            final AuthenticatedTenantContext tenant, final PreparedSubmissionV1 submission) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(submission, "submission");
        return submission.isManaged() ? resolveManaged(tenant, submission) : resolveNative(tenant, submission);
    }

    private SubmissionTransportPlan resolveManaged(
            final AuthenticatedTenantContext tenant, final PreparedSubmissionV1 submission) {
        final byte[] frame = submission.managedFrame();
        final PreparedCommand command = decodeCanonicalV1(frame);
        final RouteSnapshotV1 route = exactRoute(tenant, command);
        final int partition = command.shardId().partition();
        final CredentialBindingKey binding = credentialBinding(route.credentialBinding());
        if (route.ingress() instanceof KafkaIngressRouteResourceV1 kafka) {
            final KafkaIngressResource resource = new KafkaIngressResource(
                    command.shardId(),
                    kafka.authenticatedClusterId(),
                    kafka.canonicalPhysicalTopic(),
                    kafka.nativeTopicUuid(),
                    partition);
            final KafkaProduceRequest request = KafkaProduceRequest.from(resource, command, frame);
            final KafkaCommandTransportKey key = new KafkaCommandTransportKey(
                    kafka.authenticatedClusterId(),
                    kafka.canonicalPhysicalTopic(),
                    kafka.nativeTopicUuid(),
                    partition,
                    binding);
            return new SubmissionTransportPlan(
                    submission,
                    new ManagedRouteAuthority(route),
                    key,
                    request,
                    new SubmissionProjectionKey(PreparedSubmissionBranch.MANAGED, AdapterKindV1.KAFKA));
        }
        if (!(route.ingress() instanceof PulsarIngressRouteResourceV1 pulsar)) {
            throw failure(StableCode.INGRESS_ROUTE_MISMATCH, null);
        }
        final PulsarPhysicalPartitionIdentityV1 physical = pulsar.partition(partition);
        final PulsarIngressResource resource = new PulsarIngressResource(
                command.shardId(),
                pulsar.authenticatedClusterId(),
                physical.resourceIncarnation(),
                physical.physicalTopic(),
                physical.physicalTopicCreationTimestamp(),
                partition);
        final PulsarSendRequest request = PulsarSendRequest.from(resource, command, frame);
        final PulsarCommandTransportKey key = new PulsarCommandTransportKey(
                pulsar.authenticatedClusterId(),
                physical.physicalTopic(),
                new com.nereusstream.delay.transport.Bytes32(physical.resourceIncarnation()),
                physical.physicalTopicCreationTimestamp(),
                partition,
                binding);
        return new SubmissionTransportPlan(
                submission,
                new ManagedRouteAuthority(route),
                key,
                request,
                new SubmissionProjectionKey(PreparedSubmissionBranch.MANAGED, AdapterKindV1.PULSAR));
    }

    private SubmissionTransportPlan resolveNative(
            final AuthenticatedTenantContext tenant, final PreparedSubmissionV1 submission) {
        final NativePreparedDeliveryV1 prepared = submission.nativePrepared();
        final NativeCapabilitySnapshotV1 snapshot = prepared.capabilitySnapshot();
        try {
            if (!Arrays.equals(snapshot.sdkPrincipalScopeDigest(), tenant.principalScopeHash())
                    || !prepared.target().equals(snapshot.target())
                    || prepared.physicalPartition() != snapshot.physicalPartition()
                    || !Bytes.constantTimeEquals(
                            prepared.preparedRef().preparedBytesSha256(), Bytes.sha256(prepared.canonicalBytes()))
                    || trustedClock.nowEpochMs() >= prepared.capabilityExpiryEpochMs()) {
                throw failure(StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE, null);
            }
        } catch (SubmissionPlanException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw failure(StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE, failure);
        }
        final var target = prepared.target();
        final PulsarTargetResource resource = new PulsarTargetResource(
                target.authenticatedClusterId(),
                target.resourceIncarnation(),
                target.physicalTopic(),
                target.physicalTopicCreationTimestamp(),
                prepared.physicalPartition());
        final PulsarNativeSendRequest request = PulsarNativeSendRequest.from(resource, prepared);
        final CredentialBindingKey binding = new CredentialBindingKey(
                snapshot.credentialBindingGeneration(),
                new Digest32(snapshot.credentialBindingDigest()),
                new Digest32(snapshot.resolvedCredentialFingerprintDigest()));
        final PulsarCommandTransportKey key = new PulsarCommandTransportKey(
                target.authenticatedClusterId(),
                target.physicalTopic(),
                new com.nereusstream.delay.transport.Bytes32(target.resourceIncarnation()),
                target.physicalTopicCreationTimestamp(),
                prepared.physicalPartition(),
                binding);
        return new SubmissionTransportPlan(
                submission,
                new NativeTargetAuthority(prepared),
                key,
                request,
                new SubmissionProjectionKey(PreparedSubmissionBranch.NATIVE, AdapterKindV1.PULSAR));
    }

    private RouteSnapshotV1 exactRoute(final AuthenticatedTenantContext tenant, final PreparedCommand command) {
        try {
            final RouteSnapshotV1 route = routes.exact(command.shardId().routeIncarnation(), tenant);
            if (route == null) {
                throw failure(StableCode.ROUTE_SNAPSHOT_UNAVAILABLE, null);
            }
            route.requireTenantScope(tenant.authenticatedTenantScopeHash(), tenant.tenantRoutingScope());
            if (route.ingress().partitionCount() <= command.shardId().partition()
                    || command.shardId().partition() < 0) {
                throw failure(StableCode.INGRESS_ROUTE_MISMATCH, null);
            }
            if (!Arrays.equals(
                    command.shardId().routeIncarnation().bytes(),
                    route.routeIncarnation().bytes())) {
                throw failure(StableCode.INGRESS_ROUTE_MISMATCH, null);
            }
            return route;
        } catch (SubmissionPlanException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw failure(StableCode.ROUTE_SNAPSHOT_UNAVAILABLE, failure);
        }
    }

    private static PreparedCommand decodeCanonicalV1(final byte[] frame) {
        try {
            final PreparedCommand command = CommandCodec.decodeFrameV1(frame);
            if (!Arrays.equals(frame, CommandCodec.encodeFrameV1(command))) {
                throw failure(StableCode.INVALID_PREPARED_COMMAND, null);
            }
            return command;
        } catch (SubmissionPlanException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw failure(StableCode.INVALID_PREPARED_COMMAND, failure);
        }
    }

    private static CredentialBindingKey credentialBinding(final IngressCredentialBindingRefV1 binding) {
        return new CredentialBindingKey(
                binding.generation(),
                new Digest32(binding.bindingDigest()),
                new Digest32(binding.resolvedCredentialFingerprintDigest()));
    }

    private static SubmissionPlanException failure(final StableCode code, final Throwable cause) {
        return new SubmissionPlanException(code, cause);
    }
}
