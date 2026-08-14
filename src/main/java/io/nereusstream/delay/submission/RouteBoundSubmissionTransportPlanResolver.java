package io.nereusstream.delay.submission;

import io.nereusstream.delay.adapter.KafkaIngressResource;
import io.nereusstream.delay.adapter.KafkaProduceRequest;
import io.nereusstream.delay.adapter.PulsarIngressResource;
import io.nereusstream.delay.adapter.PulsarNativeSendRequest;
import io.nereusstream.delay.adapter.PulsarSendRequest;
import io.nereusstream.delay.adapter.PulsarTargetResource;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.IngressCredentialBindingRefV1;
import io.nereusstream.delay.protocol.KafkaIngressRouteResourceV1;
import io.nereusstream.delay.protocol.NativeCapabilitySnapshotV1;
import io.nereusstream.delay.protocol.NativePreparedDeliveryV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PreparedSubmissionV1;
import io.nereusstream.delay.protocol.PulsarIngressRouteResourceV1;
import io.nereusstream.delay.protocol.PulsarPhysicalPartitionIdentityV1;
import io.nereusstream.delay.protocol.RouteSnapshotV1;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.route.RouteSnapshotProvider;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.semantic.TrustedClock;
import io.nereusstream.delay.transport.CredentialBindingKey;
import io.nereusstream.delay.transport.Digest32;
import io.nereusstream.delay.transport.KafkaCommandTransportKey;
import io.nereusstream.delay.transport.PulsarCommandTransportKey;

import java.util.Arrays;
import java.util.Objects;

/** Exact prepared-submission resolver with no active-route reselection. */
public final class RouteBoundSubmissionTransportPlanResolver implements SubmissionTransportPlanResolver {
    private final RouteSnapshotProvider routes;
    private final TrustedClock trustedClock;

    public RouteBoundSubmissionTransportPlanResolver(final RouteSnapshotProvider routes,
                                                     final TrustedClock trustedClock) {
        this.routes = Objects.requireNonNull(routes, "routes");
        this.trustedClock = Objects.requireNonNull(trustedClock, "trustedClock");
    }

    @Override
    public SubmissionTransportPlan resolve(final AuthenticatedTenantContext tenant,
                                           final PreparedSubmissionV1 submission) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(submission, "submission");
        return submission.isManaged() ? resolveManaged(tenant, submission) : resolveNative(tenant, submission);
    }

    private SubmissionTransportPlan resolveManaged(final AuthenticatedTenantContext tenant,
                                                   final PreparedSubmissionV1 submission) {
        final byte[] frame = submission.managedFrame();
        final PreparedCommand command = decodeCanonicalV1(frame);
        final RouteSnapshotV1 route = exactRoute(tenant, command);
        final int partition = command.shardId().partition();
        final CredentialBindingKey binding = credentialBinding(route.credentialBinding());
        if (route.ingress() instanceof KafkaIngressRouteResourceV1 kafka) {
            final KafkaIngressResource resource = new KafkaIngressResource(command.shardId(),
                    kafka.authenticatedClusterId(), kafka.nativeTopicUuid(), partition);
            final KafkaProduceRequest request = KafkaProduceRequest.from(resource, command, frame);
            final KafkaCommandTransportKey key = new KafkaCommandTransportKey(kafka.authenticatedClusterId(),
                    kafka.canonicalPhysicalTopic(), kafka.nativeTopicUuid(), partition, binding);
            return new SubmissionTransportPlan(submission, new ManagedRouteAuthority(route), key, request,
                    new SubmissionProjectionKey(PreparedSubmissionBranch.MANAGED, AdapterKindV1.KAFKA));
        }
        if (!(route.ingress() instanceof PulsarIngressRouteResourceV1 pulsar)) {
            throw failure(StableCode.INGRESS_ROUTE_MISMATCH, null);
        }
        final PulsarPhysicalPartitionIdentityV1 physical = pulsar.partition(partition);
        final PulsarIngressResource resource = new PulsarIngressResource(command.shardId(),
                pulsar.authenticatedClusterId(), physical.resourceIncarnation(), physical.physicalTopic(),
                physical.physicalTopicCreationTimestamp(), partition);
        final PulsarSendRequest request = PulsarSendRequest.from(resource, command, frame);
        final PulsarCommandTransportKey key = new PulsarCommandTransportKey(pulsar.authenticatedClusterId(),
                physical.physicalTopic(), new io.nereusstream.delay.transport.Bytes32(physical.resourceIncarnation()),
                physical.physicalTopicCreationTimestamp(), partition, binding);
        return new SubmissionTransportPlan(submission, new ManagedRouteAuthority(route), key, request,
                new SubmissionProjectionKey(PreparedSubmissionBranch.MANAGED, AdapterKindV1.PULSAR));
    }

    private SubmissionTransportPlan resolveNative(final AuthenticatedTenantContext tenant,
                                                  final PreparedSubmissionV1 submission) {
        final NativePreparedDeliveryV1 prepared = submission.nativePrepared();
        final NativeCapabilitySnapshotV1 snapshot = prepared.capabilitySnapshot();
        try {
            if (!Arrays.equals(snapshot.sdkPrincipalScopeDigest(), tenant.principalScopeHash())
                    || !prepared.target().equals(snapshot.target())
                    || prepared.physicalPartition() != snapshot.physicalPartition()
                    || !Bytes.constantTimeEquals(prepared.preparedRef().preparedBytesSha256(),
                    Bytes.sha256(prepared.canonicalBytes()))
                    || trustedClock.nowEpochMs() >= prepared.capabilityExpiryEpochMs()) {
                throw failure(StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE, null);
            }
        } catch (SubmissionPlanException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw failure(StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE, failure);
        }
        final var target = prepared.target();
        final PulsarTargetResource resource = new PulsarTargetResource(target.authenticatedClusterId(),
                target.resourceIncarnation(), target.physicalTopic(), target.physicalTopicCreationTimestamp(),
                prepared.physicalPartition());
        final PulsarNativeSendRequest request = PulsarNativeSendRequest.from(resource, prepared);
        final CredentialBindingKey binding = new CredentialBindingKey(snapshot.credentialBindingGeneration(),
                new Digest32(snapshot.credentialBindingDigest()),
                new Digest32(snapshot.resolvedCredentialFingerprintDigest()));
        final PulsarCommandTransportKey key = new PulsarCommandTransportKey(target.authenticatedClusterId(),
                target.physicalTopic(), new io.nereusstream.delay.transport.Bytes32(target.resourceIncarnation()),
                target.physicalTopicCreationTimestamp(), prepared.physicalPartition(), binding);
        return new SubmissionTransportPlan(submission, new NativeTargetAuthority(prepared), key, request,
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
            if (!Arrays.equals(command.shardId().routeIncarnation().bytes(), route.routeIncarnation().bytes())) {
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
        return new CredentialBindingKey(binding.generation(), new Digest32(binding.bindingDigest()),
                new Digest32(binding.resolvedCredentialFingerprintDigest()));
    }

    private static SubmissionPlanException failure(final StableCode code, final Throwable cause) {
        return new SubmissionPlanException(code, cause);
    }
}
