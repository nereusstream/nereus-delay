package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.ActivationBarrierV1;
import io.nereusstream.delay.protocol.ActiveLaneStateV1;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalLaneTupleV1;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.ChannelKindV1;
import io.nereusstream.delay.protocol.ChannelResourceIdentityV1;
import io.nereusstream.delay.protocol.CredentialUseKindV1;
import io.nereusstream.delay.protocol.CredentialUseLeaseV1;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProtocolTestFixtures;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.ReadyCertificateV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.AdmissionGate;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.runtime.LaneQuotaUsageProjection;
import io.nereusstream.delay.runtime.LaneRecord;
import io.nereusstream.delay.runtime.RuntimeReadiness;
import io.nereusstream.delay.store.ColumnFamily;
import io.nereusstream.delay.store.KeyCodec;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LaneActivationCoordinatorTest {
    @TempDir
    Path tempDir;

    @Test
    void activatesTypedLaneOnlyWithCertificateProofAndRetriesExactly() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 44);
        final UUID sourceTopic = UUID.nameUUIDFromBytes(Bytes.utf8("lane-activation-source"));
        final SourceAssignment assignment = new SourceAssignment(shardId,
                Bytes.sha256(Bytes.utf8("lane-activation-assignment")), 7,
                new KafkaActivationBarrier(shardId, "lane-activation-source-cluster", sourceTopic, 0));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = backend.acquire(assignment, "lane-activation-owner",
                Bytes.sha256(Bytes.utf8("lane-activation-session")), 100, 10_000).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final OwnerIdentityV1 owner = new OwnerIdentityV1(Bytes.utf8("lane-activation-deployment"),
                Bytes.utf8("lane-activation-worker"), lease.ownerEpoch(),
                Bytes.sha256(Bytes.utf8("lane-activation-fence")));
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("lane-activation-destination"), 1,
                Bytes.sha256(Bytes.utf8("lane-activation-destination-semantic")), ProfileKindV1.DESTINATION);
        final ProfileRefV1 capability = new ProfileRefV1(Bytes.utf8("lane-activation-capability"), 1,
                Bytes.sha256(Bytes.utf8("lane-activation-capability-semantic")),
                ProfileKindV1.DELIVERY_CAPABILITY);
        final byte[] tuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        final CanonicalLaneTupleV1.Projection tupleProjection = CanonicalLaneTupleV1.project(tuple);
        final io.nereusstream.delay.protocol.DestinationLaneId laneId =
                io.nereusstream.delay.protocol.DestinationLaneId.derive(tuple);
        final byte[] laneIncarnation = bytes(16, 44);
        final ActiveLaneStateV1 initial = new ActiveLaneStateV1(laneId, laneIncarnation, AdmissionGate.OPEN,
                RuntimeReadiness.RECOVERING_EVIDENCE, null, 1, 1, tupleProjection.destinationProfile(),
                tupleProjection.capabilityProfile(), tuple, 1, zeroChargeVector(), null, null,
                io.nereusstream.delay.protocol.LaneCircuitStateV1.CLOSED, 0, 0, 0, 0, null, null, null);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("lane-activation"));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final byte[] quota = LaneQuotaUsageProjection.empty()
                    .ensureLane(laneId, laneIncarnation, 1).canonicalBytes();
            store.write(batch -> {
                batch.putValue(ColumnFamily.META, 2, KeyCodec.metaLane(laneId),
                        io.nereusstream.delay.protocol.LaneRecordEnvelopeV1.active(initial).canonicalBytes());
                batch.putValue(ColumnFamily.META, 7, KeyCodec.metaQuota(3), quota);
            });
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            final OwnedDelayShard owned = new OwnedDelayShard(shard, lease, owner);
            owned.markCatchingUp(authority, assignment, SourceReplaySuccessor.strictKafka(), 101);
            owned.recordCatchup(new KafkaSourcePosition(shardId, "lane-activation-source-cluster", sourceTopic,
                    0, null, 1_000));

            final ChannelResourceIdentityV1 channel = channel(tupleProjection, laneId.bytes(), laneIncarnation);
            final ReadyCertificateV1 certificate = certificate(owner, store.metadata().storeIncarnation(),
                    laneId.bytes(), laneIncarnation, channel, sourceTopic);
            final EvidenceCursorV1 evidenceCursor = certificate.evidenceCursors().get(0);
            final LaneActivationPrerequisites proof = new LaneActivationPrerequisites(channel, certificate,
                    List.of(evidenceCursor), evidence(2_000, 2_001));
            final LaneActivationCoordinator coordinator = new LaneActivationCoordinator(owned, authority);
            final AtomicReference<LaneActivationCoordinator.ActivationRequest> requestSeen =
                    new AtomicReference<>();

            final LaneRecord activated = coordinator.activate(laneId, 2_000, request -> {
                requestSeen.set(request);
                assertEquals(owner, request.owner());
                assertArrayEquals(tuple, request.laneState().canonicalLaneTuple());
                assertArrayEquals(laneIncarnation, request.laneIncarnation());
                return proof;
            });
            assertNotNull(requestSeen.get());
            assertEquals(RuntimeReadiness.READY, activated.runtimeReadiness());
            assertEquals(2, activated.laneVersion());
            final ActiveLaneStateV1 ready = shard.getActiveLaneStateV1(laneId);
            assertNotNull(ready);
            assertEquals(RuntimeReadiness.READY, ready.runtimeReadiness());
            assertArrayEquals(certificate.canonicalBytes(), ready.readyCertificate());

            final LaneRecord retry = coordinator.activate(laneId, 2_000, ignored -> proof);
            assertEquals(RuntimeReadiness.READY, retry.runtimeReadiness());
            assertEquals(activated.laneVersion(), retry.laneVersion());
            assertArrayEquals(certificate.canonicalBytes(), shard.getActiveLaneStateV1(laneId).readyCertificate());

            final OwnerIdentityV1 foreignOwner = new OwnerIdentityV1(Bytes.utf8("foreign-deployment"),
                    Bytes.utf8("foreign-worker"), owner.ownerEpoch(),
                    Bytes.sha256(Bytes.utf8("foreign-fence")));
            final ReadyCertificateV1 foreignCertificate = certificate(foreignOwner,
                    store.metadata().storeIncarnation(), laneId.bytes(), laneIncarnation, channel, sourceTopic);
            final LaneActivationPrerequisites foreignProof = new LaneActivationPrerequisites(channel,
                    foreignCertificate, foreignCertificate.evidenceCursors(), evidence(2_000, 2_001));
            assertThrows(IllegalArgumentException.class,
                    () -> coordinator.activate(laneId, 2_000, ignored -> foreignProof));
            assertArrayEquals(certificate.canonicalBytes(), shard.getActiveLaneStateV1(laneId).readyCertificate());
        }
    }

    private static ChannelResourceIdentityV1 channel(final CanonicalLaneTupleV1.Projection tuple,
                                                      final byte[] laneId, final byte[] laneIncarnation) {
        final byte[] producer = Bytes.utf8("lane-activation-producer");
        final byte[] guard = Bytes.sha256(Bytes.utf8("lane-activation-resource-guard"));
        final byte[] binding = Bytes.sha256(Bytes.utf8("lane-activation-binding"));
        final byte[] fingerprint = Bytes.sha256(Bytes.utf8("lane-activation-fingerprint"));
        final TrustedUtcIntervalEvidence issuedAt = evidence(1_000, 1_001);
        final byte[] prefix = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, AdapterKindV1.KAFKA.wireValue());
            CanonicalProtobuf.uint32(output, 2, ChannelKindV1.BASELINE_PRODUCER.wireValue());
            CanonicalProtobuf.bytes(output, 3, laneId);
            CanonicalProtobuf.bytes(output, 4, laneIncarnation);
            CanonicalProtobuf.bytes(output, 5, tuple.targetResource().canonicalBytes());
            CanonicalProtobuf.uint32(output, 6, tuple.physicalPartition());
            CanonicalProtobuf.uint64(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, 0);
            CanonicalProtobuf.bytes(output, 9, producer);
            CanonicalProtobuf.bytes(output, 10, Bytes.sha256(producer));
            CanonicalProtobuf.bytes(output, 13, guard);
        });
        final CredentialUseLeaseV1 lease = new CredentialUseLeaseV1(tuple.destinationProfile(),
                CredentialUseKindV1.DESTINATION_CHANNEL,
                CredentialUseLeaseV1.destinationChannelHolderScope(prefix), 1, binding, fingerprint, issuedAt,
                9_000, 1);
        return new ChannelResourceIdentityV1(AdapterKindV1.KAFKA, ChannelKindV1.BASELINE_PRODUCER, laneId,
                laneIncarnation, tuple.targetResource(), tuple.physicalPartition(), 1, 0, producer,
                Bytes.sha256(producer), null, null, guard, 1, binding, fingerprint, lease);
    }

    private static ReadyCertificateV1 certificate(final OwnerIdentityV1 owner, final byte[] storeIncarnation,
                                                  final byte[] laneId, final byte[] laneIncarnation,
                                                  final ChannelResourceIdentityV1 channel, final UUID sourceTopic) {
        final byte[] barrier = ActivationBarrierV1.kafka(channel.targetResource(),
                (int) channel.physicalPartition(), 0, 0).canonicalBytes();
        final byte[] cursor = EvidenceCursorV1.kafka(laneId, laneIncarnation, uuidBytes(sourceTopic), 0,
                1, 2_000, 1, 1).canonicalBytes();
        final byte[] prefix = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, owner.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, storeIncarnation);
            CanonicalProtobuf.bytes(output, 4, laneId);
            CanonicalProtobuf.bytes(output, 5, laneIncarnation);
            CanonicalProtobuf.bytes(output, 6, channel.canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, barrier);
            CanonicalProtobuf.bytes(output, 8, cursor);
            CanonicalProtobuf.uint32(output, 9, 1);
            CanonicalProtobuf.uint32(output, 10, 1);
            CanonicalProtobuf.int64(output, 11, 8_000);
            CanonicalProtobuf.bytes(output, 12, evidence(1_000, 1_001).canonicalBytes());
            CanonicalProtobuf.uint64(output, 13, channel.credentialBindingGeneration());
            CanonicalProtobuf.bytes(output, 14, channel.credentialBindingDigest());
            CanonicalProtobuf.bytes(output, 15, channel.resolvedCredentialVersionFingerprintDigest());
        });
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(prefix);
            while (reader.hasRemaining()) {
                writeField(output, reader.next());
            }
            CanonicalProtobuf.bytes(output, 16,
                    Bytes.sha256(Bytes.utf8("nereus-delay-ready-certificate-v1\0"), prefix));
        });
        return ReadyCertificateV1.decode(encoded);
    }

    private static TrustedUtcIntervalEvidence evidence(final long earliest, final long latest) {
        return new TrustedUtcIntervalEvidence(earliest, latest,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("lane-activation-clock"), 1, 1, 1,
                Bytes.sha256(Bytes.utf8("lane-activation-time")), 0, null);
    }

    private static PublishAdmissionBody.ChargeVector zeroChargeVector() {
        return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 1, 0);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static byte[] uuidBytes(final UUID value) {
        return java.nio.ByteBuffer.allocate(16).putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits()).array();
    }

    private static void writeField(final ByteArrayOutputStream output,
                                   final CanonicalProtobuf.Reader.Field field) {
        if (field.wireType() == 0) {
            CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
        } else {
            CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
        }
    }
}
