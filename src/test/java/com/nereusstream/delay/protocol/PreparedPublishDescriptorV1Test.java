package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class PreparedPublishDescriptorV1Test {
    @Test
    void admissionDescriptorExposesTheExactTypedProjection() {
        final PublishAdmissionBodyTest.Fixture fixture =
                PublishAdmissionBodyTest.Fixture.create(new ShardId(RouteIncarnation.random(), 3));
        final PublishAdmissionBody admission = PublishAdmissionBody.decode(fixture.body());

        final PreparedPublishDescriptorV1 descriptor = admission.descriptor().value();

        assertArrayEquals(fixture.descriptor(), descriptor.canonicalBytes());
        assertArrayEquals(
                Bytes.sha256(Bytes.utf8("nereus-delay-prepared-publish-v1\0"), fixture.descriptor()),
                descriptor.preparedPublishHash());
        assertEquals(AdapterKindV1.KAFKA, descriptor.adapterKind());
        assertEquals(fixture.messageId(), descriptor.messageId());
        assertEquals(0, descriptor.generation());
        assertEquals(1, descriptor.attemptNo());
        assertEquals(DeliveryMode.MANAGED, descriptor.reservedMetadata().deliveryMode());
        assertEquals(descriptor, PreparedPublishDescriptorV1.decode(descriptor.canonicalBytes()));
    }

    @Test
    void reservedMetadataCannotBeRelabeledToAnotherShard() {
        final PublishAdmissionBodyTest.Fixture fixture =
                PublishAdmissionBodyTest.Fixture.create(new ShardId(RouteIncarnation.random(), 4));
        final PublishAdmissionBody admission = PublishAdmissionBody.decode(fixture.body());
        final PreparedPublishDescriptorV1 descriptor = admission.descriptor().value();
        final ReservedPublishMetadataV1 reserved = descriptor.reservedMetadata();
        final ReservedPublishMetadataV1 wrongShard = new ReservedPublishMetadataV1(
                RouteIncarnation.random(),
                reserved.shardPartition(),
                reserved.messageId(),
                reserved.generation(),
                reserved.publishAttemptId(),
                reserved.destinationProfileSemanticHash(),
                reserved.capabilityProfileSemanticHash(),
                reserved.deliverAtEpochMs(),
                reserved.deliveryMode());

        assertThrows(
                IllegalArgumentException.class,
                () -> new PreparedPublishDescriptorV1(
                        descriptor.adapterKind(),
                        descriptor.destinationLaneId(),
                        descriptor.laneIncarnation(),
                        descriptor.destinationProfile(),
                        descriptor.capabilityProfile(),
                        descriptor.targetResource(),
                        descriptor.physicalPartition(),
                        descriptor.channel(),
                        descriptor.messageId(),
                        descriptor.generation(),
                        descriptor.publishAttemptId(),
                        descriptor.attemptNo(),
                        descriptor.payload(),
                        descriptor.businessMetadata(),
                        wrongShard,
                        descriptor.deliverAtEpochMs(),
                        descriptor.expireAtEpochMs(),
                        descriptor.actionAtEpochMs()));
    }
}
