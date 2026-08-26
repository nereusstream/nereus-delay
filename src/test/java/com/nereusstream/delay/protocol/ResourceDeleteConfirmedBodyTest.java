package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class ResourceDeleteConfirmedBodyTest {
    @Test
    void deletedEvidenceRoundTripsWithExactOptionalIdentityFields() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 21);
        final byte[] resourceHash = Bytes.sha256(Bytes.utf8("resource"));
        final byte[] mutationId = Bytes.sha256(Bytes.utf8("retire-mutation"));
        final byte[] body = body(
                shard,
                mutationId,
                Bytes.sha256(Bytes.utf8("retire-hash")),
                resourceHash,
                4,
                ResourceDeleteConfirmedBody.DeleteOutcome.DELETED,
                resourceHash,
                Bytes.utf8("version-1"),
                Bytes.utf8("etag"));

        final ResourceDeleteConfirmedBody decoded = ResourceDeleteConfirmedBody.decode(body);

        assertEquals(ResourceDeleteConfirmedBody.DeleteOutcome.DELETED, decoded.outcome());
        assertArrayEquals(mutationId, decoded.intent().mutationId());
        assertArrayEquals(resourceHash, decoded.evidence().resourceIdentityHash());
        assertArrayEquals(Bytes.utf8("version-1"), decoded.evidence().observedImmutableVersion());
        assertArrayEquals(Bytes.utf8("etag"), decoded.evidence().observedEtag());
    }

    @Test
    void intentPreservesFullUnsignedResourceStateVersion() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 24);
        final byte[] resourceHash = Bytes.sha256(Bytes.utf8("large-version-resource"));
        final long expectedVersion = Long.MIN_VALUE;
        final byte[] body = body(
                shard,
                Bytes.sha256(Bytes.utf8("large-version-mutation")),
                Bytes.sha256(Bytes.utf8("large-version-hash")),
                resourceHash,
                expectedVersion,
                ResourceDeleteConfirmedBody.DeleteOutcome.DELETED,
                resourceHash,
                Bytes.utf8("version"),
                Bytes.utf8("etag"));

        final ResourceDeleteConfirmedBody decoded = ResourceDeleteConfirmedBody.decode(body);

        assertEquals(expectedVersion, decoded.intent().expectedResourceStateVersion());
    }

    @Test
    void alreadyAbsentEvidenceForbidsObservedVersionAndEtag() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 22);
        final byte[] resourceHash = Bytes.sha256(Bytes.utf8("absent-resource"));
        final byte[] body = body(
                shard,
                Bytes.sha256(Bytes.utf8("absent-mutation")),
                Bytes.sha256(Bytes.utf8("absent-hash")),
                resourceHash,
                5,
                ResourceDeleteConfirmedBody.DeleteOutcome.ALREADY_ABSENT,
                resourceHash,
                new byte[0],
                new byte[0]);

        final ResourceDeleteConfirmedBody decoded = ResourceDeleteConfirmedBody.decode(body);

        assertEquals(ResourceDeleteConfirmedBody.DeleteOutcome.ALREADY_ABSENT, decoded.outcome());
        assertEquals(0, decoded.evidence().observedImmutableVersion().length);
        assertEquals(0, decoded.evidence().observedEtag().length);
    }

    @Test
    void intentAndEvidenceMismatchesAreRejected() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 23);
        final byte[] resourceHash = Bytes.sha256(Bytes.utf8("resource"));
        final byte[] body = body(
                shard,
                Bytes.sha256(Bytes.utf8("mutation")),
                Bytes.sha256(Bytes.utf8("hash")),
                resourceHash,
                1,
                ResourceDeleteConfirmedBody.DeleteOutcome.DELETED,
                Bytes.sha256(Bytes.utf8("different-resource")),
                Bytes.utf8("version"),
                new byte[0]);
        assertThrows(IllegalArgumentException.class, () -> ResourceDeleteConfirmedBody.decode(body));

        final byte[] absentWithIdentity = body(
                shard,
                Bytes.sha256(Bytes.utf8("mutation-2")),
                Bytes.sha256(Bytes.utf8("hash-2")),
                resourceHash,
                1,
                ResourceDeleteConfirmedBody.DeleteOutcome.ALREADY_ABSENT,
                resourceHash,
                Bytes.utf8("version"),
                new byte[0]);
        assertThrows(IllegalArgumentException.class, () -> ResourceDeleteConfirmedBody.decode(absentWithIdentity));
    }

    @Test
    void confirmationIntervalMustFollowTheCompleteObservationInterval() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 25);
        final byte[] resourceHash = Bytes.sha256(Bytes.utf8("time-order-resource"));
        final TrustedUtcIntervalEvidence observedAt = time(3_000, 3_010, "observed-time");
        final TrustedUtcIntervalEvidence confirmedAt = time(3_005, 3_006, "confirmed-time");

        assertThrows(
                IllegalArgumentException.class,
                () -> ResourceDeleteConfirmedBody.decode(body(
                        shard,
                        Bytes.sha256(Bytes.utf8("time-order-mutation")),
                        Bytes.sha256(Bytes.utf8("time-order-hash")),
                        resourceHash,
                        1,
                        ResourceDeleteConfirmedBody.DeleteOutcome.ALREADY_ABSENT,
                        resourceHash,
                        new byte[0],
                        new byte[0],
                        observedAt,
                        confirmedAt)));
    }

    @Test
    void providerReturnedPayloadIdentityFieldsMustMatchExactResourceIdentity() {
        final byte[] identity = CanonicalProtobuf.message(
                output -> CanonicalProtobuf.bytes(output, 1, CanonicalProtobuf.message(payload -> {
                    CanonicalProtobuf.bytes(payload, 1, profileRef());
                    CanonicalProtobuf.bytes(payload, 2, Bytes.utf8("container"));
                    CanonicalProtobuf.bytes(payload, 3, Bytes.utf8("object"));
                    CanonicalProtobuf.bytes(payload, 4, Bytes.utf8("version-1"));
                    CanonicalProtobuf.bytes(payload, 5, Bytes.utf8("etag-1"));
                    CanonicalProtobuf.uint32(payload, 6, 4);
                    CanonicalProtobuf.bytes(payload, 7, Bytes.sha256(Bytes.utf8("payload")));
                })));

        ResourceRetireIntentBody.validateExternalDeleteIdentity(
                ResourceKind.PAYLOAD_OBJECT, identity, Bytes.utf8("version-1"), Bytes.utf8("etag-1"));
        assertThrows(
                IllegalArgumentException.class,
                () -> ResourceRetireIntentBody.validateExternalDeleteIdentity(
                        ResourceKind.PAYLOAD_OBJECT, identity, Bytes.utf8("version-2"), Bytes.utf8("etag-1")));
    }

    @Test
    void deletedObjectEvidenceMustCarryThePinnedImmutableIdentity() {
        final byte[] identity = CanonicalProtobuf.message(
                output -> CanonicalProtobuf.bytes(output, 1, CanonicalProtobuf.message(payload -> {
                    CanonicalProtobuf.bytes(payload, 1, profileRef());
                    CanonicalProtobuf.bytes(payload, 2, Bytes.utf8("container"));
                    CanonicalProtobuf.bytes(payload, 3, Bytes.utf8("object"));
                    CanonicalProtobuf.bytes(payload, 4, Bytes.utf8("version-1"));
                    CanonicalProtobuf.bytes(payload, 5, Bytes.utf8("etag-1"));
                    CanonicalProtobuf.uint64(payload, 6, 4);
                    CanonicalProtobuf.bytes(payload, 7, Bytes.sha256(Bytes.utf8("payload")));
                })));

        assertThrows(
                IllegalArgumentException.class,
                () -> ResourceRetireIntentBody.validateExternalDeleteIdentity(
                        ResourceKind.PAYLOAD_OBJECT,
                        identity,
                        new byte[0],
                        new byte[0],
                        ResourceDeleteConfirmedBody.DeleteOutcome.DELETED));
        assertThrows(
                IllegalArgumentException.class,
                () -> ResourceRetireIntentBody.validateExternalDeleteIdentity(
                        ResourceKind.PAYLOAD_OBJECT,
                        identity,
                        Bytes.utf8("version-1"),
                        new byte[0],
                        ResourceDeleteConfirmedBody.DeleteOutcome.DELETED));
        ResourceRetireIntentBody.validateExternalDeleteIdentity(
                ResourceKind.PAYLOAD_OBJECT,
                identity,
                Bytes.utf8("version-1"),
                Bytes.utf8("etag-1"),
                ResourceDeleteConfirmedBody.DeleteOutcome.DELETED);
    }

    private static byte[] body(
            final ShardId shard,
            final byte[] mutationId,
            final byte[] mutationHash,
            final byte[] resourceHash,
            final long expectedVersion,
            final ResourceDeleteConfirmedBody.DeleteOutcome outcome,
            final byte[] evidenceResourceHash,
            final byte[] observedVersion,
            final byte[] etag) {
        final TrustedUtcIntervalEvidence time = time(2_000, 2_001, "clock");
        final TrustedUtcIntervalEvidence confirmedAt = time(2_002, 2_003, "confirmed-clock");
        return body(
                shard,
                mutationId,
                mutationHash,
                resourceHash,
                expectedVersion,
                outcome,
                evidenceResourceHash,
                observedVersion,
                etag,
                time,
                confirmedAt);
    }

    private static byte[] body(
            final ShardId shard,
            final byte[] mutationId,
            final byte[] mutationHash,
            final byte[] resourceHash,
            final long expectedVersion,
            final ResourceDeleteConfirmedBody.DeleteOutcome outcome,
            final byte[] evidenceResourceHash,
            final byte[] observedVersion,
            final byte[] etag,
            final TrustedUtcIntervalEvidence observedAt,
            final TrustedUtcIntervalEvidence confirmedAt) {
        final byte[] intent = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, mutationId);
            CanonicalProtobuf.bytes(output, 2, mutationHash);
            CanonicalProtobuf.bytes(output, 3, resourceHash);
            CanonicalProtobuf.uint64Bits(output, 4, expectedVersion);
        });
        final byte[] evidence = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, evidenceResourceHash);
            CanonicalProtobuf.bytes(output, 2, Bytes.sha256(Bytes.utf8("provider-request")));
            CanonicalProtobuf.uint32(output, 3, outcome.wireValue());
            if (observedVersion.length != 0) {
                CanonicalProtobuf.bytes(output, 4, observedVersion);
            }
            if (etag.length != 0) {
                CanonicalProtobuf.bytes(output, 5, etag);
            }
            CanonicalProtobuf.bytes(output, 6, Bytes.sha256(Bytes.utf8("response")));
            CanonicalProtobuf.bytes(output, 7, observedAt.canonicalBytes());
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject(shard));
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.RESOURCE_DELETE_CONFIRMED.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, intent);
            CanonicalProtobuf.uint32(output, 11, outcome.wireValue());
            CanonicalProtobuf.bytes(output, 12, evidence);
            CanonicalProtobuf.bytes(output, 13, confirmedAt.canonicalBytes());
        });
    }

    private static TrustedUtcIntervalEvidence time(final long earliest, final long latest, final String sourceId) {
        return new TrustedUtcIntervalEvidence(
                earliest,
                latest,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8(sourceId),
                1,
                4,
                4,
                Bytes.sha256(Bytes.utf8("time-" + sourceId)),
                0,
                null);
    }

    private static byte[] subject(final ShardId shard) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
    }

    private static byte[] profileRef() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, Bytes.utf8("object-store"));
            CanonicalProtobuf.uint32(output, 2, 1);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(Bytes.utf8("profile")));
            CanonicalProtobuf.uint32(output, 4, ProfileKind.OBJECT_STORE.wireValue());
        });
    }
}
