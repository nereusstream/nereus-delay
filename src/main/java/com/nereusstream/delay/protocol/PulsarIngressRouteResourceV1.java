package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Signed Route resource branch for all exact physical Pulsar command topics. */
public final class PulsarIngressRouteResourceV1 implements IngressRouteResourceV1 {
    private final String authenticatedClusterId;
    private final String physicalTopicBase;
    private final List<PulsarPhysicalPartitionIdentityV1> partitions;

    public PulsarIngressRouteResourceV1(
            final String authenticatedClusterId,
            final String physicalTopicBase,
            final List<PulsarPhysicalPartitionIdentityV1> partitions) {
        this.authenticatedClusterId = nfc(authenticatedClusterId, "authenticatedClusterId");
        this.physicalTopicBase = nfc(physicalTopicBase, "physicalTopicBase");
        Objects.requireNonNull(partitions, "partitions");
        if (partitions.isEmpty()) {
            throw new IllegalArgumentException("Pulsar Route must contain at least one partition");
        }
        final List<PulsarPhysicalPartitionIdentityV1> copy = new ArrayList<>(partitions.size());
        for (int index = 0; index < partitions.size(); index++) {
            final PulsarPhysicalPartitionIdentityV1 partition =
                    Objects.requireNonNull(partitions.get(index), "partitions[" + index + "]");
            if (partition.partition() != index
                    || !partition.physicalTopic().equals(expectedPhysicalTopic(physicalTopicBase, index))) {
                throw new IllegalArgumentException("Pulsar Route partitions are incomplete or not canonical");
            }
            copy.add(partition);
        }
        this.partitions = List.copyOf(copy);
    }

    @Override
    public String authenticatedClusterId() {
        return authenticatedClusterId;
    }

    public String physicalTopicBase() {
        return physicalTopicBase;
    }

    public List<PulsarPhysicalPartitionIdentityV1> partitions() {
        return partitions;
    }

    @Override
    public int partitionCount() {
        return partitions.size();
    }

    @Override
    public AdapterKindV1 adapterKind() {
        return AdapterKindV1.PULSAR;
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(
                output -> CanonicalProtobuf.bytes(output, 2, CanonicalProtobuf.message(fields -> {
                    CanonicalProtobuf.bytes(fields, 1, authenticatedClusterId.getBytes(StandardCharsets.UTF_8));
                    CanonicalProtobuf.bytes(fields, 2, physicalTopicBase.getBytes(StandardCharsets.UTF_8));
                    CanonicalProtobuf.uint32(fields, 3, partitions.size());
                    for (PulsarPhysicalPartitionIdentityV1 partition : partitions) {
                        CanonicalProtobuf.bytes(fields, 4, partition.canonicalBytes());
                    }
                })));
    }

    public static PulsarIngressRouteResourceV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> outer = QueryCodecSupport.read(encoded, "IngressRouteResourceV1");
        QueryCodecSupport.requireNumbers(outer, new int[] {2}, "IngressRouteResourceV1");
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(QueryCodecSupport.nested(outer.get(0), 2), "PulsarIngressRouteResourceV1", true);
        if (fields.size() < 4
                || fields.get(0).number() != 1
                || fields.get(1).number() != 2
                || fields.get(2).number() != 3) {
            throw new IllegalArgumentException("PulsarIngressRouteResourceV1 fields are incomplete");
        }
        final int partitionCount = QueryCodecSupport.uint32(fields.get(2), 3);
        if (fields.size() != 3 + partitionCount) {
            throw new IllegalArgumentException("Pulsar Route partition set is incomplete");
        }
        final List<PulsarPhysicalPartitionIdentityV1> partitions = new ArrayList<>(partitionCount);
        for (int index = 0; index < partitionCount; index++) {
            if (fields.get(index + 3).number() != 4) {
                throw new IllegalArgumentException("Pulsar Route partition field order is invalid");
            }
            partitions.add(
                    PulsarPhysicalPartitionIdentityV1.decode(QueryCodecSupport.nested(fields.get(index + 3), 4)));
        }
        final PulsarIngressRouteResourceV1 result = new PulsarIngressRouteResourceV1(
                utf8(QueryCodecSupport.bytes(fields.get(0), 1)),
                utf8(QueryCodecSupport.bytes(fields.get(1), 2)),
                partitions);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "IngressRouteResourceV1");
        return result;
    }

    public PulsarPhysicalPartitionIdentityV1 partition(final int partition) {
        if (partition < 0 || partition >= partitions.size()) {
            throw new IllegalArgumentException("partition outside Pulsar Route");
        }
        return partitions.get(partition);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PulsarIngressRouteResourceV1 that
                && authenticatedClusterId.equals(that.authenticatedClusterId)
                && physicalTopicBase.equals(that.physicalTopicBase)
                && partitions.equals(that.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(authenticatedClusterId, physicalTopicBase, partitions);
    }

    private static String expectedPhysicalTopic(final String base, final int partition) {
        return base + "-partition-" + partition;
    }

    private static String utf8(final byte[] value) {
        final String decoded = new String(value, StandardCharsets.UTF_8);
        if (!Arrays.equals(decoded.getBytes(StandardCharsets.UTF_8), value)) {
            throw new IllegalArgumentException("Pulsar Route text is not valid UTF-8");
        }
        return decoded;
    }

    private static String nfc(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()
                || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be nonblank NFC UTF-8");
        }
        return value;
    }
}
