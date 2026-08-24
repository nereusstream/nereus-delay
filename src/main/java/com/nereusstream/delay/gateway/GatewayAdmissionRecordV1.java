package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.transport.Digest32;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Canonical, tenant-scoped durable Gateway admission lease record. */
final class GatewayAdmissionRecordV1 {
    static final int VERSION = 1;
    private static final int LEASE_ID_LENGTH = 16;
    private static final int MAX_LEASES = 4096;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-gateway-admission-record-v1\0");
    private static final Comparator<Lease> LEASE_ORDER = (left, right) -> compareBytes(left.leaseId, right.leaseId);

    private final Digest32 tenantScopeHash;
    private final long revision;
    private final List<Lease> leases;

    GatewayAdmissionRecordV1(final Digest32 tenantScopeHash, final long revision, final List<Lease> leases) {
        this.tenantScopeHash = Objects.requireNonNull(tenantScopeHash, "tenantScopeHash");
        if (revision <= 0) {
            throw new IllegalArgumentException("Gateway admission revision must be positive");
        }
        final List<Lease> copied = new ArrayList<>(Objects.requireNonNull(leases, "leases"));
        if (copied.size() > MAX_LEASES) {
            throw new IllegalArgumentException("Gateway admission record has too many leases");
        }
        copied.sort(LEASE_ORDER);
        for (int index = 0; index < copied.size(); index++) {
            if (index > 0 && compareBytes(copied.get(index - 1).leaseId, copied.get(index).leaseId) == 0) {
                throw new IllegalArgumentException("Gateway admission lease identity is duplicated");
            }
        }
        this.revision = revision;
        this.leases = List.copyOf(copied);
    }

    static GatewayAdmissionRecordV1 empty(final Digest32 tenantScopeHash) {
        return new GatewayAdmissionRecordV1(tenantScopeHash, 1, List.of());
    }

    Digest32 tenantScopeHash() {
        return tenantScopeHash;
    }

    long revision() {
        return revision;
    }

    List<Lease> leases() {
        return leases;
    }

    GatewayAdmissionRecordV1 withLeases(final List<Lease> nextLeases) {
        if (revision == Long.MAX_VALUE) {
            throw new IllegalStateException("Gateway admission revision is exhausted");
        }
        return new GatewayAdmissionRecordV1(tenantScopeHash, revision + 1, nextLeases);
    }

    byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            final byte[] withoutDigest = canonicalWithoutDigest();
            output.writeBytes(withoutDigest);
            CanonicalProtobuf.bytes(output, 5, recordDigest(withoutDigest));
        });
    }

    static GatewayAdmissionRecordV1 decode(final byte[] encoded) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() < 4
                || field(fields, 0, 1).wireType() != 0
                || field(fields, 1, 2).wireType() != 2
                || field(fields, 2, 3).wireType() != 0) {
            throw new IllegalArgumentException("Gateway admission record fields are incomplete");
        }
        if (uint(field(fields, 0, 1), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported Gateway admission record version");
        }
        final byte[] tenantBytes = bytes(field(fields, 1, 2), 2);
        Bytes.requireLength(tenantBytes, Digest32.LENGTH, "tenantScopeHash");
        final Digest32 tenant = new Digest32(tenantBytes);
        final long revision = uint64(field(fields, 2, 3), 3);
        final List<Lease> leases = new ArrayList<>();
        int index = 3;
        while (index < fields.size() && fields.get(index).number() == 4) {
            leases.add(Lease.decode(bytes(fields.get(index++), 4)));
        }
        if (index >= fields.size() || fields.get(index).number() != 5 || index + 1 != fields.size()) {
            throw new IllegalArgumentException("Gateway admission record digest is missing or out of order");
        }
        final GatewayAdmissionRecordV1 result = new GatewayAdmissionRecordV1(tenant, revision, leases);
        if (!Bytes.constantTimeEquals(bytes(fields.get(index), 5), result.recordDigest())) {
            throw new IllegalArgumentException("Gateway admission record digest mismatch");
        }
        if (!java.util.Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("Gateway admission record is not canonical");
        }
        return result;
    }

    private byte[] canonicalWithoutDigest() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, tenantScopeHash.bytes());
            CanonicalProtobuf.uint64(output, 3, revision);
            for (Lease lease : leases) {
                CanonicalProtobuf.bytes(output, 4, lease.canonicalBytes());
            }
        });
    }

    private byte[] recordDigest() {
        return recordDigest(canonicalWithoutDigest());
    }

    private static byte[] recordDigest(final byte[] withoutDigest) {
        return Bytes.sha256(DIGEST_DOMAIN, withoutDigest);
    }

    private static CanonicalProtobuf.Reader.Field field(
            final List<CanonicalProtobuf.Reader.Field> fields, final int index, final int number) {
        if (index >= fields.size() || fields.get(index).number() != number) {
            throw new IllegalArgumentException("Gateway admission field " + number + " is missing");
        }
        return fields.get(index);
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.wireType() != 0) {
            throw new IllegalArgumentException("Gateway admission field " + number + " is not uint");
        }
        return field.unsignedValue();
    }

    private static long uint64(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = uint(field, number);
        if (value < 0) {
            throw new IllegalArgumentException("Gateway admission field " + number + " exceeds signed range");
        }
        return value;
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.wireType() != 2) {
            throw new IllegalArgumentException("Gateway admission field " + number + " is not bytes");
        }
        return field.rawValue();
    }

    private static int compareBytes(final byte[] left, final byte[] right) {
        for (int index = 0; index < left.length && index < right.length; index++) {
            final int comparison = Integer.compare(Byte.toUnsignedInt(left[index]), Byte.toUnsignedInt(right[index]));
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    /** One durable, expiring admission reservation. */
    static final class Lease {
        private final byte[] leaseId;
        private final GatewayIngressOperationV1 operation;
        private final long estimatedRequestBytes;
        private final long expiresAtEpochMs;

        Lease(
                final byte[] leaseId,
                final GatewayIngressOperationV1 operation,
                final long estimatedRequestBytes,
                final long expiresAtEpochMs) {
            Bytes.requireLength(leaseId, LEASE_ID_LENGTH, "leaseId");
            if (allZero(leaseId) || estimatedRequestBytes <= 0 || expiresAtEpochMs < 0) {
                throw new IllegalArgumentException("invalid Gateway admission lease");
            }
            this.leaseId = Bytes.copy(leaseId);
            this.operation = Objects.requireNonNull(operation, "operation");
            this.estimatedRequestBytes = estimatedRequestBytes;
            this.expiresAtEpochMs = expiresAtEpochMs;
        }

        byte[] leaseId() {
            return Bytes.copy(leaseId);
        }

        GatewayIngressOperationV1 operation() {
            return operation;
        }

        long estimatedRequestBytes() {
            return estimatedRequestBytes;
        }

        long expiresAtEpochMs() {
            return expiresAtEpochMs;
        }

        byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                final byte[] withoutDigest = canonicalWithoutDigest();
                output.writeBytes(withoutDigest);
                CanonicalProtobuf.bytes(output, 6, digest(withoutDigest));
            });
        }

        private byte[] digest() {
            return digest(canonicalWithoutDigest());
        }

        private static byte[] digest(final byte[] withoutDigest) {
            return Bytes.sha256(Bytes.utf8("nereus-delay-gateway-admission-lease-v1\0"), withoutDigest);
        }

        static Lease decode(final byte[] encoded) {
            final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
            final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
            while (reader.hasRemaining()) {
                fields.add(reader.next());
            }
            if (fields.size() != 6) {
                throw new IllegalArgumentException("Gateway admission lease fields are incomplete");
            }
            if (uint(field(fields, 0, 1), 1) != 1) {
                throw new IllegalArgumentException("unsupported Gateway admission lease version");
            }
            final byte[] id = bytes(field(fields, 1, 2), 2);
            final GatewayIngressOperationV1 operation = operation(uint(field(fields, 2, 3), 3));
            final long requestBytes = uint64(field(fields, 3, 4), 4);
            final long expires = uint64(field(fields, 4, 5), 5);
            final byte[] digest = bytes(field(fields, 5, 6), 6);
            final Lease result = new Lease(id, operation, requestBytes, expires);
            if (!Bytes.constantTimeEquals(digest, result.digest())) {
                throw new IllegalArgumentException("Gateway admission lease digest is malformed");
            }
            if (!java.util.Arrays.equals(encoded, result.canonicalBytes())) {
                throw new IllegalArgumentException("Gateway admission lease is not canonical");
            }
            return result;
        }

        private byte[] canonicalWithoutDigest() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.uint32(output, 1, 1);
                CanonicalProtobuf.bytes(output, 2, leaseId);
                CanonicalProtobuf.uint32(output, 3, operationWire(operation));
                CanonicalProtobuf.uint64(output, 4, estimatedRequestBytes);
                CanonicalProtobuf.uint64(output, 5, expiresAtEpochMs);
            });
        }

        private static long operationWire(final GatewayIngressOperationV1 operation) {
            return switch (operation) {
                case SCHEDULE -> 1;
                case RETRY_UNCERTAIN -> 2;
                case CONTROL -> 3;
            };
        }

        private static GatewayIngressOperationV1 operation(final long wire) {
            return switch ((int) wire) {
                case 1 -> GatewayIngressOperationV1.SCHEDULE;
                case 2 -> GatewayIngressOperationV1.RETRY_UNCERTAIN;
                case 3 -> GatewayIngressOperationV1.CONTROL;
                default -> throw new IllegalArgumentException("unknown Gateway admission operation: " + wire);
            };
        }

        private static boolean allZero(final byte[] value) {
            for (byte item : value) {
                if (item != 0) {
                    return false;
                }
            }
            return true;
        }
    }
}
