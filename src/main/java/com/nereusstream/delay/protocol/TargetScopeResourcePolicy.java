package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Immutable allocation of one shared physical Source/registry backing to a finite Scope manager.
 * These canonical bytes are a declaration, not proof of backing ownership, paid Native capacity,
 * manager signature authority, or qualification of a process, clock or native client.
 */
public final class TargetScopeResourcePolicy {
    public static final int VERSION = 1;
    public static final int MAX_CHILDREN = 4096;
    public static final int MAX_PROCESS_LOANS = 256;
    public static final int MAX_MANAGERS = 32;
    public static final int MAX_IDENTITY_BYTES = 1024;
    public static final int MAX_CANONICAL_BYTES = 1_240_000;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-scope-resource-policy\0");
    private static final byte[] BINDING_DOMAIN = Bytes.utf8("nereus-delay-target-scope-resource-binding\0");

    /**
     * Source records mean original positions and Source bytes mean complete signed frame bytes.
     * Registry records and bytes mean records and complete wire bytes, respectively. Dimensions
     * may independently be zero; no process receives a fresh copy of any of these balances.
     */
    public record Shares(long sourceRecords, long sourceBytes, long registryRecords, long registryBytes) {
        public static final int MAX_CANONICAL_BYTES = 46;
        public static final Shares ZERO = new Shares(0, 0, 0, 0);

        public Shares {
            finiteNonnegative(sourceRecords, "Source positions");
            finiteNonnegative(sourceBytes, "complete Source frame bytes");
            finiteNonnegative(registryRecords, "registry records");
            finiteNonnegative(registryBytes, "registry wire bytes");
        }

        public Shares plus(Shares other) {
            Objects.requireNonNull(other);
            return new Shares(Math.addExact(sourceRecords, other.sourceRecords),
                    Math.addExact(sourceBytes, other.sourceBytes),
                    Math.addExact(registryRecords, other.registryRecords),
                    Math.addExact(registryBytes, other.registryBytes));
        }

        public boolean covers(Shares other) {
            Objects.requireNonNull(other);
            return sourceRecords >= other.sourceRecords && sourceBytes >= other.sourceBytes
                    && registryRecords >= other.registryRecords && registryBytes >= other.registryBytes;
        }

        public Shares subtract(Shares other) {
            if (!covers(other)) throw new IllegalArgumentException("shared resource shares underflow");
            return new Shares(Math.subtractExact(sourceRecords, other.sourceRecords),
                    Math.subtractExact(sourceBytes, other.sourceBytes),
                    Math.subtractExact(registryRecords, other.registryRecords),
                    Math.subtractExact(registryBytes, other.registryBytes));
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION);
                CanonicalProtobuf.uint64(out, 2, sourceRecords);
                CanonicalProtobuf.uint64(out, 3, sourceBytes);
                CanonicalProtobuf.uint64(out, 4, registryRecords);
                CanonicalProtobuf.uint64(out, 5, registryBytes);
            });
        }

        public static Shares decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 5, false, "resource shares");
            QueryCodecSupport.requireNumbers(f, new int[]{1, 2, 3, 4, 5}, "resource shares");
            requireVersion(f.getFirst(), "resource shares");
            final var value = new Shares(QueryCodecSupport.uint64Bits(f.get(1), 2),
                    QueryCodecSupport.uint64Bits(f.get(2), 3), QueryCodecSupport.uint64Bits(f.get(3), 4),
                    QueryCodecSupport.uint64Bits(f.get(4), 5));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "resource shares");
            return value;
        }
    }

    /** Signed schema-1 reservation coefficients, independent of future codec MAX constants. */
    public record Format(int rootFixedBytes, int childSlotBytes, int processLoanSlotBytes, int birthMarkerBytes) {
        public static final int MAX_CANONICAL_BYTES = 30;

        public Format {
            if (rootFixedBytes != 192 || childSlotBytes != 470 || processLoanSlotBytes != 134 || birthMarkerBytes != 160)
                throw new IllegalArgumentException("unknown shared resource Root format");
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION);
                CanonicalProtobuf.uint32(out, 2, rootFixedBytes);
                CanonicalProtobuf.uint32(out, 3, childSlotBytes);
                CanonicalProtobuf.uint32(out, 4, processLoanSlotBytes);
                CanonicalProtobuf.uint32(out, 5, birthMarkerBytes);
            });
        }

        public static Format decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 5, false, "resource Root format");
            QueryCodecSupport.requireNumbers(f, new int[]{1, 2, 3, 4, 5}, "resource Root format");
            requireVersion(f.getFirst(), "resource Root format");
            final var value = new Format(QueryCodecSupport.uint32(f.get(1), 2), QueryCodecSupport.uint32(f.get(2), 3),
                    QueryCodecSupport.uint32(f.get(3), 4), QueryCodecSupport.uint32(f.get(4), 5));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "resource Root format");
            return value;
        }
    }

    /**
     * Prepaid management storage, including the maximum closed Root, its first-birth marker and
     * all additional history records. Root wire overhead is separate from its canonical value.
     * Bounds do not authorize their owner's physical allocation or cross-key atomic creation.
     */
    public record Management(int maximumRootBytes, long rootWireOverheadBytes, long maximumBirthMarkerWireBytes,
            long maximumHistoryRecords, long maximumHistoryWireBytes) {
        public static final int MAX_CANONICAL_BYTES = 53;

        public Management {
            if (maximumRootBytes < 1 || maximumRootBytes > TargetScopeResourceRoot.MAX_CANONICAL_BYTES)
                throw new IllegalArgumentException("finite resource Root byte bound required");
            finiteNonnegative(rootWireOverheadBytes, "Root wire overhead");
            finitePositive(maximumBirthMarkerWireBytes, "first-birth marker wire bound");
            finiteNonnegative(maximumHistoryRecords, "management history records");
            finiteNonnegative(maximumHistoryWireBytes, "management history wire bytes");
            if ((maximumHistoryRecords == 0) != (maximumHistoryWireBytes == 0))
                throw new IllegalArgumentException("management history record and wire bounds disagree");
            new Shares(0, 0, Math.addExact(2, maximumHistoryRecords),
                    Math.addExact(Math.addExact(maximumRootBytes, rootWireOverheadBytes),
                            Math.addExact(maximumBirthMarkerWireBytes, maximumHistoryWireBytes)));
        }

        public Shares minimumShares() {
            return new Shares(0, 0, Math.addExact(2, maximumHistoryRecords),
                    Math.addExact(Math.addExact(maximumRootBytes, rootWireOverheadBytes),
                            Math.addExact(maximumBirthMarkerWireBytes, maximumHistoryWireBytes)));
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION);
                CanonicalProtobuf.uint32(out, 2, maximumRootBytes);
                CanonicalProtobuf.uint64(out, 3, rootWireOverheadBytes);
                CanonicalProtobuf.uint64(out, 4, maximumBirthMarkerWireBytes);
                CanonicalProtobuf.uint64(out, 5, maximumHistoryRecords);
                CanonicalProtobuf.uint64(out, 6, maximumHistoryWireBytes);
            });
        }

        public static Management decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 6, false, "resource management");
            QueryCodecSupport.requireNumbers(f, new int[]{1, 2, 3, 4, 5, 6}, "resource management");
            requireVersion(f.getFirst(), "resource management");
            final var value = new Management(QueryCodecSupport.uint32(f.get(1), 2),
                    QueryCodecSupport.uint64Bits(f.get(2), 3), QueryCodecSupport.uint64Bits(f.get(3), 4),
                    QueryCodecSupport.uint64Bits(f.get(4), 5), QueryCodecSupport.uint64Bits(f.get(5), 6));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "resource management");
            return value;
        }
    }

    /**
     * Frozen physical expansion and per-record overhead for the independent actual owner to fund.
     * Logical Native63/64 payment never proves possession of these deployment/storage bytes.
     */
    public record BackingCharge(long sourceByteNumerator, long sourceByteDenominator, long sourceRecordOverheadBytes,
            long registryByteNumerator, long registryByteDenominator, long registryRecordOverheadBytes) {
        public static final int MAX_CANONICAL_BYTES = 68;

        public BackingCharge {
            finitePositive(sourceByteNumerator, "Source backing numerator");
            finitePositive(sourceByteDenominator, "Source backing denominator");
            finiteNonnegative(sourceRecordOverheadBytes, "Source backing record overhead");
            finitePositive(registryByteNumerator, "registry backing numerator");
            finitePositive(registryByteDenominator, "registry backing denominator");
            finiteNonnegative(registryRecordOverheadBytes, "registry backing record overhead");
            if (sourceByteNumerator < sourceByteDenominator || registryByteNumerator < registryByteDenominator)
                throw new IllegalArgumentException("physical backing expansion cannot discount logical bytes");
        }

        public long sourceBackingBytes(Shares shares) {
            Objects.requireNonNull(shares);
            return backingBytes(shares.sourceBytes(), shares.sourceRecords(), sourceByteNumerator,
                    sourceByteDenominator, sourceRecordOverheadBytes);
        }

        public long registryBackingBytes(Shares shares) {
            Objects.requireNonNull(shares);
            return backingBytes(shares.registryBytes(), shares.registryRecords(), registryByteNumerator,
                    registryByteDenominator, registryRecordOverheadBytes);
        }

        private static long backingBytes(long bytes, long records, long numerator, long denominator, long overhead) {
            final long product = Math.multiplyExact(bytes, numerator);
            final long expanded = Math.addExact(product / denominator, product % denominator == 0 ? 0 : 1);
            final long result = Math.addExact(expanded, Math.multiplyExact(records, overhead));
            finiteNonnegative(result, "physical backing bytes");
            return result;
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION);
                CanonicalProtobuf.uint64(out, 2, sourceByteNumerator);
                CanonicalProtobuf.uint64(out, 3, sourceByteDenominator);
                CanonicalProtobuf.uint64(out, 4, sourceRecordOverheadBytes);
                CanonicalProtobuf.uint64(out, 5, registryByteNumerator);
                CanonicalProtobuf.uint64(out, 6, registryByteDenominator);
                CanonicalProtobuf.uint64(out, 7, registryRecordOverheadBytes);
            });
        }

        public static BackingCharge decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 7, false, "resource backing charge");
            QueryCodecSupport.requireNumbers(f, new int[]{1, 2, 3, 4, 5, 6, 7}, "resource backing charge");
            requireVersion(f.getFirst(), "resource backing charge");
            final var value = new BackingCharge(QueryCodecSupport.uint64Bits(f.get(1), 2),
                    QueryCodecSupport.uint64Bits(f.get(2), 3), QueryCodecSupport.uint64Bits(f.get(3), 4),
                    QueryCodecSupport.uint64Bits(f.get(4), 5), QueryCodecSupport.uint64Bits(f.get(5), 6),
                    QueryCodecSupport.uint64Bits(f.get(6), 7));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "resource backing charge");
            return value;
        }
    }

    /** The exact original physical/Native/deployment binding; no substitute quota identity is introduced. */
    public record Binding(SourcePosition sourceAnchor, byte[] registryNamespaceBinding, String registryKeyPrefix,
            String rootKey, String firstBirthMarkerKey, byte[] deploymentAllocationId, byte[] birthNonce,
            TargetQuotaScope scope, byte[] accountingIncarnation, byte[] lineage, byte[] backingAllocationDigest,
            byte[] originalHistoryDigest, byte[] producerDomainDigest, List<AuthorIdentity> serviceAliases,
            List<ProtocolTuple> protocols) {
        public static final int MAX_CANONICAL_BYTES = 1_200_000;

        public Binding {
            sourceAnchor = TargetSourcePosition.requireBounded(sourceAnchor);
            registryNamespaceBinding = assigned32(registryNamespaceBinding, "actual registry namespace");
            registryKeyPrefix = canonicalKey(registryKeyPrefix, "registry prefix");
            rootKey = canonicalKey(rootKey, "resource Root key");
            firstBirthMarkerKey = canonicalKey(firstBirthMarkerKey, "first-birth marker key");
            if (!rootKey.startsWith(registryKeyPrefix + "/")
                    || !firstBirthMarkerKey.startsWith(registryKeyPrefix + "/") || rootKey.equals(firstBirthMarkerKey))
                throw new IllegalArgumentException("resource Root and marker require distinct exact keys in the original prefix");
            deploymentAllocationId = assigned32(deploymentAllocationId, "deployment backing allocation ID");
            birthNonce = assigned32(birthNonce, "deployment backing birth nonce");
            Objects.requireNonNull(scope);
            if (scope.target() != null || !scope.shard().equals(sourceAnchor.shardId()))
                throw new IllegalArgumentException("resource manager requires the exact original Native SHARD scope");
            accountingIncarnation = TargetCompatibilityCodec.assigned(accountingIncarnation, 16, "accounting incarnation");
            lineage = TargetCompatibilityCodec.assigned(lineage, 16, "resource lineage");
            backingAllocationDigest = assigned32(backingAllocationDigest, "actual backing allocation digest");
            originalHistoryDigest = assigned32(originalHistoryDigest, "complete original history digest");
            producerDomainDigest = assigned32(producerDomainDigest, "complete producer domain digest");
            serviceAliases = identities(serviceAliases, 32, true, "SERVICE aliases");
            protocols = protocolValues(protocols);
        }

        @Override public byte[] registryNamespaceBinding() { return Bytes.copy(registryNamespaceBinding); }
        @Override public byte[] deploymentAllocationId() { return Bytes.copy(deploymentAllocationId); }
        @Override public byte[] birthNonce() { return Bytes.copy(birthNonce); }
        @Override public byte[] accountingIncarnation() { return Bytes.copy(accountingIncarnation); }
        @Override public byte[] lineage() { return Bytes.copy(lineage); }
        @Override public byte[] backingAllocationDigest() { return Bytes.copy(backingAllocationDigest); }
        @Override public byte[] originalHistoryDigest() { return Bytes.copy(originalHistoryDigest); }
        @Override public byte[] producerDomainDigest() { return Bytes.copy(producerDomainDigest); }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION);
                CanonicalProtobuf.bytes(out, 2, sourceAnchor.canonicalBytes());
                CanonicalProtobuf.bytes(out, 3, registryNamespaceBinding);
                CanonicalProtobuf.bytes(out, 4, Bytes.utf8(registryKeyPrefix));
                CanonicalProtobuf.bytes(out, 5, Bytes.utf8(rootKey));
                CanonicalProtobuf.bytes(out, 6, Bytes.utf8(firstBirthMarkerKey));
                CanonicalProtobuf.bytes(out, 7, deploymentAllocationId);
                CanonicalProtobuf.bytes(out, 8, birthNonce);
                CanonicalProtobuf.bytes(out, 9, scope.canonicalBytes());
                CanonicalProtobuf.bytes(out, 10, accountingIncarnation);
                CanonicalProtobuf.bytes(out, 11, lineage);
                CanonicalProtobuf.bytes(out, 12, backingAllocationDigest);
                CanonicalProtobuf.bytes(out, 13, originalHistoryDigest);
                CanonicalProtobuf.bytes(out, 14, producerDomainDigest);
                for (var alias : serviceAliases) CanonicalProtobuf.bytes(out, 15, alias.canonicalBytes());
                for (var protocol : protocols) CanonicalProtobuf.bytes(out, 16, protocol.canonicalBytes());
            });
        }

        public byte[] digest() { return Bytes.sha256(BINDING_DOMAIN, canonicalBytes()); }

        public static Binding decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 14 + 32 + 16, true, "resource binding");
            if (f.size() < 16) throw new IllegalArgumentException("incomplete resource binding");
            requireVersion(f.getFirst(), "resource binding");
            int at = 14;
            final var aliases = new ArrayList<AuthorIdentity>();
            while (at < f.size() && f.get(at).number() == 15) {
                final var raw = QueryCodecSupport.bytes(f.get(at++), 15);
                if (aliases.size() == 32 || raw.length > MAX_IDENTITY_BYTES)
                    throw new IllegalArgumentException("resource alias bound exceeded");
                aliases.add(AuthorIdentity.decode(raw));
            }
            final var protocols = new ArrayList<ProtocolTuple>();
            while (at < f.size() && f.get(at).number() == 16) {
                final var raw = QueryCodecSupport.bytes(f.get(at++), 16);
                if (protocols.size() == 16 || raw.length > 40)
                    throw new IllegalArgumentException("resource protocol bound exceeded");
                protocols.add(ProtocolTuple.decode(raw));
            }
            if (at != f.size()) throw new IllegalArgumentException("unknown resource binding field");
            final var value = new Binding(TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                    QueryCodecSupport.fixed(f.get(2), 3, 32), text(f.get(3), 4), text(f.get(4), 5), text(f.get(5), 6),
                    QueryCodecSupport.fixed(f.get(6), 7, 32), QueryCodecSupport.fixed(f.get(7), 8, 32),
                    TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(8), 9)),
                    QueryCodecSupport.fixed(f.get(9), 10, 16), QueryCodecSupport.fixed(f.get(10), 11, 16),
                    QueryCodecSupport.fixed(f.get(11), 12, 32), QueryCodecSupport.fixed(f.get(12), 13, 32),
                    QueryCodecSupport.fixed(f.get(13), 14, 32), aliases, protocols);
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "resource binding");
            return value;
        }

        @Override public boolean equals(Object other) {
            return other instanceof Binding that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
        }
        @Override public int hashCode() { return Arrays.hashCode(digest()); }
    }

    private final Binding binding;
    private final byte[] managerAuthorizationDigest;
    private final List<AuthorIdentity> authorizedManagers;
    private final int maximumChildren;
    private final int maximumProcessLoans;
    private final Shares totalShares;
    private final Shares managementShares;
    private final Format format;
    private final Management management;
    private final BackingCharge backingCharge;
    private final byte[] digest;

    /**
     * managerAuthorizationDigest pins an independent external manager grant/history, never this
     * Policy's digest. A child Header.birthAuthorizationDigest must equal this Policy.digest().
     * totalShares must already be owned from one common parent backing or authoritative disjoint
     * allocations; constructing this value neither issues such shares nor establishes their origin.
     */
    public TargetScopeResourcePolicy(Binding binding, byte[] managerAuthorizationDigest,
            List<AuthorIdentity> authorizedManagers, int maximumChildren, int maximumProcessLoans,
            Shares totalShares, Shares managementShares, Format format, Management management, BackingCharge backingCharge) {
        this.binding = Objects.requireNonNull(binding);
        if (binding.canonicalBytes().length > Binding.MAX_CANONICAL_BYTES)
            throw new IllegalArgumentException("resource binding exceeds its codec bound");
        this.managerAuthorizationDigest = assigned32(managerAuthorizationDigest, "independent manager authorization digest");
        this.authorizedManagers = identities(authorizedManagers, MAX_MANAGERS, false, "authorized manager identities");
        if (maximumChildren < 1 || maximumChildren > MAX_CHILDREN
                || maximumProcessLoans < 0 || maximumProcessLoans > MAX_PROCESS_LOANS)
            throw new IllegalArgumentException("explicit finite child and process-loan bounds required");
        this.maximumChildren = maximumChildren;
        this.maximumProcessLoans = maximumProcessLoans;
        this.totalShares = Objects.requireNonNull(totalShares);
        this.managementShares = Objects.requireNonNull(managementShares);
        this.format = Objects.requireNonNull(format);
        this.management = Objects.requireNonNull(management);
        this.backingCharge = Objects.requireNonNull(backingCharge);
        if (!totalShares.covers(managementShares) || !managementShares.covers(management.minimumShares())
                || management.maximumBirthMarkerWireBytes() < Math.addExact(format.birthMarkerBytes(), management.rootWireOverheadBytes()))
            throw new IllegalArgumentException("prepaid parent management shares do not cover Root, first marker and full history");
        backingCharge.sourceBackingBytes(totalShares);
        backingCharge.registryBackingBytes(totalShares);
        digest = Bytes.sha256(DOMAIN, fields());
        if (canonicalBytes().length > MAX_CANONICAL_BYTES || maximumRootReservation() > management.maximumRootBytes())
            throw new IllegalArgumentException("configured complete resource Root exceeds its prepaid management reservation");
    }

    public int schemaVersion() { return VERSION; }
    public Binding binding() { return binding; }
    public byte[] managerAuthorizationDigest() { return Bytes.copy(managerAuthorizationDigest); }
    public List<AuthorIdentity> authorizedManagers() { return authorizedManagers; }
    public int maximumChildren() { return maximumChildren; }
    public int maximumProcessLoans() { return maximumProcessLoans; }
    public Shares totalShares() { return totalShares; }
    public Shares managementShares() { return managementShares; }
    public Shares allocatableShares() { return totalShares.subtract(managementShares); }
    public Format format() { return format; }
    public Management management() { return management; }
    public BackingCharge backingCharge() { return backingCharge; }
    public byte[] digest() { return Bytes.copy(digest); }
    public TargetScopeResourceRoot.Reference reference() {
        return new TargetScopeResourceRoot.Reference(binding.deploymentAllocationId(), binding.birthNonce(), digest);
    }

    public long maximumRootReservation() {
        return Math.addExact(Math.addExact(canonicalBytes().length, format.rootFixedBytes()),
                Math.addExact(Math.multiplyExact((long) maximumChildren, format.childSlotBytes()),
                        Math.multiplyExact((long) maximumProcessLoans, format.processLoanSlotBytes())));
    }

    /** Structural binding only; the actual Authority still verifies the immutable paid definition. */
    public void requireHeader(TargetSloControlScopeHeader header) {
        Objects.requireNonNull(header);
        if (!binding.scope().equals(header.scope()) || !Arrays.equals(binding.lineage(), header.lineage())
                || !Arrays.equals(binding.sourceAnchor().canonicalBytes(), header.sourceAnchor().canonicalBytes())
                || !Arrays.equals(binding.registryNamespaceBinding(), header.registryNamespaceBinding())
                || !binding.registryKeyPrefix().equals(header.registryKeyPrefix())
                || !binding.serviceAliases().equals(header.serviceAliases()) || !binding.protocols().equals(header.protocols())
                || !Arrays.equals(digest, header.birthAuthorizationDigest()))
            throw new IllegalArgumentException("child Header changed the original resource policy binding");
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, binding.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, managerAuthorizationDigest);
            for (var identity : authorizedManagers) CanonicalProtobuf.bytes(out, 4, identity.canonicalBytes());
            CanonicalProtobuf.uint32(out, 5, maximumChildren);
            CanonicalProtobuf.uint32(out, 6, maximumProcessLoans);
            CanonicalProtobuf.bytes(out, 7, totalShares.canonicalBytes());
            CanonicalProtobuf.bytes(out, 8, managementShares.canonicalBytes());
            CanonicalProtobuf.bytes(out, 9, format.canonicalBytes());
            CanonicalProtobuf.bytes(out, 10, management.canonicalBytes());
            CanonicalProtobuf.bytes(out, 11, backingCharge.canonicalBytes());
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> { out.writeBytes(fields()); CanonicalProtobuf.bytes(out, 12, digest); });
    }

    public static TargetScopeResourcePolicy decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 11 + MAX_MANAGERS, true, "resource policy");
        if (f.size() < 12) throw new IllegalArgumentException("incomplete resource policy");
        requireVersion(f.getFirst(), "resource policy");
        int at = 3;
        final var managers = new ArrayList<AuthorIdentity>();
        while (at < f.size() && f.get(at).number() == 4) {
            final var raw = QueryCodecSupport.bytes(f.get(at++), 4);
            if (managers.size() == MAX_MANAGERS || raw.length > MAX_IDENTITY_BYTES)
                throw new IllegalArgumentException("manager identity bound exceeded");
            managers.add(AuthorIdentity.decode(raw));
        }
        if (at + 8 != f.size()) throw new IllegalArgumentException("invalid resource policy suffix");
        final var value = new TargetScopeResourcePolicy(Binding.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                QueryCodecSupport.fixed(f.get(2), 3, 32), managers, QueryCodecSupport.uint32(f.get(at), 5),
                QueryCodecSupport.uint32(f.get(at + 1), 6), Shares.decode(QueryCodecSupport.bytes(f.get(at + 2), 7)),
                Shares.decode(QueryCodecSupport.bytes(f.get(at + 3), 8)), Format.decode(QueryCodecSupport.bytes(f.get(at + 4), 9)),
                Management.decode(QueryCodecSupport.bytes(f.get(at + 5), 10)),
                BackingCharge.decode(QueryCodecSupport.bytes(f.get(at + 6), 11)));
        if (!Arrays.equals(value.digest, QueryCodecSupport.fixed(f.get(at + 7), 12, 32)))
            throw new IllegalArgumentException("resource policy digest mismatch");
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "resource policy");
        return value;
    }

    private record CanonicalOrder<T>(T value,byte[] key) {}

    private static List<AuthorIdentity> identities(List<AuthorIdentity> supplied, int maximum, boolean service, String name) {
        if (supplied == null || supplied.isEmpty() || supplied.size() > maximum)
            throw new IllegalArgumentException("finite nonempty " + name + " required");
        final var values = new ArrayList<CanonicalOrder<AuthorIdentity>>(supplied.size());
        for (var value : supplied) {
            Objects.requireNonNull(value);
            final byte[] key=value.canonicalBytes();
            if (key.length > MAX_IDENTITY_BYTES || service && value.kind() != AuthorIdentity.Kind.SERVICE)
                throw new IllegalArgumentException("invalid " + name);
            values.add(new CanonicalOrder<>(value,key));
        }
        values.sort((a, b) -> Arrays.compareUnsigned(a.key,b.key));
        for (int n = 1; n < values.size(); n++) if (values.get(n - 1).value.equals(values.get(n).value))
            throw new IllegalArgumentException("duplicate " + name);
        return values.stream().map(CanonicalOrder::value).toList();
    }

    private static List<ProtocolTuple> protocolValues(List<ProtocolTuple> supplied) {
        if (supplied == null || supplied.isEmpty() || supplied.size() > 16)
            throw new IllegalArgumentException("finite nonempty protocol domain required");
        final var values = new ArrayList<CanonicalOrder<ProtocolTuple>>(supplied.size());
        for (var value : supplied) {
            if (Objects.requireNonNull(value).recordKind() != ProtocolTuple.SYSTEM_MUTATION)
                throw new IllegalArgumentException("invalid resource protocol tuple");
            final byte[] key=value.canonicalBytes();
            if(key.length>40)throw new IllegalArgumentException("invalid resource protocol tuple");
            values.add(new CanonicalOrder<>(value,key));
        }
        values.sort((a, b) -> Arrays.compareUnsigned(a.key,b.key));
        for (int n = 1; n < values.size(); n++) if (values.get(n - 1).value.equals(values.get(n).value))
            throw new IllegalArgumentException("duplicate resource protocol tuple");
        return values.stream().map(CanonicalOrder::value).toList();
    }

    private static String canonicalKey(String value, String name) {
        Objects.requireNonNull(value, name);
        final var bytes = value.getBytes(StandardCharsets.UTF_8);
        if (value.isBlank() || value.endsWith("/") || value.indexOf('\0') >= 0 || bytes.length > 4096
                || !value.equals(new String(bytes, StandardCharsets.UTF_8)) || !Normalizer.isNormalized(value, Normalizer.Form.NFC))
            throw new IllegalArgumentException(name + " must be bounded canonical NFC UTF-8 without trailing slash");
        return value;
    }

    private static String text(CanonicalProtobuf.Reader.Field field, int number) {
        final var bytes = QueryCodecSupport.bytes(field, number);
        if (bytes.length > 4096) throw new IllegalArgumentException("registry key exceeds its bound");
        final var value = new String(bytes, StandardCharsets.UTF_8);
        if (!Arrays.equals(bytes, value.getBytes(StandardCharsets.UTF_8)))
            throw new IllegalArgumentException("invalid registry key UTF-8");
        return value;
    }

    private static byte[] assigned32(byte[] value, String name) {
        return TargetCompatibilityCodec.assigned(value, 32, name);
    }

    private static void requireVersion(CanonicalProtobuf.Reader.Field field, String name) {
        if (QueryCodecSupport.uint32(field, 1) != VERSION) throw new IllegalArgumentException("unknown " + name + " schema");
    }

    private static void finiteNonnegative(long value, String name) {
        if (value < 0 || value == Long.MAX_VALUE) throw new IllegalArgumentException(name + " must be finite nonnegative");
    }

    private static void finitePositive(long value, String name) {
        if (value < 1 || value == Long.MAX_VALUE) throw new IllegalArgumentException(name + " must be finite positive");
    }

    @Override public boolean equals(Object other) {
        return other instanceof TargetScopeResourcePolicy that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }
    @Override public int hashCode() { return Arrays.hashCode(digest); }
}
