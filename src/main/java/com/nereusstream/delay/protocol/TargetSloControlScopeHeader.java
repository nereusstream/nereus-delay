package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Finite original registry Scope declaration. Neither its construction nor its digest grants authority. */
public final class TargetSloControlScopeHeader {
    public static final int LEGACY_VERSION = 1;
    public static final int VERSION = 2;
    public static final int MAX_SLOTS = 4096;
    public static final int MAX_SERVICE_ALIASES = 32;
    public static final int MAX_PROTOCOLS = 16;
    public static final int MAX_REGISTRY_KEY_PREFIX_BYTES = 4096;
    public static final int LEGACY_MAX_CANONICAL_BYTES = 1_087_100;
    public static final int MAX_CANONICAL_BYTES =
            LEGACY_MAX_CANONICAL_BYTES + TargetScopeExposurePolicy.MAX_CANONICAL_BYTES + 6;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-slo-control-scope-header\0");
    private static final byte[] EXPOSURE_DOMAIN = Bytes.utf8("nereus-delay-target-slo-control-scope-header-2\0");

    /** Closed codec ceilings, independently authorized as part of the original resource contract. */
    public record Limits(int maximumSlots, int maximumRootBytes, int maximumEntryBytes, long maximumEntriesBytes) {
        public static final int MAX_CANONICAL_BYTES = 40;

        public Limits {
            if (maximumSlots < 1
                    || maximumSlots > MAX_SLOTS
                    || maximumRootBytes < 1
                    || maximumRootBytes > TargetSloControlScopeRoot.MAX_CANONICAL_BYTES
                    || maximumEntryBytes < 1
                    || maximumEntryBytes > TargetSloControlRegisteredInput.MAX_CANONICAL_BYTES
                    || maximumEntriesBytes < maximumEntryBytes
                    || maximumEntriesBytes == Long.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "SLO Control Scope requires finite positive slot/root/entry/total bounds");
            }
            Math.multiplyExact((long) maximumSlots, maximumEntryBytes);
            Math.addExact(maximumEntriesBytes, maximumRootBytes);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, LEGACY_VERSION);
                CanonicalProtobuf.uint32(out, 2, maximumSlots);
                CanonicalProtobuf.uint32(out, 3, maximumRootBytes);
                CanonicalProtobuf.uint32(out, 4, maximumEntryBytes);
                CanonicalProtobuf.uint64(out, 5, maximumEntriesBytes);
            });
        }

        public static Limits decode(byte[] encoded) {
            final var f =
                    TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 5, false, "SLO Control Scope limits");
            QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3, 4, 5}, "SLO Control Scope limits");
            if (QueryCodecSupport.uint32(f.getFirst(), 1) != LEGACY_VERSION) {
                throw new IllegalArgumentException("unknown Scope limits schema");
            }
            final var value = new Limits(
                    QueryCodecSupport.uint32(f.get(1), 2),
                    QueryCodecSupport.uint32(f.get(2), 3),
                    QueryCodecSupport.uint32(f.get(3), 4),
                    QueryCodecSupport.uint64Bits(f.get(4), 5));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "SLO Control Scope limits");
            return value;
        }
    }

    /** Acyclic Source reference: it does not include the digest of the future full signed Source frame. */
    public record Reference(byte[] scopeId, long epoch, byte[] headerDigest) {
        public static final int MAX_CANONICAL_BYTES = 128;

        public Reference {
            scopeId = TargetCompatibilityCodec.assigned(scopeId, 32, "SLO Control Scope ID");
            headerDigest = TargetCompatibilityCodec.assigned(headerDigest, 32, "SLO Control Scope header digest");
            if (epoch < 1 || epoch == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Scope epoch must be finite positive");
            }
        }

        @Override
        public byte[] scopeId() {
            return Bytes.copy(scopeId);
        }

        @Override
        public byte[] headerDigest() {
            return Bytes.copy(headerDigest);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, LEGACY_VERSION);
                CanonicalProtobuf.bytes(out, 2, scopeId);
                CanonicalProtobuf.uint64(out, 3, epoch);
                CanonicalProtobuf.bytes(out, 4, headerDigest);
            });
        }

        public static Reference decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(
                    encoded, MAX_CANONICAL_BYTES, 4, false, "SLO Control Scope reference");
            QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3, 4}, "SLO Control Scope reference");
            if (QueryCodecSupport.uint32(f.getFirst(), 1) != LEGACY_VERSION) {
                throw new IllegalArgumentException("unknown Scope reference schema");
            }
            final var value = new Reference(
                    QueryCodecSupport.fixed(f.get(1), 2, 32),
                    QueryCodecSupport.uint64Bits(f.get(2), 3),
                    QueryCodecSupport.fixed(f.get(3), 4, 32));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "SLO Control Scope reference");
            return value;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Reference that
                    && epoch == that.epoch
                    && Arrays.equals(scopeId, that.scopeId)
                    && Arrays.equals(headerDigest, that.headerDigest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(scopeId), epoch, Arrays.hashCode(headerDigest));
        }
    }

    private final int schemaVersion;
    private final byte[] scopeId;
    private final long epoch;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final SourcePosition sourceAnchor;
    private final byte[] registryNamespaceBinding;
    private final String registryKeyPrefix;
    private final List<AuthorIdentity> serviceAliases;
    private final List<ProtocolTuple> protocols;
    private final byte[] birthAuthorizationDigest;
    private final byte[] resourceContractDigest;
    private final Limits limits;
    private final TargetScopeExposurePolicy exposurePolicy;
    private final byte[] digest;

    public TargetSloControlScopeHeader(
            byte[] scopeId,
            long epoch,
            TargetQuotaScope scope,
            byte[] lineage,
            SourcePosition sourceAnchor,
            byte[] registryNamespaceBinding,
            String registryKeyPrefix,
            List<AuthorIdentity> serviceAliases,
            List<ProtocolTuple> protocols,
            byte[] birthAuthorizationDigest,
            byte[] resourceContractDigest,
            Limits limits) {
        this(
                LEGACY_VERSION,
                scopeId,
                epoch,
                scope,
                lineage,
                sourceAnchor,
                registryNamespaceBinding,
                registryKeyPrefix,
                serviceAliases,
                protocols,
                birthAuthorizationDigest,
                resourceContractDigest,
                limits,
                null);
    }

    /** Explicit new-format declaration; the original constructor retains schema-1 bytes and ceilings. */
    public TargetSloControlScopeHeader(
            byte[] scopeId,
            long epoch,
            TargetQuotaScope scope,
            byte[] lineage,
            SourcePosition sourceAnchor,
            byte[] registryNamespaceBinding,
            String registryKeyPrefix,
            List<AuthorIdentity> serviceAliases,
            List<ProtocolTuple> protocols,
            byte[] birthAuthorizationDigest,
            byte[] resourceContractDigest,
            Limits limits,
            TargetScopeExposurePolicy exposurePolicy) {
        this(
                VERSION,
                scopeId,
                epoch,
                scope,
                lineage,
                sourceAnchor,
                registryNamespaceBinding,
                registryKeyPrefix,
                serviceAliases,
                protocols,
                birthAuthorizationDigest,
                resourceContractDigest,
                limits,
                Objects.requireNonNull(exposurePolicy));
    }

    private TargetSloControlScopeHeader(
            int schemaVersion,
            byte[] scopeId,
            long epoch,
            TargetQuotaScope scope,
            byte[] lineage,
            SourcePosition sourceAnchor,
            byte[] registryNamespaceBinding,
            String registryKeyPrefix,
            List<AuthorIdentity> serviceAliases,
            List<ProtocolTuple> protocols,
            byte[] birthAuthorizationDigest,
            byte[] resourceContractDigest,
            Limits limits,
            TargetScopeExposurePolicy exposurePolicy) {
        if ((schemaVersion != LEGACY_VERSION && schemaVersion != VERSION)
                || (schemaVersion == LEGACY_VERSION) != (exposurePolicy == null)) {
            throw new IllegalArgumentException("Scope header schema and exposure policy must be explicitly paired");
        }
        this.schemaVersion = schemaVersion;
        this.exposurePolicy = exposurePolicy;
        this.scopeId = TargetCompatibilityCodec.assigned(scopeId, 32, "SLO Control Scope ID");
        if (epoch < 1 || epoch == Long.MAX_VALUE) {
            throw new IllegalArgumentException("Scope epoch must be finite positive");
        }
        this.epoch = epoch;
        this.scope = Objects.requireNonNull(scope);
        this.limits = Objects.requireNonNull(limits);
        this.lineage = TargetCompatibilityCodec.assigned(lineage, 16, "SLO Control Scope lineage");
        this.sourceAnchor = TargetSourcePosition.requireBounded(sourceAnchor);
        if (scope.target() != null || !scope.shard().equals(sourceAnchor.shardId())) {
            throw new IllegalArgumentException("SLO Control Scope requires its exact original Source Shard");
        }
        this.registryNamespaceBinding =
                TargetCompatibilityCodec.assigned(registryNamespaceBinding, 32, "actual registry namespace binding");
        this.registryKeyPrefix = canonicalPrefix(registryKeyPrefix);
        this.serviceAliases = services(serviceAliases);
        this.protocols = tuples(protocols);
        this.birthAuthorizationDigest =
                TargetCompatibilityCodec.assigned(birthAuthorizationDigest, 32, "Scope birth authorization digest");
        this.resourceContractDigest =
                TargetCompatibilityCodec.assigned(resourceContractDigest, 32, "Scope resource contract digest");
        digest = Bytes.sha256(schemaVersion == LEGACY_VERSION ? DOMAIN : EXPOSURE_DOMAIN, fields());
        if (canonicalBytes().length
                        > (schemaVersion == LEGACY_VERSION ? LEGACY_MAX_CANONICAL_BYTES : MAX_CANONICAL_BYTES)
                || schemaVersion == LEGACY_VERSION
                        && limits.maximumRootBytes() > TargetSloControlScopeRoot.LEGACY_MAX_CANONICAL_BYTES
                || TargetSloControlScopeRoot.maximumRootReservation(this) > limits.maximumRootBytes()) {
            throw new IllegalArgumentException(
                    "Scope header and every possible MEMBER/seal revision exceed the original root reservation");
        }
    }

    public int schemaVersion() {
        return schemaVersion;
    }

    public byte[] scopeId() {
        return Bytes.copy(scopeId);
    }

    public long epoch() {
        return epoch;
    }

    public TargetQuotaScope scope() {
        return scope;
    }

    public byte[] lineage() {
        return Bytes.copy(lineage);
    }

    public SourcePosition sourceAnchor() {
        return sourceAnchor;
    }

    public byte[] registryNamespaceBinding() {
        return Bytes.copy(registryNamespaceBinding);
    }

    public String registryKeyPrefix() {
        return registryKeyPrefix;
    }

    public List<AuthorIdentity> serviceAliases() {
        return serviceAliases;
    }

    public List<ProtocolTuple> protocols() {
        return protocols;
    }

    public byte[] birthAuthorizationDigest() {
        return Bytes.copy(birthAuthorizationDigest);
    }

    public byte[] resourceContractDigest() {
        return Bytes.copy(resourceContractDigest);
    }

    public Limits limits() {
        return limits;
    }
    /** Null only for the original schema-1 domain; it cannot borrow new-format exposure space. */
    public TargetScopeExposurePolicy exposurePolicy() {
        return exposurePolicy;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public Reference reference() {
        return new Reference(scopeId, epoch, digest);
    }

    public boolean matches(Reference reference) {
        return reference().equals(reference);
    }

    /**
     * Structural Scope membership only. Original birth, actor, alias and complete-domain authority remain independent.
     */
    public void requireInput(TargetSloControlRegisteredInput input) {
        final var actual = Objects.requireNonNull(input);
        if (!matches(actual.headerReference()) || actual.entryBytes() > limits.maximumEntryBytes()) {
            throw new IllegalArgumentException("registered input changed its exact Scope reference or entry bound");
        }
        final var mutation = actual.mutation();
        final var request = actual.prepared().request().branch();
        final ProtocolTuple tuple;
        if (request instanceof TargetSloWriterControlRequest writer) {
            final var body = TargetSloWriterControlBody.decode(mutation.canonicalBody());
            if (!reference().equals(body.admissionScope())) {
                throw new IllegalArgumentException("writer Control lacks its exact scoped admission reference");
            }
            requireScope(writer.scope(), writer.lineage());
            tuple = body.tuple();
            if (writer.contract() != null) {
                requireAnchor(writer.contract().sourceAnchor());
                requireService(writer.contract().service());
            } else {
                requireWriterAlias(writer.reference().writerIdentityDigest());
                requirePhysicalSource(writer.fenceAt().source());
            }
        } else if (request instanceof TargetSloObservationControlRequest observation) {
            final var body = TargetSloObservationControlBody.decode(mutation.canonicalBody());
            if (!reference().equals(body.admissionScope())) {
                throw new IllegalArgumentException("observation Control lacks its exact scoped admission reference");
            }
            requireScope(observation.scope(), observation.lineage());
            tuple = body.tuple();
            if (observation.policy() != null) {
                if (!observation.policy().requiresWriterEpoch()) {
                    throw new IllegalArgumentException(
                            "new scoped ENABLE cannot introduce an unbounded legacy SERVICE domain");
                }
                requireAnchor(observation.policy().sourceAnchor());
                requireService(observation.policy().service());
                if (observation.policy().requiredWriterEpoch() != null) {
                    requireWriterAlias(
                            observation.policy().requiredWriterEpoch().writerIdentityDigest());
                }
            }
        } else {
            throw new IllegalArgumentException("Scope accepts only original SLO writer/observation Control inputs");
        }
        if (!protocols.contains(tuple)) {
            throw new IllegalArgumentException("SLO Control tuple is outside its original finite Scope range");
        }
    }

    private void requireScope(TargetQuotaScope actual, byte[] actualLineage) {
        if (!scope.equals(actual) || !Arrays.equals(lineage, actualLineage)) {
            throw new IllegalArgumentException("SLO Control input changed its original tenant/Shard/lineage");
        }
    }

    private void requireAnchor(SourcePosition actual) {
        requirePhysicalSource(actual);
    }

    private void requirePhysicalSource(SourcePosition actual) {
        TargetSourcePosition.requireBounded(actual);
        if (!scope.shard().equals(actual.shardId()) || !sourceAnchor.sameSourceIdentity(actual)) {
            throw new IllegalArgumentException("SLO Control input changed its original physical Source");
        }
    }

    private void requireService(AuthorIdentity actual) {
        requireWriterAlias(Bytes.sha256(actual.canonicalBytes()));
    }

    private void requireWriterAlias(byte[] writerDigest) {
        if (serviceAliases.stream()
                .noneMatch(alias -> Arrays.equals(Bytes.sha256(alias.canonicalBytes()), writerDigest))) {
            throw new IllegalArgumentException("SLO Control writer is outside its original finite SERVICE alias range");
        }
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, schemaVersion);
            CanonicalProtobuf.bytes(out, 2, scopeId);
            CanonicalProtobuf.uint64(out, 3, epoch);
            CanonicalProtobuf.bytes(out, 4, scope.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, lineage);
            CanonicalProtobuf.bytes(out, 6, sourceAnchor.canonicalBytes());
            CanonicalProtobuf.bytes(out, 7, registryNamespaceBinding);
            CanonicalProtobuf.bytes(out, 8, Bytes.utf8(registryKeyPrefix));
            for (var alias : serviceAliases) {
                CanonicalProtobuf.bytes(out, 9, alias.canonicalBytes());
            }
            for (var protocol : protocols) {
                CanonicalProtobuf.bytes(out, 10, protocol.canonicalBytes());
            }
            CanonicalProtobuf.bytes(out, 11, birthAuthorizationDigest);
            CanonicalProtobuf.bytes(out, 12, resourceContractDigest);
            CanonicalProtobuf.bytes(out, 13, limits.canonicalBytes());
            if (schemaVersion == VERSION) {
                CanonicalProtobuf.bytes(out, 14, exposurePolicy.canonicalBytes());
            }
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, schemaVersion == LEGACY_VERSION ? 14 : 15, digest);
        });
    }

    public static TargetSloControlScopeHeader decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(
                encoded,
                MAX_CANONICAL_BYTES,
                13 + MAX_SERVICE_ALIASES + MAX_PROTOCOLS,
                true,
                "SLO Control Scope header");
        if (f.size() < 14) {
            throw new IllegalArgumentException("incomplete SLO Control Scope header");
        }
        for (int n = 0; n < 8; n++) {
            if (f.get(n).number() != n + 1) {
                throw new IllegalArgumentException("invalid Scope header field order");
            }
        }
        final int version = QueryCodecSupport.uint32(f.getFirst(), 1);
        if (version != LEGACY_VERSION && version != VERSION
                || version == LEGACY_VERSION && encoded.length > LEGACY_MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("unknown or oversized Scope header schema");
        }
        int at = 8;
        final var services = new ArrayList<AuthorIdentity>();
        final var protocols = new ArrayList<ProtocolTuple>();
        while (at < f.size() && f.get(at).number() == 9) {
            final var raw = QueryCodecSupport.bytes(f.get(at++), 9);
            if (services.size() == MAX_SERVICE_ALIASES || raw.length > TargetSloObservationPolicy.MAX_SERVICE_BYTES) {
                throw new IllegalArgumentException("Scope SERVICE alias bound exceeded");
            }
            services.add(AuthorIdentity.decode(raw));
        }
        while (at < f.size() && f.get(at).number() == 10) {
            final var raw = QueryCodecSupport.bytes(f.get(at++), 10);
            if (protocols.size() == MAX_PROTOCOLS || raw.length > 40) {
                throw new IllegalArgumentException("Scope protocol range bound exceeded");
            }
            protocols.add(ProtocolTuple.decode(raw));
        }
        if (at + (version == LEGACY_VERSION ? 4 : 5) != f.size()) {
            throw new IllegalArgumentException("invalid Scope header suffix");
        }
        final var prefix = QueryCodecSupport.bytes(f.get(7), 8);
        if (prefix.length > MAX_REGISTRY_KEY_PREFIX_BYTES) {
            throw new IllegalArgumentException("registry key prefix exceeds bound");
        }
        final var prefixText = new String(prefix, StandardCharsets.UTF_8);
        if (!Arrays.equals(prefix, prefixText.getBytes(StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("invalid registry key prefix UTF-8");
        }
        final var value = new TargetSloControlScopeHeader(
                version,
                QueryCodecSupport.fixed(f.get(1), 2, 32),
                QueryCodecSupport.uint64Bits(f.get(2), 3),
                TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(3), 4)),
                QueryCodecSupport.fixed(f.get(4), 5, 16),
                TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(5), 6)),
                QueryCodecSupport.fixed(f.get(6), 7, 32),
                prefixText,
                services,
                protocols,
                QueryCodecSupport.fixed(f.get(at), 11, 32),
                QueryCodecSupport.fixed(f.get(at + 1), 12, 32),
                Limits.decode(QueryCodecSupport.bytes(f.get(at + 2), 13)),
                version == LEGACY_VERSION
                        ? null
                        : TargetScopeExposurePolicy.decode(QueryCodecSupport.bytes(f.get(at + 3), 14)));
        if (!Arrays.equals(
                value.digest,
                QueryCodecSupport.fixed(
                        f.get(at + (version == LEGACY_VERSION ? 3 : 4)), version == LEGACY_VERSION ? 14 : 15, 32))) {
            throw new IllegalArgumentException("Scope header digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "SLO Control Scope header");
        return value;
    }

    private static String canonicalPrefix(String value) {
        Objects.requireNonNull(value, "original registry key prefix");
        final var bytes = value.getBytes(StandardCharsets.UTF_8);
        if (value.isBlank()
                || value.endsWith("/")
                || value.indexOf('\0') >= 0
                || bytes.length > MAX_REGISTRY_KEY_PREFIX_BYTES
                || !value.equals(new String(bytes, StandardCharsets.UTF_8))
                || !Normalizer.isNormalized(value, Normalizer.Form.NFC)) {
            throw new IllegalArgumentException(
                    "registry key prefix must be bounded canonical NFC UTF-8 without trailing slash");
        }
        return value;
    }

    private record CanonicalOrder<T>(T value, byte[] key) {}

    private static List<AuthorIdentity> services(List<AuthorIdentity> aliases) {
        if (aliases == null || aliases.isEmpty() || aliases.size() > MAX_SERVICE_ALIASES) {
            throw new IllegalArgumentException("finite SERVICE aliases required");
        }
        final var ordered = new ArrayList<CanonicalOrder<AuthorIdentity>>(aliases.size());
        for (var alias : aliases) {
            if (Objects.requireNonNull(alias).kind() != AuthorIdentity.Kind.SERVICE) {
                throw new IllegalArgumentException("invalid Scope SERVICE alias");
            }
            final byte[] key = alias.canonicalBytes();
            if (key.length > TargetSloObservationPolicy.MAX_SERVICE_BYTES) {
                throw new IllegalArgumentException("invalid Scope SERVICE alias");
            }
            ordered.add(new CanonicalOrder<>(alias, key));
        }
        ordered.sort((a, b) -> Arrays.compareUnsigned(a.key, b.key));
        for (int n = 1; n < ordered.size(); n++) {
            if (Arrays.equals(ordered.get(n - 1).key, ordered.get(n).key)) {
                throw new IllegalArgumentException("duplicate Scope SERVICE alias");
            }
        }
        return ordered.stream().map(CanonicalOrder::value).toList();
    }

    private static List<ProtocolTuple> tuples(List<ProtocolTuple> tuples) {
        if (tuples == null || tuples.isEmpty() || tuples.size() > MAX_PROTOCOLS) {
            throw new IllegalArgumentException("finite Scope protocol range required");
        }
        final var ordered = new ArrayList<CanonicalOrder<ProtocolTuple>>(tuples.size());
        for (var tuple : tuples) {
            if (Objects.requireNonNull(tuple).recordKind() != ProtocolTuple.SYSTEM_MUTATION) {
                throw new IllegalArgumentException("Scope protocols must be System Mutation tuples");
            }
            ordered.add(new CanonicalOrder<>(tuple, tuple.canonicalBytes()));
        }
        ordered.sort((a, b) -> Arrays.compareUnsigned(a.key, b.key));
        for (int n = 1; n < ordered.size(); n++) {
            if (ordered.get(n - 1).value.equals(ordered.get(n).value)) {
                throw new IllegalArgumentException("duplicate Scope protocol tuple");
            }
        }
        return ordered.stream().map(CanonicalOrder::value).toList();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetSloControlScopeHeader that
                && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
