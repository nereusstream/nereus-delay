package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Complete original provider public material. Shape, signatures and response hashes alone grant no authority. */
public record TargetUncommittedObjectClosure(
        byte[] intentDigest,
        ProfileRef profile,
        byte[] container,
        byte[] objectKey,
        byte[] closedHistory,
        List<Version> versions) {
    public static final int SCHEMA_VERSION = 1,
            MAX_VERSIONS = 32,
            MAX_BYTES = 4 * 1024 * 1024,
            MAX_PUBLIC_BYTES = 1024 * 1024;

    public record Deletion(byte[] requestIdHash, byte[] responseHash, byte[] publicHistory) {
        public Deletion {
            requestIdHash = TargetCompatibilityCodec.assigned(requestIdHash, 32, "actual version delete request ID");
            responseHash = TargetCompatibilityCodec.assigned(responseHash, 32, "actual version delete response");
            publicHistory = bounded(publicHistory, MAX_PUBLIC_BYTES, "original exact provider delete history");
        }

        @Override
        public byte[] requestIdHash() {
            return Bytes.copy(requestIdHash);
        }

        @Override
        public byte[] responseHash() {
            return Bytes.copy(responseHash);
        }

        @Override
        public byte[] publicHistory() {
            return Bytes.copy(publicHistory);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.bytes(out, 1, requestIdHash);
                CanonicalProtobuf.bytes(out, 2, responseHash);
                CanonicalProtobuf.bytes(out, 3, publicHistory);
            });
        }

        static Deletion decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(
                    encoded, MAX_PUBLIC_BYTES + 128, 3, false, "original provider version deletion");
            QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3}, "original provider version deletion");
            final var v = new Deletion(
                    QueryCodecSupport.fixed(f.get(0), 1, 32),
                    QueryCodecSupport.fixed(f.get(1), 2, 32),
                    QueryCodecSupport.bytes(f.get(2), 3));
            QueryCodecSupport.requireCanonical(encoded, v.canonicalBytes(), "original provider version deletion");
            return v;
        }
    }

    public record Version(PayloadObjectResource resource, Deletion deletion) {
        public Version {
            Objects.requireNonNull(resource);
            if (resource.etag() == null) {
                throw new IllegalArgumentException("exact version inventory requires a provider ETag");
            }
            if (resource.canonicalBytes().length > MAX_BYTES) {
                throw new IllegalArgumentException("provider version exceeds inventory bound");
            }
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.bytes(out, 1, resource.canonicalBytes());
                if (deletion != null) {
                    CanonicalProtobuf.bytes(out, 2, deletion.canonicalBytes());
                }
            });
        }

        static Version decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_BYTES, 2, false, "closed provider version");
            QueryCodecSupport.requireNumbers(
                    f, f.size() == 1 ? new int[] {1} : new int[] {1, 2}, "closed provider version");
            final var v = new Version(
                    PayloadObjectResource.decode(QueryCodecSupport.bytes(f.get(0), 1)),
                    f.size() == 2 ? Deletion.decode(QueryCodecSupport.bytes(f.get(1), 2)) : null);
            QueryCodecSupport.requireCanonical(encoded, v.canonicalBytes(), "closed provider version");
            return v;
        }
    }

    public TargetUncommittedObjectClosure {
        intentDigest = TargetCompatibilityCodec.assigned(intentDigest, 32, "original uncommitted intent digest");
        Objects.requireNonNull(profile);
        if (profile.profileKind() != ProfileKind.OBJECT_STORE) {
            throw new IllegalArgumentException("original object Profile required");
        }
        container = bounded(container, 1024, "original container");
        objectKey = bounded(objectKey, 4096, "original reservation object key");
        closedHistory = bounded(closedHistory, MAX_PUBLIC_BYTES, "complete durable writer-closure history");
        if (versions == null || versions.size() > MAX_VERSIONS) {
            throw new IllegalArgumentException("complete finite version inventory required; truncation is forbidden");
        }
        final var ordered = new ArrayList<>(versions);
        ordered.sort((a, b) -> Arrays.compareUnsigned(
                a.resource().immutableVersion(), b.resource().immutableVersion()));
        byte[] previous = null;
        long bytes =
                closedHistory.length + container.length + objectKey.length + profile.canonicalBytes().length + 128L;
        for (var version : ordered) {
            final var resource = version.resource();
            if (!profile.equals(resource.objectStoreProfile())
                    || !Arrays.equals(container, resource.container())
                    || !Arrays.equals(objectKey, resource.objectKey())
                    || previous != null && Arrays.compareUnsigned(previous, resource.immutableVersion()) >= 0) {
                throw new IllegalArgumentException("provider inventory changed exact ownership or repeated a version");
            }
            previous = resource.immutableVersion();
            bytes = Math.addExact(bytes, version.canonicalBytes().length + 8L);
            if (bytes > MAX_BYTES) {
                throw new IllegalArgumentException("complete provider inventory exceeds finite bound");
            }
        }
        versions = List.copyOf(ordered);
    }

    private static byte[] bounded(byte[] bytes, int maximum, String name) {
        if (bytes == null || bytes.length == 0 || bytes.length > maximum) {
            throw new IllegalArgumentException(name + " outside finite bound");
        }
        return Bytes.copy(bytes);
    }

    @Override
    public byte[] intentDigest() {
        return Bytes.copy(intentDigest);
    }

    @Override
    public byte[] container() {
        return Bytes.copy(container);
    }

    @Override
    public byte[] objectKey() {
        return Bytes.copy(objectKey);
    }

    @Override
    public byte[] closedHistory() {
        return Bytes.copy(closedHistory);
    }

    public boolean complete() {
        return versions.stream().allMatch(v -> v.deletion() != null);
    }
    /** An empty complete closed inventory is CLOSED_NO_OBJECT, never a claim of NEVER_CREATED. */
    public boolean closedNoObject() {
        return versions.isEmpty();
    }

    public void requireFor(TargetPayloadObjectRetiringRecord intent) {
        if (!intent.body().uncommittedObjectReclaim()) {
            throw new IllegalArgumentException("uncommitted original intent required");
        }
        final var facts = intent.uncommittedOperation().facts();
        if (!Arrays.equals(intentDigest, Bytes.sha256(intent.canonicalBytes())) || !profile.equals(facts.profile())) {
            throw new IllegalArgumentException("provider closure changed original pending intent/Profile");
        }
    }

    public TargetUncommittedObjectClosure withVersions(List<Version> values) {
        if (values.size() != versions.size()) {
            throw new IllegalArgumentException("closed inventory cannot grow/shrink during deletion");
        }
        for (int i = 0; i < values.size(); i++) {
            if (!Arrays.equals(
                    values.get(i).resource().canonicalBytes(),
                    versions.get(i).resource().canonicalBytes())) {
                throw new IllegalArgumentException("closed inventory changed immutable version");
            }
        }
        return new TargetUncommittedObjectClosure(intentDigest, profile, container, objectKey, closedHistory, values);
    }

    public byte[] canonicalBytes() {
        final byte[] bytes = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, SCHEMA_VERSION);
            CanonicalProtobuf.bytes(out, 2, intentDigest);
            CanonicalProtobuf.bytes(out, 3, profile.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, container);
            CanonicalProtobuf.bytes(out, 5, objectKey);
            CanonicalProtobuf.bytes(out, 6, closedHistory);
            for (var version : versions) {
                CanonicalProtobuf.bytes(out, 7, version.canonicalBytes());
            }
        });
        if (bytes.length > MAX_BYTES) {
            throw new IllegalArgumentException("encoded complete inventory exceeds finite bound");
        }
        return bytes;
    }

    public static TargetUncommittedObjectClosure decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(
                encoded, MAX_BYTES, MAX_VERSIONS + 6, true, "uncommitted closed inventory");
        if (f.size() < 6 || QueryCodecSupport.uint32(f.get(0), 1) != SCHEMA_VERSION) {
            throw new IllegalArgumentException("unknown closed inventory schema");
        }
        for (int i = 0; i < 6; i++) {
            if (f.get(i).number() != i + 1) {
                throw new IllegalArgumentException("closed inventory field mismatch");
            }
        }
        final var versions = new ArrayList<Version>();
        for (int i = 6; i < f.size(); i++) {
            versions.add(Version.decode(QueryCodecSupport.bytes(f.get(i), 7)));
        }
        final var v = new TargetUncommittedObjectClosure(
                QueryCodecSupport.fixed(f.get(1), 2, 32),
                ProfileRef.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                QueryCodecSupport.bytes(f.get(3), 4),
                QueryCodecSupport.bytes(f.get(4), 5),
                QueryCodecSupport.bytes(f.get(5), 6),
                versions);
        QueryCodecSupport.requireCanonical(encoded, v.canonicalBytes(), "uncommitted closed inventory");
        return v;
    }
}
