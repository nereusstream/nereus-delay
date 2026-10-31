package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.TargetWorkerRuntimeConfigLoader;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource;
import com.nereusstream.delay.protocol.TargetRetirementAuthorizationHistory;
import java.io.IOException;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Independently purpose-pinned public history. Mutable head files only locate signed immutable generations. */
final class KafkaClientArtifactTargetRetirementHistoryConfiguration {
    private static final int MAX_BASELINES = 4, MAX_UPDATES = 16;
    private static final String PURPOSE = "TARGET_RETIREMENT_SOURCE_AUTHORIZATION_HISTORY";

    private KafkaClientArtifactTargetRetirementHistoryConfiguration() {}

    record Anchor(Path path, byte[] digest) {
        Anchor {
            digest = Bytes.copy(digest);
        }

        @Override
        public byte[] digest() {
            return Bytes.copy(digest);
        }
    }

    record Inputs(
            byte[] root,
            Path authority,
            int keyVersion,
            Path key,
            Path head,
            List<Anchor> anchors,
            long maximumRetainedBytes) {
        Inputs {
            root = Bytes.copy(root);
            anchors = List.copyOf(anchors);
        }

        @Override
        public byte[] root() {
            return Bytes.copy(root);
        }
    }

    static Inputs prepare(Path path, byte[] actualRoot) throws IOException {
        final var f = TargetWorkerRuntimeConfigLoader.loadFields(path);
        if (!f.text("history.authorityPurpose").equals(PURPOSE)) {
            throw new IllegalArgumentException("independent retirement history purpose pin required");
        }
        final byte[] root = f.hex("history.deploymentRootDigest", 32, 32);
        if (!Arrays.equals(root, actualRoot)) {
            throw new IllegalArgumentException("history pin changed actual deployment Root");
        }
        final var authority = f.absolutePath("history.authorityIdentityFile");
        final long version = f.unsigned64("history.authorityKeyVersion");
        final var key = f.absolutePath("history.authorityPublicKeyFile");
        final var head = f.absolutePath("history.headFile");
        final int count = f.integer("history.baselines.count");
        if (version == 0 || Long.compareUnsigned(version, 0xffff_ffffL) > 0 || count < 1 || count > MAX_BASELINES) {
            throw new IllegalArgumentException("history requires bounded independent authority/baseline pins");
        }
        final List<Anchor> anchors = new ArrayList<>();
        final var paths = new HashSet<Path>();
        long pathBytes = Bytes.utf8(authority.toString()).length
                + Bytes.utf8(key.toString()).length
                + Bytes.utf8(head.toString()).length;
        for (int i = 0; i < count; i++) {
            final String prefix = "history.baselines." + i + ".";
            final var file = f.absolutePath(prefix + "snapshotFile");
            if (!paths.add(file)) {
                throw new IllegalArgumentException("duplicate history baseline path");
            }
            anchors.add(new Anchor(file, f.hex(prefix + "snapshotDigest", 32, 32)));
            pathBytes = Math.addExact(pathBytes, Bytes.utf8(file.toString()).length);
        }
        final long bytes = f.number("history.maximumInputBytes");
        f.requireComplete();
        // Retained baseline/current ancestry plus a full incoming generation set, parse/verify images and head.
        final long minimum = Math.addExact(
                (1L << 20) + 4L * pathBytes,
                Math.multiplyExact(4L * (count + 2L * MAX_UPDATES), TargetRetirementAuthorizationHistory.MAX_BYTES));
        if (bytes < minimum || bytes > 64L * 1024 * 1024) {
            throw new IllegalArgumentException("history reservation omits complete finite load/refresh overlap");
        }
        return new Inputs(root, authority, (int) version, key, head, anchors, bytes);
    }

    static Loaded load(Inputs input, byte[] actualNamespace, Runnable current) throws IOException {
        current.run();
        final var authority =
                AuthorIdentity.decode(TargetWorkerRuntimeConfigLoader.loadBoundedImage(input.authority(), 1024));
        if (authority.kind() != AuthorIdentity.Kind.CONTROL) {
            throw new IllegalArgumentException("history authority must be independently pinned CONTROL");
        }
        current.run();
        final byte[] encoded = TargetWorkerRuntimeConfigLoader.loadBoundedImage(input.key(), 1024);
        final PublicKey key;
        try {
            if (encoded.length != 44) {
                throw new IllegalArgumentException("history authority requires public Ed25519 X509 key");
            }
            key = KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(encoded));
        } catch (java.security.GeneralSecurityException invalid) {
            throw new IllegalArgumentException("invalid pinned retirement history key", invalid);
        }
        final List<List<TargetRetirementAuthorizationHistory.Verified>> chains = new ArrayList<>();
        for (var anchor : input.anchors()) {
            current.run();
            final var snapshot = TargetRetirementAuthorizationHistory.Snapshot.decode(
                    TargetWorkerRuntimeConfigLoader.loadBoundedImage(
                            anchor.path(), TargetRetirementAuthorizationHistory.MAX_BYTES));
            if (!Arrays.equals(snapshot.digest(), anchor.digest())) {
                throw new IllegalArgumentException("history baseline differs from independently pinned digest");
            }
            final var verified = TargetRetirementAuthorizationHistory.verify(
                    snapshot, authority, input.keyVersion(), key, input.root(), actualNamespace);
            if (chains.stream().anyMatch(chain -> sameChain(chain.getFirst().snapshot(), snapshot))) {
                throw new IllegalArgumentException("duplicate/overlapping history baseline scope");
            }
            chains.add(List.of(verified));
            current.run();
        }
        return new Loaded(input, authority, key, actualNamespace, List.copyOf(chains));
    }

    private static boolean sameChain(
            TargetRetirementAuthorizationHistory.Snapshot a, TargetRetirementAuthorizationHistory.Snapshot b) {
        return a.scope().equals(b.scope())
                && Arrays.equals(a.lineage(), b.lineage())
                && a.afterExclusive().sameSourceIdentity(b.afterExclusive());
    }

    static final class Loaded {
        private final Inputs input;
        private final AuthorIdentity authority;
        private final PublicKey key;
        private final byte[] namespace;
        private final List<List<TargetRetirementAuthorizationHistory.Verified>> baselines;
        private List<List<TargetRetirementAuthorizationHistory.Verified>> accepted;
        private volatile boolean stopped;

        Loaded(
                Inputs input,
                AuthorIdentity authority,
                PublicKey key,
                byte[] namespace,
                List<List<TargetRetirementAuthorizationHistory.Verified>> baselines) {
            this.input = input;
            this.authority = authority;
            this.key = key;
            this.namespace = Bytes.copy(namespace);
            this.baselines = baselines;
            accepted = baselines;
        }

        void requireCurrent() {
            if (stopped) {
                throw new IllegalStateException("original history owner stopped");
            }
        }

        void requestStop() {
            stopped = true;
        }
        /** Uses the caller's existing owned-I/O. Nothing selects latest by a directory name or timestamp. */
        synchronized Captured capture(
                TargetExternalEvidenceObjectResource resource,
                com.nereusstream.delay.ownership.SourceReplayMutation entry,
                Runnable current)
                throws IOException {
            final Runnable guard = () -> {
                current.run();
                requireCurrent();
            };
            guard.run();
            final var f = TargetWorkerRuntimeConfigLoader.loadFields(input.head());
            final int count = f.integer("history.versions.count");
            if (count < 0 || count > MAX_UPDATES) {
                throw new IllegalArgumentException("history head exceeds finite generation count");
            }
            final List<Anchor> versions = new ArrayList<>();
            final var seen = new HashSet<Path>();
            for (int i = 0; i < count; i++) {
                final String prefix = "history.versions." + i + ".";
                final var path = f.absolutePath(prefix + "snapshotFile");
                if (!seen.add(path)) {
                    throw new IllegalArgumentException("duplicate immutable history path");
                }
                versions.add(new Anchor(path, f.hex(prefix + "snapshotDigest", 32, 32)));
            }
            f.requireComplete();
            guard.run();
            final List<List<TargetRetirementAuthorizationHistory.Verified>> next = new ArrayList<>();
            for (var chain : baselines) {
                next.add(new ArrayList<>(chain));
            }
            for (var version : versions) {
                guard.run();
                final var snapshot = TargetRetirementAuthorizationHistory.Snapshot.decode(
                        TargetWorkerRuntimeConfigLoader.loadBoundedImage(
                                version.path(), TargetRetirementAuthorizationHistory.MAX_BYTES));
                if (!Arrays.equals(snapshot.digest(), version.digest())) {
                    throw new IllegalArgumentException("history locator changed immutable snapshot digest");
                }
                final var verified = TargetRetirementAuthorizationHistory.verify(
                        snapshot, authority, input.keyVersion(), key, input.root(), namespace);
                final var chain = next.stream()
                        .filter(c -> sameChain(c.getFirst().snapshot(), snapshot))
                        .findFirst()
                        .orElseThrow(() -> new TargetRetirementAuthorizationHistory.Unavailable(
                                "history generation has no independently pinned baseline"));
                snapshot.requireExtensionOf(chain.getLast().snapshot());
                chain.add(verified);
                guard.run();
            }
            for (int i = 0; i < next.size(); i++) {
                final var old = accepted.get(i).getLast().snapshot();
                final var chain = next.get(i);
                if (chain.stream()
                        .noneMatch(v -> v.snapshot().generation() == old.generation()
                                && Arrays.equals(v.snapshot().digest(), old.digest()))) {
                    throw new TargetRetirementAuthorizationHistory.Unavailable(
                            "history head rolled back or forked already accepted sealed facts");
                }
            }
            guard.run();
            final List<List<TargetRetirementAuthorizationHistory.Verified>> frozen =
                    next.stream().map(List::copyOf).toList();
            // Authentic extensions are retained even if this particular Source point is still beyond coverage.
            accepted = frozen;
            final var chain = frozen.stream()
                    .filter(c -> {
                        final var s = c.getLast().snapshot();
                        return s.scope().shard().equals(resource.shard())
                                && Arrays.equals(s.scope().tenantScope(), resource.tenantScope())
                                && Arrays.equals(s.lineage(), resource.recoveryLineage())
                                && s.afterExclusive().sameSourceIdentity(entry.position());
                    })
                    .findFirst()
                    .orElseThrow(() -> new TargetRetirementAuthorizationHistory.Unavailable(
                            "no pinned history for this exact Source scope"));
            final Optional<TargetRetirementAuthorizationHistory.Grant> grant;
            if (entry.mutation().authorIdentityLength() > 1024) {
                chain.getLast().requireCoverage(resource, entry.position());
                grant = Optional.empty();
            } else {
                grant = chain.getLast()
                        .select(
                                resource,
                                entry.position(),
                                AuthorIdentity.decode(entry.mutation().authorIdentity()),
                                entry.mutation().signingKeyVersion(),
                                TargetRetirementAuthorizationHistory.Action.of(
                                        entry.mutation().type()));
            }
            guard.run();
            return new Captured(
                    authority,
                    input.keyVersion(),
                    key,
                    chain,
                    grant.filter(value -> entry.mutation().verifySignature(value.writerKey())),
                    new TargetRetirementAuthorizationHistory.Pin(
                            authority,
                            input.keyVersion(),
                            key,
                            chain.getFirst().snapshot().digest(),
                            input.root(),
                            namespace));
        }
    }

    record Captured(
            AuthorIdentity authority,
            int keyVersion,
            PublicKey key,
            List<TargetRetirementAuthorizationHistory.Verified> chain,
            Optional<TargetRetirementAuthorizationHistory.Grant> grant,
            TargetRetirementAuthorizationHistory.Pin pin) {
        Captured {
            Objects.requireNonNull(authority);
            Objects.requireNonNull(key);
            chain = List.copyOf(chain);
            Objects.requireNonNull(grant);
            Objects.requireNonNull(pin);
        }

        long retainedBytes() {
            long bytes = authority.canonicalBytes().length
                    + key.getEncoded().length
                    + pin.key().getEncoded().length
                    + 352L;
            for (var value : chain) {
                bytes = Math.addExact(bytes, value.snapshot().canonicalBytes().length);
            }
            return bytes;
        }
        /** Public proof data requires the independent Root pin again when restored; these embedded bytes do not self-authorize. */
        TargetRetirementAuthorizationHistory.PublicProof proof() {
            return new TargetRetirementAuthorizationHistory.PublicProof(
                    authority,
                    keyVersion,
                    key.getEncoded(),
                    chain.getFirst().snapshot().digest(),
                    chain.stream()
                            .map(TargetRetirementAuthorizationHistory.Verified::snapshot)
                            .toList());
        }

        byte[] publicInputs() {
            return proof().canonicalBytes();
        }
    }
}
