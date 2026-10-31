package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CheckpointResource;
import com.nereusstream.delay.protocol.TargetMigrationObjectReference;
import com.nereusstream.delay.protocol.TargetMigrationPlan;
import com.nereusstream.delay.runtime.TargetMigrationImporter;
import com.nereusstream.delay.runtime.TargetMigrationStore;
import com.nereusstream.delay.store.CheckpointFileInventory;
import com.nereusstream.delay.store.CheckpointManifest;
import com.nereusstream.delay.store.LegacyMigrationBundle;
import com.nereusstream.delay.store.RenewableS3CompatibleCheckpointObjectStoreAdapter;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

/** Fixed companion chunks and an explicitly non-restoreable prepared-C image on the original checkpoint SDK. */
public final class TargetMigrationCheckpointArtifacts
        implements LegacyMigrationBundle.ImmutableObjects, TargetMigrationImporter.CandidateSealer {
    /** Real original resource/custody rights. Reservations survive response loss and are never refunded here. */
    public interface Protection {
        void requireLocalCurrent();
        /** Reacquisition retains the same exact object obligation; it must not erase or charge UNKNOWN twice. */
        void reserveBundle(long maximumEncodedBytes, int maximumObjects);

        void reservePrepared(TargetMigrationPlan plan, long imageAndManifestBytes, int maximumObjects);

        void requireWrite(byte[] lineage, byte[] checkpointId, String fixedName, long bytes, byte[] sha256);

        void requireRead(CheckpointResource exact, long maximumBytes);

        void requirePreparedImage(TargetMigrationPlan plan, TargetCheckpointRootVerifier.LedgerProof actual);
    }

    private final TargetMigrationProcessDriver.Opening opening;
    private final RenewableS3CompatibleCheckpointObjectStoreAdapter adapter;
    private final CheckpointManifest legacy;
    private final LegacyMigrationBundle.Limits limits;
    private final Protection protection;
    private final Runnable current;
    private final Runnable enter, exit;
    private boolean reserved;

    TargetMigrationCheckpointArtifacts(
            TargetMigrationProcessDriver.Opening opening,
            RenewableS3CompatibleCheckpointObjectStoreAdapter adapter,
            CheckpointManifest legacy,
            LegacyMigrationBundle.Limits limits,
            Protection protection,
            Runnable current,
            Runnable enter,
            Runnable exit) {
        this.opening = Objects.requireNonNull(opening);
        this.adapter = Objects.requireNonNull(adapter);
        this.legacy = Objects.requireNonNull(legacy);
        this.limits = Objects.requireNonNull(limits);
        this.protection = Objects.requireNonNull(protection);
        this.current = Objects.requireNonNull(current);
        this.enter = Objects.requireNonNull(enter);
        this.exit = Objects.requireNonNull(exit);
        if (legacy.storeFormatVersion() != 1
                || !legacy.shardId().equals(opening.configuration().scope().shard())) {
            throw new IllegalArgumentException("migration companions changed their original legacy Manifest/Source");
        }
    }

    void requireBinding(CheckpointManifest exact, LegacyMigrationBundle.Limits bounds, Protection held) {
        if (!Arrays.equals(legacy.canonicalJsonBytes(), exact.canonicalJsonBytes())
                || !limits.equals(bounds)
                || protection != held) {
            throw new IllegalStateException(
                    "migration artifacts changed their original Manifest, finite bounds or protection");
        }
    }

    @Override
    public void requireReserved(long maximumEncodedBytes, int maximumObjects) {
        enter.run();
        try {
            current.run();
            if (maximumEncodedBytes < 1
                    || maximumEncodedBytes == Long.MAX_VALUE
                    || maximumObjects < 1
                    || maximumObjects > limits.maxChunks()) {
                throw new IllegalArgumentException("migration companion lacks finite exact object reservations");
            }
            protection.reserveBundle(maximumEncodedBytes, maximumObjects);
            protection.requireLocalCurrent();
            reserved = true;
        } finally {
            exit.run();
        }
    }

    @Override
    public CheckpointResource put(
            LegacyMigrationBundle.Section section, int chunk, boolean manifest, byte[] exactBytes) {
        enter.run();
        try {
            current.run();
            if (!reserved
                    || chunk < 0
                    || chunk >= limits.maxChunks()
                    || exactBytes == null
                    || exactBytes.length == 0
                    || exactBytes.length > limits.maxChunkBytes()) {
                throw new IllegalArgumentException("migration companion is outside its fixed chunk reservation");
            }
            final String name = Bytes.hex(opening.configuration().migrationId()) + "/bundle/"
                    + Objects.requireNonNull(section).name().toLowerCase(java.util.Locale.ROOT) + "/"
                    + (manifest ? "manifest-" : "chunk-") + chunk + ".pb";
            protection.requireWrite(
                    legacy.recoveryLineageId(),
                    legacy.checkpointId(),
                    name,
                    exactBytes.length,
                    Bytes.sha256(exactBytes));
            final var object = adapter.putMigrationObject(
                    legacy.recoveryLineageId(), legacy.checkpointId(), name, exactBytes, limits.maxChunkBytes());
            protection.requireLocalCurrent();
            current.run();
            return object;
        } finally {
            exit.run();
        }
    }

    @Override
    public byte[] readExact(CheckpointResource object, int maximumBytes) {
        enter.run();
        try {
            current.run();
            if (maximumBytes < 1
                    || maximumBytes > limits.maxChunkBytes()
                    || !Arrays.equals(object.recoveryLineageId(), legacy.recoveryLineageId())
                    || !Arrays.equals(object.checkpointId(), legacy.checkpointId())) {
                throw new IllegalArgumentException("migration companion read changed original bounded identity");
            }
            protection.requireRead(object, maximumBytes);
            final var bytes = adapter.readMigrationObject(object, maximumBytes);
            current.run();
            return bytes;
        } finally {
            exit.run();
        }
    }

    @Override
    public TargetMigrationObjectReference seal(TargetMigrationPlan plan, ShardStore store, ShardStore.ReadView cut) {
        enter.run();
        try {
            return sealActual(plan, store, cut);
        } finally {
            exit.run();
        }
    }

    private TargetMigrationObjectReference sealActual(
            TargetMigrationPlan plan, ShardStore store, ShardStore.ReadView cut) {
        current.run();
        if (!plan.scope().equals(opening.configuration().scope())
                || !Arrays.equals(plan.migrationId(), opening.configuration().migrationId())
                || store.sharedResources() != opening.graph().resources()) {
            throw new IllegalArgumentException("prepared-C image changed its actual migration/Store/resource graph");
        }
        TargetMigrationStore.requireIdentity(store.metadata(), plan);
        final var runtime = opening.graph().config().checkpoint();
        final byte[] identity = Bytes.sha256(
                Bytes.utf8("nereus-delay-migration-prepared-c-image\0"), plan.migrationId(), plan.digest());
        final byte[] checkpointId = Arrays.copyOf(identity, 16);
        final Path path = opening.configuration().candidateDirectory().resolve("prepared-c-" + Bytes.hex(identity));
        final Path liveRoot =
                opening.graph().config().store().rootPath().toAbsolutePath().normalize();
        if (path.startsWith(liveRoot) || liveRoot.startsWith(path)) {
            throw new IllegalArgumentException("prepared-C image overlaps the actual Worker Store root");
        }
        protection.reservePrepared(
                plan,
                Math.addExact(
                        runtime.physical().maxTotalFileBytes(),
                        runtime.physical().maxManifestBytes()),
                Math.addExact(runtime.physical().maxFiles(), 1));
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            store.createMigrationPreparedCheckpoint(
                    path,
                    cut,
                    plan,
                    runtime.physical(),
                    runtime.quota(),
                    runtime.ledger(),
                    protection::requireLocalCurrent);
        }
        final var proof = TargetCheckpointRootVerifier.auditMigrationPreparedImage(
                path, plan, runtime.physical(), runtime.quota(), runtime.ledger());
        protection.requirePreparedImage(plan, proof);
        current.run();
        store.withReadView(cut, () -> {
            protection.requireLocalCurrent();
            return Boolean.TRUE;
        });
        final var inventory = CheckpointFileInventory.collect(path, runtime.physical());
        final String prefix = Bytes.hex(plan.migrationId()) + "/prepared-c/";
        final var entries = new ArrayList<byte[]>();
        final byte[] rootDigest =
                TargetMigrationStore.requirePrepared(store, plan).digest();
        long encodedManifestBytes = CanonicalProtobuf.message(out -> {
                    CanonicalProtobuf.uint32(out, 1, 1);
                    CanonicalProtobuf.bytes(out, 2, plan.canonicalBytes());
                    CanonicalProtobuf.bytes(out, 3, rootDigest);
                    CanonicalProtobuf.bytes(out, 4, plan.baseline().canonicalBytes());
                    CanonicalProtobuf.uint32(out, 6, 1);
                    CanonicalProtobuf.bytes(out, 7, new byte[32]);
                })
                .length;
        if (encodedManifestBytes > runtime.physical().maxManifestBytes()) {
            throw new IllegalStateException("prepared-C identity exceeds its original fixed manifest bound");
        }
        for (var file : inventory) {
            current.run();
            final String name = prefix + "files/" + file.name();
            protection.requireWrite(plan.newLineage(), checkpointId, name, file.length(), file.checksum());
            final var uploaded = adapter.putMigrationFile(
                    plan.newLineage(),
                    checkpointId,
                    name,
                    path.resolve(file.name()),
                    file.length(),
                    file.checksum(),
                    runtime.physical().maxIndividualFileBytes());
            final byte[] entry = CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.bytes(out, 1, Bytes.utf8(file.name()));
                CanonicalProtobuf.bytes(out, 2, uploaded.canonicalBytes());
            });
            encodedManifestBytes = Math.addExact(
                    encodedManifestBytes,
                    CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, 5, entry)).length);
            if (encodedManifestBytes > runtime.physical().maxManifestBytes()) {
                throw new IllegalStateException("prepared-C fixed file manifest exceeds its original physical bound");
            }
            entries.add(entry);
        }
        // Deliberately no CheckpointManifest/READY/Catalog representation for this pre-A object.
        final byte[] body = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, plan.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, rootDigest);
            CanonicalProtobuf.bytes(out, 4, plan.baseline().canonicalBytes());
            for (var entry : entries) {
                CanonicalProtobuf.bytes(out, 5, entry);
            }
            CanonicalProtobuf.uint32(out, 6, 1); // MIGRATION_PREPARED; normal restore has no decoder for this form.
        });
        final byte[] manifest = CanonicalProtobuf.message(out -> {
            out.writeBytes(body);
            CanonicalProtobuf.bytes(
                    out, 7, Bytes.sha256(Bytes.utf8("nereus-delay-migration-prepared-c-manifest-v1\0"), body));
        });
        if (manifest.length > runtime.physical().maxManifestBytes()) {
            throw new IllegalStateException("prepared-C fixed file manifest exceeds its original physical bound");
        }
        protection.requireWrite(
                plan.newLineage(), checkpointId, prefix + "manifest.pb", manifest.length, Bytes.sha256(manifest));
        final var resource = adapter.putMigrationObject(
                plan.newLineage(),
                checkpointId,
                prefix + "manifest.pb",
                manifest,
                runtime.physical().maxManifestBytes());
        protection.requireRead(resource, manifest.length);
        if (!Arrays.equals(manifest, adapter.readMigrationObject(resource, manifest.length))) {
            throw new IllegalStateException("prepared-C exact immutable manifest read differs");
        }
        protection.requirePreparedImage(plan, proof);
        current.run();
        store.withReadView(cut, () -> {
            protection.requireLocalCurrent();
            return Boolean.TRUE;
        });
        return new TargetMigrationObjectReference(resource);
    }
}
