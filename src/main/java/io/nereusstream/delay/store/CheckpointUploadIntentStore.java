package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.CheckpointResourceV1;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.CheckpointUploadStateV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Optional;

/**
 * Deterministic local projection of the checkpoint upload-intent CAS.
 *
 * <p>The production protocol stores this value in Oxia and compares the
 * active Owner Lease/session, lineage head and catalog generation in the same
 * transaction.  This class only supplies the exact value/state transition
 * semantics for local tests and embedded orchestration; it does not upload,
 * publish, delete or attest an Object Store object.</p>
 */
public final class CheckpointUploadIntentStore implements CheckpointUploadIntentAuthority {
    private static final int MAGIC = 0x4E435549; // N C U I
    private static final int FORMAT_VERSION = 1;
    private static final int HEADER_LENGTH = Integer.BYTES * 3;
    private static final int DIGEST_LENGTH = 32;
    private static final int MAX_INTENT_BYTES = 8 * 1024 * 1024;
    private static final byte[] DIGEST_DOMAIN =
            io.nereusstream.delay.protocol.Bytes.utf8("nereus-delay-checkpoint-upload-intent-state-v1\0");
    private static final Object JVM_LOCK = new Object();

    private CheckpointUploadIntentV1 current;
    private final Path stateFile;

    /** Creates the process-local in-memory projection used by embedded tests. */
    public CheckpointUploadIntentStore() {
        this.stateFile = null;
    }

    /**
     * Creates a crash-durable local projection backed by one state file.
     *
     * <p>The file is an implementation-local recovery seam.  It is not the
     * production Oxia upload-intent authority or an Object Store publication
     * record.  Callers must provide a path dedicated to this one intent.</p>
     */
    public CheckpointUploadIntentStore(final Path stateFile) {
        this.stateFile = normalizeStateFile(stateFile);
        try {
            ensureParentDirectory();
        } catch (IOException failure) {
            throw new IllegalStateException("cannot initialize checkpoint upload intent state", failure);
        }
    }

    /**
     * Creates a PENDING_UPLOAD intent.  A retry with byte-identical intent is
     * idempotent; a different value cannot replace the active intent.
     */
    public synchronized CheckpointUploadIntentV1 create(final CheckpointUploadIntentV1 pending) {
        Objects.requireNonNull(pending, "pending");
        requireState(pending, CheckpointUploadStateV1.PENDING_UPLOAD);
        return withExclusiveLock(() -> {
            final CheckpointUploadIntentV1 existing = readCurrent();
            if (existing == null) {
                writeCurrent(pending);
                return pending;
            }
            if (existing.equals(pending)) {
                return existing;
            }
            throw new IllegalStateException("checkpoint upload intent CAS conflict");
        });
    }

    /**
     * Atomically projects PENDING_UPLOAD to PUBLISHED after an exact expected
     * value match.  The resource identity is additionally checked by the
     * immutable intent codec.
     */
    public synchronized CheckpointUploadIntentV1 publish(final CheckpointUploadIntentV1 expectedPending,
                                                          final CheckpointResourceV1 resource) {
        Objects.requireNonNull(expectedPending, "expectedPending");
        requireState(expectedPending, CheckpointUploadStateV1.PENDING_UPLOAD);
        Objects.requireNonNull(resource, "resource");
        return withExclusiveLock(() -> {
            requireExpectedPending(expectedPending, readCurrent());
            final CheckpointUploadIntentV1 next = next(expectedPending, CheckpointUploadStateV1.PUBLISHED,
                    resource, null);
            writeCurrent(next);
            return next;
        });
    }

    /**
     * Rereads the exact PUBLISHED successor after a publication response loss.
     * A different pending value, revision or resource identity is not treated
     * as the caller's response.
     */
    public synchronized Optional<CheckpointUploadIntentV1> currentPublishedFor(
            final CheckpointUploadIntentV1 expectedPending) {
        Objects.requireNonNull(expectedPending, "expectedPending");
        requireState(expectedPending, CheckpointUploadStateV1.PENDING_UPLOAD);
        return withExclusiveLock(() -> {
            final CheckpointUploadIntentV1 existing = readCurrent();
            if (existing == null || existing.state() != CheckpointUploadStateV1.PUBLISHED) {
                return Optional.empty();
            }
            final CheckpointUploadIntentV1 expectedPublished = next(expectedPending,
                    CheckpointUploadStateV1.PUBLISHED, existing.publishedManifest(), null);
            return existing.equals(expectedPublished) ? Optional.of(existing) : Optional.empty();
        });
    }

    /**
     * Atomically competes for the PENDING_UPLOAD to REAPING transition.  The
     * evidence is retained in the value so a later reaper cannot treat a
     * deadline alone as delete authority.  The local projection enforces the
     * trusted-time lower bound; the caller must still prove owner abandonment
     * or lease loss through the external authority before invoking it.
     */
    public synchronized CheckpointUploadIntentV1 beginReaping(
            final CheckpointUploadIntentV1 expectedPending,
            final TrustedUtcIntervalEvidence evidence) {
        Objects.requireNonNull(expectedPending, "expectedPending");
        requireState(expectedPending, CheckpointUploadStateV1.PENDING_UPLOAD);
        Objects.requireNonNull(evidence, "evidence");
        evidence.requireEarliestAtLeast(expectedPending.uploadDeadlineEpochMs());
        return withExclusiveLock(() -> {
            final CheckpointUploadIntentV1 existing = readCurrent();
            if (existing != null && existing.state() == CheckpointUploadStateV1.REAPING) {
                final CheckpointUploadIntentV1 expectedReaping = next(expectedPending,
                        CheckpointUploadStateV1.REAPING, null, evidence);
                if (existing.equals(expectedReaping)) {
                    return existing;
                }
                throw new IllegalStateException("checkpoint reaping successor does not match current state");
            }
            requireExpectedPending(expectedPending, existing);
            final CheckpointUploadIntentV1 next = next(expectedPending, CheckpointUploadStateV1.REAPING,
                    null, evidence);
            writeCurrent(next);
            return next;
        });
    }

    /**
     * Enters REAPING only after the local catalog/pin necessary-condition
     * guard agrees that the pending checkpoint is not currently protected.
     * This overload still does not replace the production Oxia transaction:
     * callers must combine it with owner-abandonment, provider quiescence and
     * exact-version delete authority.
     */
    public synchronized CheckpointUploadIntentV1 beginReaping(
            final CheckpointUploadIntentV1 expectedPending,
            final TrustedUtcIntervalEvidence evidence,
            final RecoveryCatalogAuthority catalog) {
        final CheckpointReapingGuard.Decision decision = CheckpointReapingGuard.evaluate(expectedPending, evidence,
                catalog);
        if (decision != CheckpointReapingGuard.Decision.REAPING_ALLOWED) {
            throw new IllegalStateException("checkpoint reaping guard rejected: " + decision);
        }
        return beginReaping(expectedPending, evidence);
    }

    /** Returns the current local projection, if an intent has been created. */
    public synchronized Optional<CheckpointUploadIntentV1> current() {
        return withExclusiveLock(() -> Optional.ofNullable(readCurrent()));
    }

    @Override
    public synchronized Optional<CheckpointUploadIntentV1> current(final CheckpointUploadIntentV1 identity) {
        Objects.requireNonNull(identity, "identity");
        return withExclusiveLock(() -> {
            final CheckpointUploadIntentV1 existing = readCurrent();
            return existing == null || !existing.equals(identity) ? Optional.empty() : Optional.of(existing);
        });
    }

    private void requireExpectedPending(final CheckpointUploadIntentV1 expectedPending,
                                         final CheckpointUploadIntentV1 existing) {
        Objects.requireNonNull(expectedPending, "expectedPending");
        requireState(expectedPending, CheckpointUploadStateV1.PENDING_UPLOAD);
        if (existing == null || !existing.equals(expectedPending)) {
            throw new IllegalStateException("checkpoint upload intent expected value does not match current state");
        }
    }

    private CheckpointUploadIntentV1 readCurrent() throws IOException {
        if (stateFile == null) {
            return current;
        }
        ensureParentDirectory();
        if (!Files.exists(stateFile, LinkOption.NOFOLLOW_LINKS)) {
            return null;
        }
        rejectSymbolicLink(stateFile, "checkpoint upload intent state");
        if (!Files.isRegularFile(stateFile, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("checkpoint upload intent state is not a regular file: " + stateFile);
        }
        final long fileLength = Files.size(stateFile);
        if (fileLength > HEADER_LENGTH + MAX_INTENT_BYTES + DIGEST_LENGTH) {
            throw new IOException("checkpoint upload intent state exceeds bounded size: " + stateFile);
        }
        return decode(Files.readAllBytes(stateFile));
    }

    private void writeCurrent(final CheckpointUploadIntentV1 next) throws IOException {
        Objects.requireNonNull(next, "next");
        if (stateFile == null) {
            current = next;
            return;
        }
        ensureParentDirectory();
        rejectSymbolicLink(stateFile, "checkpoint upload intent state");
        final byte[] payload = next.canonicalBytes();
        if (payload.length == 0 || payload.length > MAX_INTENT_BYTES) {
            throw new IOException("checkpoint upload intent exceeds bounded size");
        }
        final byte[] digest = io.nereusstream.delay.protocol.Bytes.sha256(DIGEST_DOMAIN, payload);
        final ByteBuffer output = ByteBuffer.allocate(HEADER_LENGTH + payload.length + digest.length)
                .order(ByteOrder.BIG_ENDIAN);
        output.putInt(MAGIC).putInt(FORMAT_VERSION).putInt(payload.length).put(payload).put(digest);
        final Path temporary = Files.createTempFile(stateFile.getParent(), ".checkpoint-upload-intent-", ".tmp");
        Throwable primaryFailure = null;
        try {
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                final ByteBuffer buffer = ByteBuffer.wrap(output.array());
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
            try {
                Files.move(temporary, stateFile, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException unsupported) {
                throw new IOException("checkpoint upload intent state requires atomic rename", unsupported);
            }
            try (FileChannel directory = FileChannel.open(stateFile.getParent(), StandardOpenOption.READ)) {
                directory.force(true);
            }
        } catch (IOException | RuntimeException | Error failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            try {
                Files.deleteIfExists(temporary);
            } catch (IOException | RuntimeException | Error cleanupFailure) {
                if (primaryFailure != null && cleanupFailure != primaryFailure) {
                    primaryFailure.addSuppressed(cleanupFailure);
                } else if (primaryFailure == null) {
                    throwCleanupFailure(cleanupFailure);
                }
            }
        }
    }

    private static void throwCleanupFailure(final Throwable failure) throws IOException {
        if (failure instanceof IOException ioFailure) {
            throw ioFailure;
        }
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error errorFailure) {
            throw errorFailure;
        }
        throw new IllegalStateException("unexpected cleanup failure", failure);
    }

    private <T> T withExclusiveLock(final IoAction<T> action) {
        if (stateFile == null) {
            try {
                return action.run();
            } catch (IOException failure) {
                throw new IllegalStateException("checkpoint upload intent state I/O failed", failure);
            }
        }
        synchronized (JVM_LOCK) {
            try {
                ensureParentDirectory();
                final Path lockFile = stateFile.resolveSibling(stateFile.getFileName() + ".lock");
                rejectSymbolicLink(lockFile, "checkpoint upload intent lock");
                try (FileChannel channel = FileChannel.open(lockFile, StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE); FileLock fileLock = channel.lock()) {
                    if (!fileLock.isValid()) {
                        throw new IOException("checkpoint upload intent lock is not valid");
                    }
                    return action.run();
                }
            } catch (IOException failure) {
                throw new IllegalStateException("checkpoint upload intent state I/O failed", failure);
            }
        }
    }

    private void ensureParentDirectory() throws IOException {
        if (stateFile == null) {
            return;
        }
        final Path parent = stateFile.getParent();
        if (parent == null) {
            throw new IOException("checkpoint upload intent state must have a parent directory");
        }
        LocalStatePathGuard.ensureRealDirectoryPath(parent, "checkpoint upload intent state parent");
    }

    private static CheckpointUploadIntentV1 decode(final byte[] encoded) {
        if (encoded.length < HEADER_LENGTH + DIGEST_LENGTH) {
            throw new IllegalStateException("checkpoint upload intent state is truncated");
        }
        final ByteBuffer input = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        if (input.getInt() != MAGIC || input.getInt() != FORMAT_VERSION) {
            throw new IllegalStateException("checkpoint upload intent state has an unknown header");
        }
        final int payloadLength = input.getInt();
        if (payloadLength <= 0 || payloadLength > MAX_INTENT_BYTES
                || encoded.length != HEADER_LENGTH + payloadLength + DIGEST_LENGTH) {
            throw new IllegalStateException("checkpoint upload intent state has an invalid length");
        }
        final byte[] payload = new byte[payloadLength];
        input.get(payload);
        final byte[] digest = new byte[DIGEST_LENGTH];
        input.get(digest);
        if (!io.nereusstream.delay.protocol.Bytes.constantTimeEquals(digest,
                io.nereusstream.delay.protocol.Bytes.sha256(DIGEST_DOMAIN, payload))) {
            throw new IllegalStateException("checkpoint upload intent state checksum mismatch");
        }
        try {
            return CheckpointUploadIntentV1.decode(payload);
        } catch (RuntimeException malformed) {
            throw new IllegalStateException("checkpoint upload intent state is malformed", malformed);
        }
    }

    private static Path normalizeStateFile(final Path value) {
        final Path normalized = Objects.requireNonNull(value, "stateFile").toAbsolutePath().normalize();
        if (normalized.getFileName() == null) {
            throw new IllegalArgumentException("stateFile must name a file");
        }
        return normalized;
    }

    private static void rejectSymbolicLink(final Path path, final String name) throws IOException {
        if (Files.isSymbolicLink(path)) {
            throw new IOException(name + " must not be a symbolic link: " + path);
        }
    }

    @FunctionalInterface
    private interface IoAction<T> {
        T run() throws IOException;
    }

    private static void requireState(final CheckpointUploadIntentV1 intent,
                                     final CheckpointUploadStateV1 state) {
        if (intent.state() != state) {
            throw new IllegalArgumentException("checkpoint upload intent must be " + state);
        }
    }

    private static CheckpointUploadIntentV1 next(final CheckpointUploadIntentV1 expected,
                                                  final CheckpointUploadStateV1 state,
                                                  final CheckpointResourceV1 resource,
                                                  final TrustedUtcIntervalEvidence evidence) {
        return new CheckpointUploadIntentV1(
                expected.shard(), expected.recoveryLineageId(), expected.checkpointId(), expected.owner(),
                expected.sourceStoreIncarnation(), expected.uploadToken(), expected.baseCatalogGeneration(),
                expected.parentCheckpointId(), expected.parentManifestSha256(), expected.objectStoreProfile(),
                expected.checkpointCreatedAt(), expected.uploadDeadlineEpochMs(), state,
                incrementRevision(expected.stateRevision()), resource, evidence);
    }

    private static long incrementRevision(final long revision) {
        if (revision == -1L) {
            throw new IllegalStateException("checkpoint upload intent state revision exhausted");
        }
        return revision + 1;
    }
}
