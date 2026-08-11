package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ControlOperationQueryResponseV1;
import io.nereusstream.delay.protocol.ControlOperationReceiptV1;
import io.nereusstream.delay.protocol.ControlOperationStateTransitionV1;
import io.nereusstream.delay.protocol.CurrentControlOperationV1;
import io.nereusstream.delay.store.LocalStatePathGuard;

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

/**
 * Crash-durable local Control Operation authority for embedded/conformance
 * deployments.
 *
 * <p>Each operation is stored as one checksummed canonical receipt/current
 * pair. A process-wide JVM lock plus an on-disk file lock makes register and
 * revision CAS linearize across multiple authority instances. The temporary
 * file, atomic rename and directory fsync ensure that a response lost after a
 * successful write can be retried with the exact receipt and next projection.
 * This is still a local authority: production must use the Oxia-backed
 * implementation for cross-worker routing, authenticated scope and session
 * fencing.</p>
 */
public final class PersistentControlOperationAuthority implements ControlOperationAuthority {
    private static final int MAGIC = 0x4E444F50; // N D O P
    private static final int FORMAT_VERSION = 1;
    private static final int DIGEST_LENGTH = 32;
    private static final int HEADER_LENGTH = Integer.BYTES * 4;
    private static final int MAX_COMPONENT_BYTES = 8 * 1024 * 1024;
    private static final byte[] DIGEST_DOMAIN =
            Bytes.utf8("nereus-delay-control-operation-state-v1\\0");
    private static final Object JVM_LOCK = new Object();

    private final Path root;

    /** Creates an authority rooted at a directory reserved for control state. */
    public PersistentControlOperationAuthority(final Path root) {
        this.root = normalizeRoot(root);
        try {
            ensureRoot(true);
        } catch (IOException failure) {
            throw new IllegalStateException("cannot initialize control operation state directory", failure);
        }
    }

    @Override
    public ControlOperationQueryResponseV1 register(final ControlOperationReceiptV1 receipt,
                                                     final CurrentControlOperationV1 initial) {
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(initial, "initial");
        if (!matchesIdentity(receipt, initial) || initial.operationRevision() != receipt.operationRevision()) {
            return ControlOperationQueryResponseV1.integrityError();
        }
        return withExclusiveLock(() -> {
            final Path statePath = statePath(receipt);
            final State existing = readIfPresent(statePath, receipt.operationId());
            if (existing == null) {
                write(statePath, new State(receipt, initial));
                return ControlOperationQueryResponseV1.current(initial);
            }
            if (!existing.receipt().equals(receipt)) {
                return ControlOperationQueryResponseV1.notFoundOrNotAuthorized();
            }
            if (existing.current().equals(initial)) {
                return ControlOperationQueryResponseV1.current(existing.current());
            }
            return ControlOperationQueryResponseV1.integrityError();
        });
    }

    @Override
    public ControlOperationQueryResponseV1 advance(final ControlOperationReceiptV1 receipt,
                                                    final long expectedRevision,
                                                    final CurrentControlOperationV1 next) {
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(next, "next");
        if (expectedRevision <= 0) {
            return ControlOperationQueryResponseV1.invalidReceipt();
        }
        if (expectedRevision == Long.MAX_VALUE) {
            return ControlOperationQueryResponseV1.integrityError();
        }
        if (!matchesIdentity(receipt, next)
                || !isExactSuccessor(expectedRevision, next.operationRevision())) {
            return ControlOperationQueryResponseV1.integrityError();
        }
        return withExclusiveLock(() -> {
            final State existing = readIfPresent(statePath(receipt), receipt.operationId());
            if (existing == null || !existing.receipt().equals(receipt)) {
                return ControlOperationQueryResponseV1.notFoundOrNotAuthorized();
            }
            try {
                ControlOperationStateTransitionV1.validate(existing.current().state(), next.state());
                ControlOperationStateTransitionV1.validateTargets(existing.current().targetStates(), next.targetStates());
            } catch (IllegalArgumentException invalidTransition) {
                return ControlOperationQueryResponseV1.integrityError();
            }
            if (existing.current().equals(next)) {
                // The first CAS may have committed before its response was
                // lost. Exact CURRENT reread is the only idempotent success.
                return ControlOperationQueryResponseV1.current(existing.current());
            }
            if (existing.current().operationRevision() != expectedRevision) {
                return ControlOperationQueryResponseV1.integrityError();
            }
            write(statePath(receipt), new State(receipt, next));
            return ControlOperationQueryResponseV1.current(next);
        });
    }

    @Override
    public ControlOperationQueryResponseV1 query(final ControlOperationReceiptV1 receipt,
                                                  final long nowEpochMs) {
        if (receipt == null || nowEpochMs < 0) {
            return ControlOperationQueryResponseV1.invalidReceipt();
        }
        return withExclusiveLock(() -> {
            final State existing = readIfPresent(statePath(receipt), receipt.operationId());
            if (existing == null || !existing.receipt().equals(receipt)
                    || nowEpochMs > receipt.queryUntilEpochMs()) {
                return ControlOperationQueryResponseV1.notFoundOrNotAuthorized();
            }
            return ControlOperationQueryResponseV1.current(existing.current());
        });
    }

    private <T> T withExclusiveLock(final IoAction<T> action) {
        synchronized (JVM_LOCK) {
            try {
                ensureRoot(false);
                final Path lockPath = root.resolve("authority.lock");
                rejectSymbolicLink(lockPath, "control authority lock");
                try (FileChannel channel = FileChannel.open(lockPath, StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE); FileLock fileLock = channel.lock()) {
                    if (!fileLock.isValid()) {
                        throw new IOException("control authority lock is not valid");
                    }
                    return action.run();
                }
            } catch (IOException failure) {
                throw new IllegalStateException("control operation authority I/O failed", failure);
            }
        }
    }

    private State readIfPresent(final Path statePath, final byte[] operationId) throws IOException {
        final byte[] encoded = LocalStatePathGuard.readRegularFileNoFollow(statePath,
                HEADER_LENGTH + (long) MAX_COMPONENT_BYTES * 2 + DIGEST_LENGTH,
                "control operation state");
        if (encoded == null) {
            return null;
        }
        return decode(encoded, operationId, statePath);
    }

    private void write(final Path statePath, final State state) throws IOException {
        rejectSymbolicLink(statePath, "control operation state");
        final byte[] encoded = encode(state);
        final Path temporary = Files.createTempFile(root, ".control-operation-", ".tmp");
        Throwable primaryFailure = null;
        try {
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                final ByteBuffer buffer = ByteBuffer.wrap(encoded);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
            try {
                Files.move(temporary, statePath, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException unsupported) {
                throw new IOException("control operation state requires atomic rename", unsupported);
            }
            forceDirectory();
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

    private void forceDirectory() throws IOException {
        try (FileChannel directory = FileChannel.open(root, StandardOpenOption.READ)) {
            directory.force(true);
        }
    }

    private byte[] encode(final State state) {
        final byte[] receipt = state.receipt().frame();
        final byte[] current = state.current().canonicalBytes();
        checkComponentLength(receipt, "receipt");
        checkComponentLength(current, "current");
        final byte[] digest = Bytes.sha256(DIGEST_DOMAIN, receipt, current);
        final ByteBuffer output = ByteBuffer.allocate(HEADER_LENGTH + receipt.length + current.length + digest.length)
                .order(ByteOrder.BIG_ENDIAN);
        output.putInt(MAGIC).putInt(FORMAT_VERSION).putInt(receipt.length).putInt(current.length);
        output.put(receipt).put(current).put(digest);
        return output.array();
    }

    private static State decode(final byte[] encoded, final byte[] operationId, final Path statePath) {
        if (encoded.length < HEADER_LENGTH + DIGEST_LENGTH) {
            throw corrupt(statePath, "control operation state is truncated");
        }
        final ByteBuffer input = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        if (input.getInt() != MAGIC || input.getInt() != FORMAT_VERSION) {
            throw corrupt(statePath, "control operation state has an unknown header");
        }
        final int receiptLength = input.getInt();
        final int currentLength = input.getInt();
        if (receiptLength <= 0 || currentLength <= 0 || receiptLength > MAX_COMPONENT_BYTES
                || currentLength > MAX_COMPONENT_BYTES) {
            throw corrupt(statePath, "control operation state component length is invalid");
        }
        final long expectedLength = HEADER_LENGTH + (long) receiptLength + currentLength + DIGEST_LENGTH;
        if (expectedLength != encoded.length) {
            throw corrupt(statePath, "control operation state has trailing or missing bytes");
        }
        final byte[] receiptBytes = new byte[receiptLength];
        final byte[] currentBytes = new byte[currentLength];
        input.get(receiptBytes).get(currentBytes);
        final byte[] digest = new byte[DIGEST_LENGTH];
        input.get(digest);
        if (!Bytes.constantTimeEquals(digest, Bytes.sha256(DIGEST_DOMAIN, receiptBytes, currentBytes))) {
            throw corrupt(statePath, "control operation state checksum mismatch");
        }
        final ControlOperationReceiptV1 receipt;
        final CurrentControlOperationV1 current;
        try {
            receipt = ControlOperationReceiptV1.decodeFrame(receiptBytes);
            current = CurrentControlOperationV1.decode(currentBytes);
        } catch (RuntimeException malformed) {
            throw corrupt(statePath, "control operation state contains malformed canonical values", malformed);
        }
        if (!Bytes.constantTimeEquals(operationId, receipt.operationId())
                || !stateFileName(receipt.operationId()).equals(statePath.getFileName().toString())
                || !matchesIdentity(receipt, current)) {
            throw corrupt(statePath, "control operation state identity mismatch");
        }
        return new State(receipt, current);
    }

    private Path statePath(final ControlOperationReceiptV1 receipt) {
        return root.resolve(stateFileName(receipt.operationId()));
    }

    private static String stateFileName(final byte[] operationId) {
        Bytes.requireLength(operationId, 32, "operationId");
        return Bytes.hex(operationId) + ".state";
    }

    private void ensureRoot(final boolean create) throws IOException {
        if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
            if (!create) {
                throw new IOException("control operation state root is missing: " + root);
            }
            LocalStatePathGuard.ensureRealDirectoryPath(root, "control operation state root");
            return;
        }
        LocalStatePathGuard.ensureRealDirectoryPath(root, "control operation state root");
    }

    private static void rejectSymbolicLink(final Path path, final String name) throws IOException {
        if (Files.isSymbolicLink(path)) {
            throw new IOException(name + " must not be a symbolic link: " + path);
        }
    }

    private static Path normalizeRoot(final Path value) {
        return Objects.requireNonNull(value, "root").toAbsolutePath().normalize();
    }

    private static boolean matchesIdentity(final ControlOperationReceiptV1 receipt,
                                           final CurrentControlOperationV1 current) {
        return Bytes.constantTimeEquals(receipt.operationId(), current.operationId())
                && Bytes.constantTimeEquals(receipt.requestHash(), current.requestHash())
                && Bytes.constantTimeEquals(receipt.authenticatedScopeHash(), current.authenticatedScopeHash());
    }

    private static boolean isExactSuccessor(final long expectedRevision, final long nextRevision) {
        return expectedRevision > 0 && expectedRevision < Long.MAX_VALUE
                && nextRevision == expectedRevision + 1;
    }

    private static void checkComponentLength(final byte[] value, final String name) {
        if (value.length == 0 || value.length > MAX_COMPONENT_BYTES) {
            throw new IllegalStateException(name + " exceeds control operation state limit");
        }
    }

    private static IllegalStateException corrupt(final Path path, final String message) {
        return new IllegalStateException(message + ": " + path);
    }

    private static IllegalStateException corrupt(final Path path, final String message, final Throwable cause) {
        return new IllegalStateException(message + ": " + path, cause);
    }

    @FunctionalInterface
    private interface IoAction<T> {
        T run() throws IOException;
    }

    private record State(ControlOperationReceiptV1 receipt, CurrentControlOperationV1 current) {
        private State {
            Objects.requireNonNull(receipt, "receipt");
            Objects.requireNonNull(current, "current");
        }
    }
}
