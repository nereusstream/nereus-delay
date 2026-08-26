package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.OpaquePayloadUploadHandle;
import com.nereusstream.delay.protocol.PayloadAttestationResponse;
import com.nereusstream.delay.protocol.PayloadProofTrustSetRef;
import com.nereusstream.delay.protocol.PayloadProofTrustSetSemantic;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.PayloadUploadHandleResponse;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.UploadHandleKind;
import com.nereusstream.delay.runtime.PayloadReservation;
import com.nereusstream.delay.store.LocalStatePathGuard;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.Objects;

/**
 * Crash-durable local/test Object Store adapter for large payload bytes.
 *
 * <p>The handle, reservation and proof rules are shared with
 * {@link InMemoryPayloadObjectStore}; only immutable payload bytes are stored
 * below {@code rootPath}. Objects are written to a private temporary file,
 * fsynced and atomically published. Existing objects are accepted only when
 * their complete bytes are identical. Reservation metadata and handles remain
 * process-local, so a new process must re-register the exact durable
 * reservation before it can issue a handle. This class is local evidence, not
 * a production Object Store credential, consistency or Oxia authority.</p>
 */
public final class FilesystemPayloadObjectStore {
    private final InMemoryPayloadObjectStore delegate;

    public FilesystemPayloadObjectStore(
            final Path rootPath,
            final ProfileSemanticEnvelope profile,
            final byte[] tenantRoutingScope,
            final PayloadProofTrustSetSemantic trustSet,
            final int proofKeyVersion,
            final PrivateKey proofSigningKey) {
        this(rootPath, profile, tenantRoutingScope, trustSet, proofKeyVersion, Long.MAX_VALUE, proofSigningKey);
    }

    public FilesystemPayloadObjectStore(
            final Path rootPath,
            final ProfileSemanticEnvelope profile,
            final byte[] tenantRoutingScope,
            final PayloadProofTrustSetSemantic trustSet,
            final int proofKeyVersion,
            final long maxUploadHandleLifetimeMs,
            final PrivateKey proofSigningKey) {
        final ObjectFileBackend backend = new ObjectFileBackend(rootPath, profile);
        this.delegate = new InMemoryPayloadObjectStore(
                profile,
                tenantRoutingScope,
                trustSet,
                proofKeyVersion,
                maxUploadHandleLifetimeMs,
                proofSigningKey,
                backend);
    }

    /** Legacy/local registration without a durable Registry Prepare binding. */
    public void register(final PayloadReservation reservation) {
        delegate.register(reservation);
    }

    /**
     * Registers a Registry reservation only when this filesystem adapter is
     * the exact Object Store Profile and proof authority pinned by Prepare.
     */
    public void register(
            final PayloadReservation reservation,
            final PayloadProofTrustSetRef pinnedTrustSet,
            final ProfileRef pinnedObjectStoreProfile) {
        delegate.register(reservation, pinnedTrustSet, pinnedObjectStoreProfile);
    }

    public PayloadReservationReceipt reservationReceipt(final PayloadReservation reservation) {
        return delegate.reservationReceipt(reservation);
    }

    public PayloadUploadHandleResponse issueUploadHandle(
            final byte[] reservationId, final UploadHandleKind kind, final long nowEpochMs) {
        return delegate.issueUploadHandle(reservationId, kind, nowEpochMs);
    }

    public PayloadUploadHandleResponse issueUploadHandle(
            final PayloadReservationReceipt receipt, final UploadHandleKind kind, final long nowEpochMs) {
        return delegate.issueUploadHandle(receipt, kind, nowEpochMs);
    }

    public void upload(final OpaquePayloadUploadHandle handle, final byte[] payload, final long nowEpochMs) {
        delegate.upload(handle, payload, nowEpochMs);
    }

    public void upload(
            final PayloadReservationReceipt receipt,
            final OpaquePayloadUploadHandle handle,
            final byte[] payload,
            final long nowEpochMs) {
        delegate.upload(receipt, handle, payload, nowEpochMs);
    }

    public PayloadAttestationResponse attest(final OpaquePayloadUploadHandle handle, final long nowEpochMs) {
        return delegate.attest(handle, nowEpochMs);
    }

    public PayloadAttestationResponse attest(
            final PayloadReservationReceipt receipt, final OpaquePayloadUploadHandle handle, final long nowEpochMs) {
        return delegate.attest(receipt, handle, nowEpochMs);
    }

    private static final class ObjectFileBackend implements PayloadObjectBackend {
        private final Path objectsRoot;
        private final long maxObjectBytes;

        private ObjectFileBackend(final Path rootPath, final ProfileSemanticEnvelope profile) {
            Objects.requireNonNull(profile, "profile");
            if (!(profile.body() instanceof com.nereusstream.delay.protocol.ObjectStoreProfileSemantic objectStore)) {
                throw new IllegalArgumentException("payload adapter requires an Object Store profile");
            }
            this.maxObjectBytes = objectStore.maxObjectBytes();
            final Path normalizedRoot = Objects.requireNonNull(rootPath, "rootPath")
                    .toAbsolutePath()
                    .normalize();
            if (Files.isSymbolicLink(normalizedRoot)) {
                throw new IllegalArgumentException("payload Object Store root must not be a symbolic link");
            }
            this.objectsRoot = normalizedRoot.resolve("objects").normalize();
            try {
                LocalStatePathGuard.ensureRealDirectoryPath(objectsRoot, "payload Object Store root");
            } catch (IOException failure) {
                throw new IllegalStateException("cannot initialize payload Object Store root", failure);
            }
        }

        @Override
        public synchronized byte[] read(final String objectIdentity) {
            final Path object = objectPath(objectIdentity);
            try {
                return LocalStatePathGuard.readRegularFileNoFollow(
                        object, maxObjectBytes, "payload Object Store object");
            } catch (IOException failure) {
                throw new IllegalStateException("cannot read payload Object Store object: " + object, failure);
            }
        }

        @Override
        public synchronized void putIfAbsent(final String objectIdentity, final byte[] payload, final long maxBytes) {
            Objects.requireNonNull(payload, "payload");
            if (maxBytes != maxObjectBytes) {
                throw new IllegalArgumentException("payload backend size bound changed");
            }
            if (payload.length > maxObjectBytes) {
                throw new IllegalArgumentException("payload exceeds Object Store profile maximum");
            }
            final Path object = objectPath(objectIdentity);
            final Path parent = object.getParent();
            try {
                LocalStatePathGuard.ensureRealDirectoryPath(parent, "payload Object Store object parent");
                if (Files.exists(object, LinkOption.NOFOLLOW_LINKS)) {
                    compareExisting(object, payload);
                    return;
                }
                final Path temporary = Files.createTempFile(parent, ".payload-", ".tmp");
                Throwable primaryFailure = null;
                try {
                    try (FileChannel channel = FileChannel.open(
                            temporary,
                            StandardOpenOption.WRITE,
                            StandardOpenOption.TRUNCATE_EXISTING,
                            LinkOption.NOFOLLOW_LINKS)) {
                        final ByteBuffer buffer = ByteBuffer.wrap(payload);
                        while (buffer.hasRemaining()) {
                            channel.write(buffer);
                        }
                        channel.force(true);
                    }
                    try {
                        Files.move(temporary, object, StandardCopyOption.ATOMIC_MOVE);
                    } catch (FileAlreadyExistsException raced) {
                        compareExisting(object, payload);
                    } catch (AtomicMoveNotSupportedException unsupported) {
                        throw new IOException("payload Object Store requires atomic rename", unsupported);
                    }
                    forceDirectory(parent);
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
                            throw new IllegalStateException(
                                    "cannot clean payload Object Store temporary file", cleanupFailure);
                        }
                    }
                }
            } catch (IOException failure) {
                throw new IllegalStateException("cannot publish payload Object Store object: " + object, failure);
            }
        }

        private Path objectPath(final String objectIdentity) {
            Objects.requireNonNull(objectIdentity, "objectIdentity");
            if (objectIdentity.length() != 64 || !isLowerHex(objectIdentity)) {
                throw new IllegalArgumentException("payload object identity must be a 32-byte lowercase hex value");
            }
            return objectsRoot
                    .resolve(objectIdentity.substring(0, 2))
                    .resolve(objectIdentity.substring(2, 4))
                    .resolve(objectIdentity + ".payload")
                    .normalize();
        }

        private static boolean isLowerHex(final String value) {
            for (int index = 0; index < value.length(); index++) {
                final char character = value.charAt(index);
                if (!((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f'))) {
                    return false;
                }
            }
            return true;
        }

        private static void compareExisting(final Path object, final byte[] expected) throws IOException {
            if (Files.isSymbolicLink(object) || !Files.isRegularFile(object, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("payload Object Store object is not a regular file: " + object);
            }
            final byte[] existing =
                    LocalStatePathGuard.readRegularFileNoFollow(object, expected.length, "payload Object Store object");
            if (existing == null || !Arrays.equals(existing, expected)) {
                throw new IllegalStateException("immutable Object Store object identity conflict");
            }
        }

        private static void forceDirectory(final Path directory) throws IOException {
            try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
                channel.force(true);
            }
        }
    }
}
