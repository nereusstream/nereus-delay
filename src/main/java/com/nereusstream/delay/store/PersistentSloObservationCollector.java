package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.SloObservationOutbox;
import com.nereusstream.delay.protocol.SloThresholdDirection;
import com.nereusstream.delay.protocol.TargetSloCustodyReceipt;
import com.nereusstream.delay.protocol.TargetSloObservationExport;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Crash-durable local projection of the at-least-once SLO collector merge.
 *
 * <p>The state file retains the exact canonical outbox projection for every
 * sample in deterministic sample-id order. Each operation rereads the file
 * while holding a JVM and on-disk lock, then publishes a replacement through
 * a checksummed temporary file, atomic rename and directory fsync. This is
 * an embedded durability seam; it is not the production collector's
 * authorization, rolling-window policy or metric publication authority.</p>
 */
public final class PersistentSloObservationCollector {
    private static final int MAGIC = 0x4E534F43; // N S O C
    private static final int FORMAT_VERSION = 1;
    private static final int TARGET_FORMAT_VERSION = 2;
    private static final int HEADER_LENGTH = Integer.BYTES * 3;
    private static final int DIGEST_LENGTH = 32;
    private static final int MAX_STATE_BYTES = 64 * 1024 * 1024;
    private static final int MAX_SAMPLE_BYTES = 8 * 1024 * 1024;
    private static final int MAX_SAMPLES = 65_536;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-slo-collector-state\0");
    private static final Object JVM_LOCK = new Object();

    private final Path stateFile;
    private final SloObservationCollectorLimits limits;
    private final TargetLimits targetLimits;
    private SloObservationCollector delegate;
    private TreeMap<String, TargetEntry> targets = new TreeMap<>();

    /** Complete collector payload/entry limits, explicitly supplied by the independently qualified contract. */
    public record TargetLimits(
            int maximumEntries, long maximumCanonicalBytes, int maximumEntryBytes, int maximumEventsPerExport) {
        public TargetLimits {
            if (maximumEntries <= 0
                    || maximumEntries == Integer.MAX_VALUE
                    || maximumCanonicalBytes <= 0
                    || maximumCanonicalBytes > MAX_STATE_BYTES - HEADER_LENGTH - DIGEST_LENGTH
                    || maximumEntryBytes <= 0
                    || maximumEntryBytes > maximumCanonicalBytes
                    || maximumEventsPerExport < 0
                    || maximumEventsPerExport > Integer.MAX_VALUE - 6) {
                throw new IllegalArgumentException(
                        "Target collector requires explicit finite complete record/byte bounds");
            }
        }
    }

    public interface OriginalAuthority {
        /** Original Control, Source, binding, endpoint/time and complete retained collector material; outside file locks. */
        void requireOriginal(TargetSloObservationExport export);
        /** Authenticates the original protected gap confirmation and reconciled old frames, never a read failure. */
        default void requireRecoveryGap(
                TargetSloObservationExport export,
                com.nereusstream.delay.protocol.TargetSloObservationEventRecord event) {
            throw new IllegalStateException("original collector recovery-gap qualification unavailable");
        }
        /** Actual protected Native62/Control/Fence history and original per-frame key/deadline authority, outside file locks. */
        default void requireWriterEpoch(
                TargetSloObservationExport export,
                com.nereusstream.delay.protocol.TargetSloObservationEventRecord event) {
            throw new IllegalStateException("original collector writer-epoch qualification unavailable");
        }
        /** Complete original ENABLE and its initial actual Native62 anchor, including exports with no Final yet. */
        default void requireWriterPolicy(TargetSloObservationExport export) {
            throw new IllegalStateException("original collector writer-policy qualification unavailable");
        }
        /** All callbacks require already-held independent history and run outside collector file locks. */
        default void requireTargetOriginal(TargetSloObservationExport export) {
            requireCurrent();
            requireOriginal(export);
            requireCurrent();
            if (export.generation().policy().requiresWriterEpoch()) {
                requireWriterPolicy(export);
                requireCurrent();
            }
            for (var event : export.events()) {
                if (event.body().writerEpoch() != null) {
                    requireWriterEpoch(export, event);
                    requireCurrent();
                }
                if (event.body().evidence() != null && event.body().evidence().isRecoveryGap()) {
                    requireRecoveryGap(export, event);
                    requireCurrent();
                }
            }
            requireCurrent();
        }
        /** Only local checks of the already-held original protection/contract. */
        void requireCurrent();
        /** A locally held independently authenticated collector-contract key; never remote I/O. */
        default PublicKey receiptKey(TargetSloObservationExport export, TargetSloCustodyReceipt receipt) {
            throw new IllegalStateException("original Target collector contract key is unavailable");
        }
    }

    @FunctionalInterface
    public interface ReceiptSigner {
        /** Local signing only; the receipt must not be published before the enclosing atomic state fsync succeeds. */
        TargetSloCustodyReceipt sign(TargetSloObservationExport export);
    }

    private record TargetEntry(TargetSloObservationExport export, TargetSloCustodyReceipt receipt) {
        private TargetEntry {
            Objects.requireNonNull(export);
            Objects.requireNonNull(receipt).requireSample(export.generation(), export.sample());
            if (!Arrays.equals(export.digest(), receipt.exportDigest())) {
                throw new IllegalArgumentException("collector receipt changed its exact retained export");
            }
        }

        private byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, 1);
                CanonicalProtobuf.bytes(out, 2, export.canonicalBytes());
                CanonicalProtobuf.bytes(out, 3, receipt.canonicalBytes());
            });
        }
    }

    /** Creates a bounded-by-file-size collector projection without a policy envelope. */
    public PersistentSloObservationCollector(final Path stateFile) {
        this(stateFile, null);
    }

    /** Creates a collector projection with an explicit sample/byte envelope. */
    public PersistentSloObservationCollector(final Path stateFile, final SloObservationCollectorLimits limits) {
        this(stateFile, limits, null);
    }

    public PersistentSloObservationCollector(
            final Path stateFile, final SloObservationCollectorLimits limits, final TargetLimits targetLimits) {
        this.stateFile = normalizeStateFile(stateFile);
        this.limits = limits;
        this.targetLimits = targetLimits;
        try {
            ensureParentDirectory();
            this.delegate = load();
        } catch (IOException failure) {
            throw new IllegalStateException("cannot initialize SLO collector state", failure);
        }
    }

    /** Merges one exported outbox record and durably publishes the result. */
    public SloObservationOutbox merge(final SloObservationOutbox incoming, final SloThresholdDirection direction) {
        Objects.requireNonNull(incoming, "incoming");
        Objects.requireNonNull(direction, "direction");
        return mutate(() -> {
            if (targets.values().stream()
                    .anyMatch(value ->
                            Arrays.equals(value.export().sample().observation().sampleId(), incoming.sampleId()))) {
                throw new IllegalStateException(
                        "Target custody requires the complete authenticated export, not a v1-only merge");
            }
            return delegate.merge(incoming, direction);
        });
    }

    /** Stores the exact first export and signed receipt with the v1 merge in one existing atomic state publication. */
    public TargetSloCustodyReceipt mergeTarget(
            TargetSloObservationExport incoming,
            TargetLimits requested,
            OriginalAuthority authority,
            ReceiptSigner signer) {
        requireTargetLimits(requested);
        Objects.requireNonNull(authority);
        Objects.requireNonNull(signer);
        requireExportLimit(Objects.requireNonNull(incoming));
        final var key = Bytes.hex(incoming.sample().digest());
        final var observed = read(() -> targets.get(key));
        requireOriginal(incoming, authority);
        if (observed != null) {
            requireOriginal(observed.export(), authority);
            requireReceipt(observed, authority);
        }
        return mutate(
                () -> {
                    authority.requireCurrent();
                    final var existing = targets.get(key);
                    if (existing != null) {
                        if (observed == null || !Arrays.equals(existing.canonicalBytes(), observed.canonicalBytes())) {
                            throw new IllegalStateException(
                                    "Target collector version changed while original authentication was pending; retry exact input");
                        }
                        if (!Arrays.equals(
                                existing.export().sample().canonicalBytes(),
                                incoming.sample().canonicalBytes())) {
                            throw new IllegalStateException(
                                    "Target collector sample digest identifies different bytes");
                        }
                        requireReceipt(existing, authority);
                        return existing.receipt();
                    }
                    if (observed != null) {
                        throw new IllegalStateException("Target collector lost a retained exact version");
                    }
                    for (var value : targets.values()) {
                        if (Arrays.equals(
                                        value.export().sample().observation().sampleId(),
                                        incoming.sample().observation().sampleId())
                                && !Arrays.equals(
                                        value.export().sample().bindingDigest(),
                                        incoming.sample().bindingDigest())) {
                            throw new IllegalStateException(
                                    "Target collector cannot rebind a sample to another generation or Control");
                        }
                    }
                    final var entry = new TargetEntry(
                            incoming,
                            Objects.requireNonNull(signer.sign(incoming), "signed original collector receipt"));
                    requireReceipt(entry, authority);
                    if (entry.canonicalBytes().length > targetLimits.maximumEntryBytes()
                            || targets.size() >= targetLimits.maximumEntries()) {
                        throw new IllegalStateException("Target collector exact-version capacity exceeded");
                    }
                    mergeTargetProjection(incoming.sample().observation());
                    targets.put(key, entry);
                    authority.requireCurrent();
                    return entry.receipt();
                },
                authority::requireCurrent);
    }

    /** Actual restored Target entries are independently reauthenticated; the state checksum grants no authority. */
    public List<TargetSloObservationExport> targetSnapshot(TargetLimits requested, OriginalAuthority authority) {
        requireTargetLimits(requested);
        Objects.requireNonNull(authority);
        final var entries = read(() -> List.copyOf(targets.values()));
        final var result = new ArrayList<TargetSloObservationExport>();
        for (var entry : entries) {
            requireOriginal(entry.export(), authority);
            requireReceipt(entry, authority);
            result.add(entry.export());
        }
        authority.requireCurrent();
        return List.copyOf(result);
    }

    /** Returns the first fixed wrapper, even when a retry supplies a newer generation-count/export-cut context. */
    public TargetSloObservationExport targetExport(
            byte[] sampleContentDigest, TargetLimits requested, OriginalAuthority authority) {
        requireTargetLimits(requested);
        Objects.requireNonNull(authority);
        Bytes.requireLength(sampleContentDigest, 32, "exact Target sample content");
        final var entry = read(() -> targets.get(Bytes.hex(sampleContentDigest)));
        if (entry == null) {
            return null;
        }
        requireOriginal(entry.export(), authority);
        requireReceipt(entry, authority);
        return entry.export();
    }

    private static void requireOriginal(TargetSloObservationExport exported, OriginalAuthority authority) {
        authority.requireTargetOriginal(exported);
    }

    private void requireTargetLimits(TargetLimits requested) {
        if (targetLimits == null || !targetLimits.equals(Objects.requireNonNull(requested))) {
            throw new IllegalStateException(
                    "Target collector custody requires its original explicitly bounded contract");
        }
    }

    private static void requireReceipt(TargetEntry entry, OriginalAuthority authority) {
        authority.requireCurrent();
        entry.receipt()
                .requireSample(entry.export().generation(), entry.export().sample());
        final var key = Objects.requireNonNull(
                authority.receiptKey(entry.export(), entry.receipt()), "original collector-contract key");
        if (!entry.receipt().verifySignature(key)) {
            throw new IllegalStateException("Target collector custody signature is invalid");
        }
        authority.requireCurrent();
    }

    private void mergeTargetProjection(SloObservationOutbox incoming) {
        final var current = delegate.get(incoming.sampleId());
        if (current != null
                && incoming.finalObservation() != null
                && current.finalObservation() != null
                && Long.compareUnsigned(
                                incoming.finalObservation().observationRevision(),
                                current.finalObservation().observationRevision())
                        < 0) {
            requireDominates(current, incoming);
            return;
        }
        delegate.merge(incoming, incoming.start().objective().requiredDirection());
    }

    private static void requireDominates(SloObservationOutbox aggregate, SloObservationOutbox incoming) {
        if (!Arrays.equals(aggregate.start().canonicalBytes(), incoming.start().canonicalBytes())) {
            throw new IllegalStateException("Target collector aggregate lost original Start");
        }
        if (incoming.finalObservation() == null) {
            return;
        }
        if (aggregate.finalObservation() == null
                || Long.compareUnsigned(
                                aggregate.finalObservation().observationRevision(),
                                incoming.finalObservation().observationRevision())
                        < 0
                || !Arrays.equals(
                        incoming.mergeFinal(
                                        aggregate.finalObservation(),
                                        incoming.start().objective().requiredDirection())
                                .canonicalBytes(),
                        aggregate.canonicalBytes())) {
            throw new IllegalStateException("Target collector aggregate omits a retained exact observation");
        }
    }

    /** Returns a deterministic sample projection sorted by canonical sample ID. */
    public List<SloObservationOutbox> snapshot() {
        return read(delegate::snapshot);
    }

    public SloObservationOutbox get(final byte[] sampleId) {
        return read(() -> delegate.get(sampleId));
    }

    public int size() {
        return read(delegate::size);
    }

    public SloObservationCollector.Usage usage() {
        return read(delegate::usage);
    }

    private <T> T mutate(final IoAction<T> action) {
        return mutate(action, () -> {});
    }

    private <T> T mutate(final IoAction<T> action, final Runnable protection) {
        return withExclusiveLock(() -> {
            delegate = load();
            final List<SloObservationOutbox> before = delegate.snapshot();
            final var beforeTargets = new TreeMap<>(targets);
            try {
                final T result = action.run();
                protection.run();
                persist(delegate.snapshot());
                protection.run();
                return result;
            } catch (RuntimeException | IOException | Error failure) {
                delegate = rebuild(before);
                targets = beforeTargets;
                if (failure instanceof IOException ioFailure) {
                    throw new IllegalStateException("SLO collector state I/O failed", ioFailure);
                }
                throw failure;
            }
        });
    }

    private <T> T read(final IoAction<T> action) {
        return withExclusiveLock(() -> {
            delegate = load();
            return action.run();
        });
    }

    private SloObservationCollector load() throws IOException {
        final long maximumBytes = targetLimits == null
                ? MAX_STATE_BYTES
                : targetLimits.maximumCanonicalBytes() + HEADER_LENGTH + DIGEST_LENGTH;
        final byte[] encoded =
                LocalStatePathGuard.readRegularFileNoFollow(stateFile, maximumBytes, "SLO collector state");
        if (encoded == null) {
            targets = new TreeMap<>();
            return new SloObservationCollector(limits);
        }
        return decodeState(encoded);
    }

    private SloObservationCollector rebuild(final List<SloObservationOutbox> values) {
        final SloObservationCollector result = new SloObservationCollector(limits);
        for (SloObservationOutbox value : values) {
            result.merge(value, value.start().objective().requiredDirection());
        }
        return result;
    }

    private void persist(final List<SloObservationOutbox> values) throws IOException {
        ensureParentDirectory();
        rejectSymbolicLink(stateFile, "SLO collector state");
        final int version = targets.isEmpty() ? FORMAT_VERSION : TARGET_FORMAT_VERSION;
        if (!targets.isEmpty()) {
            requireTargetPayloadLimits(values);
        }
        final byte[] payload = encodeSnapshot(values, version, List.copyOf(targets.values()));
        if (payload.length > MAX_STATE_BYTES - HEADER_LENGTH - DIGEST_LENGTH) {
            throw new IOException("SLO collector state exceeds bounded size");
        }
        final byte[] digest = Bytes.sha256(DIGEST_DOMAIN, payload);
        final ByteBuffer output = ByteBuffer.allocate(HEADER_LENGTH + payload.length + digest.length)
                .order(ByteOrder.BIG_ENDIAN);
        output.putInt(MAGIC).putInt(version).putInt(payload.length).put(payload).put(digest);
        final Path temporary = Files.createTempFile(stateFile.getParent(), ".slo-collector-", ".tmp");
        Throwable primaryFailure = null;
        try {
            try (FileChannel channel =
                    FileChannel.open(temporary, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                final ByteBuffer buffer = ByteBuffer.wrap(output.array());
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
            try {
                Files.move(temporary, stateFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException unsupported) {
                throw new IOException("SLO collector state requires atomic rename", unsupported);
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
        synchronized (JVM_LOCK) {
            try {
                ensureParentDirectory();
                final Path lockFile = stateFile.resolveSibling(stateFile.getFileName() + ".lock");
                rejectSymbolicLink(lockFile, "SLO collector lock");
                try (FileChannel channel =
                                FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                        FileLock fileLock = channel.lock()) {
                    if (!fileLock.isValid()) {
                        throw new IOException("SLO collector lock is not valid");
                    }
                    return action.run();
                }
            } catch (IOException failure) {
                throw new IllegalStateException("SLO collector state I/O failed", failure);
            }
        }
    }

    private void ensureParentDirectory() throws IOException {
        final Path parent = stateFile.getParent();
        if (parent == null) {
            throw new IOException("SLO collector state must have a parent directory");
        }
        LocalStatePathGuard.ensureRealDirectoryPath(parent, "SLO collector state parent");
    }

    private static byte[] encodeSnapshot(
            final List<SloObservationOutbox> values, final int version, final List<TargetEntry> targetValues) {
        Objects.requireNonNull(values, "values");
        final List<SloObservationOutbox> sorted = new ArrayList<>(values);
        sorted.sort(Comparator.comparing(value -> Bytes.hex(value.sampleId())));
        if (sorted.size() > MAX_SAMPLES) {
            throw new IllegalStateException("SLO collector sample count exceeds bound");
        }
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, version);
            for (SloObservationOutbox value : sorted) {
                final byte[] sample = value.canonicalBytes();
                if (sample.length > MAX_SAMPLE_BYTES) {
                    throw new IllegalStateException("SLO collector sample exceeds bound");
                }
                CanonicalProtobuf.bytes(output, 2, sample);
            }
            for (var entry : targetValues) {
                CanonicalProtobuf.bytes(output, 3, entry.canonicalBytes());
            }
        });
    }

    private SloObservationCollector decodeState(final byte[] encoded) {
        if (encoded.length < HEADER_LENGTH + DIGEST_LENGTH) {
            throw new IllegalStateException("SLO collector state is truncated");
        }
        final ByteBuffer input = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        if (input.getInt() != MAGIC) {
            throw new IllegalStateException("SLO collector state has an unknown header");
        }
        final int version = input.getInt();
        if (version != FORMAT_VERSION && version != TARGET_FORMAT_VERSION) {
            throw new IllegalStateException("SLO collector state has an unknown version");
        }
        if (version == TARGET_FORMAT_VERSION && targetLimits == null) {
            throw new IllegalStateException("Target collector restore requires explicit original Target limits");
        }
        final int payloadLength = input.getInt();
        if (payloadLength <= 0
                || payloadLength > MAX_STATE_BYTES - HEADER_LENGTH - DIGEST_LENGTH
                || version == TARGET_FORMAT_VERSION && payloadLength > targetLimits.maximumCanonicalBytes()
                || encoded.length != HEADER_LENGTH + payloadLength + DIGEST_LENGTH) {
            throw new IllegalStateException("SLO collector state has an invalid length");
        }
        final byte[] payload = new byte[payloadLength];
        input.get(payload);
        final byte[] digest = new byte[DIGEST_LENGTH];
        input.get(digest);
        if (!Bytes.constantTimeEquals(digest, Bytes.sha256(DIGEST_DOMAIN, payload))) {
            throw new IllegalStateException("SLO collector state checksum mismatch");
        }
        final Snapshot snapshot = decodePayload(payload, version);
        if (!Arrays.equals(payload, encodeSnapshot(snapshot.values(), version, snapshot.targets()))) {
            throw new IllegalStateException("SLO collector state is not canonical");
        }
        final var result = rebuild(snapshot.values());
        final var restored = new TreeMap<String, TargetEntry>();
        final var bindings = new TreeMap<String, byte[]>();
        for (var entry : snapshot.targets()) {
            final var sample = entry.export().sample();
            final var prior =
                    bindings.putIfAbsent(Bytes.hex(sample.observation().sampleId()), sample.bindingDigest());
            if (prior != null && !Arrays.equals(prior, sample.bindingDigest())) {
                throw new IllegalStateException("Target collector state rebinds an original sample");
            }
            requireDominates(
                    Objects.requireNonNull(result.get(sample.observation().sampleId()), "Target collector aggregate"),
                    sample.observation());
            if (restored.putIfAbsent(Bytes.hex(sample.digest()), entry) != null) {
                throw new IllegalStateException("duplicate Target collector exact version");
            }
        }
        targets = restored;
        if (!targets.isEmpty()) {
            requireTargetPayloadLimits(snapshot.values());
        }
        return result;
    }

    private record Snapshot(List<SloObservationOutbox> values, List<TargetEntry> targets) {}

    private Snapshot decodePayload(final byte[] payload, final int version) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(payload, true);
        if (!reader.hasRemaining() || uint32(reader.next(), 1) != version) {
            throw new IllegalStateException("SLO collector snapshot has invalid version field");
        }
        final List<SloObservationOutbox> values = new ArrayList<>();
        final var targetValues = new ArrayList<TargetEntry>();
        String previousId = null;
        String previousTarget = null;
        while (reader.hasRemaining()) {
            final var field = reader.next();
            if (field.number() == 2) {
                if (!targetValues.isEmpty() || values.size() == MAX_SAMPLES) {
                    throw new IllegalStateException("SLO collector sample order/count exceeds bound");
                }
                final byte[] sampleBytes = bytes(field, 2);
                if (sampleBytes.length > MAX_SAMPLE_BYTES) {
                    throw new IllegalStateException("SLO collector sample exceeds bound");
                }
                final var value = SloObservationOutbox.decode(sampleBytes);
                final var currentId = Bytes.hex(value.sampleId());
                if (previousId != null && previousId.compareTo(currentId) >= 0) {
                    throw new IllegalStateException("SLO collector samples are not strictly sorted");
                }
                previousId = currentId;
                values.add(value);
            } else if (field.number() == 3 && version == TARGET_FORMAT_VERSION) {
                if (targetValues.size() == targetLimits.maximumEntries()) {
                    throw new IllegalStateException("Target collector exact version count exceeds bound");
                }
                final var entry = decodeTargetEntry(bytes(field, 3));
                final var key = Bytes.hex(entry.export().sample().digest());
                if (previousTarget != null && previousTarget.compareTo(key) >= 0) {
                    throw new IllegalStateException("Target collector exact versions are not strictly sorted");
                }
                previousTarget = key;
                targetValues.add(entry);
            } else {
                throw new IllegalStateException("SLO collector snapshot has unexpected field");
            }
        }
        if (version == TARGET_FORMAT_VERSION && targetValues.isEmpty()) {
            throw new IllegalStateException("Target collector format2 omits its Target entries");
        }
        return new Snapshot(List.copyOf(values), List.copyOf(targetValues));
    }

    private static int uint32(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() > Integer.MAX_VALUE) {
            throw new IllegalStateException("SLO collector version field is invalid");
        }
        return (int) field.unsignedValue();
    }

    private TargetEntry decodeTargetEntry(byte[] encoded) {
        if (encoded.length > targetLimits.maximumEntryBytes()) {
            throw new IllegalStateException("Target collector entry exceeds its complete byte bound");
        }
        final var reader = new CanonicalProtobuf.Reader(encoded, false);
        final var fields = new ArrayList<CanonicalProtobuf.Reader.Field>();
        while (reader.hasRemaining()) {
            if (fields.size() == 3) {
                throw new IllegalStateException("Target collector entry exceeds its field bound");
            }
            fields.add(reader.next());
        }
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "Target collector entry");
        if (uint32(fields.getFirst(), 1) != 1) {
            throw new IllegalStateException("unknown Target collector entry schema");
        }
        final var export = TargetSloObservationExport.decode(
                bytes(fields.get(1), 2), targetLimits.maximumEventsPerExport(), targetLimits.maximumEntryBytes());
        final var receipt = TargetSloCustodyReceipt.decode(bytes(fields.get(2), 3));
        final var entry = new TargetEntry(export, receipt);
        if (!Arrays.equals(encoded, entry.canonicalBytes())) {
            throw new IllegalStateException("Target collector entry is not canonical");
        }
        return entry;
    }

    private void requireExportLimit(TargetSloObservationExport value) {
        if (value.events().size() > targetLimits.maximumEventsPerExport()) {
            throw new IllegalStateException("Target collector event count exceeds its contract");
        }
        long bytes = 2L
                + 34L
                + bytesFieldSize(value.generation().canonicalBytes().length)
                + bytesFieldSize(value.sample().canonicalBytes().length)
                + bytesFieldSize(value.cut().canonicalBytes().length);
        for (var event : value.events()) {
            if (bytes > targetLimits.maximumEntryBytes()) {
                throw new IllegalStateException("Target collector export exceeds its byte contract");
            }
            bytes = Math.addExact(bytes, bytesFieldSize(event.canonicalBytes().length));
        }
        if (bytes > targetLimits.maximumEntryBytes()) {
            throw new IllegalStateException("Target collector export exceeds its byte contract");
        }
    }

    private void requireTargetPayloadLimits(List<SloObservationOutbox> values) {
        if (targetLimits == null || targets.size() > targetLimits.maximumEntries()) {
            throw new IllegalStateException("Target collector lacks its finite complete state contract");
        }
        long bytes = 2;
        for (var value : values) {
            bytes = Math.addExact(bytes, bytesFieldSize(value.canonicalBytes().length));
        }
        for (var value : targets.values()) {
            requireExportLimit(value.export());
            final int entryBytes = value.canonicalBytes().length;
            if (entryBytes > targetLimits.maximumEntryBytes()) {
                throw new IllegalStateException("Target collector entry exceeds its complete byte contract");
            }
            bytes = Math.addExact(bytes, bytesFieldSize(entryBytes));
        }
        if (bytes > targetLimits.maximumCanonicalBytes()) {
            throw new IllegalStateException("Target collector complete canonical state exceeds its byte contract");
        }
    }

    private static long bytesFieldSize(int length) {
        long prefix = 2;
        for (int remaining = length; remaining >= 128; remaining >>>= 7) {
            prefix++;
        }
        return Math.addExact(prefix, length);
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalStateException("SLO collector field is not bytes");
        }
        return field.rawValue();
    }

    private static Path normalizeStateFile(final Path value) {
        final Path normalized =
                Objects.requireNonNull(value, "stateFile").toAbsolutePath().normalize();
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
}
