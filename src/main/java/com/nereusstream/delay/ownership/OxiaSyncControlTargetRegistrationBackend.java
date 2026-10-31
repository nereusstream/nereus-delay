package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.ControlTargetMutationBinding;
import com.nereusstream.delay.protocol.ControlTargetRef;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TargetScopeResourcePolicy;
import com.nereusstream.delay.protocol.TargetScopeResourceRoot;
import com.nereusstream.delay.protocol.TargetSloControlRegisteredInput;
import com.nereusstream.delay.protocol.TargetSloControlScopeHeader;
import com.nereusstream.delay.protocol.TargetSloControlScopeRoot;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Durable Oxia backend for immutable Prepared Control target registration.
 *
 * <p>Each operation ID is one Oxia record. Registration is an
 * {@code IfRecordDoesNotExist} CAS and all later reads return the exact
 * canonical Prepared bytes. A response-loss retry is classified as
 * idempotent only after that exact reread.</p>
 *
 * <p>This backend deliberately does not add actor authorization or source
 * ordering; those remain required by the surrounding Control authority. The
 * handle-bound constructor additionally fences every record I/O to the exact
 * ephemeral Oxia session.</p>
 */
public final class OxiaSyncControlTargetRegistrationBackend
        implements OxiaControlTargetRegistrationAuthority.CasBackend {
    private static final int RECORD_VERSION = 1;
    private static final int SCOPE_INPUT_RECORD_VERSION = 2;
    private static final int MAX_PREPARED_BYTES = 8 * 1024 * 1024;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-oxia-control-target\0");
    private static final byte[] SCOPE_INPUT_DOMAIN = Bytes.utf8("nereus-delay-oxia-control-target-slo-scope-entry\0");
    private static final byte[] SCOPE_BIRTH_DOMAIN = Bytes.utf8("nereus-delay-oxia-control-target-slo-scope-birth\0");

    private final RecordClient client;
    private final String keyPrefix;
    private final Runnable sessionCheck;
    private final Supplier<byte[]> namespaceBinding;
    private final Runnable scopeResourceRequestTimeoutCheck;

    /** Creates a backend over an already configured Oxia client. */
    public OxiaSyncControlTargetRegistrationBackend(final SyncOxiaClient client, final String keyPrefix) {
        this(new SyncRecordClient(client), keyPrefix);
    }

    /** Creates a backend fenced to the exact ephemeral session of a handle. */
    public OxiaSyncControlTargetRegistrationBackend(
            final OxiaSyncOwnerLeaseBackend.ClientHandle handle, final String keyPrefix) {
        this(
                new SyncRecordClient(Objects.requireNonNull(handle, "handle").client()),
                keyPrefix,
                handle.backend()::assertConnectedSession,
                () -> handle.backend().requireOriginalStorageNamespaceBinding(handle.client()),
                handle.backend()::requireConfiguredRequestTimeout);
    }

    /** Package-private constructor used by deterministic CAS tests. */
    OxiaSyncControlTargetRegistrationBackend(final RecordClient client, final String keyPrefix) {
        this(client, keyPrefix, () -> {});
    }

    /** Package-private constructor used to exercise the session fence. */
    OxiaSyncControlTargetRegistrationBackend(
            final RecordClient client, final String keyPrefix, final Runnable sessionCheck) {
        this(
                client,
                keyPrefix,
                sessionCheck,
                () -> {
                    throw new IllegalStateException(
                            "SLO Scope requires its actual managed SDK storage namespace binding");
                },
                () -> {
                    throw new IllegalStateException("Scope resources require the actual managed SDK request timeout");
                });
    }

    private OxiaSyncControlTargetRegistrationBackend(
            final RecordClient client,
            final String keyPrefix,
            final Runnable sessionCheck,
            final Supplier<byte[]> namespaceBinding,
            final Runnable scopeResourceRequestTimeoutCheck) {
        this.sessionCheck = Objects.requireNonNull(sessionCheck, "sessionCheck");
        this.namespaceBinding = Objects.requireNonNull(namespaceBinding, "namespaceBinding");
        this.scopeResourceRequestTimeoutCheck =
                Objects.requireNonNull(scopeResourceRequestTimeoutCheck, "scopeResourceRequestTimeoutCheck");
        this.client = new SessionBoundRecordClient(Objects.requireNonNull(client, "client"), this.sessionCheck);
        this.keyPrefix = canonicalKeyPrefix(keyPrefix);
    }

    /** Same original registry client/session/namespace; borrowed clients retain only their old point behavior. */
    public TargetSloControlScopeAuthority scopeAuthority(TargetSloControlScopeAuthority.Protections protections) {
        sessionCheck.run();
        final var binding = Objects.requireNonNull(namespaceBinding.get(), "actual managed registry namespace binding");
        Bytes.requireLength(binding, 32, "actual managed registry namespace binding");
        if (Arrays.equals(binding, new byte[32])) {
            throw new IllegalStateException("actual registry namespace binding is unassigned");
        }
        sessionCheck.run();
        return new TargetSloControlScopeAuthority(new ScopeStorage(binding), Objects.requireNonNull(protections));
    }

    /** Uses only the original managed Registry client and its configured finite request timeout. */
    @Override
    public TargetScopeResourceManager.Storage scopeResourceStorage(TargetScopeResourcePolicy policy) {
        sessionCheck.run();
        scopeResourceRequestTimeoutCheck.run();
        final var binding = Objects.requireNonNull(namespaceBinding.get(), "actual managed registry namespace binding");
        Bytes.requireLength(binding, 32, "actual managed registry namespace binding");
        if (Arrays.equals(binding, new byte[32])) {
            throw new IllegalStateException("actual registry namespace binding is unassigned");
        }
        final var storage = new ScopeResourceStorage(binding, Objects.requireNonNull(policy));
        storage.requireCurrent();
        return storage;
    }

    @Override
    public ControlTargetRegistrationAuthority.RegistrationResult register(final PreparedControlOperation prepared) {
        Objects.requireNonNull(prepared, "prepared");
        final String key = operationKey(prepared.operationId());
        final Entry existing = read(key, prepared.operationId());
        if (existing != null) {
            return classify(existing, prepared);
        }
        final byte[] value = encode(prepared);
        try {
            putExact(key, value, Set.of(PutOption.IfRecordDoesNotExist));
            return ControlTargetRegistrationAuthority.RegistrationResult.RECORDED;
        } catch (KeyAlreadyExistsException | UnexpectedVersionIdException race) {
            final Entry observed = read(key, prepared.operationId());
            if (observed == null) {
                throw new IllegalStateException("Oxia target registration disappeared after CAS", race);
            }
            return classify(observed, prepared);
        } catch (RuntimeException responseFailure) {
            final Entry observed = read(key, prepared.operationId());
            if (observed != null && exact(observed.prepared(), prepared)) {
                return ControlTargetRegistrationAuthority.RegistrationResult.ALREADY_RECORDED;
            }
            throw responseFailure;
        }
    }

    @Override
    public Optional<PreparedControlOperation> find(final byte[] operationId) {
        return Optional.ofNullable(read(operationKey(operationId), operationId)).map(Entry::prepared);
    }

    @Override
    public void validateMutation(
            final PreparedControlOperation prepared, final ControlTargetRef target, final SystemMutation mutation) {
        Objects.requireNonNull(prepared, "prepared");
        final Entry registered = read(operationKey(prepared.operationId()), prepared.operationId());
        if (registered == null || !exact(registered.prepared(), prepared)) {
            throw new IllegalArgumentException("Control operation has not been registered exactly");
        }
        ControlTargetMutationBinding.validate(registered.prepared(), target, mutation);
    }

    private static ControlTargetRegistrationAuthority.RegistrationResult classify(
            final Entry existing, final PreparedControlOperation requested) {
        if (!exact(existing.prepared(), requested)) {
            throw new IllegalArgumentException("Control operation ID is already registered with different bytes");
        }
        return ControlTargetRegistrationAuthority.RegistrationResult.ALREADY_RECORDED;
    }

    private Entry read(final String key, final byte[] operationId) {
        final GetResult result = client.get(key);
        if (result == null) {
            return null;
        }
        if (!key.equals(result.key()) || result.value() == null || result.version() == null) {
            throw new IllegalStateException("Oxia target registration response has an invalid record identity");
        }
        return decode(result.value(), operationId, result.version().versionId());
    }

    private long putExact(final String key, final byte[] value, final Set<PutOption> options)
            throws UnexpectedVersionIdException, KeyAlreadyExistsException {
        final PutResult result = client.put(key, value, options);
        if (result == null || !key.equals(result.key()) || result.version() == null) {
            throw new IllegalStateException("Oxia target registration put returned no exact version");
        }
        return result.version().versionId();
    }

    private String operationKey(final byte[] operationId) {
        Bytes.requireLength(operationId, 32, "operationId");
        boolean nonZero = false;
        for (byte value : operationId) {
            nonZero |= value != 0;
        }
        if (!nonZero) {
            throw new IllegalArgumentException("operationId must be non-zero");
        }
        return keyPrefix + "/operation/" + Bytes.hex(operationId);
    }

    private static boolean exact(final PreparedControlOperation left, final PreparedControlOperation right) {
        return Bytes.constantTimeEquals(left.canonicalBytes(), right.canonicalBytes());
    }

    private static byte[] encode(final PreparedControlOperation prepared) {
        final byte[] preparedBytes = prepared.canonicalBytes();
        if (preparedBytes.length == 0 || preparedBytes.length > MAX_PREPARED_BYTES) {
            throw new IllegalArgumentException("Prepared Control bytes exceed Oxia target limit");
        }
        final byte[] digest = Bytes.sha256(DIGEST_DOMAIN, preparedBytes);
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECORD_VERSION);
            CanonicalProtobuf.bytes(output, 2, preparedBytes);
            CanonicalProtobuf.bytes(output, 3, digest);
        });
    }

    private static byte[] encodeScopeInput(TargetSloControlRegisteredInput input) {
        final var original = Objects.requireNonNull(input).canonicalBytes();
        if (original.length > TargetSloControlRegisteredInput.MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("full Scope input exceeds its bounded immutable record");
        }
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, SCOPE_INPUT_RECORD_VERSION);
            CanonicalProtobuf.bytes(out, 2, original);
            CanonicalProtobuf.bytes(out, 3, Bytes.sha256(SCOPE_INPUT_DOMAIN, original));
        });
    }

    private static Entry decode(final byte[] encoded, final byte[] operationId, final long versionId) {
        if (encoded == null || encoded.length > TargetSloControlRegisteredInput.MAX_CANONICAL_BYTES + 256) {
            throw new IllegalStateException("Oxia target registration record exceeds bounded size");
        }
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        final CanonicalProtobuf.Reader.Field version = next(reader, 1);
        final long recordVersion = uint(version, 1);
        if (recordVersion != RECORD_VERSION && recordVersion != SCOPE_INPUT_RECORD_VERSION) {
            throw new IllegalStateException("unsupported Oxia target registration record version");
        }
        final byte[] originalBytes = bytes(next(reader, 2), 2);
        final byte[] digest = bytes(next(reader, 3), 3);
        final int maximum = recordVersion == RECORD_VERSION
                ? MAX_PREPARED_BYTES
                : TargetSloControlRegisteredInput.MAX_CANONICAL_BYTES;
        if (originalBytes.length == 0
                || originalBytes.length > maximum
                || digest.length != 32
                || reader.hasRemaining()
                || !Bytes.constantTimeEquals(
                        digest,
                        Bytes.sha256(
                                recordVersion == RECORD_VERSION ? DIGEST_DOMAIN : SCOPE_INPUT_DOMAIN, originalBytes))) {
            throw new IllegalStateException("Oxia target registration record is non-canonical or corrupt");
        }
        try {
            final var scoped = recordVersion == SCOPE_INPUT_RECORD_VERSION
                    ? TargetSloControlRegisteredInput.decode(originalBytes)
                    : null;
            final PreparedControlOperation prepared =
                    scoped == null ? PreparedControlOperation.decode(originalBytes) : scoped.prepared();
            if (!Bytes.constantTimeEquals(operationId, prepared.operationId())
                    || !Arrays.equals(encoded, scoped == null ? encode(prepared) : encodeScopeInput(scoped))) {
                throw new IllegalStateException("Oxia target registration record identity mismatch");
            }
            return new Entry(prepared, scoped, versionId);
        } catch (RuntimeException malformed) {
            if (malformed instanceof IllegalStateException stateFailure
                    && "Oxia target registration record identity mismatch".equals(stateFailure.getMessage())) {
                throw stateFailure;
            }
            throw new IllegalStateException("Oxia target registration record contains malformed values", malformed);
        }
    }

    private static CanonicalProtobuf.Reader.Field next(final CanonicalProtobuf.Reader reader, final int number) {
        if (!reader.hasRemaining()) {
            throw new IllegalStateException("missing Oxia target registration field " + number);
        }
        final CanonicalProtobuf.Reader.Field field = reader.next();
        if (field.number() != number) {
            throw new IllegalStateException("unexpected Oxia target registration field " + field.number());
        }
        return field;
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalStateException("invalid Oxia target registration bytes field " + number);
        }
        return field.rawValue();
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalStateException("invalid Oxia target registration varint field " + number);
        }
        return field.unsignedValue();
    }

    private static String canonicalKeyPrefix(final String value) {
        Objects.requireNonNull(value, "keyPrefix");
        final byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (value.isBlank()
                || value.endsWith("/")
                || value.indexOf('\0') >= 0
                || !value.equals(new String(encoded, StandardCharsets.UTF_8))
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException("keyPrefix must be a nonblank NFC UTF-8 path without trailing '/'");
        }
        return value;
    }

    /** Original-client bridge only; management/resource/history protections remain with the Scope authority. */
    private final class ScopeStorage implements TargetSloControlScopeAuthority.Storage {
        private final byte[] actualNamespace;

        private ScopeStorage(byte[] actualNamespace) {
            this.actualNamespace = Bytes.copy(actualNamespace);
        }

        @Override
        public byte[] namespaceBinding() {
            return Bytes.copy(actualNamespace);
        }

        @Override
        public String keyPrefix() {
            return keyPrefix;
        }

        @Override
        public void requireCurrent() {
            sessionCheck.run();
            if (!Bytes.constantTimeEquals(actualNamespace, namespaceBinding.get())) {
                throw new IllegalStateException("Scope storage lost its actual original SDK namespace binding");
            }
            sessionCheck.run();
        }

        private void requireHeader(TargetSloControlScopeHeader header) {
            requireCurrent();
            if (!Arrays.equals(actualNamespace, Objects.requireNonNull(header).registryNamespaceBinding())
                    || !keyPrefix.equals(header.registryKeyPrefix())) {
                throw new IllegalArgumentException(
                        "Scope header differs from its actual original registry namespace/key prefix");
            }
        }

        private String scopeBase(byte[] scopeId) {
            Bytes.requireLength(scopeId, 32, "original Scope ID");
            if (Arrays.equals(scopeId, new byte[32])) {
                throw new IllegalArgumentException("Scope ID is unassigned");
            }
            return keyPrefix + "/e4-scope/" + Bytes.hex(scopeId);
        }

        private GetResult getBounded(String key, int maximumBytes) {
            final var observed = client.get(key);
            if (observed == null) {
                return null;
            }
            if (!key.equals(observed.key())
                    || observed.version() == null
                    || observed.value() == null
                    || observed.version().versionId() < 0
                    || observed.value().length > maximumBytes) {
                throw new IllegalStateException("Scope registry read returned an invalid key/version/bounded value");
            }
            return observed;
        }

        @Override
        public TargetSloControlScopeAuthority.RootRecord readRoot(TargetSloControlScopeHeader.Reference reference) {
            requireCurrent();
            final var requested = Objects.requireNonNull(reference);
            final var observed =
                    getBounded(scopeBase(requested.scopeId()) + "/root", TargetSloControlScopeRoot.MAX_CANONICAL_BYTES);
            if (observed == null) {
                return null;
            }
            final var root = TargetSloControlScopeRoot.decode(observed.value());
            requireHeader(root.header());
            if (!root.header().reference().equals(requested)) {
                throw new IllegalArgumentException(
                        "original Scope ID is already bound to another immutable header/epoch");
            }
            if (root.schemaVersion() == TargetSloControlScopeRoot.LEGACY_VERSION) {
                return new TargetSloControlScopeAuthority.RootRecord(
                        root, observed.version().versionId());
            }
            final var birth = readBirth(root.header());
            if (birth == null) {
                throw new IllegalStateException("Root2 lost its actual immutable original birth marker");
            }
            return new TargetSloControlScopeAuthority.RootRecord(
                    root, observed.version().versionId(), birth.versionId());
        }

        private BirthEntry readBirth(TargetSloControlScopeHeader header) {
            final var observed = getBounded(
                    scopeBase(header.scopeId()) + "/birth", TargetSloControlScopeHeader.MAX_CANONICAL_BYTES + 256);
            if (observed == null) {
                return null;
            }
            final var reader = new CanonicalProtobuf.Reader(observed.value());
            if (uint(next(reader, 1), 1) != 1) {
                throw new IllegalStateException("unknown Scope birth marker schema");
            }
            final var fullHeader = bytes(next(reader, 2), 2);
            final var digest = bytes(next(reader, 3), 3);
            if (fullHeader.length > TargetSloControlScopeHeader.MAX_CANONICAL_BYTES
                    || digest.length != 32
                    || reader.hasRemaining()
                    || !Bytes.constantTimeEquals(digest, Bytes.sha256(SCOPE_BIRTH_DOMAIN, fullHeader))) {
                throw new IllegalStateException("Scope birth marker is corrupt or exceeds its immutable bound");
            }
            final var original = TargetSloControlScopeHeader.decode(fullHeader);
            requireHeader(original);
            if (!original.equals(header) || !Arrays.equals(observed.value(), encodeBirth(original))) {
                throw new IllegalArgumentException("Scope ID birth marker cannot be reused for another header/epoch");
            }
            return new BirthEntry(original, observed.version().versionId());
        }

        private byte[] encodeBirth(TargetSloControlScopeHeader header) {
            final var fullHeader = header.canonicalBytes();
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, 1);
                CanonicalProtobuf.bytes(out, 2, fullHeader);
                CanonicalProtobuf.bytes(out, 3, Bytes.sha256(SCOPE_BIRTH_DOMAIN, fullHeader));
            });
        }

        @Override
        public TargetSloControlScopeAuthority.RootRecord birth(TargetSloControlScopeHeader header) {
            requireHeader(header);
            final var marker = readBirth(header);
            final var current = readRoot(header.reference());
            if (current != null) {
                if (marker == null) {
                    throw new IllegalStateException(
                            "Scope Root lacks its original immutable birth marker; retain UNKNOWN");
                }
                return current;
            }
            if (marker != null) {
                throw new IllegalStateException(
                        "original Scope birth marker exists without its Root; retain UNKNOWN and do not reconstruct");
            }
            final long markerVersion;
            try {
                markerVersion = putExact(
                        scopeBase(header.scopeId()) + "/birth",
                        encodeBirth(header),
                        Set.of(PutOption.IfRecordDoesNotExist));
            } catch (KeyAlreadyExistsException | UnexpectedVersionIdException | RuntimeException originalFailure) {
                // Even an exact marker reread cannot prove this call owns an unstarted Root creation.
                return reconcileBirth(header, originalFailure);
            }
            final var confirmedMarker = readBirth(header);
            if (confirmedMarker == null || confirmedMarker.versionId() != markerVersion) {
                throw new IllegalStateException(
                        "original immutable Scope birth marker was not confirmed; retain UNKNOWN");
            }
            final var initial = TargetSloControlScopeRoot.empty(header);
            try {
                putExact(
                        scopeBase(header.scopeId()) + "/root",
                        initial.canonicalBytes(),
                        Set.of(PutOption.IfRecordDoesNotExist));
            } catch (KeyAlreadyExistsException | UnexpectedVersionIdException | RuntimeException originalFailure) {
                return reconcileBirth(header, originalFailure);
            }
            final var created = readRoot(header.reference());
            if (created == null) {
                throw new IllegalStateException("Scope Root creation has no actual reread; retain UNKNOWN");
            }
            return created;
        }

        private TargetSloControlScopeAuthority.RootRecord reconcileBirth(
                TargetSloControlScopeHeader header, Throwable originalFailure) {
            try {
                final var marker = readBirth(header);
                final var root = readRoot(header.reference());
                if (marker != null && root != null) {
                    return root;
                }
            } catch (RuntimeException observationFailure) {
                if (observationFailure != originalFailure) {
                    originalFailure.addSuppressed(observationFailure);
                }
            }
            if (originalFailure instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException("Scope birth is unknown after conditional create", originalFailure);
        }

        @Override
        public TargetSloControlScopeAuthority.RootRecord compareRoot(
                TargetSloControlScopeAuthority.RootRecord expected, TargetSloControlScopeRoot next) {
            Objects.requireNonNull(expected);
            Objects.requireNonNull(next);
            requireHeader(expected.value().header());
            if (!expected.value().header().equals(next.header())) {
                throw new IllegalArgumentException("Scope CAS cannot replace its original header");
            }
            if (expected.value().equals(next)) {
                return sameBirth(expected, readRoot(next.header().reference()));
            }
            next.requireTransitionFrom(expected.value());
            if (expected.value().schemaVersion() != TargetSloControlScopeRoot.LEGACY_VERSION) {
                final var birth = readBirth(expected.value().header());
                if (birth == null
                        || expected.birthVersionId() == null
                        || birth.versionId() != expected.birthVersionId()) {
                    throw new IllegalStateException("Scope exposure CAS changed its actual original birth identity");
                }
            }
            final var reference = next.header().reference();
            try {
                putExact(
                        scopeBase(reference.scopeId()) + "/root",
                        next.canonicalBytes(),
                        Set.of(PutOption.IfVersionIdEquals(expected.versionId())));
            } catch (KeyAlreadyExistsException | UnexpectedVersionIdException race) {
                final var observed = sameBirth(expected, rootAfterFailure(reference, race));
                if (observed == null) {
                    throw new IllegalStateException("original Scope Root disappeared after CAS; retain UNKNOWN", race);
                }
                return observed;
            } catch (RuntimeException originalFailure) {
                final var observed = sameBirth(expected, rootAfterFailure(reference, originalFailure));
                if (observed != null && observed.value().equals(next)) {
                    return observed;
                }
                throw originalFailure;
            }
            final var observed = sameBirth(expected, readRoot(reference));
            if (observed == null) {
                throw new IllegalStateException("Scope CAS has no actual Root reread; retain UNKNOWN");
            }
            return observed;
        }

        private TargetSloControlScopeAuthority.RootRecord sameBirth(
                TargetSloControlScopeAuthority.RootRecord expected,
                TargetSloControlScopeAuthority.RootRecord observed) {
            if (observed != null
                    && expected.value().schemaVersion() != TargetSloControlScopeRoot.LEGACY_VERSION
                    && !Objects.equals(expected.birthVersionId(), observed.birthVersionId())) {
                throw new IllegalStateException(
                        "Scope CAS observation changed its actual original birth; retain UNKNOWN");
            }
            return observed;
        }

        private TargetSloControlScopeAuthority.RootRecord rootAfterFailure(
                TargetSloControlScopeHeader.Reference reference, Throwable originalFailure) {
            try {
                return readRoot(reference);
            } catch (RuntimeException observationFailure) {
                if (observationFailure != originalFailure) {
                    originalFailure.addSuppressed(observationFailure);
                }
                if (originalFailure instanceof RuntimeException runtime) {
                    throw runtime;
                }
                throw new IllegalStateException(
                        "Scope CAS result is unknown after failed observation", originalFailure);
            }
        }

        @Override
        public TargetSloControlScopeAuthority.InputRecord readInput(byte[] operationId) {
            requireCurrent();
            final var entry = read(operationKey(operationId), operationId);
            if (entry == null) {
                return null;
            }
            if (entry.scoped() == null) {
                throw new IllegalStateException(
                        "legacy point registration is not a full immutable Scope input and cannot be upgraded");
            }
            return new TargetSloControlScopeAuthority.InputRecord(entry.scoped(), entry.versionId());
        }

        @Override
        public TargetSloControlScopeAuthority.InputRecord putInput(TargetSloControlRegisteredInput exact) {
            requireCurrent();
            Objects.requireNonNull(exact);
            final var operationId = exact.operationId();
            final var existing = readInput(operationId);
            if (existing != null) {
                requireExactInput(existing, exact);
                return existing;
            }
            final long version;
            try {
                version = putExact(
                        operationKey(operationId), encodeScopeInput(exact), Set.of(PutOption.IfRecordDoesNotExist));
            } catch (KeyAlreadyExistsException | UnexpectedVersionIdException race) {
                final var observed = readInput(operationId);
                if (observed == null) {
                    throw new IllegalStateException(
                            "reserved Scope immutable input disappeared after CAS; retain UNKNOWN", race);
                }
                requireExactInput(observed, exact);
                return observed;
            } catch (RuntimeException originalFailure) {
                try {
                    final var observed = readInput(operationId);
                    if (observed != null && observed.value().equals(exact)) {
                        return observed;
                    }
                } catch (RuntimeException observationFailure) {
                    if (observationFailure != originalFailure) {
                        originalFailure.addSuppressed(observationFailure);
                    }
                }
                throw originalFailure;
            }
            final var observed = readInput(operationId);
            if (observed == null || observed.versionId() != version) {
                throw new IllegalStateException(
                        "original Scope immutable entry version was not confirmed; retain UNKNOWN");
            }
            requireExactInput(observed, exact);
            return observed;
        }

        private void requireExactInput(
                TargetSloControlScopeAuthority.InputRecord observed, TargetSloControlRegisteredInput exact) {
            if (!observed.value().equals(exact)) {
                throw new IllegalArgumentException(
                        "operation ID already holds different immutable full Scope input bytes");
            }
        }
    }

    /** Two bounded records in the original Registry; creation is deliberately not a two-key transaction. */
    private final class ScopeResourceStorage implements TargetScopeResourceManager.Storage {
        private final byte[] actualNamespace;
        private final TargetScopeResourcePolicy policy;
        private final byte[] policyBytes;
        private final TargetScopeResourceRoot initialRoot;
        private final byte[] firstBirthMarker;
        private final int maximumRootBytes;
        private Long originalBirthVersionId;

        private ScopeResourceStorage(byte[] actualNamespace, TargetScopeResourcePolicy requested) {
            this.actualNamespace = Bytes.copy(actualNamespace);
            policyBytes = requested.canonicalBytes();
            policy = TargetScopeResourcePolicy.decode(policyBytes);
            final var binding = policy.binding();
            if (!Bytes.constantTimeEquals(actualNamespace, binding.registryNamespaceBinding())
                    || !keyPrefix.equals(binding.registryKeyPrefix())) {
                throw new IllegalArgumentException(
                        "resource policy differs from its actual original registry namespace/key prefix");
            }
            maximumRootBytes =
                    Math.min(policy.management().maximumRootBytes(), TargetScopeResourceRoot.MAX_CANONICAL_BYTES);
            initialRoot = TargetScopeResourceRoot.open(policy);
            boundedRootBytes(initialRoot);
            firstBirthMarker = initialRoot.firstBirthMarker();
            if (firstBirthMarker.length == 0
                    || firstBirthMarker.length > TargetScopeResourceRoot.FIRST_BIRTH_MARKER_MAX_CANONICAL_BYTES) {
                throw new IllegalArgumentException("resource first-birth marker exceeds its immutable bound");
            }
        }

        @Override
        public byte[] namespaceBinding() {
            return Bytes.copy(actualNamespace);
        }

        @Override
        public String keyPrefix() {
            return keyPrefix;
        }

        @Override
        public void requireCurrent() {
            sessionCheck.run();
            scopeResourceRequestTimeoutCheck.run();
            if (!Bytes.constantTimeEquals(actualNamespace, namespaceBinding.get())) {
                throw new IllegalStateException("resource storage lost its actual original SDK namespace binding");
            }
            sessionCheck.run();
        }

        private void requirePolicy(TargetScopeResourcePolicy candidate) {
            if (!policy.reference().equals(Objects.requireNonNull(candidate).reference())
                    || !Bytes.constantTimeEquals(policyBytes, candidate.canonicalBytes())) {
                throw new IllegalArgumentException(
                        "resource storage cannot replace its full immutable allocation policy");
            }
        }

        private byte[] boundedRootBytes(TargetScopeResourceRoot root) {
            requirePolicy(root.policy());
            if (!policy.reference().equals(root.reference())) {
                throw new IllegalArgumentException("resource Root has another immutable allocation reference");
            }
            final var encoded = root.canonicalBytes();
            if (encoded.length == 0 || encoded.length > maximumRootBytes) {
                throw new IllegalArgumentException("resource Root exceeds its prepaid canonical byte bound");
            }
            return encoded;
        }

        private GetResult getBounded(String key, int maximumBytes) {
            requireCurrent();
            final var observed = client.get(key);
            requireCurrent();
            if (observed == null) {
                return null;
            }
            if (!key.equals(observed.key())
                    || observed.version() == null
                    || observed.value() == null
                    || observed.value().length == 0
                    || observed.value().length > maximumBytes) {
                throw new IllegalStateException("resource registry read returned an invalid key/version/bounded value");
            }
            requireVersion(observed.version().versionId());
            return observed;
        }

        private void requireVersion(long versionId) {
            if (versionId < 0 || versionId == Long.MAX_VALUE) {
                throw new IllegalStateException("resource registry requires an exact actual SDK version");
            }
        }

        private long putResourceExact(String key, byte[] value, Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            requireCurrent();
            final long versionId = putExact(key, value, options);
            requireVersion(versionId);
            requireCurrent();
            return versionId;
        }

        private synchronized void observeBirth(long versionId) {
            if (originalBirthVersionId != null && originalBirthVersionId.longValue() != versionId) {
                throw new IllegalStateException(
                        "resource storage changed its actual original first-birth version; retain UNKNOWN");
            }
            originalBirthVersionId = versionId;
        }

        private synchronized void requireNeverBorn() {
            if (originalBirthVersionId != null) {
                throw new IllegalStateException(
                        "resource storage lost its original first-birth marker; retain UNKNOWN");
            }
        }

        private Long readBirthVersion() {
            final var observed = getBounded(
                    policy.binding().firstBirthMarkerKey(),
                    TargetScopeResourceRoot.FIRST_BIRTH_MARKER_MAX_CANONICAL_BYTES);
            if (observed == null) {
                return null;
            }
            // Exact canonical expected bytes also reject unknown schemas, fields, digests and allocation identities.
            if (!Bytes.constantTimeEquals(firstBirthMarker, observed.value())) {
                throw new IllegalStateException(
                        "resource first-birth marker differs from its full original policy reference");
            }
            final long versionId = observed.version().versionId();
            observeBirth(versionId);
            return versionId;
        }

        @Override
        public TargetScopeResourceManager.RootRecord readRoot() {
            requireCurrent();
            final Long beforeBirth = readBirthVersion();
            final var observed = getBounded(policy.binding().rootKey(), maximumRootBytes);
            final Long afterBirth = readBirthVersion();
            if (!Objects.equals(beforeBirth, afterBirth)) {
                throw new IllegalStateException(
                        "resource first-birth marker changed while reading its Root; retain UNKNOWN");
            }
            if (observed == null) {
                if (beforeBirth != null) {
                    throw new IllegalStateException(
                            "resource first-birth marker exists without its Root; retain UNKNOWN and do not reconstruct");
                }
                requireNeverBorn();
                requireCurrent();
                return null;
            }
            if (beforeBirth == null) {
                throw new IllegalStateException(
                        "resource Root lost its original immutable first-birth marker; retain UNKNOWN");
            }
            final var root = TargetScopeResourceRoot.decode(observed.value());
            if (!Bytes.constantTimeEquals(observed.value(), boundedRootBytes(root))) {
                throw new IllegalStateException("resource Root is not its exact canonical value");
            }
            requireCurrent();
            return new TargetScopeResourceManager.RootRecord(
                    root, observed.version().versionId(), beforeBirth);
        }

        @Override
        public TargetScopeResourceManager.RootRecord initialize(TargetScopeResourcePolicy requested) {
            requirePolicy(requested);
            final var current = readRoot();
            if (current != null) {
                return current;
            }
            final long markerVersion;
            try {
                markerVersion = putResourceExact(
                        policy.binding().firstBirthMarkerKey(),
                        firstBirthMarker,
                        Set.of(PutOption.IfRecordDoesNotExist));
            } catch (KeyAlreadyExistsException | UnexpectedVersionIdException | RuntimeException originalFailure) {
                // An exact marker reread cannot prove this call owns an unstarted Root creation.
                return reconcileInitialization(originalFailure);
            }
            final var confirmedMarker = readBirthVersion();
            if (confirmedMarker == null || confirmedMarker.longValue() != markerVersion) {
                throw new IllegalStateException(
                        "resource first-birth creation has no exact actual marker confirmation; retain UNKNOWN");
            }
            try {
                putResourceExact(
                        policy.binding().rootKey(),
                        boundedRootBytes(initialRoot),
                        Set.of(PutOption.IfRecordDoesNotExist));
            } catch (KeyAlreadyExistsException | UnexpectedVersionIdException | RuntimeException originalFailure) {
                return sameBirth(markerVersion, reconcileInitialization(originalFailure));
            }
            final var created = sameBirth(markerVersion, readRoot());
            if (created == null) {
                throw new IllegalStateException("resource Root initialization has no actual reread; retain UNKNOWN");
            }
            return created;
        }

        private TargetScopeResourceManager.RootRecord reconcileInitialization(Throwable originalFailure) {
            try {
                final var observed = readRoot();
                if (observed != null) {
                    return observed;
                }
            } catch (RuntimeException observationFailure) {
                if (observationFailure != originalFailure) {
                    originalFailure.addSuppressed(observationFailure);
                }
            }
            if (originalFailure instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException(
                    "resource initialization is unknown after conditional creation", originalFailure);
        }

        @Override
        public TargetScopeResourceManager.RootRecord compareRoot(
                TargetScopeResourceManager.RootRecord expected, TargetScopeResourceRoot next) {
            Objects.requireNonNull(expected);
            Objects.requireNonNull(next);
            requireVersion(expected.versionId());
            requireVersion(expected.birthVersionId());
            final var expectedBytes = boundedRootBytes(expected.value());
            final var nextBytes = boundedRootBytes(next);
            final boolean unchanged = Bytes.constantTimeEquals(expectedBytes, nextBytes);
            if (!unchanged) {
                next.requireTransitionFrom(expected.value());
            }
            final var current = sameBirth(expected.birthVersionId(), readRoot());
            if (current == null) {
                throw new IllegalStateException("original resource Root disappeared before CAS; retain UNKNOWN");
            }
            if (current.versionId() != expected.versionId()) {
                return current;
            }
            if (!Bytes.constantTimeEquals(expectedBytes, current.value().canonicalBytes())) {
                throw new IllegalStateException("resource expected bytes differ at the same actual SDK version");
            }
            if (unchanged) {
                return current;
            }
            try {
                putResourceExact(
                        policy.binding().rootKey(),
                        nextBytes,
                        Set.of(PutOption.IfVersionIdEquals(expected.versionId())));
            } catch (KeyAlreadyExistsException | UnexpectedVersionIdException race) {
                final var observed = sameBirth(expected.birthVersionId(), rootAfterFailure(race));
                if (observed == null) {
                    throw new IllegalStateException(
                            "original resource Root disappeared after CAS; retain UNKNOWN", race);
                }
                return observed;
            } catch (RuntimeException originalFailure) {
                final var observed = sameBirth(expected.birthVersionId(), rootAfterFailure(originalFailure));
                if (observed != null
                        && Bytes.constantTimeEquals(nextBytes, observed.value().canonicalBytes())) {
                    return observed;
                }
                throw originalFailure;
            }
            final var observed = sameBirth(expected.birthVersionId(), readRoot());
            if (observed == null) {
                throw new IllegalStateException("resource Root CAS has no actual reread; retain UNKNOWN");
            }
            return observed;
        }

        private TargetScopeResourceManager.RootRecord sameBirth(
                long expectedBirthVersion, TargetScopeResourceManager.RootRecord observed) {
            if (observed != null && observed.birthVersionId() != expectedBirthVersion) {
                throw new IllegalStateException(
                        "resource Root observation changed its actual original birth; retain UNKNOWN");
            }
            return observed;
        }

        private TargetScopeResourceManager.RootRecord rootAfterFailure(Throwable originalFailure) {
            try {
                return readRoot();
            } catch (RuntimeException observationFailure) {
                if (observationFailure != originalFailure) {
                    originalFailure.addSuppressed(observationFailure);
                }
                if (originalFailure instanceof RuntimeException runtime) {
                    throw runtime;
                }
                throw new IllegalStateException(
                        "resource Root CAS is unknown after failed observation", originalFailure);
            }
        }
    }

    private record BirthEntry(TargetSloControlScopeHeader header, long versionId) {}

    interface RecordClient {
        GetResult get(String key);

        PutResult put(String key, byte[] value, Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException;
    }

    private record Entry(PreparedControlOperation prepared, TargetSloControlRegisteredInput scoped, long versionId) {
        private Entry {
            Objects.requireNonNull(prepared, "prepared");
        }
    }

    private static final class SyncRecordClient implements RecordClient {
        private final SyncOxiaClient delegate;

        private SyncRecordClient(final SyncOxiaClient delegate) {
            this.delegate = Objects.requireNonNull(delegate, "client");
        }

        @Override
        public GetResult get(final String key) {
            return delegate.get(key);
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            return delegate.put(key, value, options);
        }
    }

    /**
     * Checks the caller's Oxia session around every record operation. A
     * successful registration whose response is lost after the marker
     * disappears is therefore never reported as recorded.
     */
    private static final class SessionBoundRecordClient implements RecordClient {
        private final RecordClient delegate;
        private final Runnable sessionCheck;

        private SessionBoundRecordClient(final RecordClient delegate, final Runnable sessionCheck) {
            this.delegate = delegate;
            this.sessionCheck = sessionCheck;
        }

        @Override
        public GetResult get(final String key) {
            sessionCheck.run();
            try {
                final GetResult result = delegate.get(key);
                sessionCheck.run();
                return result;
            } catch (RuntimeException failure) {
                sessionCheck.run();
                throw failure;
            }
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            sessionCheck.run();
            try {
                final PutResult result = delegate.put(key, value, options);
                sessionCheck.run();
                return result;
            } catch (KeyAlreadyExistsException | UnexpectedVersionIdException expectedCasRace) {
                sessionCheck.run();
                throw expectedCasRace;
            } catch (RuntimeException failure) {
                sessionCheck.run();
                throw failure;
            }
        }
    }
}
