package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.CredentialBindingHeadV1;
import io.nereusstream.delay.protocol.CredentialBindingProtectionV1;
import io.nereusstream.delay.protocol.CredentialBindingV1;
import io.nereusstream.delay.protocol.CredentialUseKindV1;
import io.nereusstream.delay.protocol.CredentialUseLeaseV1;
import io.nereusstream.delay.protocol.DestinationProfileSemanticV1;
import io.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.RotateEquivalentSecretRequestV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Single-record Oxia authority for one immutable credential Profile catalog
 * entry, its generation Head, protections and bounded use-lease issuance.
 *
 * <p>Every Profile version is stored in one canonical record. Initial
 * publication, equivalent-secret rotation and protection-before-lease issuance
 * therefore use one version CAS each. The backend does not resolve private
 * material or authorize a control actor, but it does require every binding's
 * equivalence attestation to verify against the configured immutable trust
 * set.</p>
 */
public final class OxiaSyncProfileCatalogBackend implements CredentialProfileAuthority {
    private static final int RECORD_VERSION = 1;
    private static final int MAX_RECORD_BYTES = 8 * 1024 * 1024;
    private static final int MAX_BINDINGS = 4096;
    private static final int MAX_CAS_ATTEMPTS = 32;
    private static final byte[] DIGEST_DOMAIN =
            Bytes.utf8("nereus-delay-oxia-profile-catalog-v1\0");
    private static final String PROFILE_SUFFIX = "/profile";

    private final RecordClient client;
    private final String keyPrefix;
    private final long maximumLeaseTtlMs;
    private final long maximumAttestationAgeMs;
    private final CredentialAttestationTrustSet attestationTrustSet;

    /** Creates a Profile catalog over an already configured Oxia client. */
    public OxiaSyncProfileCatalogBackend(final SyncOxiaClient client, final String keyPrefix,
                                         final long maximumLeaseTtlMs,
                                         final long maximumAttestationAgeMs,
                                         final CredentialAttestationTrustSet attestationTrustSet) {
        this(new SyncRecordClient(client), keyPrefix, maximumLeaseTtlMs, maximumAttestationAgeMs,
                attestationTrustSet);
    }

    /** Package-private constructor used by deterministic CAS tests. */
    OxiaSyncProfileCatalogBackend(final RecordClient client, final String keyPrefix,
                                  final long maximumLeaseTtlMs,
                                  final long maximumAttestationAgeMs,
                                  final CredentialAttestationTrustSet attestationTrustSet) {
        this.client = Objects.requireNonNull(client, "client");
        this.keyPrefix = canonicalKeyPrefix(keyPrefix);
        if (maximumLeaseTtlMs <= 0 || maximumAttestationAgeMs <= 0) {
            throw new IllegalArgumentException("credential lease bounds must be positive");
        }
        this.maximumLeaseTtlMs = maximumLeaseTtlMs;
        this.maximumAttestationAgeMs = maximumAttestationAgeMs;
        this.attestationTrustSet = Objects.requireNonNull(attestationTrustSet, "attestationTrustSet");
    }

    /** Publishes generation one and its initial protection projection atomically. */
    public CredentialBindingHeadV1 publish(final ProfileSemanticEnvelopeV1 profile,
                                           final CredentialBindingV1 binding) {
        requireBindableProfile(profile);
        Objects.requireNonNull(binding, "binding");
        if (!profile.ref().equals(binding.profile()) || binding.secretGeneration() != 1) {
            throw new IllegalArgumentException("Profile publication requires an exact generation-1 binding");
        }
        requireCredentialScope(profile, binding);
        requireCredentialAttestation(profile, binding);
        return mutate(profile.ref(), current -> {
            if (current != null) {
                if (!current.profile().equals(profile)) {
                    throw new IllegalStateException("Profile version already has different semantic bytes");
                }
                final CredentialBindingV1 existing = current.bindings().get(1L);
                if (existing == null || !existing.equals(binding)) {
                    throw new IllegalStateException("Profile generation one binding bytes conflict");
                }
                return Change.unchanged(current, current.head());
            }
            final CredentialBindingProtectionV1 protection = CredentialBindingProtectionV1.forBinding(
                    binding, 0, 0, 0, 0, 1);
            final CredentialBindingHeadV1 head = CredentialBindingHeadV1.forBinding(binding, 1);
            return Change.changed(new State(profile, head, Map.of(1L, binding), Map.of(1L, protection)), head);
        });
    }

    /** Applies the exact checked successor of the current credential Head. */
    public CredentialBindingHeadV1 rotate(final RotateEquivalentSecretRequestV1 request) {
        Objects.requireNonNull(request, "request");
        final CredentialBindingV1 nextBinding = request.newBinding();
        return mutate(request.profile(), current -> {
            if (current == null) {
                throw new IllegalStateException("Profile has no durable credential catalog record");
            }
            final CredentialBindingHeadV1 head = current.head();
            final long nextRevision = increment(request.expectedBindingHeadRevision(),
                    "credential Head revision");
            final CredentialBindingV1 existingNext = current.bindings().get(request.newSecretGeneration());
            if (head.secretGeneration() == request.newSecretGeneration()
                    && head.headRevision() == nextRevision
                    && Bytes.constantTimeEquals(head.bindingDigest(), nextBinding.bindingDigest())) {
                if (existingNext == null || !existingNext.equals(nextBinding)) {
                    throw new IllegalStateException("rotation Head agrees but immutable binding bytes differ");
                }
                return Change.unchanged(current, head);
            }
            final CredentialBindingV1 expected = current.bindings().get(request.expectedSecretGeneration());
            if (expected == null
                    || !Bytes.constantTimeEquals(expected.bindingDigest(), head.bindingDigest())) {
                throw new IllegalStateException("credential Head has no matching immutable generation");
            }
            if (head.secretGeneration() != request.expectedSecretGeneration()
                    || head.headRevision() != request.expectedBindingHeadRevision()
                    || !Bytes.constantTimeEquals(head.bindingDigest(), request.expectedBindingDigest())) {
                throw new IllegalStateException("credential Head CAS precondition failed");
            }
            requireCredentialScope(current.profile(), nextBinding);
            requireCredentialAttestation(current.profile(), nextBinding);
            if (existingNext != null && !existingNext.equals(nextBinding)) {
                throw new IllegalStateException("credential generation already has different immutable bytes");
            }
            final Map<Long, CredentialBindingV1> bindings = new TreeMap<>(Long::compareUnsigned);
            bindings.putAll(current.bindings());
            bindings.put(nextBinding.secretGeneration(), nextBinding);
            final Map<Long, CredentialBindingProtectionV1> protections = new TreeMap<>(Long::compareUnsigned);
            protections.putAll(current.protections());
            if (!protections.containsKey(nextBinding.secretGeneration())) {
                protections.put(nextBinding.secretGeneration(), CredentialBindingProtectionV1.forBinding(
                        nextBinding, 0, 0, 0, 0, 1));
            }
            final CredentialBindingHeadV1 nextHead = CredentialBindingHeadV1.forBinding(nextBinding, nextRevision);
            return Change.changed(new State(current.profile(), nextHead, bindings, protections), nextHead);
        });
    }

    /**
     * Issues a bounded lease only after the current Head and binding match the
     * request. The corresponding protection horizon is raised monotonically
     * in the same Oxia record CAS before the lease is returned.
     */
    public CredentialUseLeaseV1 issueCredentialUseLease(final ProfileRefV1 profile,
                                                        final CredentialUseKindV1 kind,
                                                        final byte[] holderScopeDigest,
                                                        final long expectedSecretGeneration,
                                                        final byte[] expectedBindingDigest,
                                                        final byte[] resolvedCredentialFingerprintDigest,
                                                        final TrustedUtcIntervalEvidence issuedAt,
                                                        final long validUntilEpochMs,
                                                        final long expectedHeadRevision) {
        requireBindableProfileRef(profile);
        Objects.requireNonNull(kind, "kind");
        requireKindMatchesProfile(profile, kind);
        Bytes.requireLength(holderScopeDigest, CredentialUseLeaseV1.HASH_LENGTH, "holderScopeDigest");
        Bytes.requireLength(expectedBindingDigest, CredentialBindingV1.HASH_LENGTH, "expectedBindingDigest");
        Bytes.requireLength(resolvedCredentialFingerprintDigest, CredentialUseLeaseV1.HASH_LENGTH,
                "resolvedCredentialFingerprintDigest");
        Objects.requireNonNull(issuedAt, "issuedAt");
        if (expectedSecretGeneration == 0 || expectedHeadRevision == 0) {
            throw new IllegalArgumentException("credential lease Head identity must be non-zero");
        }
        return mutate(profile, current -> {
            if (current == null) {
                throw new IllegalStateException("Profile has no durable credential catalog record");
            }
            final CredentialBindingHeadV1 head = current.head();
            if (head.secretGeneration() != expectedSecretGeneration
                    || head.headRevision() != expectedHeadRevision
                    || !Bytes.constantTimeEquals(head.bindingDigest(), expectedBindingDigest)) {
                throw new IllegalStateException("credential lease Head CAS precondition failed");
            }
            final CredentialBindingV1 binding = current.bindings().get(expectedSecretGeneration);
            final CredentialBindingProtectionV1 protection = current.protections().get(expectedSecretGeneration);
            if (binding == null || protection == null) {
                throw new IllegalStateException("credential generation lacks binding or protection");
            }
            if (!Bytes.constantTimeEquals(resolvedCredentialFingerprintDigest,
                    binding.equivalenceAttestation().resolvedCredentialFingerprintDigest())) {
                throw new IllegalArgumentException("resolved credential fingerprint does not match binding");
            }
            requireCredentialAttestation(current.profile(), binding);
            binding.equivalenceAttestation().requireNotAfterAtMost(maximumAttestationAgeMs);
            if (issuedAt.earliestEpochMs() < binding.equivalenceAttestation().verifiedAt().latestEpochMs()) {
                throw new IllegalArgumentException("lease evidence predates credential attestation");
            }
            if (validUntilEpochMs > binding.equivalenceAttestation().notAfterEpochMs()) {
                throw new IllegalArgumentException("credential lease outlives its attestation");
            }
            final long currentProtectionUntil = protectionUntil(protection, kind);
            final long nextProtectionUntil = Math.max(currentProtectionUntil, validUntilEpochMs);
            if (nextProtectionUntil == currentProtectionUntil) {
                final CredentialUseLeaseV1 lease = new CredentialUseLeaseV1(profile, kind, holderScopeDigest,
                        expectedSecretGeneration, binding.bindingDigest(), resolvedCredentialFingerprintDigest,
                        issuedAt, validUntilEpochMs, protection.protectionRevision());
                lease.requireTtlAtMost(maximumLeaseTtlMs);
                lease.requireProtectedBy(protection);
                return Change.unchanged(current, lease);
            }
            final long nextProtectionRevision = increment(protection.protectionRevision(),
                    "credential protection revision");
            final CredentialBindingProtectionV1 nextProtection = protectionForLease(protection, kind,
                    nextProtectionUntil, nextProtectionRevision);
            final CredentialUseLeaseV1 lease = new CredentialUseLeaseV1(profile, kind, holderScopeDigest,
                    expectedSecretGeneration, binding.bindingDigest(), resolvedCredentialFingerprintDigest,
                    issuedAt, validUntilEpochMs, nextProtection.protectionRevision());
            lease.requireTtlAtMost(maximumLeaseTtlMs);
            lease.requireProtectedBy(nextProtection);
            final Map<Long, CredentialBindingProtectionV1> protections = new TreeMap<>(Long::compareUnsigned);
            protections.putAll(current.protections());
            protections.put(expectedSecretGeneration, nextProtection);
            return Change.changed(new State(current.profile(), head, current.bindings(), protections), lease);
        });
    }

    @Override
    public ProfileSemanticEnvelopeV1 resolve(final ProfileRefV1 reference) {
        Objects.requireNonNull(reference, "reference");
        final State state = read(reference);
        return state == null ? null : state.profile();
    }

    @Override
    public CredentialBindingV1 resolveBinding(final ProfileRefV1 profile, final long secretGeneration) {
        Objects.requireNonNull(profile, "profile");
        if (secretGeneration == 0) {
            throw new IllegalArgumentException("secretGeneration must be non-zero");
        }
        final State state = read(profile);
        return state == null ? null : state.bindings().get(secretGeneration);
    }

    @Override
    public CredentialBindingHeadV1 resolveHead(final ProfileRefV1 profile) {
        Objects.requireNonNull(profile, "profile");
        final State state = read(profile);
        return state == null ? null : state.head();
    }

    @Override
    public CredentialBindingProtectionV1 resolveProtection(final ProfileRefV1 profile,
                                                           final long secretGeneration) {
        Objects.requireNonNull(profile, "profile");
        if (secretGeneration == 0) {
            throw new IllegalArgumentException("secretGeneration must be non-zero");
        }
        final State state = read(profile);
        return state == null ? null : state.protections().get(secretGeneration);
    }

    private <T> T mutate(final ProfileRefV1 profile, final Mutation<T> mutation) {
        final String key = profileKey(profile);
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            final GetResult currentResult = client.get(key);
            final State current = decodeRecord(currentResult, profile);
            final Change<T> change = Objects.requireNonNull(mutation.apply(current), "profile catalog mutation");
            final byte[] before = current == null ? null : encodeRecord(current);
            final byte[] after = encodeRecord(change.state());
            if (after.length > MAX_RECORD_BYTES) {
                throw new IllegalStateException("Oxia Profile catalog record exceeds size bound");
            }
            if (before != null && Arrays.equals(before, after)) {
                return change.result();
            }
            try {
                final Set<PutOption> options = currentResult == null
                        ? Set.of(PutOption.IfRecordDoesNotExist)
                        : Set.of(PutOption.IfVersionIdEquals(currentResult.version().versionId()));
                final PutResult stored = client.put(key, after, options);
                if (stored == null || !key.equals(stored.key()) || stored.version() == null) {
                    throw new IllegalStateException("Oxia Profile catalog put returned no exact version");
                }
                return change.result();
            } catch (UnexpectedVersionIdException | KeyAlreadyExistsException conflict) {
                // A concurrent profile owner won the record CAS; apply the
                // exact request to its newly observed canonical state.
            } catch (RuntimeException responseFailure) {
                final GetResult observed = client.get(key);
                if (observed != null && key.equals(observed.key()) && observed.version() != null
                        && Arrays.equals(after, observed.value())) {
                    return change.result();
                }
                throw responseFailure;
            }
        }
        throw new IllegalStateException("Oxia Profile catalog CAS did not converge");
    }

    private State read(final ProfileRefV1 profile) {
        return decodeRecord(client.get(profileKey(profile)), profile);
    }

    private State decodeRecord(final GetResult result, final ProfileRefV1 requestedProfile) {
        if (result == null) {
            return null;
        }
        final String key = profileKey(requestedProfile);
        if (!key.equals(result.key()) || result.value() == null || result.version() == null) {
            throw new IllegalStateException("Oxia Profile catalog response has an invalid record identity");
        }
        if (result.value().length > MAX_RECORD_BYTES) {
            throw new IllegalStateException("Oxia Profile catalog record exceeds size bound");
        }
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(result.value());
        final CanonicalProtobuf.Reader.Field version = next(reader, 1);
        if (uint(version, 1) != RECORD_VERSION) {
            throw new IllegalStateException("unsupported Oxia Profile catalog record version");
        }
        final byte[] stateBytes = bytes(next(reader, 2), 2, MAX_RECORD_BYTES);
        final byte[] digest = bytes(next(reader, 3), 3, CredentialBindingV1.HASH_LENGTH);
        if (reader.hasRemaining() || !Bytes.constantTimeEquals(digest, Bytes.sha256(DIGEST_DOMAIN, stateBytes))) {
            throw new IllegalStateException("Oxia Profile catalog record is non-canonical or corrupt");
        }
        final State state = decodeState(stateBytes);
        if (!Arrays.equals(result.value(), encodeRecord(state))) {
            throw new IllegalStateException("Oxia Profile catalog record bytes are not canonical");
        }
        if (!state.profile().ref().equals(requestedProfile)) {
            throw new IllegalStateException("Oxia Profile catalog record Profile identity differs");
        }
        return state;
    }

    private static byte[] encodeRecord(final State state) {
        final byte[] stateBytes = encodeState(state);
        final byte[] digest = Bytes.sha256(DIGEST_DOMAIN, stateBytes);
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECORD_VERSION);
            CanonicalProtobuf.bytes(output, 2, stateBytes);
            CanonicalProtobuf.bytes(output, 3, digest);
        });
    }

    private State decodeState(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final CanonicalProtobuf.Reader.Field profileField = next(reader, 1);
        final ProfileSemanticEnvelopeV1 profile = ProfileSemanticEnvelopeV1.decode(bytes(profileField, 1,
                MAX_RECORD_BYTES));
        requireBindableProfile(profile);
        CredentialBindingHeadV1 head = null;
        final Map<Long, CredentialBindingV1> bindings = new TreeMap<>(Long::compareUnsigned);
        final Map<Long, CredentialBindingProtectionV1> protections = new TreeMap<>(Long::compareUnsigned);
        int phase = 2;
        long previousBinding = 0;
        long previousProtection = 0;
        boolean hasPreviousBinding = false;
        boolean hasPreviousProtection = false;
        while (reader.hasRemaining()) {
            final CanonicalProtobuf.Reader.Field field = reader.next();
            if (field.number() == 2 && phase == 2) {
                if (head != null) {
                    throw new IllegalStateException("duplicate Oxia Profile catalog Head");
                }
                head = CredentialBindingHeadV1.decode(bytes(field, 2, MAX_RECORD_BYTES));
            } else if (field.number() == 3 && (phase == 2 || phase == 3)) {
                phase = 3;
                final CredentialBindingV1 binding = CredentialBindingV1.decode(bytes(field, 3, MAX_RECORD_BYTES));
                requireBindingIdentity(profile, binding);
                requireCredentialScope(profile, binding);
                requireCredentialAttestation(profile, binding);
                if (bindings.size() >= MAX_BINDINGS) {
                    throw new IllegalStateException("Oxia Profile catalog binding count exceeds bound");
                }
                if (hasPreviousBinding && Long.compareUnsigned(binding.secretGeneration(), previousBinding) <= 0) {
                    throw new IllegalStateException("Oxia Profile catalog bindings are not unsigned sorted");
                }
                if (bindings.put(binding.secretGeneration(), binding) != null) {
                    throw new IllegalStateException("duplicate Oxia Profile catalog binding generation");
                }
                previousBinding = binding.secretGeneration();
                hasPreviousBinding = true;
            } else if (field.number() == 4 && (phase == 3 || phase == 4)) {
                phase = 4;
                final CredentialBindingProtectionV1 protection = CredentialBindingProtectionV1.decode(
                        bytes(field, 4, MAX_RECORD_BYTES));
                if (!profile.ref().equals(protection.profile())) {
                    throw new IllegalStateException("Oxia Profile catalog protection Profile differs");
                }
                if (protections.size() >= MAX_BINDINGS) {
                    throw new IllegalStateException("Oxia Profile catalog protection count exceeds bound");
                }
                if (hasPreviousProtection
                        && Long.compareUnsigned(protection.secretGeneration(), previousProtection) <= 0) {
                    throw new IllegalStateException("Oxia Profile catalog protections are not unsigned sorted");
                }
                if (protections.put(protection.secretGeneration(), protection) != null) {
                    throw new IllegalStateException("duplicate Oxia Profile catalog protection generation");
                }
                previousProtection = protection.secretGeneration();
                hasPreviousProtection = true;
            } else {
                throw new IllegalStateException("Oxia Profile catalog fields are out of order");
            }
        }
        if (head == null || bindings.isEmpty() || !bindings.keySet().equals(protections.keySet())) {
            throw new IllegalStateException("Oxia Profile catalog state has incomplete generation projections");
        }
        if (!profile.ref().equals(head.profile())) {
            throw new IllegalStateException("Oxia Profile catalog Head Profile differs");
        }
        final CredentialBindingV1 headBinding = bindings.get(head.secretGeneration());
        if (headBinding == null || !Bytes.constantTimeEquals(head.bindingDigest(), headBinding.bindingDigest())) {
            throw new IllegalStateException("Oxia Profile catalog Head has no matching binding");
        }
        for (Map.Entry<Long, CredentialBindingProtectionV1> entry : protections.entrySet()) {
            final CredentialBindingV1 binding = bindings.get(entry.getKey());
            if (!Bytes.constantTimeEquals(binding.bindingDigest(), entry.getValue().bindingDigest())) {
                throw new IllegalStateException("Oxia Profile catalog protection has no matching binding");
            }
        }
        final State state = new State(profile, head, bindings, protections);
        if (!Arrays.equals(encoded, encodeState(state))) {
            throw new IllegalStateException("Oxia Profile catalog state bytes are not canonical");
        }
        return state;
    }

    private static byte[] encodeState(final State state) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, state.profile().canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, state.head().canonicalBytes());
            state.bindings().values().stream()
                    .sorted(Comparator.comparing(CredentialBindingV1::secretGeneration,
                            Long::compareUnsigned))
                    .forEach(binding -> CanonicalProtobuf.bytes(output, 3, binding.canonicalBytes()));
            state.protections().values().stream()
                    .sorted(Comparator.comparing(CredentialBindingProtectionV1::secretGeneration,
                            Long::compareUnsigned))
                    .forEach(protection -> CanonicalProtobuf.bytes(output, 4, protection.canonicalBytes()));
        });
    }

    private static void requireBindableProfile(final ProfileSemanticEnvelopeV1 profile) {
        Objects.requireNonNull(profile, "profile");
        requireBindableProfileRef(profile.ref());
        if (!(profile.body() instanceof DestinationProfileSemanticV1)
                && !(profile.body() instanceof ObjectStoreProfileSemanticV1)) {
            throw new IllegalArgumentException("credential Profile body is not bindable");
        }
    }

    private static void requireBindableProfileRef(final ProfileRefV1 profile) {
        Objects.requireNonNull(profile, "profile");
        if (profile.profileKind() != ProfileKindV1.DESTINATION
                && profile.profileKind() != ProfileKindV1.OBJECT_STORE) {
            throw new IllegalArgumentException("credential Profile must be DESTINATION or OBJECT_STORE");
        }
    }

    private static void requireKindMatchesProfile(final ProfileRefV1 profile, final CredentialUseKindV1 kind) {
        if ((kind == CredentialUseKindV1.DESTINATION_CHANNEL
                && profile.profileKind() != ProfileKindV1.DESTINATION)
                || (kind == CredentialUseKindV1.OBJECT_STORE_ADAPTER
                && profile.profileKind() != ProfileKindV1.OBJECT_STORE)) {
            throw new IllegalArgumentException("credential lease kind/profile mismatch");
        }
    }

    private static void requireCredentialScope(final ProfileSemanticEnvelopeV1 profile,
                                               final CredentialBindingV1 binding) {
        requireBindingIdentity(profile, binding);
        final byte[] expectedScope = profile.body() instanceof DestinationProfileSemanticV1 destination
                ? destination.credentialAuthorizationScopeDigest()
                : ((ObjectStoreProfileSemanticV1) profile.body()).credentialAuthorizationScopeDigest();
        binding.equivalenceAttestation().requireAuthorizationScopeDigest(expectedScope);
    }

    private void requireCredentialAttestation(final ProfileSemanticEnvelopeV1 profile,
                                              final CredentialBindingV1 binding) {
        attestationTrustSet.verify(profile, binding);
    }

    private static void requireBindingIdentity(final ProfileSemanticEnvelopeV1 profile,
                                               final CredentialBindingV1 binding) {
        if (!profile.ref().equals(binding.profile())) {
            throw new IllegalArgumentException("credential binding Profile differs");
        }
        binding.equivalenceAttestation().requireCandidate(profile.ref(), binding.secretGeneration(),
                binding.secretReferenceSha256());
    }

    private static long protectionUntil(final CredentialBindingProtectionV1 protection,
                                        final CredentialUseKindV1 kind) {
        return kind == CredentialUseKindV1.OBJECT_STORE_ADAPTER
                ? protection.objectStoreLeaseProtectionUntilEpochMs()
                : protection.managedChannelProtectionUntilEpochMs();
    }

    private static CredentialBindingProtectionV1 protectionForLease(
            final CredentialBindingProtectionV1 protection, final CredentialUseKindV1 kind,
            final long until, final long revision) {
        final long managed = kind == CredentialUseKindV1.DESTINATION_CHANNEL
                ? until : protection.managedChannelProtectionUntilEpochMs();
        final long objectStore = kind == CredentialUseKindV1.OBJECT_STORE_ADAPTER
                ? until : protection.objectStoreLeaseProtectionUntilEpochMs();
        return CredentialBindingProtectionV1.create(protection.profile(), protection.secretGeneration(),
                protection.bindingDigest(), managed, objectStore,
                protection.nativeCapabilityProtectionUntilEpochMs(), protection.uploadHandleProtectionUntilEpochMs(),
                revision);
    }

    private String profileKey(final ProfileRefV1 profile) {
        return keyPrefix + "/profile/" + profile.profileKind().wireValue() + "/"
                + Bytes.hex(profile.profileId()) + "/" + Long.toUnsignedString(profile.version())
                + PROFILE_SUFFIX;
    }

    private static CanonicalProtobuf.Reader.Field next(final CanonicalProtobuf.Reader reader, final int number) {
        if (!reader.hasRemaining()) {
            throw new IllegalStateException("missing Oxia Profile catalog field " + number);
        }
        final CanonicalProtobuf.Reader.Field field = reader.next();
        if (field.number() != number) {
            throw new IllegalStateException("unexpected Oxia Profile catalog field " + field.number());
        }
        return field;
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number, final int maxLength) {
        if (field.number() != number || field.wireType() != 2 || field.rawValue().length > maxLength) {
            throw new IllegalStateException("invalid Oxia Profile catalog bytes field " + number);
        }
        return field.rawValue();
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalStateException("invalid Oxia Profile catalog uint field " + number);
        }
        return field.unsignedValue();
    }

    private static long increment(final long value, final String name) {
        if (value == -1L) {
            throw new IllegalArgumentException(name + " exhausted");
        }
        return value + 1;
    }

    private static String canonicalKeyPrefix(final String value) {
        Objects.requireNonNull(value, "keyPrefix");
        final byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (value.isBlank() || value.endsWith("/") || value.indexOf('\0') >= 0
                || !value.equals(new String(encoded, StandardCharsets.UTF_8))
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException("keyPrefix must be nonblank NFC UTF-8 path without trailing '/'");
        }
        return value;
    }

    private record State(ProfileSemanticEnvelopeV1 profile, CredentialBindingHeadV1 head,
                         Map<Long, CredentialBindingV1> bindings,
                         Map<Long, CredentialBindingProtectionV1> protections) {
        private State {
            Objects.requireNonNull(profile, "profile");
            Objects.requireNonNull(head, "head");
            final Map<Long, CredentialBindingV1> orderedBindings = new TreeMap<>(Long::compareUnsigned);
            orderedBindings.putAll(Objects.requireNonNull(bindings, "bindings"));
            final Map<Long, CredentialBindingProtectionV1> orderedProtections =
                    new TreeMap<>(Long::compareUnsigned);
            orderedProtections.putAll(Objects.requireNonNull(protections, "protections"));
            bindings = Map.copyOf(orderedBindings);
            protections = Map.copyOf(orderedProtections);
        }
    }

    private record Change<T>(State state, T result) {
        private Change {
            Objects.requireNonNull(state, "state");
            Objects.requireNonNull(result, "result");
        }

        private static <T> Change<T> changed(final State state, final T result) {
            return new Change<>(state, result);
        }

        private static <T> Change<T> unchanged(final State state, final T result) {
            return new Change<>(state, result);
        }
    }

    @FunctionalInterface
    private interface Mutation<T> {
        Change<T> apply(State current);
    }

    interface RecordClient {
        GetResult get(String key);

        PutResult put(String key, byte[] value, Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException;
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
}
