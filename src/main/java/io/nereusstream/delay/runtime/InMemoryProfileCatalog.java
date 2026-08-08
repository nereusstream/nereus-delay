package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CredentialBindingHeadV1;
import io.nereusstream.delay.protocol.CredentialBindingProtectionV1;
import io.nereusstream.delay.protocol.CredentialBindingV1;
import io.nereusstream.delay.protocol.DeprecateDestinationProfileRequestV1;
import io.nereusstream.delay.protocol.DestinationProfileSemanticV1;
import io.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.PublishDestinationProfileRequestV1;
import io.nereusstream.delay.protocol.RotateEquivalentSecretRequestV1;
import io.nereusstream.delay.protocol.ControlReasonV1;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Deterministic local catalog for exact immutable Profiles and bindings.
 *
 * <p>This is a recovery/test authority seam. It models the atomic shape of
 * generation-1 publication, checked Head rotation and deprecation intent, but
 * it does not claim to perform authenticated control routing, source-ordered
 * shard activation, retained-generation quota CAS or Oxia session fencing.</p>
 */
public final class InMemoryProfileCatalog implements ProfileCatalog {
    private final Map<ProfileRefV1, Entry> entries = new HashMap<>();

    /** Publishes a non-credential-bearing immutable Profile exactly once. */
    public synchronized void publish(final ProfileSemanticEnvelopeV1 profile) {
        Objects.requireNonNull(profile, "profile");
        if (requiresCredentialBinding(profile.profileKind())) {
            throw new IllegalArgumentException("DESTINATION and OBJECT_STORE Profiles require generation-1 binding");
        }
        putProfile(profile, null);
    }

    /** Publishes a credential-bearing Profile and its exact generation-1 binding. */
    public synchronized void publish(final ProfileSemanticEnvelopeV1 profile,
                                     final CredentialBindingV1 binding) {
        Objects.requireNonNull(profile, "profile");
        Objects.requireNonNull(binding, "binding");
        if (!requiresCredentialBinding(profile.profileKind())) {
            throw new IllegalArgumentException("only DESTINATION and OBJECT_STORE Profiles have bindings");
        }
        if (!profile.ref().equals(binding.profile())
                || binding.secretGeneration() != PublishDestinationProfileRequestV1.INITIAL_SECRET_GENERATION) {
            throw new IllegalArgumentException("Profile publication requires an exact generation-1 binding");
        }
        requireCredentialScope(profile, binding);
        putProfile(profile, binding);
    }

    public synchronized void publish(final PublishDestinationProfileRequestV1 request) {
        Objects.requireNonNull(request, "request");
        publish(request.profile(), request.credentialBinding());
    }

    /** Records the global deprecation intent; shard activation/close remains separate. */
    public synchronized void deprecate(final DeprecateDestinationProfileRequestV1 request) {
        Objects.requireNonNull(request, "request");
        final Entry entry = requireEntry(request.profile());
        if (entry.deprecationReason == null) {
            entry.deprecationReason = request.reason();
        } else if (!entry.deprecationReason.equals(request.reason())) {
            throw new IllegalStateException("Profile deprecation already has another reason");
        }
    }

    /**
     * Applies one checked equivalent-generation rotation and returns the new
     * Head. A retry after the new Head is already visible is idempotent when
     * the exact derived binding is present.
     */
    public synchronized CredentialBindingHeadV1 rotate(final RotateEquivalentSecretRequestV1 request) {
        Objects.requireNonNull(request, "request");
        final Entry entry = requireEntry(request.profile());
        if (entry.head == null) {
            throw new IllegalStateException("Profile has no credential binding Head");
        }
        final CredentialBindingV1 nextBinding = request.newBinding();
        requireCredentialScope(entry.profile, nextBinding);
        final CredentialBindingHeadV1 current = entry.head;
        final long nextRevision = checkedIncrement(request.expectedBindingHeadRevision(), "binding Head revision");
        final CredentialBindingV1 expectedBinding = entry.bindings.get(request.expectedSecretGeneration());
        if (current.secretGeneration() == request.newSecretGeneration()
                && current.headRevision() == nextRevision
                && expectedBinding != null
                && Bytes.constantTimeEquals(expectedBinding.bindingDigest(), request.expectedBindingDigest())
                && Bytes.constantTimeEquals(current.bindingDigest(), nextBinding.bindingDigest())) {
            final CredentialBindingV1 existing = entry.bindings.get(request.newSecretGeneration());
            if (nextBinding.equals(existing)) {
                return current;
            }
            throw new IllegalStateException("rotation Head agrees but immutable binding bytes differ");
        }
        if (expectedBinding == null
                || !Bytes.constantTimeEquals(expectedBinding.bindingDigest(), current.bindingDigest())) {
            throw new IllegalStateException("credential binding Head has no matching immutable generation");
        }
        if (current.secretGeneration() != request.expectedSecretGeneration()
                || current.headRevision() != request.expectedBindingHeadRevision()
                || !Bytes.constantTimeEquals(current.bindingDigest(), request.expectedBindingDigest())) {
            throw new IllegalStateException("credential binding Head CAS precondition failed");
        }
        final CredentialBindingV1 existing = entry.bindings.get(request.newSecretGeneration());
        if (existing != null && !existing.equals(nextBinding)) {
            throw new IllegalStateException("credential generation already has different immutable bytes");
        }
        final CredentialBindingHeadV1 nextHead = CredentialBindingHeadV1.forBinding(nextBinding, nextRevision);
        if (existing == null) {
            entry.bindings.put(request.newSecretGeneration(), nextBinding);
            entry.protections.put(request.newSecretGeneration(), CredentialBindingProtectionV1.forBinding(
                    nextBinding, 0, 0, 0, 0, 1));
        } else if (!entry.protections.containsKey(request.newSecretGeneration())) {
            throw new IllegalStateException("credential generation is missing its protection record");
        }
        entry.head = nextHead;
        return nextHead;
    }

    @Override
    public synchronized ProfileSemanticEnvelopeV1 resolve(final ProfileRefV1 reference) {
        Objects.requireNonNull(reference, "reference");
        final Entry entry = entries.get(reference);
        return entry != null && entry.profile.ref().equals(reference) ? entry.profile : null;
    }

    @Override
    public synchronized CredentialBindingV1 resolveBinding(final ProfileRefV1 profile,
                                                           final long secretGeneration) {
        Objects.requireNonNull(profile, "profile");
        if (secretGeneration == 0) {
            throw new IllegalArgumentException("secretGeneration must be non-zero");
        }
        final Entry entry = entries.get(profile);
        return entry == null ? null : entry.bindings.get(secretGeneration);
    }

    @Override
    public synchronized CredentialBindingHeadV1 resolveHead(final ProfileRefV1 profile) {
        Objects.requireNonNull(profile, "profile");
        final Entry entry = entries.get(profile);
        return entry == null ? null : entry.head;
    }

    @Override
    public synchronized CredentialBindingProtectionV1 resolveProtection(final ProfileRefV1 profile,
                                                                         final long secretGeneration) {
        Objects.requireNonNull(profile, "profile");
        if (secretGeneration == 0) {
            throw new IllegalArgumentException("secretGeneration must be non-zero");
        }
        final Entry entry = entries.get(profile);
        return entry == null ? null : entry.protections.get(secretGeneration);
    }

    public synchronized boolean isDeprecated(final ProfileRefV1 profile) {
        Objects.requireNonNull(profile, "profile");
        final Entry entry = entries.get(profile);
        return entry != null && entry.deprecationReason != null;
    }

    public synchronized ControlReasonV1 deprecationReason(final ProfileRefV1 profile) {
        Objects.requireNonNull(profile, "profile");
        final Entry entry = entries.get(profile);
        return entry == null ? null : entry.deprecationReason;
    }

    public synchronized int size() {
        return entries.size();
    }

    public synchronized int bindingCount() {
        return entries.values().stream().mapToInt(entry -> entry.bindings.size()).sum();
    }

    private void putProfile(final ProfileSemanticEnvelopeV1 profile, final CredentialBindingV1 binding) {
        final ProfileRefV1 reference = profile.ref();
        final Entry existing = entries.get(reference);
        if (existing != null) {
            if (!existing.profile.equals(profile)
                    || (binding != null && !binding.equals(existing.bindings.get(binding.secretGeneration())))) {
                throw new IllegalStateException("Profile reference changed immutable bytes");
            }
            return;
        }
        for (ProfileRefV1 existingReference : entries.keySet()) {
            if (sameVersion(existingReference, reference)) {
                throw new IllegalStateException("Profile version already has another semantic hash");
            }
        }
        final Entry entry = new Entry(profile);
        if (binding != null) {
            entry.bindings.put(binding.secretGeneration(), binding);
            entry.protections.put(binding.secretGeneration(), CredentialBindingProtectionV1.forBinding(
                    binding, 0, 0, 0, 0, 1));
            entry.head = CredentialBindingHeadV1.forBinding(binding, 1);
        }
        entries.put(reference, entry);
    }

    private Entry requireEntry(final ProfileRefV1 reference) {
        final Entry entry = entries.get(reference);
        if (entry == null || !entry.profile.ref().equals(reference)) {
            throw new IllegalStateException("Profile is not published with exact semantic bytes");
        }
        return entry;
    }

    private static boolean requiresCredentialBinding(final ProfileKindV1 kind) {
        return kind == ProfileKindV1.DESTINATION || kind == ProfileKindV1.OBJECT_STORE;
    }

    private static void requireCredentialScope(final ProfileSemanticEnvelopeV1 profile,
                                               final CredentialBindingV1 binding) {
        final byte[] expectedScope;
        if (profile.body() instanceof DestinationProfileSemanticV1 destination) {
            expectedScope = destination.credentialAuthorizationScopeDigest();
        } else if (profile.body() instanceof ObjectStoreProfileSemanticV1 objectStore) {
            expectedScope = objectStore.credentialAuthorizationScopeDigest();
        } else {
            throw new IllegalArgumentException("credential binding Profile body is not bindable");
        }
        binding.equivalenceAttestation().requireAuthorizationScopeDigest(expectedScope);
    }

    private static boolean sameVersion(final ProfileRefV1 left, final ProfileRefV1 right) {
        return left.profileKind() == right.profileKind() && left.version() == right.version()
                && Arrays.equals(left.profileId(), right.profileId());
    }

    private static long checkedIncrement(final long value, final String name) {
        if (value == -1L) {
            throw new IllegalArgumentException(name + " exhausted");
        }
        return value + 1;
    }

    private static final class Entry {
        private final ProfileSemanticEnvelopeV1 profile;
        private final Map<Long, CredentialBindingV1> bindings = new HashMap<>();
        private final Map<Long, CredentialBindingProtectionV1> protections = new HashMap<>();
        private CredentialBindingHeadV1 head;
        private ControlReasonV1 deprecationReason;

        private Entry(final ProfileSemanticEnvelopeV1 profile) {
            this.profile = Objects.requireNonNull(profile, "profile");
        }
    }
}
