package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Immutable historical issuer-key and policy-activation records stored in Oxia. */
public final class OxiaSyncHandoffPolicyTrustStore implements HandoffPolicyTrustStore {
    private static final int RECORD_GENERATION = 1;
    private static final byte[] ISSUER_DOMAIN = Bytes.utf8("nereus-delay-handoff-policy-issuer-record\0");
    private static final byte[] ACTIVATION_DOMAIN = Bytes.utf8("nereus-delay-handoff-policy-activation-record\0");
    private static final String TRUST_SEGMENT = "/handoff-policy/trust";

    private final RecordClient client;
    private final String keyPrefix;

    public OxiaSyncHandoffPolicyTrustStore(final SyncOxiaClient client, final String keyPrefix) {
        this(new SyncRecordClient(client), keyPrefix);
    }

    OxiaSyncHandoffPolicyTrustStore(final RecordClient client, final String keyPrefix) {
        this.client = Objects.requireNonNull(client, "client");
        this.keyPrefix = canonicalKeyPrefix(keyPrefix);
    }

    /** Installs one immutable issuer-key generation at an exact source position. */
    public void installIssuerKey(final int issuerKeyGeneration, final PublicKey key, final SourcePosition activeFrom) {
        if (issuerKeyGeneration <= 0) {
            throw new IllegalArgumentException("issuerKeyGeneration must be positive");
        }
        final byte[] encoded = encodeIssuer(issuerKeyGeneration, Objects.requireNonNull(key, "key"), activeFrom);
        createImmutable(issuerKey(issuerKeyGeneration), encoded);
    }

    /** Installs one immutable activation marker for a policy generation. */
    public void activatePolicy(
            final byte[] policyScopeDigest, final long policyGeneration, final SourcePosition activeFrom) {
        final byte[] scope = scope(policyScopeDigest);
        if (policyGeneration == 0) {
            throw new IllegalArgumentException("policyGeneration must be non-zero");
        }
        final byte[] encoded = encodeActivation(scope, policyGeneration, activeFrom);
        createImmutable(activationKey(scope, policyGeneration), encoded);
    }

    @Override
    public Optional<PublicKey> issuerKey(final int issuerKeyGeneration, final SourcePosition sourcePosition) {
        if (issuerKeyGeneration <= 0) {
            return Optional.empty();
        }
        final GetResult result = client.get(issuerKey(issuerKeyGeneration));
        if (result == null) {
            return Optional.empty();
        }
        final IssuerRecord record = decodeIssuer(result, issuerKeyGeneration);
        if (!atOrBefore(record.activeFrom(), Objects.requireNonNull(sourcePosition, "sourcePosition"))) {
            return Optional.empty();
        }
        return Optional.of(record.key());
    }

    @Override
    public Optional<SourcePosition> activationPosition(final byte[] policyScopeDigest, final long policyGeneration) {
        final byte[] scope = scope(policyScopeDigest);
        if (policyGeneration == 0) {
            return Optional.empty();
        }
        final GetResult result = client.get(activationKey(scope, policyGeneration));
        return result == null ? Optional.empty() : Optional.of(decodeActivation(result, scope, policyGeneration));
    }

    private void createImmutable(final String key, final byte[] bytes) {
        final GetResult current = client.get(key);
        if (current != null) {
            requireExact(current, key, bytes);
            return;
        }
        try {
            final PutResult result = client.put(key, bytes, Set.of(PutOption.IfRecordDoesNotExist));
            if (result == null || !key.equals(result.key()) || result.version() == null) {
                throw new IllegalStateException("Oxia handoff trust put returned no exact version");
            }
        } catch (KeyAlreadyExistsException conflict) {
            requireExact(client.get(key), key, bytes);
            return;
        } catch (UnexpectedVersionIdException impossible) {
            throw new IllegalStateException("Oxia create-only trust record returned a version conflict", impossible);
        } catch (RuntimeException responseFailure) {
            final GetResult observed = client.get(key);
            if (observed == null || !Arrays.equals(bytes, observed.value())) {
                throw responseFailure;
            }
        }
        requireExact(client.get(key), key, bytes);
    }

    private IssuerRecord decodeIssuer(final GetResult result, final int expectedGeneration) {
        final String key = issuerKey(expectedGeneration);
        final byte[] value = requireRecord(result, key);
        try {
            final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(value);
            requireUnsigned(field(reader, 1, 0), RECORD_GENERATION, "issuer record generation");
            requireUnsigned(field(reader, 2, 0), expectedGeneration, "issuer key generation");
            final PublicKey publicKey = decodePublicKey(bytes(field(reader, 3, 2), "issuer public key"));
            final SourcePosition activeFrom = SourcePositionCodec.decode(bytes(field(reader, 4, 2), "activeFrom"));
            final byte[] digest = fixed(bytes(field(reader, 5, 2), "issuer digest"), "issuer digest");
            if (reader.hasRemaining()
                    || !Bytes.constantTimeEquals(
                            digest,
                            Bytes.sha256(
                                    ISSUER_DOMAIN,
                                    issuerFields(
                                            expectedGeneration, publicKey.getEncoded(), activeFrom.canonicalBytes())))
                    || !Arrays.equals(value, encodeIssuer(expectedGeneration, publicKey, activeFrom))) {
                throw new IllegalStateException("Oxia handoff issuer record is non-canonical or corrupt");
            }
            return new IssuerRecord(publicKey, activeFrom);
        } catch (RuntimeException failure) {
            throw new IllegalStateException("cannot decode Oxia handoff issuer record", failure);
        }
    }

    private SourcePosition decodeActivation(
            final GetResult result, final byte[] expectedScope, final long expectedGeneration) {
        final String key = activationKey(expectedScope, expectedGeneration);
        final byte[] value = requireRecord(result, key);
        try {
            final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(value);
            requireUnsigned(field(reader, 1, 0), RECORD_GENERATION, "activation record generation");
            final byte[] scope = fixed(bytes(field(reader, 2, 2), "policy scope"), "policy scope");
            final long generation = field(reader, 3, 0).unsignedValue();
            final SourcePosition activeFrom = SourcePositionCodec.decode(bytes(field(reader, 4, 2), "activeFrom"));
            final byte[] digest = fixed(bytes(field(reader, 5, 2), "activation digest"), "activation digest");
            final byte[] fields = activationFields(scope, generation, activeFrom.canonicalBytes());
            if (reader.hasRemaining()
                    || !Arrays.equals(scope, expectedScope)
                    || generation != expectedGeneration
                    || !Bytes.constantTimeEquals(digest, Bytes.sha256(ACTIVATION_DOMAIN, fields))
                    || !Arrays.equals(value, encodeActivation(scope, generation, activeFrom))) {
                throw new IllegalStateException("Oxia handoff activation record is non-canonical or corrupt");
            }
            return activeFrom;
        } catch (RuntimeException failure) {
            throw new IllegalStateException("cannot decode Oxia handoff activation record", failure);
        }
    }

    private static byte[] encodeIssuer(final int generation, final PublicKey key, final SourcePosition activeFrom) {
        final byte[] fields = issuerFields(
                generation,
                Objects.requireNonNull(key, "key").getEncoded(),
                Objects.requireNonNull(activeFrom, "activeFrom").canonicalBytes());
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fields);
            CanonicalProtobuf.bytes(output, 5, Bytes.sha256(ISSUER_DOMAIN, fields));
        });
    }

    private static byte[] issuerFields(final int generation, final byte[] key, final byte[] activeFrom) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECORD_GENERATION);
            CanonicalProtobuf.uint32(output, 2, generation);
            CanonicalProtobuf.bytes(output, 3, key);
            CanonicalProtobuf.bytes(output, 4, activeFrom);
        });
    }

    private static byte[] encodeActivation(final byte[] scope, final long generation, final SourcePosition activeFrom) {
        final byte[] fields = activationFields(
                scope,
                generation,
                Objects.requireNonNull(activeFrom, "activeFrom").canonicalBytes());
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fields);
            CanonicalProtobuf.bytes(output, 5, Bytes.sha256(ACTIVATION_DOMAIN, fields));
        });
    }

    private static byte[] activationFields(final byte[] scope, final long generation, final byte[] activeFrom) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECORD_GENERATION);
            CanonicalProtobuf.bytes(output, 2, scope);
            CanonicalProtobuf.uint64Bits(output, 3, generation);
            CanonicalProtobuf.bytes(output, 4, activeFrom);
        });
    }

    private static CanonicalProtobuf.Reader.Field field(
            final CanonicalProtobuf.Reader reader, final int number, final int wireType) {
        final CanonicalProtobuf.Reader.Field field = reader.next();
        if (field.number() != number || field.wireType() != wireType) {
            throw new IllegalArgumentException("unexpected canonical trust record field");
        }
        return field;
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final String name) {
        final byte[] value = field.rawValue();
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must be non-empty");
        }
        return value;
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, 32, name);
        return value;
    }

    private static void requireUnsigned(
            final CanonicalProtobuf.Reader.Field field, final long expected, final String name) {
        if (field.unsignedValue() != expected) {
            throw new IllegalArgumentException(name + " mismatch");
        }
    }

    private static PublicKey decodePublicKey(final byte[] encoded) {
        try {
            return KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(encoded));
        } catch (GeneralSecurityException failure) {
            throw new IllegalArgumentException("invalid handoff policy issuer key", failure);
        }
    }

    private static boolean atOrBefore(final SourcePosition left, final SourcePosition right) {
        try {
            return left.sameSourceIdentity(right) && left.compareTo(right) <= 0;
        } catch (IllegalArgumentException incompatible) {
            return false;
        }
    }

    private byte[] requireRecord(final GetResult result, final String key) {
        if (result == null || !key.equals(result.key()) || result.value() == null || result.version() == null) {
            throw new IllegalStateException("Oxia handoff trust response has an invalid record identity");
        }
        return result.value();
    }

    private static void requireExact(final GetResult result, final String key, final byte[] expected) {
        if (result == null
                || !key.equals(result.key())
                || result.value() == null
                || result.version() == null
                || !Arrays.equals(expected, result.value())) {
            throw new IllegalStateException("immutable Oxia handoff trust record conflicts");
        }
    }

    private String issuerKey(final int generation) {
        return keyPrefix + TRUST_SEGMENT + "/issuer/" + Integer.toUnsignedString(generation);
    }

    private String activationKey(final byte[] scope, final long generation) {
        return keyPrefix + TRUST_SEGMENT + "/activation/" + Bytes.hex(scope) + "/" + Long.toUnsignedString(generation);
    }

    private static byte[] scope(final byte[] value) {
        Bytes.requireLength(value, 32, "policyScopeDigest");
        return Bytes.copy(value);
    }

    private static String canonicalKeyPrefix(final String value) {
        final String prefix = Objects.requireNonNull(value, "keyPrefix").trim();
        if (prefix.isEmpty() || prefix.endsWith("/") || prefix.contains("//")) {
            throw new IllegalArgumentException("keyPrefix is not canonical");
        }
        return prefix.startsWith("/") ? prefix : "/" + prefix;
    }

    interface RecordClient {
        GetResult get(String key);

        PutResult put(String key, byte[] value, Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException;
    }

    private record SyncRecordClient(SyncOxiaClient delegate) implements RecordClient {
        private SyncRecordClient {
            Objects.requireNonNull(delegate, "client");
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

    private record IssuerRecord(PublicKey key, SourcePosition activeFrom) {}
}
