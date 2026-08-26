package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.RecoveryPin;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * Exact Oxia record semantics for one session-bound Recovery Pin.
 *
 * <p>The owning authority supplies validation of the current catalog or
 * publication projection. This class only owns the separate ephemeral pin
 * record, its session identity checks, and response-loss/CAS convergence.</p>
 */
final class OxiaSessionBoundRecoveryPinStore {
    private static final int MAX_CAS_ATTEMPTS = 32;
    private static final int MAX_PIN_BYTES = 4 * 1024;
    private static final byte[] SESSION_IDENTITY_DOMAIN = Bytes.utf8("nereus-delay-oxia-session-identity\0");

    private final RecordClient client;
    private final String pinRecordKey;
    private final byte[] sessionIdentity;

    OxiaSessionBoundRecoveryPinStore(final RecordClient client, final String pinRecordKey) {
        this.client = Objects.requireNonNull(client, "client");
        this.pinRecordKey = canonicalKey(pinRecordKey);
        final byte[] configuredSession = client.sessionIdentity();
        if (configuredSession != null) {
            Bytes.requireLength(configuredSession, 32, "sessionIdentity");
            this.sessionIdentity = Bytes.copy(configuredSession);
        } else {
            this.sessionIdentity = null;
        }
    }

    RecoveryPin create(
            final RecoveryPin pin, final Runnable validateRequest, final LongSupplier currentCatalogGeneration) {
        final RecoveryPin requested = Objects.requireNonNull(pin, "pin");
        Objects.requireNonNull(validateRequest, "validateRequest");
        Objects.requireNonNull(currentCatalogGeneration, "currentCatalogGeneration");
        requireCallerSession(requested);
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            validateRequest.run();
            final PinRecord current = readPinRecord();
            if (current != null) {
                if (!current.pin().equals(requested)) {
                    throw new IllegalStateException("another RecoveryPin is already active");
                }
                requireCatalogGeneration(requested, currentCatalogGeneration);
                return current.pin();
            }
            try {
                final PutResult stored = client.put(
                        pinRecordKey,
                        requested.canonicalBytes(),
                        Set.of(PutOption.IfRecordDoesNotExist, PutOption.AsEphemeralRecord));
                validatePinPutResult(stored, requested);
                final PinRecord observed = requirePinRecord(readPinRecord(), requested);
                try {
                    requireCatalogGeneration(requested, currentCatalogGeneration);
                } catch (RuntimeException | Error generationFailure) {
                    deleteExactPin(observed, generationFailure);
                    throw generationFailure;
                }
                return observed.pin();
            } catch (KeyAlreadyExistsException | UnexpectedVersionIdException conflict) {
                // Another session won the singleton pin CAS. Re-run the
                // request validator and reread the exact pin on the next turn.
            } catch (RuntimeException responseFailure) {
                final PinRecord observed = readPinRecord();
                if (observed != null && observed.pin().equals(requested)) {
                    requireCatalogGeneration(requested, currentCatalogGeneration);
                    return observed.pin();
                }
                throw responseFailure;
            }
        }
        throw new IllegalStateException("RecoveryPin ephemeral CAS did not converge");
    }

    void release(final RecoveryPin pin) {
        final RecoveryPin requested = Objects.requireNonNull(pin, "pin");
        requireCallerSession(requested);
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            final PinRecord current = readPinRecord();
            if (current == null) {
                throw new IllegalStateException("no RecoveryPin is active");
            }
            if (!current.pin().equals(requested)) {
                throw new IllegalStateException("RecoveryPin identity/value mismatch");
            }
            try {
                if (!client.delete(
                        pinRecordKey,
                        Set.of(DeleteOption.IfVersionIdEquals(current.version().versionId())))) {
                    final PinRecord after = readPinRecord();
                    if (after == null) {
                        return;
                    }
                    throw new IllegalStateException("RecoveryPin delete returned false while the pin remained active");
                }
                if (readPinRecord() == null) {
                    return;
                }
                throw new IllegalStateException("RecoveryPin delete was not visible on exact reread");
            } catch (UnexpectedVersionIdException conflict) {
                // A concurrent exact release may have won. Re-read and apply
                // the same identity check before the next attempt.
            } catch (RuntimeException responseFailure) {
                if (readPinRecord() == null) {
                    return;
                }
                throw responseFailure;
            }
        }
        throw new IllegalStateException("RecoveryPin release CAS did not converge");
    }

    Optional<RecoveryPin> active() {
        final PinRecord active = readPinRecord();
        return active == null ? Optional.empty() : Optional.of(active.pin());
    }

    private PinRecord readPinRecord() {
        final GetResult result = client.get(pinRecordKey);
        if (result == null) {
            return null;
        }
        if (!pinRecordKey.equals(result.key()) || result.version() == null) {
            throw new IllegalStateException("Oxia RecoveryPin response has an invalid key or version");
        }
        final byte[] encoded = result.value();
        if (encoded == null || encoded.length == 0 || encoded.length > MAX_PIN_BYTES) {
            throw new IllegalStateException("Oxia RecoveryPin has an invalid size");
        }
        final RecoveryPin pin = RecoveryPin.decode(encoded);
        requireSession(result.version(), pin.oxiaSessionIdentityDigest());
        return new PinRecord(pin, result.version());
    }

    private PinRecord requirePinRecord(final PinRecord record, final RecoveryPin expected) {
        if (record == null || !record.pin().equals(expected)) {
            throw new IllegalStateException("Oxia RecoveryPin reread does not match the exact request");
        }
        return record;
    }

    private void validatePinPutResult(final PutResult result, final RecoveryPin expected) {
        if (result == null || !pinRecordKey.equals(result.key()) || result.version() == null) {
            throw new IllegalStateException("Oxia RecoveryPin put returned an invalid record identity");
        }
        requireSession(result.version(), expected.oxiaSessionIdentityDigest());
    }

    private void requireCallerSession(final RecoveryPin pin) {
        if (sessionIdentity == null) {
            throw new IllegalStateException(
                    "RecoveryPin create/release requires an identity-bearing connected Oxia session");
        }
        if (!Bytes.constantTimeEquals(sessionIdentity, pin.oxiaSessionIdentityDigest())) {
            throw new IllegalStateException("RecoveryPin is bound to another Oxia session");
        }
    }

    private static void requireCatalogGeneration(final RecoveryPin pin, final LongSupplier currentCatalogGeneration) {
        if (currentCatalogGeneration.getAsLong() != pin.observedCatalogGeneration()) {
            throw new IllegalStateException("RecoveryPin catalog generation changed during creation");
        }
    }

    private void deleteExactPin(final PinRecord record, final Throwable primary) {
        try {
            if (!client.delete(
                    pinRecordKey,
                    Set.of(DeleteOption.IfVersionIdEquals(record.version().versionId())))) {
                final PinRecord after = readPinRecord();
                if (after != null) {
                    primary.addSuppressed(new IllegalStateException(
                            "RecoveryPin cleanup returned false while the pin remained active"));
                }
            }
        } catch (RuntimeException | Error cleanupFailure) {
            primary.addSuppressed(cleanupFailure);
        } catch (UnexpectedVersionIdException cleanupRace) {
            primary.addSuppressed(cleanupRace);
        }
    }

    private static void requireSession(final Version version, final byte[] expectedIdentity) {
        if (version.sessionId().isEmpty() || version.clientIdentifier().isEmpty()) {
            throw new IllegalStateException("Oxia RecoveryPin is not bound to an ephemeral session");
        }
        final long sessionId = version.sessionId().orElseThrow();
        if (sessionId < 0) {
            throw new IllegalStateException("Oxia RecoveryPin session id is negative");
        }
        final String clientIdentifier =
                canonicalSessionClientIdentifier(version.clientIdentifier().orElseThrow());
        final byte[] actualIdentity =
                Bytes.sha256(SESSION_IDENTITY_DOMAIN, Bytes.u64be(sessionId), Bytes.lp32(Bytes.utf8(clientIdentifier)));
        if (!Bytes.constantTimeEquals(expectedIdentity, actualIdentity)) {
            throw new IllegalStateException("Oxia RecoveryPin session identity does not match its bytes");
        }
    }

    private static String canonicalSessionClientIdentifier(final String value) {
        Objects.requireNonNull(value, "clientIdentifier");
        final byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (value.isBlank()
                || value.indexOf('\0') >= 0
                || !value.equals(new String(encoded, StandardCharsets.UTF_8))
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalStateException("Oxia RecoveryPin client identity is not canonical");
        }
        return value;
    }

    private static String canonicalKey(final String value) {
        Objects.requireNonNull(value, "pinRecordKey");
        final byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (value.isBlank()
                || value.endsWith("/")
                || value.indexOf('\0') >= 0
                || !value.equals(new String(encoded, StandardCharsets.UTF_8))
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException("pinRecordKey must be a nonblank NFC UTF-8 path");
        }
        return value;
    }

    interface RecordClient {
        GetResult get(String key);

        PutResult put(String key, byte[] value, Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException;

        boolean delete(String key, Set<DeleteOption> options) throws UnexpectedVersionIdException;

        default byte[] sessionIdentity() {
            return null;
        }
    }

    private record PinRecord(RecoveryPin pin, Version version) {}
}
