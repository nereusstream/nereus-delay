package com.nereusstream.delay.gateway;

import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.semantic.TrustedClock;
import com.nereusstream.delay.transport.Digest32;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Durable tenant-scoped Gateway admission backed by one canonical Oxia CAS record.
 *
 * <p>The record contains expiring leases rather than a reconstructed counter. A
 * reserve or release is accepted only after its exact successor is committed or
 * reread after a lost response. The in-memory implementation remains useful for
 * local conformance tests; this class is the authority for a distributed Gateway
 * composition.</p>
 */
public final class OxiaGatewayAdmissionController implements GatewayAdmissionController, AutoCloseable {
    private static final int MAX_RECORD_BYTES = 512 * 1024;
    private static final String ADMISSION_SUFFIX = "/admission/";

    private final OxiaGatewayRecordClient client;
    private final TrustedClock trustedClock;
    private final Limits limits;
    private final String recordPrefix;

    /** Creates a controller over an already configured Oxia client. */
    public OxiaGatewayAdmissionController(
            final SyncOxiaClient client, final String keyPrefix, final TrustedClock trustedClock, final Limits limits) {
        this(new SyncRecordClient(client), keyPrefix, trustedClock, limits);
    }

    /** Creates a controller fenced to the exact ephemeral session of a handle. */
    public OxiaGatewayAdmissionController(
            final OxiaSyncOwnerLeaseBackend.ClientHandle handle,
            final String keyPrefix,
            final TrustedClock trustedClock,
            final Limits limits) {
        this(new SessionBoundOxiaGatewayRecordClient(handle), keyPrefix, trustedClock, limits);
    }

    /** Package-private constructor used by deterministic CAS tests. */
    OxiaGatewayAdmissionController(
            final OxiaGatewayRecordClient client,
            final String keyPrefix,
            final TrustedClock trustedClock,
            final Limits limits) {
        this.client = Objects.requireNonNull(client, "client");
        this.trustedClock = Objects.requireNonNull(trustedClock, "trustedClock");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.recordPrefix = canonicalKeyPrefix(keyPrefix) + ADMISSION_SUFFIX;
    }

    @Override
    public synchronized Decision reserve(final GatewayAdmissionRequest request) {
        Objects.requireNonNull(request, "request");
        final Digest32 tenantScopeHash = new Digest32(request.tenant().authenticatedTenantScopeHash());
        final String key = recordKey(tenantScopeHash);
        final long now = now();
        final long expiresAt = checkedExpiryAdd(now, limits.leaseMaxAgeMs());
        RuntimeException lastFailure = null;
        for (int attempt = 0; attempt <= limits.maxCasRetries(); attempt++) {
            final Entry current = read(key, tenantScopeHash);
            final GatewayAdmissionRecord base =
                    current == null ? GatewayAdmissionRecord.empty(tenantScopeHash) : current.record();
            final List<GatewayAdmissionRecord.Lease> liveLeases = liveLeases(base.leases(), now);
            final StableCode rejection = rejectionFor(liveLeases, request);
            if (rejection != null) {
                return Decision.rejected(rejection);
            }
            final GatewayAdmissionRecord.Lease candidate = new GatewayAdmissionRecord.Lease(
                    PhysicalEnqueueAttemptId.random().bytes(),
                    request.operation(),
                    request.estimatedRequestBytes(),
                    expiresAt);
            final List<GatewayAdmissionRecord.Lease> nextLeases = new ArrayList<>(liveLeases);
            nextLeases.add(candidate);
            final GatewayAdmissionRecord next = base.withLeases(nextLeases);
            try {
                put(
                        key,
                        next,
                        current == null
                                ? Set.of(PutOption.IfRecordDoesNotExist)
                                : Set.of(PutOption.IfVersionIdEquals(current.versionId())));
                return Decision.accepted(new DurableLease(this, tenantScopeHash, candidate));
            } catch (CasRaceException race) {
                lastFailure = race;
            } catch (RuntimeException responseFailure) {
                final Entry observed = read(key, tenantScopeHash);
                if (observed != null && containsExact(observed.record(), candidate)) {
                    return Decision.accepted(new DurableLease(this, tenantScopeHash, candidate));
                }
                lastFailure = responseFailure;
            }
        }
        throw new IllegalStateException("Gateway admission CAS did not converge", lastFailure);
    }

    private synchronized void release(final Digest32 tenantScopeHash, final GatewayAdmissionRecord.Lease expected) {
        final String key = recordKey(tenantScopeHash);
        RuntimeException lastFailure = null;
        for (int attempt = 0; attempt <= limits.maxCasRetries(); attempt++) {
            final Entry current = read(key, tenantScopeHash);
            if (current == null) {
                return;
            }
            final long now = now();
            final List<GatewayAdmissionRecord.Lease> nextLeases = new ArrayList<>();
            boolean found = false;
            for (GatewayAdmissionRecord.Lease lease : current.record().leases()) {
                if (sameLeaseId(lease, expected)) {
                    if (!sameLease(lease, expected)) {
                        throw new IllegalStateException("Gateway admission lease identity changed");
                    }
                    found = true;
                } else if (lease.expiresAtEpochMs() > now) {
                    nextLeases.add(lease);
                }
            }
            if (!found) {
                return;
            }
            final GatewayAdmissionRecord next = current.record().withLeases(nextLeases);
            try {
                put(key, next, Set.of(PutOption.IfVersionIdEquals(current.versionId())));
                return;
            } catch (CasRaceException race) {
                lastFailure = race;
            } catch (RuntimeException responseFailure) {
                final Entry observed = read(key, tenantScopeHash);
                if (observed == null || !containsLeaseId(observed.record(), expected)) {
                    return;
                }
                if (!containsExact(observed.record(), expected)) {
                    throw new IllegalStateException("Gateway admission lease identity changed", responseFailure);
                }
                lastFailure = responseFailure;
            }
        }
        throw new IllegalStateException("Gateway admission release CAS did not converge", lastFailure);
    }

    @Override
    public void close() {
        client.close();
    }

    private Entry read(final String key, final Digest32 tenantScopeHash) {
        final GetResult result = client.get(key);
        if (result == null) {
            return null;
        }
        if (!key.equals(result.key())
                || result.value() == null
                || result.version() == null
                || result.value().length > MAX_RECORD_BYTES) {
            throw new IllegalStateException("Oxia Gateway admission response is not exact");
        }
        final GatewayAdmissionRecord record;
        try {
            record = GatewayAdmissionRecord.decode(result.value());
        } catch (RuntimeException malformed) {
            throw new IllegalStateException("Oxia Gateway admission record is malformed", malformed);
        }
        if (!tenantScopeHash.equals(record.tenantScopeHash())) {
            throw new IllegalStateException("Oxia Gateway admission record tenant identity mismatch");
        }
        return new Entry(record, result.version().versionId());
    }

    private void put(final String key, final GatewayAdmissionRecord record, final Set<PutOption> options) {
        final byte[] value = record.canonicalBytes();
        if (value.length > MAX_RECORD_BYTES) {
            throw new IllegalStateException("Gateway admission record exceeds bounded size");
        }
        try {
            final PutResult result = client.put(key, value, options);
            if (result == null || !key.equals(result.key()) || result.version() == null) {
                throw new IllegalStateException("Oxia Gateway admission put returned no exact version");
            }
        } catch (KeyAlreadyExistsException | UnexpectedVersionIdException race) {
            throw new CasRaceException(race);
        }
    }

    private StableCode rejectionFor(
            final List<GatewayAdmissionRecord.Lease> leases, final GatewayAdmissionRequest request) {
        final Usage usage = usage(leases);
        if (request.operation() == GatewayIngressOperation.SCHEDULE) {
            if (request.estimatedRequestBytes() > limits.maxScheduleBytes()
                    || usage.scheduleBytes() > limits.maxScheduleBytes() - request.estimatedRequestBytes()) {
                return StableCode.HARD_QUOTA_EXCEEDED;
            }
            if (usage.scheduleInFlight() >= limits.maxScheduleInFlight()) {
                return StableCode.ADMISSION_CAPACITY_GATED;
            }
        } else if (request.operation() == GatewayIngressOperation.RETRY_UNCERTAIN) {
            if (usage.retryInFlight() >= limits.maxRetryInFlight()) {
                return StableCode.ADMISSION_CAPACITY_GATED;
            }
        } else if (usage.controlInFlight() >= limits.maxControlInFlight()) {
            return StableCode.ADMISSION_CAPACITY_GATED;
        }
        return null;
    }

    private static Usage usage(final List<GatewayAdmissionRecord.Lease> leases) {
        int scheduleInFlight = 0;
        long scheduleBytes = 0;
        int retryInFlight = 0;
        int controlInFlight = 0;
        for (GatewayAdmissionRecord.Lease lease : leases) {
            switch (lease.operation()) {
                case SCHEDULE -> {
                    scheduleInFlight++;
                    scheduleBytes = checkedAdd(scheduleBytes, lease.estimatedRequestBytes());
                }
                case RETRY_UNCERTAIN -> retryInFlight++;
                case CONTROL -> controlInFlight++;
            }
        }
        return new Usage(scheduleInFlight, scheduleBytes, retryInFlight, controlInFlight);
    }

    private static List<GatewayAdmissionRecord.Lease> liveLeases(
            final List<GatewayAdmissionRecord.Lease> leases, final long now) {
        final List<GatewayAdmissionRecord.Lease> live = new ArrayList<>(leases.size());
        for (GatewayAdmissionRecord.Lease lease : leases) {
            if (lease.expiresAtEpochMs() > now) {
                live.add(lease);
            }
        }
        return live;
    }

    private static boolean containsExact(
            final GatewayAdmissionRecord record, final GatewayAdmissionRecord.Lease expected) {
        return record.leases().stream().anyMatch(lease -> sameLease(lease, expected));
    }

    private static boolean containsLeaseId(
            final GatewayAdmissionRecord record, final GatewayAdmissionRecord.Lease expected) {
        return record.leases().stream().anyMatch(lease -> sameLeaseId(lease, expected));
    }

    private static boolean sameLeaseId(
            final GatewayAdmissionRecord.Lease left, final GatewayAdmissionRecord.Lease right) {
        return Bytes.constantTimeEquals(left.leaseId(), right.leaseId());
    }

    private static boolean sameLease(
            final GatewayAdmissionRecord.Lease left, final GatewayAdmissionRecord.Lease right) {
        return sameLeaseId(left, right)
                && left.operation() == right.operation()
                && left.estimatedRequestBytes() == right.estimatedRequestBytes()
                && left.expiresAtEpochMs() == right.expiresAtEpochMs();
    }

    private long now() {
        final long now = trustedClock.nowEpochMs();
        if (now < 0) {
            throw new IllegalStateException("trusted Gateway admission clock returned a negative epoch");
        }
        return now;
    }

    private String recordKey(final Digest32 tenantScopeHash) {
        return recordPrefix + Bytes.hex(tenantScopeHash.bytes());
    }

    private static long checkedAdd(final long left, final long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            throw new IllegalStateException("Gateway admission byte accounting overflow", overflow);
        }
    }

    private static long checkedExpiryAdd(final long left, final long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("Gateway admission lease time bound overflows", overflow);
        }
    }

    private static String canonicalKeyPrefix(final String value) {
        Objects.requireNonNull(value, "keyPrefix");
        final byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (value.isBlank()
                || value.endsWith("/")
                || value.indexOf('\0') >= 0
                || !value.equals(new String(encoded, StandardCharsets.UTF_8))
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException("keyPrefix must be nonblank NFC UTF-8 without trailing '/'");
        }
        return value;
    }

    /** Fixed limits shared by all Gateway admission controller instances. */
    public record Limits(
            int maxScheduleInFlight,
            long maxScheduleBytes,
            int maxRetryInFlight,
            int maxControlInFlight,
            long leaseMaxAgeMs,
            int maxCasRetries) {
        public Limits {
            if (maxScheduleInFlight <= 0
                    || maxScheduleBytes <= 0
                    || maxRetryInFlight <= 0
                    || maxControlInFlight <= 0
                    || leaseMaxAgeMs <= 0
                    || maxCasRetries <= 0
                    || maxCasRetries > 64) {
                throw new IllegalArgumentException("Gateway admission limits must be positive and bounded");
            }
        }
    }

    private record Entry(GatewayAdmissionRecord record, long versionId) {}

    private record Usage(int scheduleInFlight, long scheduleBytes, int retryInFlight, int controlInFlight) {}

    private static final class CasRaceException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private CasRaceException(final Throwable cause) {
            super("Gateway admission CAS lost", cause);
        }
    }

    private static final class DurableLease implements GatewayAdmissionLease {
        private final OxiaGatewayAdmissionController owner;
        private final Digest32 tenantScopeHash;
        private final GatewayAdmissionRecord.Lease lease;
        private boolean closed;

        private DurableLease(
                final OxiaGatewayAdmissionController owner,
                final Digest32 tenantScopeHash,
                final GatewayAdmissionRecord.Lease lease) {
            this.owner = owner;
            this.tenantScopeHash = tenantScopeHash;
            this.lease = lease;
        }

        @Override
        public GatewayIngressOperation operation() {
            return lease.operation();
        }

        @Override
        public long estimatedRequestBytes() {
            return lease.estimatedRequestBytes();
        }

        @Override
        public synchronized void close() {
            if (!closed) {
                owner.release(tenantScopeHash, lease);
                closed = true;
            }
        }
    }

    private static final class SyncRecordClient implements OxiaGatewayRecordClient {
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

        @Override
        public void close() {
            try {
                delegate.close();
            } catch (Exception failure) {
                throw new IllegalStateException("failed to close Oxia Gateway admission client", failure);
            }
        }
    }
}
