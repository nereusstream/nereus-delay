package io.nereusstream.delay.protocol;

import io.nereusstream.delay.runtime.AdmissionGate;
import io.nereusstream.delay.runtime.RuntimeReadiness;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Canonical active branch of the Registry {@code LaneRecordV1} value.
 *
 * <p>The ready-certificate field is intentionally retained as canonical nested
 * bytes here.  The certificate's provider/channel evidence type is owned by
 * the transport protocol, while this state codec owns presence, digest and
 * Lane-level cross-field rules.  A future persistence cutover can therefore
 * replace the legacy adapter without changing the outer branch or digest.</p>
 */
public final class ActiveLaneStateV1 {
    public static final int VERSION = 1;
    public static final int HASH_LENGTH = 32;
    public static final int INCARNATION_LENGTH = 16;
    private static final int MAX_TUPLE_BYTES = 1 << 20;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-active-lane-state-v1\0");

    private final DestinationLaneId laneId;
    private final byte[] laneIncarnation;
    private final AdmissionGate admissionGate;
    private final RuntimeReadiness runtimeReadiness;
    private final LaneRuntimeBlockReasonV1 runtimeBlockReason;
    private final long laneControlVersion;
    private final long laneVersion;
    private final ProfileRefV1 destinationProfile;
    private final ProfileRefV1 capabilityProfile;
    private final byte[] canonicalLaneTuple;
    private final byte[] canonicalLaneTupleSha256;
    private final long schedulerWeight;
    private final PublishAdmissionBody.ChargeVector laneUsage;
    private final Long earliestActionAtEpochMs;
    private final Long nextEligibleAtEpochMs;
    private final LaneCircuitStateV1 circuitState;
    private final long circuitOpenUntilEpochMs;
    private final long consecutiveFailures;
    private final long laneRetryBackoffUntilEpochMs;
    private final long executorRetryAtEpochMs;
    private final byte[] encodedReadyKey;
    private final byte[] readyKeySha256;
    private final byte[] readyCertificate;
    private final LaneRetirementProgressV1 retirement;
    private final byte[] stateDigest;

    public ActiveLaneStateV1(final DestinationLaneId laneId, final byte[] laneIncarnation,
                             final AdmissionGate admissionGate, final RuntimeReadiness runtimeReadiness,
                             final LaneRuntimeBlockReasonV1 runtimeBlockReason, final long laneControlVersion,
                             final long laneVersion, final ProfileRefV1 destinationProfile,
                             final ProfileRefV1 capabilityProfile, final byte[] canonicalLaneTuple,
                             final long schedulerWeight, final PublishAdmissionBody.ChargeVector laneUsage,
                             final Long earliestActionAtEpochMs, final Long nextEligibleAtEpochMs,
                             final LaneCircuitStateV1 circuitState, final long circuitOpenUntilEpochMs,
                             final long consecutiveFailures, final long laneRetryBackoffUntilEpochMs,
                             final long executorRetryAtEpochMs, final byte[] encodedReadyKey,
                             final byte[] readyCertificate, final LaneRetirementProgressV1 retirement) {
        this.laneId = Objects.requireNonNull(laneId, "laneId");
        this.laneIncarnation = fixed(laneIncarnation, INCARNATION_LENGTH, "laneIncarnation");
        this.admissionGate = Objects.requireNonNull(admissionGate, "admissionGate");
        if (admissionGate == AdmissionGate.ABSENT || admissionGate == AdmissionGate.RETIRED) {
            throw new IllegalArgumentException("ActiveLaneStateV1 cannot carry ABSENT or RETIRED gate");
        }
        this.runtimeReadiness = Objects.requireNonNull(runtimeReadiness, "runtimeReadiness");
        if ((runtimeReadiness == RuntimeReadiness.BLOCKED) != (runtimeBlockReason != null)) {
            throw new IllegalArgumentException("runtime block reason presence must match BLOCKED readiness");
        }
        this.runtimeBlockReason = runtimeBlockReason;
        if (laneControlVersion == 0 || laneVersion == 0) {
            throw new IllegalArgumentException("Lane versions must be positive");
        }
        this.laneControlVersion = laneControlVersion;
        this.laneVersion = laneVersion;
        this.destinationProfile = requireProfile(destinationProfile, ProfileKindV1.DESTINATION,
                "destinationProfile");
        this.capabilityProfile = requireProfile(capabilityProfile, ProfileKindV1.DELIVERY_CAPABILITY,
                "capabilityProfile");
        this.canonicalLaneTuple = tuple(canonicalLaneTuple);
        if (!laneId.equals(DestinationLaneId.derive(this.canonicalLaneTuple))) {
            throw new IllegalArgumentException("Lane identity does not match canonical tuple");
        }
        this.canonicalLaneTupleSha256 = Bytes.sha256(this.canonicalLaneTuple);
        this.schedulerWeight = nonZero(schedulerWeight, "schedulerWeight");
        this.laneUsage = Objects.requireNonNull(laneUsage, "laneUsage");
        this.earliestActionAtEpochMs = nonNegativeOptional(earliestActionAtEpochMs, "earliestActionAtEpochMs");
        this.nextEligibleAtEpochMs = nonNegativeOptional(nextEligibleAtEpochMs, "nextEligibleAtEpochMs");
        this.circuitState = Objects.requireNonNull(circuitState, "circuitState");
        if (circuitState == LaneCircuitStateV1.OPEN && circuitOpenUntilEpochMs <= 0) {
            throw new IllegalArgumentException("OPEN circuit must have a positive open-until time");
        }
        if (circuitState == LaneCircuitStateV1.CLOSED && circuitOpenUntilEpochMs != 0) {
            throw new IllegalArgumentException("CLOSED circuit must have circuitOpenUntil=0");
        }
        this.circuitOpenUntilEpochMs = nonNegative(circuitOpenUntilEpochMs, "circuitOpenUntilEpochMs");
        this.consecutiveFailures = consecutiveFailures;
        this.laneRetryBackoffUntilEpochMs = nonNegative(laneRetryBackoffUntilEpochMs,
                "laneRetryBackoffUntilEpochMs");
        this.executorRetryAtEpochMs = nonNegative(executorRetryAtEpochMs, "executorRetryAtEpochMs");
        this.encodedReadyKey = optionalBytes(encodedReadyKey, "encodedReadyKey");
        this.readyKeySha256 = this.encodedReadyKey == null ? null : Bytes.sha256(this.encodedReadyKey);
        if (runtimeReadiness == RuntimeReadiness.READY && readyCertificate == null) {
            throw new IllegalArgumentException("READY Lane must carry a ready certificate");
        }
        if (runtimeReadiness != RuntimeReadiness.READY && (encodedReadyKey != null || readyCertificate != null)) {
            throw new IllegalArgumentException("non-READY Lane cannot carry ready projections");
        }
        this.readyCertificate = optionalCanonicalBytes(readyCertificate, "readyCertificate");
        this.retirement = retirement;
        this.stateDigest = Bytes.sha256(DIGEST_DOMAIN, fieldsOneToTwentyFive());
    }

    public DestinationLaneId laneId() {
        return laneId;
    }

    public byte[] laneIncarnation() {
        return Bytes.copy(laneIncarnation);
    }

    public AdmissionGate admissionGate() {
        return admissionGate;
    }

    public RuntimeReadiness runtimeReadiness() {
        return runtimeReadiness;
    }

    public LaneRuntimeBlockReasonV1 runtimeBlockReason() {
        return runtimeBlockReason;
    }

    public long laneControlVersion() {
        return laneControlVersion;
    }

    public long laneVersion() {
        return laneVersion;
    }

    public ProfileRefV1 destinationProfile() {
        return destinationProfile;
    }

    public ProfileRefV1 capabilityProfile() {
        return capabilityProfile;
    }

    public byte[] canonicalLaneTuple() {
        return Bytes.copy(canonicalLaneTuple);
    }

    public byte[] canonicalLaneTupleSha256() {
        return Bytes.copy(canonicalLaneTupleSha256);
    }

    public long schedulerWeight() {
        return schedulerWeight;
    }

    public PublishAdmissionBody.ChargeVector laneUsage() {
        return laneUsage;
    }

    public Long earliestActionAtEpochMs() {
        return earliestActionAtEpochMs;
    }

    public Long nextEligibleAtEpochMs() {
        return nextEligibleAtEpochMs;
    }

    public LaneCircuitStateV1 circuitState() {
        return circuitState;
    }

    public long circuitOpenUntilEpochMs() {
        return circuitOpenUntilEpochMs;
    }

    public long consecutiveFailures() {
        return consecutiveFailures;
    }

    public long laneRetryBackoffUntilEpochMs() {
        return laneRetryBackoffUntilEpochMs;
    }

    public long executorRetryAtEpochMs() {
        return executorRetryAtEpochMs;
    }

    public byte[] encodedReadyKey() {
        return optionalCopy(encodedReadyKey);
    }

    public byte[] readyKeySha256() {
        return optionalCopy(readyKeySha256);
    }

    public byte[] readyCertificate() {
        return optionalCopy(readyCertificate);
    }

    public LaneRetirementProgressV1 retirement() {
        return retirement;
    }

    public byte[] stateDigest() {
        return Bytes.copy(stateDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneToTwentyFive());
            CanonicalProtobuf.bytes(output, 26, stateDigest);
        });
    }

    public static ActiveLaneStateV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "ActiveLaneStateV1");
        if (fields.size() < 19 || fields.get(0).number() != 1 || fields.get(fields.size() - 1).number() != 26) {
            throw new IllegalArgumentException("ActiveLaneStateV1 has invalid field count/order");
        }
        int index = 0;
        final long version = QueryCodecSupport.uint(fields.get(index), 1);
        if (version != VERSION) {
            throw new IllegalArgumentException("unsupported ActiveLaneStateV1 version");
        }
        index++;
        final DestinationLaneId laneId = new DestinationLaneId(QueryCodecSupport.fixed(fields.get(index++), 2,
                DestinationLaneId.LENGTH));
        final byte[] laneIncarnation = QueryCodecSupport.fixed(fields.get(index++), 3, INCARNATION_LENGTH);
        final AdmissionGate gate = AdmissionGate.fromWire(QueryCodecSupport.uint32(fields.get(index++), 4));
        final RuntimeReadiness readiness = RuntimeReadiness.fromWire(QueryCodecSupport.uint32(fields.get(index++), 5));
        LaneRuntimeBlockReasonV1 blockReason = null;
        if (fields.get(index).number() == 6) {
            blockReason = LaneRuntimeBlockReasonV1.fromWire(QueryCodecSupport.uint(fields.get(index++), 6));
        }
        final long controlVersion = QueryCodecSupport.uint64Bits(fields.get(index++), 7);
        final long laneVersion = QueryCodecSupport.uint64Bits(fields.get(index++), 8);
        final ProfileRefV1 destination = ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(index++), 9));
        final ProfileRefV1 capability = ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(index++), 10));
        final byte[] tuple = QueryCodecSupport.bytes(fields.get(index++), 11);
        final byte[] tupleDigest = QueryCodecSupport.fixed(fields.get(index++), 12, HASH_LENGTH);
        final long weight = QueryCodecSupport.uint64Bits(fields.get(index++), 13);
        final PublishAdmissionBody.ChargeVector usage = PublishAdmissionBody.ChargeVector.decodeCanonical(
                QueryCodecSupport.nested(fields.get(index++), 14));
        final Long earliest = optionalLong(fields, index, 15);
        if (earliest != null) {
            index++;
        }
        final Long next = optionalLong(fields, index, 16);
        if (next != null) {
            index++;
        }
        final LaneCircuitStateV1 circuit = LaneCircuitStateV1.fromWire(QueryCodecSupport.uint(fields.get(index++), 17));
        final long circuitUntil = QueryCodecSupport.uint(fields.get(index++), 18);
        final long failures = QueryCodecSupport.uint64Bits(fields.get(index++), 19);
        final long laneRetry = QueryCodecSupport.uint(fields.get(index++), 20);
        final long executorRetry = QueryCodecSupport.uint(fields.get(index++), 21);
        byte[] readyKey = null;
        byte[] readyKeyDigest = null;
        if (index < fields.size() - 1 && fields.get(index).number() == 22) {
            readyKey = QueryCodecSupport.bytes(fields.get(index++), 22);
            readyKeyDigest = QueryCodecSupport.fixed(fields.get(index++), 23, HASH_LENGTH);
        }
        byte[] certificate = null;
        if (index < fields.size() - 1 && fields.get(index).number() == 24) {
            certificate = QueryCodecSupport.bytes(fields.get(index++), 24);
        }
        LaneRetirementProgressV1 retirement = null;
        if (index < fields.size() - 1 && fields.get(index).number() == 25) {
            retirement = LaneRetirementProgressV1.decode(QueryCodecSupport.nested(fields.get(index++), 25));
        }
        if (index != fields.size() - 1) {
            throw new IllegalArgumentException("ActiveLaneStateV1 has unexpected optional fields");
        }
        final byte[] stateDigest = QueryCodecSupport.fixed(fields.get(index), 26, HASH_LENGTH);
        final ActiveLaneStateV1 result = new ActiveLaneStateV1(laneId, laneIncarnation, gate, readiness,
                blockReason, controlVersion, laneVersion, destination, capability, tuple, weight, usage,
                earliest, next, circuit, circuitUntil, failures, laneRetry, executorRetry, readyKey, certificate,
                retirement);
        if (!Bytes.constantTimeEquals(tupleDigest, result.canonicalLaneTupleSha256())) {
            throw new IllegalArgumentException("canonical Lane tuple digest mismatch");
        }
        if (readyKey == null ? readyKeyDigest != null
                : !Bytes.constantTimeEquals(readyKeyDigest, Bytes.sha256(readyKey))) {
            throw new IllegalArgumentException("ready-key digest mismatch");
        }
        if (!Bytes.constantTimeEquals(stateDigest, result.stateDigest)) {
            throw new IllegalArgumentException("ActiveLaneStateV1 digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ActiveLaneStateV1");
        return result;
    }

    private byte[] fieldsOneToTwentyFive() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, laneId.bytes());
            CanonicalProtobuf.bytes(output, 3, laneIncarnation);
            CanonicalProtobuf.uint32(output, 4, admissionGate.wireValue());
            CanonicalProtobuf.uint32(output, 5, runtimeReadiness.wireValue());
            if (runtimeBlockReason != null) {
                CanonicalProtobuf.uint32(output, 6, runtimeBlockReason.wireValue());
            }
            CanonicalProtobuf.uint64Bits(output, 7, laneControlVersion);
            CanonicalProtobuf.uint64Bits(output, 8, laneVersion);
            CanonicalProtobuf.bytes(output, 9, destinationProfile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 10, capabilityProfile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 11, canonicalLaneTuple);
            CanonicalProtobuf.bytes(output, 12, canonicalLaneTupleSha256);
            CanonicalProtobuf.uint64Bits(output, 13, schedulerWeight);
            CanonicalProtobuf.bytes(output, 14, laneUsage.canonicalBytes());
            if (earliestActionAtEpochMs != null) {
                CanonicalProtobuf.int64(output, 15, earliestActionAtEpochMs);
            }
            if (nextEligibleAtEpochMs != null) {
                CanonicalProtobuf.int64(output, 16, nextEligibleAtEpochMs);
            }
            CanonicalProtobuf.uint32(output, 17, circuitState.wireValue());
            CanonicalProtobuf.int64(output, 18, circuitOpenUntilEpochMs);
            CanonicalProtobuf.uint64Bits(output, 19, consecutiveFailures);
            CanonicalProtobuf.int64(output, 20, laneRetryBackoffUntilEpochMs);
            CanonicalProtobuf.int64(output, 21, executorRetryAtEpochMs);
            if (encodedReadyKey != null) {
                CanonicalProtobuf.bytes(output, 22, encodedReadyKey);
                CanonicalProtobuf.bytes(output, 23, readyKeySha256);
            }
            if (readyCertificate != null) {
                CanonicalProtobuf.bytes(output, 24, readyCertificate);
            }
            if (retirement != null) {
                CanonicalProtobuf.bytes(output, 25, retirement.canonicalBytes());
            }
        });
    }

    private static Long optionalLong(final List<CanonicalProtobuf.Reader.Field> fields, final int index,
                                     final int number) {
        if (index >= fields.size() - 1 || fields.get(index).number() != number) {
            return null;
        }
        return QueryCodecSupport.uint(fields.get(index), number);
    }

    private static ProfileRefV1 requireProfile(final ProfileRefV1 value, final ProfileKindV1 expected,
                                               final String name) {
        ProfileRefV1 result = Objects.requireNonNull(value, name);
        if (result.profileKind() != expected) {
            throw new IllegalArgumentException(name + " has wrong ProfileKindV1");
        }
        return result;
    }

    private static byte[] tuple(final byte[] value) {
        Objects.requireNonNull(value, "canonicalLaneTuple");
        if (value.length == 0 || value.length > MAX_TUPLE_BYTES) {
            throw new IllegalArgumentException("canonicalLaneTuple has invalid length");
        }
        return Bytes.copy(value);
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return Bytes.copy(value);
    }

    private static long nonZero(final long value, final String name) {
        if (value == 0) {
            throw new IllegalArgumentException(name + " must be nonzero");
        }
        return value;
    }

    private static long nonNegative(final long value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }

    private static Long nonNegativeOptional(final Long value, final String name) {
        return value == null ? null : nonNegative(value, name);
    }

    private static byte[] optionalBytes(final byte[] value, final String name) {
        if (value == null) {
            return null;
        }
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return Bytes.copy(value);
    }

    private static byte[] optionalCanonicalBytes(final byte[] value, final String name) {
        final byte[] result = optionalBytes(value, name);
        if (result != null) {
            final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(result, name);
            final byte[] canonical = CanonicalProtobuf.message(output -> {
                for (CanonicalProtobuf.Reader.Field field : fields) {
                    if (field.wireType() == 0) {
                        CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
                    } else {
                        CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
                    }
                }
            });
            QueryCodecSupport.requireCanonical(result, canonical, name);
            if ("readyCertificate".equals(name)) {
                ReadyCertificateV1.decode(result);
            }
        }
        return result;
    }

    private static byte[] optionalCopy(final byte[] value) {
        return value == null ? null : Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ActiveLaneStateV1 that && laneControlVersion == that.laneControlVersion
                && laneVersion == that.laneVersion && schedulerWeight == that.schedulerWeight
                && circuitOpenUntilEpochMs == that.circuitOpenUntilEpochMs
                && consecutiveFailures == that.consecutiveFailures
                && laneRetryBackoffUntilEpochMs == that.laneRetryBackoffUntilEpochMs
                && executorRetryAtEpochMs == that.executorRetryAtEpochMs && laneId.equals(that.laneId)
                && Arrays.equals(laneIncarnation, that.laneIncarnation) && admissionGate == that.admissionGate
                && runtimeReadiness == that.runtimeReadiness && runtimeBlockReason == that.runtimeBlockReason
                && destinationProfile.equals(that.destinationProfile) && capabilityProfile.equals(that.capabilityProfile)
                && Arrays.equals(canonicalLaneTuple, that.canonicalLaneTuple) && laneUsage.equals(that.laneUsage)
                && Objects.equals(earliestActionAtEpochMs, that.earliestActionAtEpochMs)
                && Objects.equals(nextEligibleAtEpochMs, that.nextEligibleAtEpochMs)
                && circuitState == that.circuitState && Arrays.equals(encodedReadyKey, that.encodedReadyKey)
                && Arrays.equals(readyCertificate, that.readyCertificate) && Objects.equals(retirement, that.retirement)
                && Arrays.equals(stateDigest, that.stateDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(laneId, Arrays.hashCode(laneIncarnation), admissionGate, runtimeReadiness,
                runtimeBlockReason, laneControlVersion, laneVersion, destinationProfile, capabilityProfile,
                Arrays.hashCode(canonicalLaneTuple), schedulerWeight, laneUsage, earliestActionAtEpochMs,
                nextEligibleAtEpochMs, circuitState, circuitOpenUntilEpochMs, consecutiveFailures,
                laneRetryBackoffUntilEpochMs, executorRetryAtEpochMs, Arrays.hashCode(encodedReadyKey),
                Arrays.hashCode(readyCertificate), retirement, Arrays.hashCode(stateDigest));
    }
}
