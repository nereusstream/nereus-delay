package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Bounded execution-slot summary. VACANT retains its last generation to prevent reuse ABA. */
public final class TargetDomainState {
    public static final int MAX_CANONICAL_BYTES =
            4 + 11 + 2 + 3 * 34 + 2 * (3 + TargetHeadRef.MAX_CANONICAL_BYTES) + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-execution-domain\0");

    public enum Lifecycle {
        ACTIVE(1),
        DRAINING(2),
        VACANT(3);
        private final int wire;

        Lifecycle(final int wire) {
            this.wire = wire;
        }

        public int wireValue() {
            return wire;
        }

        private static Lifecycle decode(final int value) {
            for (Lifecycle state : values()) {
                if (state.wire == value) {
                    return state;
                }
            }
            throw new IllegalArgumentException("unknown Target domain lifecycle");
        }
    }

    private final TargetKeyCodec.Domain domain;
    private final Lifecycle lifecycle;
    private final byte[] dispatchCompatibilityRef;
    private final byte[] controlScopeRef;
    private final byte[] nativePolicyScopeRef;
    private final TargetHeadRef ordinaryHead;
    private final TargetHeadRef nativeHead;
    private final byte[] digest;

    public TargetDomainState(
            final TargetKeyCodec.Domain domain,
            final Lifecycle lifecycle,
            final byte[] dispatchCompatibilityRef,
            final byte[] controlScopeRef,
            final byte[] nativePolicyScopeRef,
            final TargetHeadRef ordinaryHead,
            final TargetHeadRef nativeHead) {
        this.domain = Objects.requireNonNull(domain, "domain");
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
        if (lifecycle == Lifecycle.VACANT) {
            if (dispatchCompatibilityRef != null
                    || controlScopeRef != null
                    || nativePolicyScopeRef != null
                    || ordinaryHead != null
                    || nativeHead != null) {
                throw new IllegalArgumentException("vacant Target slot retains only its generation");
            }
            this.dispatchCompatibilityRef = null;
            this.controlScopeRef = null;
            this.nativePolicyScopeRef = null;
        } else {
            this.dispatchCompatibilityRef = nonzeroRef(dispatchCompatibilityRef, "dispatchCompatibilityRef");
            this.controlScopeRef = nonzeroRef(controlScopeRef, "controlScopeRef");
            this.nativePolicyScopeRef =
                    nativePolicyScopeRef == null ? null : nonzeroRef(nativePolicyScopeRef, "nativePolicyScopeRef");
        }
        if (ordinaryHead != null
                && (ordinaryHead.nativeCandidate() || !ordinaryHead.domain().equals(domain))) {
            throw new IllegalArgumentException("ordinary head does not belong to its Target domain/role");
        }
        if (nativeHead != null
                && (!nativeHead.nativeCandidate()
                        || !nativeHead.domain().equals(domain)
                        || nativePolicyScopeRef == null
                        || ordinaryHead == null
                        || !nativeHead.target().equals(ordinaryHead.target()))) {
            throw new IllegalArgumentException("native head lacks its matching domain, ordinary work or policy scope");
        }
        if (nativeHead != null
                && nativeHead.messageId().equals(ordinaryHead.messageId())
                && (nativeHead.generation() != ordinaryHead.generation()
                        || nativeHead.timeEpochMs() > ordinaryHead.timeEpochMs())) {
            throw new IllegalArgumentException("same-message Target heads disagree on generation or eligibility");
        }
        if (nativeHead != null
                && nativeHead.messageId().equals(ordinaryHead.messageId())
                && ordinaryHead.key()[0] == TargetKeyCodec.DUE_TAG
                && !Arrays.equals(
                        TargetKeyCodec.decodeCandidate(ordinaryHead.key()).sourceOrderToken(),
                        TargetKeyCodec.decodeCandidate(nativeHead.key()).sourceOrderToken())) {
            throw new IllegalArgumentException("same-message Target heads disagree on source order");
        }
        this.ordinaryHead = ordinaryHead;
        this.nativeHead = nativeHead;
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public TargetKeyCodec.Domain domain() {
        return domain;
    }

    public Lifecycle lifecycle() {
        return lifecycle;
    }

    public byte[] dispatchCompatibilityRef() {
        return copyNullable(dispatchCompatibilityRef);
    }

    public byte[] controlScopeRef() {
        return copyNullable(controlScopeRef);
    }

    public byte[] nativePolicyScopeRef() {
        return copyNullable(nativePolicyScopeRef);
    }

    public TargetHeadRef ordinaryHead() {
        return ordinaryHead;
    }

    public TargetHeadRef nativeHead() {
        return nativeHead;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, domain.slot());
            CanonicalProtobuf.uint64Bits(out, 2, domain.generation());
            CanonicalProtobuf.uint32(out, 3, lifecycle.wireValue());
            if (dispatchCompatibilityRef != null) {
                CanonicalProtobuf.bytes(out, 4, dispatchCompatibilityRef);
            }
            if (controlScopeRef != null) {
                CanonicalProtobuf.bytes(out, 5, controlScopeRef);
            }
            if (nativePolicyScopeRef != null) {
                CanonicalProtobuf.bytes(out, 6, nativePolicyScopeRef);
            }
            if (ordinaryHead != null) {
                CanonicalProtobuf.bytes(out, 7, ordinaryHead.canonicalBytes());
            }
            if (nativeHead != null) {
                CanonicalProtobuf.bytes(out, 8, nativeHead.canonicalBytes());
            }
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 9, digest);
        });
    }

    public static TargetDomainState decode(final byte[] encoded) {
        if (encoded == null || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Target domain exceeds its byte bound");
        }
        final var fields = QueryCodecSupport.read(encoded, "TargetDomainState");
        if (fields.size() < 4 || fields.size() > 9) {
            throw new IllegalArgumentException("invalid Target domain field count");
        }
        int index = 3;
        final byte[][] refs = new byte[3][];
        for (int number = 4; number <= 6; number++) {
            if (index < fields.size() && fields.get(index).number() == number) {
                refs[number - 4] = QueryCodecSupport.fixed(fields.get(index++), number, 32);
            }
        }
        TargetHeadRef ordinary = null;
        TargetHeadRef nativeHead = null;
        if (index < fields.size() && fields.get(index).number() == 7) {
            ordinary = TargetHeadRef.decode(QueryCodecSupport.bytes(fields.get(index++), 7));
        }
        if (index < fields.size() && fields.get(index).number() == 8) {
            nativeHead = TargetHeadRef.decode(QueryCodecSupport.bytes(fields.get(index++), 8));
        }
        if (index != fields.size() - 1) {
            throw new IllegalArgumentException("unknown or missing Target domain field");
        }
        final TargetDomainState result = new TargetDomainState(
                new TargetKeyCodec.Domain(
                        QueryCodecSupport.uint32(fields.get(0), 1), QueryCodecSupport.uint64Bits(fields.get(1), 2)),
                Lifecycle.decode(QueryCodecSupport.uint32(fields.get(2), 3)),
                refs[0],
                refs[1],
                refs[2],
                ordinary,
                nativeHead);
        if (!Bytes.constantTimeEquals(result.digest, QueryCodecSupport.fixed(fields.get(index), 9, 32))) {
            throw new IllegalArgumentException("Target domain digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetDomainState");
        return result;
    }

    private static byte[] nonzeroRef(final byte[] value, final String name) {
        Bytes.requireLength(value, 32, name);
        if (Arrays.equals(value, new byte[32])) {
            throw new IllegalArgumentException(name + " is unassigned");
        }
        return Bytes.copy(value);
    }

    private static byte[] copyNullable(final byte[] value) {
        return value == null ? null : Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetDomainState that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
