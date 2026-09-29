package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlOperationKind;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetNativePolicyControlRecord;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.semantic.TargetNativePolicyTrust;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.Optional;

/** Reads exact Native authority projections from one bounded Target Store view. */
public final class TargetNativePolicyStoreAuthority {
    private TargetNativePolicyStoreAuthority() {}

    public static TargetNativePolicyTrust inView(
            final TargetStoreBackend.Reader reader, final TargetQuotaScope shardScope, final byte[] lineage) {
        return new TargetNativePolicyTrust() {
            @Override
            public Optional<PublisherPermission> publisher(
                    final byte[] scopeDigest, final int keyGeneration, final SourcePosition asOf) {
                final var scope = requireScope(reader, shardScope, scopeDigest);
                requireSameShard(scope, asOf);
                final byte[] key = TargetKeyCodec.nativePublisher(scopeDigest, keyGeneration);
                final var installed = read(reader, key, shardScope, lineage);
                final byte[] closureKey = TargetKeyCodec.nativePublisherClosure(scopeDigest, keyGeneration);
                final var closure = read(reader, closureKey, shardScope, lineage);
                if (installed == null) {
                    if (closure != null) {
                        throw new IllegalStateException("Target Native publisher closure has no installation");
                    }
                    return Optional.empty();
                }
                requireOperation(installed, ControlOperationKind.INSTALL_TARGET_NATIVE_PUBLISHER_PERMISSION);
                requireSourceVisible(installed, asOf);
                final var request = installed.body().request();
                if (!scope.equals(request.scope()) || request.keyGeneration() != keyGeneration) {
                    throw new IllegalStateException("Target Native publisher key/value scope mismatch");
                }
                if (closure != null) {
                    requireOperation(closure, ControlOperationKind.CLOSE_TARGET_NATIVE_PUBLISHER_PERMISSION);
                    requireSamePermission(installed, closure);
                    requireAfter(closure.source(), installed.source(), "publisher closure");
                    if (comparePosition(closure.source(), asOf) <= 0) {
                        return Optional.empty();
                    }
                }
                return Optional.of(new PublisherPermission(
                        scope,
                        installed.author(),
                        keyGeneration,
                        request.publicKey(),
                        request.maximumLeaseMs(),
                        installed.source()));
            }

            @Override
            public Optional<Activation> activation(final byte[] scopeDigest, final long generation) {
                final var scope = requireScope(reader, shardScope, scopeDigest);
                final var record =
                        read(reader, TargetKeyCodec.nativeActivation(scopeDigest, generation), shardScope, lineage);
                if (record == null) {
                    return Optional.empty();
                }
                requireOperation(record, ControlOperationKind.ACTIVATE_TARGET_NATIVE_POLICY);
                final var request = record.body().request();
                final var snapshot = request.snapshot();
                if (!scope.equals(request.scope()) || snapshot.generation() != generation) {
                    throw new IllegalStateException("Target Native activation key/value scope mismatch");
                }
                return Optional.of(new Activation(scope, snapshot, record.source()));
            }

            @Override
            public Optional<MemberApproval> member(
                    final byte[] grantDigest, final byte[] scopeDigest, final SourcePosition asOf) {
                final var scope = requireScope(reader, shardScope, scopeDigest);
                requireSameShard(scope, asOf);
                final var approval =
                        read(reader, TargetKeyCodec.nativeMember(scopeDigest, grantDigest), shardScope, lineage);
                final var closure =
                        read(reader, TargetKeyCodec.nativeMemberClosure(scopeDigest, grantDigest), shardScope, lineage);
                if (approval == null) {
                    if (closure != null) {
                        throw new IllegalStateException("Target Native member closure has no approval");
                    }
                    return Optional.empty();
                }
                requireOperation(approval, ControlOperationKind.APPROVE_TARGET_NATIVE_MEMBER);
                if (closure != null) {
                    requireOperation(closure, ControlOperationKind.CLOSE_TARGET_NATIVE_MEMBER);
                    requireSameMember(approval, closure);
                    requireAfter(closure.source(), approval.source(), "Native member closure");
                }
                if (comparePosition(approval.source(), asOf) > 0) {
                    return Optional.empty();
                }
                final var request = approval.body().request();
                final TargetMembershipGrant grant = approval.grant();
                if (!scope.equals(request.scope()) || !Arrays.equals(grant.digest(), grantDigest)) {
                    throw new IllegalStateException("Target Native member key/value scope mismatch");
                }
                return Optional.of(
                        new MemberApproval(grant, scope, approval.source(), closure == null ? null : closure.source()));
            }
        };
    }

    static TargetNativePolicyScope requireScope(
            final TargetStoreBackend.Reader reader, final TargetQuotaScope shardScope, final byte[] scopeDigest) {
        Bytes.requireLength(scopeDigest, 32, "policyScopeDigest");
        final byte[] key = TargetKeyCodec.nativePolicyScope(scopeDigest);
        final byte[] raw = reader.get(ColumnFamily.META, key);
        if (raw == null) {
            throw new IllegalStateException("Target Native source control references an unregistered scope");
        }
        final var scope = TargetNativePolicyScope.decodeForStore(
                key,
                TargetValueEnvelope.decode(raw, TargetNativePolicyScope.VALUE_TYPE)
                        .payload(),
                shardScope.shard());
        if (!Arrays.equals(scope.digest(), scopeDigest)) {
            throw new IllegalStateException("Target Native scope key does not match its digest");
        }
        return scope;
    }

    static TargetNativePolicyControlRecord read(
            final TargetStoreBackend.Reader reader,
            final byte[] key,
            final TargetQuotaScope shardScope,
            final byte[] lineage) {
        final byte[] raw = reader.get(ColumnFamily.META, key);
        if (raw == null) {
            return null;
        }
        final var record = TargetNativePolicyControlRecord.decodeForStore(
                key,
                TargetValueEnvelope.decode(raw, TargetNativePolicyControlRecord.VALUE_TYPE)
                        .payload(),
                shardScope.shard(),
                lineage);
        final var frontier = reader.source();
        if (frontier == null) {
            throw new IllegalStateException("Target Native history exists without a source frontier");
        }
        requireAtOrBefore(record.source(), frontier, "Target Native record exceeds source frontier");
        record.mutation().requireAtOrBefore(reader.aggregate().mutation());
        return record;
    }

    static void requireOperation(final TargetNativePolicyControlRecord record, final ControlOperationKind expected) {
        if (record.body().request().operationKind() != expected) {
            throw new IllegalStateException("Target Native Store key contains another control operation");
        }
    }

    static void requireSourceVisible(final TargetNativePolicyControlRecord record, final SourcePosition asOf) {
        if (comparePosition(record.source(), asOf) > 0) {
            throw new IllegalStateException("Target Native authority is not yet active at the requested source");
        }
    }

    static void requireQueryVisible(final TargetStoreBackend.Reader reader, final SourcePosition asOf) {
        final var frontier = reader.source();
        if (frontier == null || comparePosition(asOf, frontier) > 0) {
            throw new IllegalStateException("Target Native trust query exceeds the applied source frontier");
        }
    }

    static int comparePosition(final SourcePosition left, final SourcePosition right) {
        final int order = left.compareTo(right);
        if (order == 0 && !Arrays.equals(left.canonicalBytes(), right.canonicalBytes())) {
            throw new IllegalStateException("Target Native source positions conflict at the same offset");
        }
        return order;
    }

    static void requireAtOrBefore(final SourcePosition left, final SourcePosition right, final String message) {
        if (comparePosition(left, right) > 0) {
            throw new IllegalStateException(message);
        }
    }

    static void requireAfter(final SourcePosition left, final SourcePosition right, final String name) {
        if (comparePosition(left, right) <= 0) {
            throw new IllegalStateException(name + " must follow its immutable authority record");
        }
    }

    static void requireSameShard(final TargetNativePolicyScope scope, final SourcePosition at) {
        if (!scope.sourceShard().equals(at.shardId())) {
            throw new IllegalArgumentException("Target Native lookup source belongs to another Shard");
        }
    }

    static void requireSamePermission(
            final TargetNativePolicyControlRecord install, final TargetNativePolicyControlRecord close) {
        if (!install.body().request().scope().equals(close.body().request().scope())
                || install.body().request().keyGeneration()
                        != close.body().request().keyGeneration()) {
            throw new IllegalStateException("Target Native publisher closure differs from its installation");
        }
    }

    static void requireSameMember(
            final TargetNativePolicyControlRecord approval, final TargetNativePolicyControlRecord close) {
        if (!approval.body().request().scope().equals(close.body().request().scope())
                || !Arrays.equals(
                        approval.body().request().grantDigest(),
                        close.body().request().grantDigest())
                || !approval.grant().equals(close.grant())) {
            throw new IllegalStateException("Target Native member closure differs from its approval");
        }
    }
}
