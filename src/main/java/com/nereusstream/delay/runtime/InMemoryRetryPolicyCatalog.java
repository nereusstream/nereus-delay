package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RetryPolicySemantic;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TerminalPolicySemantic;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Deterministic source-history implementation of {@link RetryPolicyCatalog}.
 *
 * <p>This is an embedded/test authority only. It models the important lookup
 * rule: a semantic value is visible only at or after its source publication
 * position, and an exact reference never resolves to another policy hash.</p>
 */
public final class InMemoryRetryPolicyCatalog implements RetryPolicyCatalog {
    private final List<Publication> publications = new ArrayList<>();
    private SourcePosition sourceIdentity;
    private SourcePosition lastPublicationPosition;

    public synchronized void publish(final RetryPolicySemantic semantic, final SourcePosition visibleAt) {
        publish(semantic, null, visibleAt);
    }

    public synchronized void publish(
            final RetryPolicySemantic semantic, final TerminalPolicySemantic terminal, final SourcePosition visibleAt) {
        Objects.requireNonNull(semantic, "semantic");
        if (terminal != null) {
            terminal.requireBinding(semantic);
        }
        Objects.requireNonNull(visibleAt, "visibleAt");
        bindSource(visibleAt);
        for (Publication publication : publications) {
            final int order = compareExact(publication.visibleAt(), visibleAt);
            if (order == 0) {
                if (publication.semantic().equals(semantic) && Objects.equals(publication.terminal(), terminal)) {
                    return;
                }
                throw new IllegalStateException("retry policy catalog position already has another semantic");
            }
        }
        if (lastPublicationPosition != null && compareExact(lastPublicationPosition, visibleAt) > 0) {
            throw new IllegalArgumentException("retry policy publication position regressed");
        }
        for (Publication publication : publications) {
            if (publication.semantic().version() == semantic.version()
                    && java.util.Arrays.equals(publication.semantic().policyId(), semantic.policyId())) {
                if (!publication.semantic().equals(semantic) || !Objects.equals(publication.terminal(), terminal)) {
                    throw new IllegalStateException(
                            "retry policy reference changed original semantic or terminal content");
                }
                return;
            }
        }
        publications.add(new Publication(semantic, terminal, visibleAt));
        lastPublicationPosition = visibleAt;
    }

    @Override
    public synchronized RetryPolicySemantic resolve(
            final RetryPolicyRef reference, final SourcePosition sourcePosition) {
        final var publication = resolvePublication(reference, sourcePosition);
        return publication == null ? null : publication.retry();
    }

    @Override
    public synchronized RetryPolicyCatalog.Publication resolvePublication(
            final RetryPolicyRef reference, final SourcePosition sourcePosition) {
        Objects.requireNonNull(reference, "reference");
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        if (sourceIdentity == null
                || !sourceIdentity.sameSourceIdentity(sourcePosition)
                || !sourceIdentity.shardId().equals(sourcePosition.shardId())
                || sourceIdentity.kind() != sourcePosition.kind()) {
            return null;
        }
        Publication best = null;
        try {
            for (Publication publication : publications) {
                if (!publication.semantic().ref().equals(reference)
                        || compareExact(publication.visibleAt(), sourcePosition) > 0) {
                    continue;
                }
                if (best == null || compareExact(best.visibleAt(), publication.visibleAt()) < 0) {
                    best = publication;
                }
            }
        } catch (IllegalArgumentException differentSource) {
            return null;
        }
        return best == null ? null : new RetryPolicyCatalog.Publication(best.semantic(), best.terminal());
    }

    public synchronized int size() {
        return publications.size();
    }

    private void bindSource(final SourcePosition position) {
        if (sourceIdentity == null) {
            sourceIdentity = position;
            return;
        }
        if (!sourceIdentity.shardId().equals(position.shardId())
                || sourceIdentity.kind() != position.kind()
                || !sourceIdentity.sameSourceIdentity(position)) {
            throw new IllegalArgumentException("retry policy catalog source identity mismatch");
        }
    }

    private static int compareExact(final SourcePosition left, final SourcePosition right) {
        final int order = left.compareTo(right);
        if (order == 0 && !Bytes.constantTimeEquals(left.canonicalBytes(), right.canonicalBytes())) {
            throw new IllegalArgumentException("retry policy source position has conflicting canonical identity");
        }
        return order;
    }

    private record Publication(
            RetryPolicySemantic semantic, TerminalPolicySemantic terminal, SourcePosition visibleAt) {
        private Publication {
            Objects.requireNonNull(semantic, "semantic");
            Objects.requireNonNull(visibleAt, "visibleAt");
        }
    }
}
