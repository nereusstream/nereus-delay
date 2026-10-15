package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlOperationKind;
import com.nereusstream.delay.protocol.TargetChannelControlRecord;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.protocol.TargetChannelSlot;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Independent lifecycle fold of retained Source channel facts; does not grant transport authority. */
public final class TargetChannelHistoryAudit {
    private TargetChannelHistoryAudit() {}

    public static void requireComplete(final List<TargetChannelControlRecord> controls,
            final List<TargetChannelIdentity> identities, final List<TargetChannelSlot> slots) {
        final Map<String, TargetChannelIdentity> byIdentity = new HashMap<>();
        final Map<String, TargetChannelSlot> bySlot = new HashMap<>();
        final Map<String, List<TargetChannelControlRecord>> history = new HashMap<>();
        final java.util.Set<String> created = new java.util.HashSet<>();
        for (final var identity : identities) {
            if (byIdentity.putIfAbsent(Bytes.hex(identity.digest()), identity) != null) {
                throw new IllegalStateException("checkpoint repeats a channel immutable identity");
            }
        }
        for (final var slot : slots) {
            if (bySlot.putIfAbsent(Bytes.hex(slot.key()), slot) != null) {
                throw new IllegalStateException("checkpoint repeats a current channel slot");
            }
        }
        for (final var event : controls) {
            final var requested = event.body().request().channel();
            final var identity = byIdentity.get(Bytes.hex(requested.digest()));
            if (identity == null || !Arrays.equals(identity.canonicalBytes(), requested.canonicalBytes())) {
                throw new IllegalStateException("channel lifecycle event lacks its exact retained NV20 identity");
            }
            final String key = Bytes.hex(TargetKeyCodec.channelSlot(identity.context().producerIdentity()));
            history.computeIfAbsent(key, ignored -> new ArrayList<>()).add(event);
        }
        if (history.size() != bySlot.size()) {
            throw new IllegalStateException("channel history and current slot inventory differ");
        }
        for (final var entry : history.entrySet()) {
            final var events = entry.getValue();
            events.sort((left, right) -> left.mutation().source().compareTo(right.mutation().source()));
            TargetChannelSlot prior = null;
            for (final var event : events) {
                final var request = event.body().request();
                final var channel = request.channel();
                if (prior != null) { event.mutation().requireAfter(prior.mutation()); }
                switch (request.operationKind()) {
                    case CREATE_TARGET_CHANNEL -> {
                        if (prior != null) {
                            throw new IllegalStateException("channel creation overwrote an existing stable slot");
                        }
                    }
                    case RENEW_TARGET_CHANNEL -> {
                        if (prior == null || prior.phase() != TargetChannelSlot.Phase.OPEN
                                || !Arrays.equals(prior.channel().digest(), request.expectedPriorDigest())) {
                            throw new IllegalStateException("channel renewal lacks its exact open predecessor");
                        }
                        channel.requireSuccessorOf(prior.channel());
                    }
                    case STOP_TARGET_CHANNEL_NEW_WORK -> {
                        if (prior == null) {
                            throw new IllegalStateException("channel stop lacks its earlier Source creation");
                        }
                        prior.channel().requireExactFrozenIdentity(channel);
                    }
                    default -> throw new IllegalStateException("unregistered channel lifecycle operation");
                }
                if (request.operationKind() != ControlOperationKind.STOP_TARGET_CHANNEL_NEW_WORK
                        && !created.add(Bytes.hex(channel.digest()))) {
                    throw new IllegalStateException("channel immutable generation has multiple creation events");
                }
                prior = new TargetChannelSlot(event);
            }
            final var current = bySlot.get(entry.getKey());
            if (current == null || prior == null
                    || !Arrays.equals(current.canonicalBytes(), prior.canonicalBytes())) {
                throw new IllegalStateException("current channel slot differs from its complete Source history fold");
            }
        }
        if (created.size() != byIdentity.size()) {
            throw new IllegalStateException("bare channel identity has no authenticated Source creation history");
        }
    }

}
