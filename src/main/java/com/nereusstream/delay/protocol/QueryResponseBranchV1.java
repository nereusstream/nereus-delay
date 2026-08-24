package com.nereusstream.delay.protocol;

/** Marker for a branch of a closed public query response union. */
public sealed interface QueryResponseBranchV1
        permits PublicQueryErrorV1,
                PendingCommandViewV1,
                PublicCommandResultV1,
                CompactCommandResultV1,
                EmptyResultV1,
                ReservedMessageViewV1,
                ActiveMessageViewV1,
                TerminalMessageViewV1,
                IdentityRetiredMessageViewV1,
                UnknownMessageViewV1 {
    byte[] canonicalBytes();
}
