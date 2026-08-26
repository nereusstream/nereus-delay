package com.nereusstream.delay.protocol;

/** Marker for a branch of a closed public query response union. */
public sealed interface QueryResponseBranch
        permits PublicQueryError,
                PendingCommandView,
                PublicCommandResult,
                CompactCommandResult,
                EmptyResult,
                ReservedMessageView,
                ActiveMessageView,
                TerminalMessageView,
                IdentityRetiredMessageView,
                UnknownMessageView {
    byte[] canonicalBytes();
}
