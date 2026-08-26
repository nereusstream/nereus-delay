package com.nereusstream.delay.protocol;

/** Canonical body common interface for the closed Profile semantic envelope. */
public interface ProfileSemanticBody {
    ProfileKind profileKind();

    int schemaVersion();

    byte[] canonicalBytes();
}
