package com.nereusstream.delay.submission;

/** Closed authority union used to prevent route reselection during submit. */
public sealed interface SubmissionRouteAuthority permits ManagedRouteAuthority, NativeTargetAuthority {}
