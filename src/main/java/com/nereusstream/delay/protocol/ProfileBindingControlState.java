package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Replay-stable local source-ordered Profile first-binding markers. */
public final class ProfileBindingControlState {
    private static final int STATE_VERSION = 1;

    private final List<ActivationMarker> activations;
    private final List<BindingClosure> closures;

    private ProfileBindingControlState(final List<ActivationMarker> activations, final List<BindingClosure> closures) {
        this.activations = validateActivations(activations);
        this.closures = validateClosures(closures, this.activations);
    }

    public static ProfileBindingControlState empty() {
        return new ProfileBindingControlState(List.of(), List.of());
    }

    public List<ActivationMarker> activations() {
        return activations;
    }

    public List<BindingClosure> closures() {
        return closures;
    }

    public boolean hasMarkers() {
        return !activations.isEmpty();
    }

    public ProfileAcceptanceV1 firstBindingAcceptance(final ProfileRefV1 profile, final SourcePosition sourcePosition) {
        Objects.requireNonNull(profile, "profile");
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        final ActivationMarker activation = activationFor(profile);
        if (activation == null || compare(activation.sourcePosition(), sourcePosition) > 0) {
            return ProfileAcceptanceV1.ABSENT;
        }
        final BindingClosure closure = closureFor(profile);
        return closure != null && compare(closure.sourcePosition(), sourcePosition) <= 0
                ? ProfileAcceptanceV1.CLOSED_FOR_FIRST_BINDING
                : ProfileAcceptanceV1.ACTIVE_FOR_FIRST_BINDING;
    }

    public ProfileBindingControlState activate(final ProfileRefV1 profile, final SourcePosition sourcePosition) {
        Objects.requireNonNull(profile, "profile");
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        final ActivationMarker existing = activationFor(profile);
        if (existing != null) {
            if (compare(existing.sourcePosition(), sourcePosition) == 0) {
                return this;
            }
            throw new IllegalArgumentException("Profile version is already activated");
        }
        if (!activations.isEmpty()
                && compare(
                                sourcePosition,
                                activations.get(activations.size() - 1).sourcePosition())
                        <= 0) {
            throw new IllegalArgumentException("Profile activation source position regressed");
        }
        final List<ActivationMarker> next = new ArrayList<>(activations);
        next.add(new ActivationMarker(profile, sourcePosition));
        return new ProfileBindingControlState(next, closures);
    }

    public ProfileBindingControlState close(
            final ProfileNewBindingClosePayloadV1 payload, final SourcePosition sourcePosition) {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        final ActivationMarker activation = activationFor(payload.profile());
        if (activation == null || compare(sourcePosition, activation.sourcePosition()) <= 0) {
            throw new IllegalArgumentException("Profile close must follow activation");
        }
        final BindingClosure existing = closureFor(payload.profile());
        if (existing != null) {
            if (compare(existing.sourcePosition(), sourcePosition) == 0
                    && existing.reason().equals(payload.reason())) {
                return this;
            }
            throw new IllegalArgumentException("Profile first-binding closure already exists");
        }
        final List<BindingClosure> next = new ArrayList<>(closures);
        next.add(new BindingClosure(payload.profile(), sourcePosition, payload.reason()));
        next.sort(Comparator.comparing(
                (BindingClosure value) -> value.profile().canonicalBytes(), ProfileBindingControlState::compareBytes));
        return new ProfileBindingControlState(activations, next);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, STATE_VERSION);
            for (ActivationMarker activation : activations) {
                CanonicalProtobuf.bytes(output, 2, activation.canonicalBytes());
            }
            for (BindingClosure closure : closures) {
                CanonicalProtobuf.bytes(output, 3, closure.canonicalBytes());
            }
        });
    }

    public static ProfileBindingControlState decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = readRepeated(encoded, "ProfileBindingControlState");
        if (fields.get(0).number() != 1 || QueryCodecSupport.uint32(fields.get(0), 1) != STATE_VERSION) {
            throw new IllegalArgumentException("invalid ProfileBindingControlState version");
        }
        final List<ActivationMarker> activations = new ArrayList<>();
        final List<BindingClosure> closures = new ArrayList<>();
        int phase = 2;
        for (int index = 1; index < fields.size(); index++) {
            final CanonicalProtobuf.Reader.Field field = fields.get(index);
            if (field.number() == 2 && phase == 2) {
                activations.add(ActivationMarker.decode(QueryCodecSupport.nested(field, 2)));
            } else if (field.number() == 3) {
                phase = 3;
                closures.add(BindingClosure.decode(QueryCodecSupport.nested(field, 3)));
            } else {
                throw new IllegalArgumentException("ProfileBindingControlState fields are out of order");
            }
        }
        final ProfileBindingControlState result = new ProfileBindingControlState(activations, closures);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ProfileBindingControlState");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ProfileBindingControlState that
                && activations.equals(that.activations)
                && closures.equals(that.closures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(activations, closures);
    }

    private ActivationMarker activationFor(final ProfileRefV1 profile) {
        return activations.stream()
                .filter(marker -> marker.profile().equals(profile))
                .findFirst()
                .orElse(null);
    }

    private BindingClosure closureFor(final ProfileRefV1 profile) {
        return closures.stream()
                .filter(marker -> marker.profile().equals(profile))
                .findFirst()
                .orElse(null);
    }

    private static List<ActivationMarker> validateActivations(final List<ActivationMarker> values) {
        Objects.requireNonNull(values, "activations");
        final List<ActivationMarker> result = new ArrayList<>(values.size());
        SourcePosition previous = null;
        for (ActivationMarker value : values) {
            Objects.requireNonNull(value, "activation marker");
            if (previous != null && compare(value.sourcePosition(), previous) <= 0) {
                throw new IllegalArgumentException("Profile activations are not source ordered");
            }
            if (result.stream().anyMatch(marker -> marker.profile().equals(value.profile()))) {
                throw new IllegalArgumentException("Profile version has duplicate activation markers");
            }
            result.add(value);
            previous = value.sourcePosition();
        }
        return List.copyOf(result);
    }

    private static List<BindingClosure> validateClosures(
            final List<BindingClosure> values, final List<ActivationMarker> activations) {
        Objects.requireNonNull(values, "closures");
        final List<BindingClosure> result = new ArrayList<>(values);
        result.sort(Comparator.comparing(
                (BindingClosure value) -> value.profile().canonicalBytes(), ProfileBindingControlState::compareBytes));
        BindingClosure previous = null;
        for (BindingClosure value : result) {
            Objects.requireNonNull(value, "binding closure");
            if (previous != null && previous.profile().equals(value.profile())) {
                throw new IllegalArgumentException("Profile version has duplicate close markers");
            }
            final ActivationMarker activation = activations.stream()
                    .filter(marker -> marker.profile().equals(value.profile()))
                    .findFirst()
                    .orElse(null);
            if (activation == null || compare(value.sourcePosition(), activation.sourcePosition()) <= 0) {
                throw new IllegalArgumentException("Profile close marker precedes activation");
            }
            previous = value;
        }
        return List.copyOf(result);
    }

    private static int compare(final SourcePosition left, final SourcePosition right) {
        try {
            final int order = left.compareTo(right);
            if (order == 0 && !Bytes.constantTimeEquals(left.canonicalBytes(), right.canonicalBytes())) {
                throw new IllegalArgumentException("Profile marker source position has conflicting canonical identity");
            }
            return order;
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("Profile markers use different source identities", exception);
        }
    }

    private static int compareBytes(final byte[] left, final byte[] right) {
        final int length = Math.min(left.length, right.length);
        for (int index = 0; index < length; index++) {
            final int result = Byte.toUnsignedInt(left[index]) - Byte.toUnsignedInt(right[index]);
            if (result != 0) {
                return result;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    private static List<CanonicalProtobuf.Reader.Field> readRepeated(final byte[] encoded, final String name) {
        Objects.requireNonNull(encoded, name);
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException(name + " is empty");
        }
        return fields;
    }

    public record ActivationMarker(ProfileRefV1 profile, SourcePosition sourcePosition) {
        public ActivationMarker {
            Objects.requireNonNull(profile, "profile");
            Objects.requireNonNull(sourcePosition, "sourcePosition");
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes());
                CanonicalProtobuf.bytes(output, 2, sourcePosition.canonicalBytes());
            });
        }

        private static ActivationMarker decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields =
                    QueryCodecSupport.read(encoded, "ProfileBindingActivationMarker");
            QueryCodecSupport.requireNumbers(fields, new int[] {1, 2}, "ProfileBindingActivationMarker");
            final ActivationMarker result = new ActivationMarker(
                    ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(0), 1)), decodePosition(fields.get(1)));
            QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ProfileBindingActivationMarker");
            return result;
        }
    }

    public record BindingClosure(ProfileRefV1 profile, SourcePosition sourcePosition, ControlReasonV1 reason) {
        public BindingClosure {
            Objects.requireNonNull(profile, "profile");
            Objects.requireNonNull(sourcePosition, "sourcePosition");
            Objects.requireNonNull(reason, "reason");
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes());
                CanonicalProtobuf.bytes(output, 2, sourcePosition.canonicalBytes());
                CanonicalProtobuf.bytes(output, 3, reason.canonicalBytes());
            });
        }

        private static BindingClosure decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields =
                    QueryCodecSupport.read(encoded, "ProfileBindingClosure");
            QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "ProfileBindingClosure");
            final BindingClosure result = new BindingClosure(
                    ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                    decodePosition(fields.get(1)),
                    ControlReasonV1.decode(QueryCodecSupport.nested(fields.get(2), 3)));
            QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ProfileBindingClosure");
            return result;
        }
    }

    private static SourcePosition decodePosition(final CanonicalProtobuf.Reader.Field field) {
        final byte[] encoded = QueryCodecSupport.bytes(field, field.number());
        final SourcePosition result = SourcePositionCodec.decode(encoded);
        if (!Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("source position is not canonical");
        }
        return result;
    }
}
