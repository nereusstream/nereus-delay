package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/** Current whole-topic permissions, independent of immutable historical Prepared authors. */
public final class TargetReceiptCurrentManagement {
    public enum Action { OBSERVE, BEGIN_INSTALLATION, CREATE_ACLS, ALTER_CONFIGS, ACTIVATE_INSTALLATION,
        BREAK_MANAGEMENT, PUBLISH_PROTECTION, PUBLISH_ANCHOR, BREAK_PROTECTION, CLOSE_EVIDENCE_OBJECT_REFERENCES,
        RETIRE_EVIDENCE_OBJECT_PAYLOAD }

    /** Trusted deployment authenticator; must acquire one consistent current identity/policy view. */
    @FunctionalInterface public interface Authenticator { Snapshot authenticate(); }
    /** Immutable local view; the root authenticator retains its own transports/resources, none transfer to this snapshot. */
    public interface Snapshot {
        ControlAuthorizationContext context();
        long policyVersion();
        byte[] policyDigest();
        /** Actual authenticator/authority confirms live identity/policy validity locally; not merely cached hashes. */
        void requireCurrent();
    }
    private record Entry(ControlAuthorizationContext subject, byte[] management, Set<Action> actions) {
        byte[] canonical() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.bytes(out, 1, subject.actorIdHash());
                CanonicalProtobuf.bytes(out, 2, subject.roleSet().canonicalBytes());
                CanonicalProtobuf.bytes(out, 3, subject.tenantResourceScopeHash());
                CanonicalProtobuf.bytes(out, 4, management);
                for (var action : Action.values()) if (actions.contains(action)) CanonicalProtobuf.uint32(out, 5, action.ordinal() + 1);
            });
        }
    }
    public static final class Inputs {
        private final List<Path> paths;
        private final long version, maximumBytes, pathBytes;
        private Inputs(List<Path> paths, long version, long maximumBytes, long pathBytes) {
            this.paths = List.copyOf(paths); this.version = version; this.maximumBytes = maximumBytes; this.pathBytes = pathBytes;
        }
        public long maximumRetainedBytes() { return maximumBytes; }
    }
    private final List<Entry> entries;
    private final long version;
    private final byte[] digest;
    private final Authenticator authenticator;
    private TargetReceiptCurrentManagement(List<Entry> entries, long version, Authenticator authenticator) {
        this.entries = List.copyOf(entries); this.version = version; this.authenticator = Objects.requireNonNull(authenticator);
        final byte[] image = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1); CanonicalProtobuf.uint64Bits(out, 2, version);
            for (var entry : entries) CanonicalProtobuf.bytes(out, 3, entry.canonical());
        });
        digest = Bytes.sha256(Bytes.utf8("nereus-delay-current-receipt-management-policy\0"), image);
    }
    public static Inputs prepare(Path path) throws IOException {
        final var f = TargetWorkerRuntimeConfigLoader.loadFields(path);
        final long version = f.unsigned64("policy.version"), maximum = f.number("policy.maximumInputBytes");
        final int count = f.integer("policy.entries");
        if (version == 0 || count < 1 || count > 509 || maximum < 1 || maximum > 16L * 1024 * 1024)
            throw new IllegalArgumentException("current management policy requires finite versioned root inputs");
        final var paths = new ArrayList<Path>(); long bytes = 24;
        for (int i = 0; i < count; i++) {
            final var entry = f.absolutePath("policy." + i + ".inputFile"); paths.add(entry);
            bytes = Math.addExact(bytes, Bytes.utf8(entry.toString()).length);
        }
        if (bytes > maximum) throw new IllegalArgumentException("current policy manifest exceeds its reserved input bound");
        f.requireComplete(); return new Inputs(paths, version, maximum, bytes);
    }
    public static TargetReceiptCurrentManagement load(Inputs inputs, Authenticator authenticator, Runnable guard) throws IOException {
        Objects.requireNonNull(inputs); Objects.requireNonNull(authenticator); Objects.requireNonNull(guard);
        final var entries = new ArrayList<Entry>(); final var identities = new HashSet<String>(); long bytes = inputs.pathBytes;
        for (var path : inputs.paths) {
            guard.run(); final var f = TargetWorkerRuntimeConfigLoader.loadFields(path);
            final var roles = EnumSet.noneOf(ControlRole.class);
            for (var role : f.text("roles").split(",", -1)) if (!roles.add(ControlRole.valueOf(role)))
                throw new IllegalArgumentException("duplicate current management role");
            if (!roles.contains(ControlRole.PLATFORM_OPERATOR)) throw new IllegalArgumentException("current management policy requires PLATFORM_OPERATOR");
            final var subject = new ControlAuthorizationContext(f.hex("actorIdHash", 32, 32), new ControlRoleSet(roles),
                    f.hex("tenantResourceScopeHash", 32, 32));
            final var management = TargetKafkaReceiptManagementImage.decode(f.image("managementImageFile", 8192));
            if (!Arrays.equals(management.topicIdentity(), f.hex("topicIdentityDigest", 32, 32)))
                throw new IllegalArgumentException("current management policy changes its pinned whole topic");
            final var actions = EnumSet.noneOf(Action.class);
            for (var action : f.text("actions").split(",", -1)) if (!actions.add(Action.valueOf(action)))
                throw new IllegalArgumentException("duplicate current management action");
            final var entry = new Entry(subject, management.canonicalBytes(), Set.copyOf(actions));
            final String id = Bytes.hex(Bytes.sha256(subject.actorIdHash(), subject.roleSet().digest(),
                    subject.tenantResourceScopeHash(), entry.management()));
            if (!identities.add(id)) throw new IllegalArgumentException("duplicate current whole-topic subject policy");
            entries.add(entry); bytes = Math.addExact(bytes, entry.canonical().length);
            if (bytes > inputs.maximumBytes) throw new IllegalArgumentException("current policy exceeds its reserved input bound");
            f.requireComplete(); guard.run();
        }
        guard.run(); return new TargetReceiptCurrentManagement(entries, inputs.version, authenticator);
    }
    public byte[] policyDigest() { return Bytes.copy(digest); }
    public long policyVersion() { return version; }

    public Permit acquire(TargetKafkaReceiptManagementImage management, Action action) {
        Objects.requireNonNull(management); Objects.requireNonNull(action);
        final Snapshot snapshot;
        try { snapshot = Objects.requireNonNull(authenticator.authenticate(), "actual current authentication snapshot"); }
        catch (RuntimeException failure) { throw new Denied("current operator authentication is unavailable", failure); }
        final var permit = new Permit(this, snapshot, Objects.requireNonNull(management).canonicalBytes(), Objects.requireNonNull(action));
        try { permit.requireCurrent(); return permit; }
        catch (RuntimeException | Error failure) {
            try { permit.close(); }
            catch (RuntimeException | Error cleanup) { if (cleanup != failure) cleanup.addSuppressed(failure); throw cleanup; }
            throw failure;
        }
    }
    private ControlAuthorizationContext require(Snapshot snapshot, byte[] management, Action action) {
        try {
            snapshot.requireCurrent();
            final var context = Objects.requireNonNull(snapshot.context(), "actual current CONTROL context");
            if (!context.roleSet().contains(ControlRole.PLATFORM_OPERATOR) || snapshot.policyVersion() != version
                    || !Arrays.equals(snapshot.policyDigest(), digest)) throw new Denied("current operator/policy view is stale or unauthorized");
            final boolean allowed = entries.stream().anyMatch(entry -> entry.actions().contains(action)
                    && Arrays.equals(entry.management(), management)
                    && Arrays.equals(entry.subject().actorIdHash(), context.actorIdHash())
                    && Arrays.equals(entry.subject().roleSet().digest(), context.roleSet().digest())
                    && Arrays.equals(entry.subject().tenantResourceScopeHash(), context.tenantResourceScopeHash()));
            if (!allowed) throw new Denied("current operator lacks this exact whole-topic action");
            snapshot.requireCurrent(); return context;
        } catch (Denied failure) { throw failure; }
        catch (RuntimeException failure) { throw new Denied("current identity/policy validity cannot be proved", failure); }
    }
    public static final class Permit implements AutoCloseable {
        private final TargetReceiptCurrentManagement authority;
        private final Snapshot snapshot;
        private final byte[] management;
        private final Action action;
        private boolean closed;
        private ControlAuthorizationContext subject;
        private Permit(TargetReceiptCurrentManagement authority, Snapshot snapshot, byte[] management, Action action) {
            this.authority = authority; this.snapshot = snapshot; this.management = management; this.action = action;
        }
        public void requireCurrent() {
            if (closed) throw new Denied("current management view is already closed");
            final var current = authority.require(snapshot, management, action);
            if (subject == null) subject = current;
            else if (!Arrays.equals(subject.actorIdHash(), current.actorIdHash())
                    || !Arrays.equals(subject.roleSet().digest(), current.roleSet().digest())
                    || !Arrays.equals(subject.tenantResourceScopeHash(), current.tenantResourceScopeHash()))
                throw new Denied("current authentication subject changed within one action view");
        }
        @Override public void close() { closed = true; }
    }
    public static final class Denied extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        public Denied(String message) { super(message); }
        public Denied(String message, Throwable failure) { super(message, failure); }
    }
}
