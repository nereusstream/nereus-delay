package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.*;
import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.*;
import java.util.*;

/** The two original closed-domain clauses that do not require an old mutable business interpreter. */
public final class TargetMigrationArchiveClosedContract {
    private TargetMigrationArchiveClosedContract() {}
    public enum Family { SCHEDULE, PREPARE, COMMIT, RESCHEDULE, CANCEL, QUERY, TERMINAL, SYSTEM, CONTROL, NEW_SCOPE, REOPEN }
    public enum Clause { CONSUMPTION_AUTHORIZATION_REVOKED, REGISTERED_RETRY_CEILING }

    /** Complete original facts. These DTOs grant nothing; the actual old authority authenticates every field. */
    public record Domain(byte[] producerIdentity, TargetQuotaScope scope, SourcePosition registeredAt,
            SourcePosition closedAt, List<ProtocolTuple> tuples, Set<Family> families, Clause clause,
            Long maximumRetryUntil, SourceReplayMutation fence, SystemMutationResult fenceFirst,
            byte[] registration, byte[] termination, byte[] permanentReopenGuard) {
        public Domain {
            producerIdentity = material(producerIdentity); registration = material(registration);
            termination = material(termination); permanentReopenGuard = material(permanentReopenGuard);
            Objects.requireNonNull(scope); Objects.requireNonNull(registeredAt); Objects.requireNonNull(closedAt);
            Objects.requireNonNull(clause); tuples = List.copyOf(tuples); families = Set.copyOf(families);
            if (scope.target() != null || tuples.isEmpty() || families.isEmpty()
                    || !registeredAt.sameSourceIdentity(closedAt) || registeredAt.compareTo(closedAt) >= 0
                    || maximumRetryUntil != null && maximumRetryUntil < 0
                    || (fence == null) != (fenceFirst == null))
                throw new IllegalArgumentException("incomplete original producer/registration/closure domain");
            final var unique = new HashSet<ProtocolTuple>();
            for (var tuple : tuples) if (!unique.add(tuple)) throw new IllegalArgumentException("repeated original domain tuple");
        }
        @Override public byte[] producerIdentity() { return Bytes.copy(producerIdentity); }
        @Override public byte[] registration() { return Bytes.copy(registration); }
        @Override public byte[] termination() { return Bytes.copy(termination); }
        @Override public byte[] permanentReopenGuard() { return Bytes.copy(permanentReopenGuard); }
        public byte[] canonicalBytes() { return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, producerIdentity); CanonicalProtobuf.bytes(out, 2, scope.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, registeredAt.canonicalBytes()); CanonicalProtobuf.bytes(out, 4, closedAt.canonicalBytes());
            for (var tuple : tuples) CanonicalProtobuf.bytes(out, 5, tuple.canonicalBytes());
            for (var family : Family.values()) if (families.contains(family)) CanonicalProtobuf.uint32(out, 6, family.ordinal() + 1);
            CanonicalProtobuf.uint32(out, 7, clause.ordinal() + 1);
            if (maximumRetryUntil != null) CanonicalProtobuf.uint64(out, 8, maximumRetryUntil);
            if (fence != null) { CanonicalProtobuf.bytes(out, 9, fence.mutation().encodeFrame());
                CanonicalProtobuf.bytes(out, 10, fence.position().canonicalBytes()); CanonicalProtobuf.bytes(out, 11, fenceFirst.encode()); }
            CanonicalProtobuf.bytes(out, 12, registration); CanonicalProtobuf.bytes(out, 13, termination);
            CanonicalProtobuf.bytes(out, 14, permanentReopenGuard);
        }); }
    }

    /**
     * Actual new-owner loan over the complete original producer/writer/registration/control inventory.
     * Authentication must use the original issuer/registry/history pins, not a key or clause from this DTO.
     * The permanent guard covers every old scope creator and remains bound to this I/T/retirement tombstone.
     * These methods authenticate raw facts and protection; they do not return readiness or a per-frame decision.
     */
    public interface Materials {
        TargetMigrationObjectReference inventory();
        byte[] inventoryBytes();
        long domainCount();
        List<Domain> domains();
        Set<Family> coveredFamilies();
        byte[] permanentScopeGuard();
        void requireAuthenticInventory(TargetMigrationRoot root, TargetMigrationRetirementRecord retirement);
        void requireAuthenticDomain(Domain exact);
        byte[] authenticatedProducer(SourceReplayEntry actual);
        ProtocolTuple authenticatedTuple(SourceReplayEntry actual);
        void requireEntryBinding(Domain exact, SourceReplayEntry actual);
        void requireCurrent();
        void requireLocalCurrent();
    }

    public static final class Qualification {
        private final Materials materials;
        private final TargetMigrationRoot root;
        private final TargetMigrationRetirementRecord retirement;
        private final TargetMigrationArchiveState state;
        private final Map<String, Domain> domains;
        private final byte[] raw;
        private Qualification(Materials materials, TargetMigrationRoot root, TargetMigrationRetirementRecord retirement,
                TargetMigrationArchiveState state, Map<String, Domain> domains, byte[] raw) {
            this.materials = materials; this.root = root; this.retirement = retirement; this.state = state;
            this.domains = Map.copyOf(domains); this.raw = raw;
        }
        public byte[] rawMaterials() { return Bytes.copy(raw); }
        public byte[] digest() { return Bytes.sha256(raw); }
        public void requireCurrent() { materials.requireLocalCurrent(); }
        public void requireState(TargetMigrationRoot actualRoot, TargetMigrationRetirementRecord actualI, TargetMigrationArchiveState actual) {
            requireCurrent();
            if (!root.equals(actualRoot) || !retirement.intentAt().equals(actualI.intentAt()) || !state.equals(actual))
                throw new IllegalStateException("closed-domain qualification changed exact original Root/I/Source cut");
        }
    }

    public static Qualification qualify(Materials materials, TargetMigrationRoot root,
            TargetMigrationRetirementRecord retirement, TargetMigrationArchiveState state) {
        Objects.requireNonNull(materials); retirement.requireRoot(root); state.requireRetirement(root, retirement);
        materials.requireCurrent(); materials.requireAuthenticInventory(root, retirement);
        final byte[] inventory = material(materials.inventoryBytes()); materials.inventory().requireContent(inventory);
        material(materials.permanentScopeGuard());
        if (!materials.coveredFamilies().equals(EnumSet.allOf(Family.class)))
            throw unavailable("old query/cancel/terminal/control/new-scope namespace is not completely replaced or originally closed");
        final var rows = List.copyOf(materials.domains());
        if (materials.domainCount() < 0 || materials.domainCount() != rows.size())
            throw unavailable("original registered producer/writer inventory is incomplete");
        final Map<String, Domain> domains = new TreeMap<>();
        for (var domain : rows) {
            materials.requireAuthenticDomain(domain);
            if (!domain.scope().equals(root.plan().scope()) || !domain.closedAt().sameSourceIdentity(state.coveredThrough().source())
                    || domain.closedAt().compareTo(state.coveredThrough().source()) > 0)
                throw unavailable("original closure is outside this actual Source/scope cut");
            if (domain.clause() == Clause.REGISTERED_RETRY_CEILING) requireFence(domain, state.coveredThrough());
            if (domains.putIfAbsent(Bytes.hex(domain.producerIdentity()), domain) != null)
                throw new IllegalArgumentException("original producer identity is split across closure domains");
        }
        final byte[] raw = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1); CanonicalProtobuf.bytes(out, 2, materials.inventory().canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, inventory); CanonicalProtobuf.bytes(out, 4, root.plan().digest());
            CanonicalProtobuf.bytes(out, 5, retirement.intention().digest()); CanonicalProtobuf.bytes(out, 6, retirement.intentAt().canonicalBytes());
            CanonicalProtobuf.bytes(out, 7, state.coveredThrough().canonicalBytes());
            CanonicalProtobuf.bytes(out, 8, material(materials.permanentScopeGuard()));
            for (var domain : domains.values()) CanonicalProtobuf.bytes(out, 9, domain.canonicalBytes());
        });
        materials.requireCurrent();
        return new Qualification(materials, root, retirement, state, domains, raw);
    }

    private static void requireFence(Domain domain, TargetQuotaMutation cut) {
        if (domain.maximumRetryUntil() == null || domain.fence() == null) throw unavailable("original registration has no finite retry ceiling and applied fence");
        final var input = domain.fence().mutation(); final var first = domain.fenceFirst();
        final var body = TargetTimeFenceBody.decode(input.canonicalBody());
        if (input.type() != SystemMutationType.TIME_FENCE || !body.shard().equals(domain.scope().shard())
                || !Arrays.equals(first.mutationId(), input.systemMutationId()) || !Arrays.equals(first.mutationHash(), input.mutationHash())
                || first.mutationType() != input.type() || first.applyStatus() != ApplyStatus.APPLIED || first.stableCode() != StableCode.OK
                || !Arrays.equals(first.authorIdentity(), input.authorIdentity()) || first.retryUntilEpochMs() != input.retryUntilEpochMs()
                || !Arrays.equals(first.appliedSourcePosition(), domain.fence().position().canonicalBytes())
                || !domain.fence().position().sameSourceIdentity(cut.source()) || domain.fence().position().compareTo(cut.source()) > 0
                || body.closeThrough() < domain.maximumRetryUntil())
            throw unavailable("original retry ceiling is not covered by its actual APPLIED same-scope fence");
    }

    public static final class Decision {
        private final Qualification qualification;
        private final SourceReplayEntry actual;
        private final SourceReplayOutcome outcome;
        private final TargetQuotaMutation before, at;
        private final long fence;
        private final boolean logical;
        private final byte[] proof;
        private Decision(Qualification q, SourceReplayEntry actual, SourceReplayOutcome outcome,
                TargetQuotaMutation before, long fence, boolean logical, Domain domain) {
            qualification = q; this.actual = actual; this.outcome = outcome; this.before = before; this.fence = fence; this.logical = logical;
            at = new TargetQuotaMutation(TargetQuotaMutation.increment(before.sequence()), actual.position(), sourceDigest(actual));
            proof = CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, 1); CanonicalProtobuf.bytes(out, 2, q.rawMaterials());
                CanonicalProtobuf.bytes(out, 3, domain.canonicalBytes()); CanonicalProtobuf.bytes(out, 4, frame(actual));
                CanonicalProtobuf.bytes(out, 5, actual.position().canonicalBytes()); CanonicalProtobuf.bytes(out, 6, before.canonicalBytes());
                CanonicalProtobuf.bytes(out, 7, Bytes.u64beBits(fence)); CanonicalProtobuf.uint32(out, 8, logical ? 1 : 0);
                CanonicalProtobuf.bytes(out, 9, outcome.isCommand() ? outcome.commandResult().encode() : outcome.systemMutationResult().encode());
            });
        }
        public SourceReplayEntry actual() { return actual; }
        public SourceReplayOutcome outcome() { return outcome; }
        public TargetQuotaMutation before() { return before; }
        public TargetQuotaMutation at() { return at; }
        public long fence() { return fence; }
        public boolean logicalFirst() { return logical; }
        public boolean physicalOnly() { return !logical; }
        public byte[] proofBytes() { return Bytes.copy(proof); }
        public void requireCurrent() { qualification.requireCurrent(); }
        public void requireReader(TargetStoreBackend.Reader reader, TargetMigrationArchiveStore.AppliedTakeover takeover, SourceReplayEntry original) {
            requireCurrent();
            if (!reader.aggregate().mutation().equals(before) || reader.closedIngressDeadlineThrough() != fence
                    || !Arrays.equals(frame(actual), frame(original)) || !Arrays.equals(actual.position().canonicalBytes(), original.position().canonicalBytes())
                    || !takeover.migration().equals(qualification.root) || !takeover.retirement().intentAt().equals(qualification.retirement.intentAt()))
                throw new IllegalStateException("closed interpretation changed actual T/Native predecessor/frame/Source/fence");
        }
        public List<TargetMigrationArchiveEntry> logicalEntries(TargetMigrationRecordReference received) {
            if (!logical) return List.of();
            if (actual instanceof SourceReplayRecord command) {
                final var result = outcome.commandResult();
                return List.of(new TargetMigrationArchiveEntry(KeyCodec.dedupeCommand(command.command().commandId()),
                            ValueEnvelope.encode(1, new CommandDedupeRecord(command.command().protocolTuple(), command.command().commandHash(), result).encode()),
                            TargetMigrationArchiveEntry.Origin.LEGACY_SOURCE_FIRST, actual.position(), at, received),
                        new TargetMigrationArchiveEntry(KeyCodec.dedupeResult(command.command().commandId()), ValueEnvelope.encode(2, result.encode()),
                            TargetMigrationArchiveEntry.Origin.LEGACY_SOURCE_FIRST, actual.position(), at, received));
            }
            final var input = ((SourceReplayMutation) actual).mutation();
            return List.of(new TargetMigrationArchiveEntry(KeyCodec.dedupeSystemMutation(input.systemMutationId()),
                    ValueEnvelope.encode(SystemMutationResult.VALUE_TYPE, outcome.systemMutationResult().encode()),
                    TargetMigrationArchiveEntry.Origin.LEGACY_SOURCE_FIRST, actual.position(), at, received));
        }
    }

    public static Decision decide(Qualification q, SourceReplayEntry actual, TargetMigrationArchiveRow auxiliary,
            TargetQuotaMutation actualBefore, long actualFence) {
        q.materials.requireCurrent();
        if (!q.state.taken()) throw unavailable("closed interpreter requires the actual permanent Source TAKEOVER");
        if (!actual.position().sameSourceIdentity(q.state.coveredThrough().source()) || actual.position().compareTo(actualBefore.source()) <= 0)
            throw new IllegalArgumentException("closed interpreter changed actual physical Source order");
        if (auxiliary != null) { auxiliary.requirePlan(q.root.plan()); TargetMigrationArchiveSemantics.requireEntry(auxiliary.entry(), q.root.plan()); }
        final byte[] producer = material(q.materials.authenticatedProducer(actual));
        final var domain = q.domains.get(Bytes.hex(producer));
        if (domain == null) throw unavailable("post-T frame contradicts the complete original producer domain; preserve frame and stop roll-forward");
        q.materials.requireEntryBinding(domain, actual);
        final ProtocolTuple tuple = Objects.requireNonNull(q.materials.authenticatedTuple(actual), "original authenticated registered tuple");
        if (actual instanceof SourceReplayRecord c && !c.command().protocolTuple().equals(tuple)
                || tuple.recordKind() != (actual instanceof SourceReplayRecord ? ProtocolTuple.CLIENT_COMMAND : ProtocolTuple.SYSTEM_MUTATION))
            throw unavailable("actual frame and authenticated original producer protocol differ");
        if (!domain.tuples().contains(tuple)) throw unavailable("post-T tuple is outside original registered closure; preserve original frame");
        final long retry = actual instanceof SourceReplayRecord c ? c.command().retryUntilEpochMs() : ((SourceReplayMutation) actual).mutation().retryUntilEpochMs();
        if (domain.maximumRetryUntil() != null && retry > domain.maximumRetryUntil())
            throw unavailable("frame exceeds the original immutable retry registration; no invented TTL/rejection");
        final long originalDomainFence = domain.clause() == Clause.REGISTERED_RETRY_CEILING
                ? TargetTimeFenceBody.decode(domain.fence().mutation().canonicalBody()).closeThrough() : IngressFenceState.OPEN;
        final boolean expired = actual.position().brokerPersistenceTimeEpochMs() > retry || actualFence >= retry || originalDomainFence >= retry;
        if (domain.clause() == Clause.REGISTERED_RETRY_CEILING && !expired)
            throw unavailable("still-legal uncovered old business contradicts TAKEOVER qualification; preserve frame and stop");
        final SourceReplayOutcome outcome;
        if (actual instanceof SourceReplayRecord command) {
            final var result = new CommandResult(ApplyStatus.REJECTED, expired ? StableCode.COMMAND_RETRY_WINDOW_EXPIRED : StableCode.UNAUTHORIZED,
                    -1, 0, null, actual.position().canonicalBytes());
            outcome = SourceReplayOutcome.command(actual.position(), result);
        } else {
            final var input = ((SourceReplayMutation) actual).mutation();
            outcome = SourceReplayOutcome.systemMutation(actual.position(), SystemMutationResult.from(input, ApplyStatus.REJECTED,
                    expired ? StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED : StableCode.UNAUTHORIZED_SYSTEM_MUTATION, actual.position().canonicalBytes()));
        }
        return new Decision(q, actual, outcome, actualBefore, actualFence, !(actual instanceof SourceReplayRecord) || !expired, domain);
    }

    /** Recomputes the finite original clause; original issuer/producer/registration pins remain the receiver's authority. */
    public static void requireClosedProof(byte[] fullProof, TargetMigrationRoot root,
            TargetMigrationRetirementRecord retirement, TargetMigrationArchiveRow row) {
        retirement.requireRoot(root); row.requirePlan(root.plan());
        final var f = QueryCodecSupport.read(fullProof, "complete original closed interpretation");
        QueryCodecSupport.requireNumbers(f, new int[] {1,2,3,4,5,6,7,8,9}, "original closed interpretation");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) throw new IllegalArgumentException("unknown original closed interpretation");
        final byte[] qualified = QueryCodecSupport.bytes(f.get(1), 2), domainBytes = QueryCodecSupport.bytes(f.get(2), 3);
        final var q = QueryCodecSupport.read(qualified, "complete original closed domain inventory", true);
        if (q.size() < 8 || QueryCodecSupport.uint32(q.getFirst(), 1) != 1
                || !Arrays.equals(QueryCodecSupport.fixed(q.get(3), 4, 32), root.plan().digest())
                || !Arrays.equals(QueryCodecSupport.fixed(q.get(4), 5, 32), retirement.intention().digest())
                || !TargetQuotaMutation.decode(QueryCodecSupport.bytes(q.get(5), 6)).equals(retirement.intentAt()))
            throw new IllegalStateException("closed proof changed original Root/I/domain inventory");
        TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(q.get(1), 2)).requireContent(QueryCodecSupport.bytes(q.get(2), 3));
        material(QueryCodecSupport.bytes(q.get(7), 8));
        final var qualifiedCut = TargetQuotaMutation.decode(QueryCodecSupport.bytes(q.get(6), 7));
        Domain selected = null; final var identities = new HashSet<String>();
        for (int index = 8; index < q.size(); index++) {
            final byte[] raw = QueryCodecSupport.bytes(q.get(index), 9); final var domain = decodeDomain(raw);
            if (!identities.add(Bytes.hex(domain.producerIdentity())) || !domain.scope().equals(root.plan().scope())
                    || !domain.closedAt().sameSourceIdentity(qualifiedCut.source()) || domain.closedAt().compareTo(qualifiedCut.source()) > 0)
                throw new IllegalStateException("closed proof split or changed original complete registered domains");
            if (domain.clause() == Clause.REGISTERED_RETRY_CEILING) requireFence(domain, qualifiedCut);
            if (Arrays.equals(raw, domainBytes)) selected = domain;
        }
        final var domain = Objects.requireNonNull(selected, "selected original producer domain is absent from fixed inventory");
        final byte[] frame = QueryCodecSupport.bytes(f.get(3), 4);
        final var source = TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(4), 5));
        final var before = TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(5), 6));
        final byte[] fenceRaw = QueryCodecSupport.bytes(f.get(6), 7);
        if (fenceRaw.length != 8) throw new IllegalArgumentException("closed proof fence is not its complete original long");
        final long fence = java.nio.ByteBuffer.wrap(fenceRaw).getLong();
        final int logical = QueryCodecSupport.uint32(f.get(7), 8);
        if (logical > 1 || !Arrays.equals(source.canonicalBytes(), row.materializedAt().source().canonicalBytes())
                || row.materializedAt().sequence() != TargetQuotaMutation.increment(before.sequence())
                || !Arrays.equals(frame, row.sourceFrame()) || source.compareTo(before.source()) <= 0
                || qualifiedCut.source().compareTo(source) >= 0)
            throw new IllegalStateException("closed proof changed actual original frame/Source/predecessor");
        final var wire = ShardLogFrame.decode(frame); final byte[] expected;
        if (wire.recordKind() == ProtocolTuple.CLIENT_COMMAND) {
            final var command = CommandCodec.decodeFrame(frame);
            if (!domain.tuples().contains(command.protocolTuple()) || !Arrays.equals(command.canonicalFrameDigest(), row.materializedAt().mutationDigest()))
                throw new IllegalStateException("closed Command changed original registered protocol/frame hash");
            final boolean expired = expired(domain, command.retryUntilEpochMs(), source, fence);
            if (logical == (expired ? 1 : 0)) throw new IllegalStateException("expired original Command was relabeled as a logical first");
            final var result = new CommandResult(ApplyStatus.REJECTED, expired ? StableCode.COMMAND_RETRY_WINDOW_EXPIRED : StableCode.UNAUTHORIZED,
                    -1, 0, null, source.canonicalBytes()); expected = result.encode();
            if (row.entry().namespace() == 1) {
                final var first = CommandDedupeRecord.decode(TargetValueEnvelope.decode(row.entry().rawValue(), 1).payload());
                if (logical == 0 || !Arrays.equals(row.entry().originalKey(), KeyCodec.dedupeCommand(command.commandId()))
                        || !first.protocolTuple().equals(command.protocolTuple()) || !Arrays.equals(first.commandHash(), command.commandHash())
                        || !first.result().equals(result)) throw new IllegalStateException("closed Command first changed exact original result");
            } else if (row.entry().namespace() == 2 && (logical == 0 || !Arrays.equals(row.entry().originalKey(), KeyCodec.dedupeResult(command.commandId()))
                    || !Arrays.equals(TargetValueEnvelope.decode(row.entry().rawValue(), 2).payload(), expected)))
                throw new IllegalStateException("closed query result changed exact original Command outcome");
        } else {
            final var input = SystemMutation.decodeTargetFrame(frame);
            if (domain.tuples().stream().noneMatch(tuple -> tuple.recordKind() == ProtocolTuple.SYSTEM_MUTATION)
                    || !Arrays.equals(Bytes.sha256(input.canonicalEnvelope()), row.materializedAt().mutationDigest()) || logical != 1)
                throw new IllegalStateException("closed System changed its original registered frame or first semantics");
            final boolean expired = expired(domain, input.retryUntilEpochMs(), source, fence);
            expected = SystemMutationResult.from(input, ApplyStatus.REJECTED,
                    expired ? StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED : StableCode.UNAUTHORIZED_SYSTEM_MUTATION, source.canonicalBytes()).encode();
            if (row.entry().namespace() == 5 && (!Arrays.equals(row.entry().originalKey(), KeyCodec.dedupeSystemMutation(input.systemMutationId()))
                    || !Arrays.equals(TargetValueEnvelope.decode(row.entry().rawValue(), SystemMutationResult.VALUE_TYPE).payload(), expected)))
                throw new IllegalStateException("closed System first changed original immutable fields/outcome");
        }
        if (!Arrays.equals(QueryCodecSupport.bytes(f.get(8), 9), expected))
            throw new IllegalStateException("closed original clause does not produce the recorded actual outcome");
        if (row.entry().namespace() == 3) {
            final var physical = TargetMigrationArchivePhysicalRecord.decode(TargetValueEnvelope.decode(row.entry().rawValue(), TargetMigrationArchiveRow.VALUE_TYPE).payload());
            if (!Arrays.equals(physical.outcome(), expected) || (physical.firstOriginalKey() == null) != (logical == 0))
                throw new IllegalStateException("closed physical evidence changed its actual first/no-first decision");
        }
    }

    private static boolean expired(Domain domain, long retry, SourcePosition source, long fence) {
        if (domain.maximumRetryUntil() != null && retry > domain.maximumRetryUntil()) throw new IllegalStateException("closed proof invented a retry bound");
        final boolean expired = source.brokerPersistenceTimeEpochMs() > retry || fence >= retry
                || domain.clause() == Clause.REGISTERED_RETRY_CEILING && TargetTimeFenceBody.decode(domain.fence().mutation().canonicalBody()).closeThrough() >= retry;
        if (!expired && domain.clause() != Clause.CONSUMPTION_AUTHORIZATION_REVOKED)
            throw new IllegalStateException("still-legal old business has no closed interpretation");
        return expired;
    }

    private static Domain decodeDomain(byte[] raw) {
        final var f = QueryCodecSupport.read(raw, "complete original producer closure", true);
        if (f.size() < 10) throw new IllegalArgumentException("incomplete original producer closure");
        int i = 4; final var tuples = new ArrayList<ProtocolTuple>(); final var families = EnumSet.noneOf(Family.class);
        while (i < f.size() && f.get(i).number() == 5) tuples.add(ProtocolTuple.decode(QueryCodecSupport.bytes(f.get(i++), 5)));
        while (i < f.size() && f.get(i).number() == 6) {
            final int family = QueryCodecSupport.uint32(f.get(i++), 6);
            if (family < 1 || family > Family.values().length || !families.add(Family.values()[family - 1])) throw new IllegalArgumentException("unknown/repeated original closure family");
        }
        final int clause = QueryCodecSupport.uint32(f.get(i++), 7); if (clause < 1 || clause > Clause.values().length) throw new IllegalArgumentException("unknown original closure clause");
        final Long retry = i < f.size() && f.get(i).number() == 8 ? QueryCodecSupport.uint(f.get(i++), 8) : null;
        SourceReplayMutation fence = null; SystemMutationResult first = null;
        if (i < f.size() && f.get(i).number() == 9) {
            final var mutation = SystemMutation.decodeTargetFrame(QueryCodecSupport.bytes(f.get(i++), 9));
            final var source = TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(i++), 10));
            first = SystemMutationResult.decode(QueryCodecSupport.bytes(f.get(i++), 11)); fence = new SourceReplayMutation(mutation, source, null, null);
        }
        if (i + 3 != f.size()) throw new IllegalArgumentException("original closure has unknown/missing raw material");
        final var domain = new Domain(QueryCodecSupport.bytes(f.get(0), 1), TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(2), 3)), TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(3), 4)),
                tuples, families, Clause.values()[clause - 1], retry, fence, first,
                QueryCodecSupport.bytes(f.get(i++), 12), QueryCodecSupport.bytes(f.get(i++), 13), QueryCodecSupport.bytes(f.get(i), 14));
        if (!Arrays.equals(raw, domain.canonicalBytes())) throw new IllegalArgumentException("noncanonical complete original closure");
        return domain;
    }

    public static void requireRawMaterials(byte[] raw, TargetMigrationPlan plan, TargetMigrationRetirementRecord retirement,
            TargetQuotaMutation coveredThrough) {
        final var q = QueryCodecSupport.read(raw, "complete original closed domain materials", true);
        if (q.size() < 8 || QueryCodecSupport.uint32(q.getFirst(), 1) != 1
                || !Arrays.equals(QueryCodecSupport.fixed(q.get(3), 4, 32), plan.digest())
                || !Arrays.equals(QueryCodecSupport.fixed(q.get(4), 5, 32), retirement.intention().digest())
                || !TargetQuotaMutation.decode(QueryCodecSupport.bytes(q.get(5), 6)).equals(retirement.intentAt())
                || !TargetQuotaMutation.decode(QueryCodecSupport.bytes(q.get(6), 7)).equals(coveredThrough))
            throw new IllegalStateException("closed material changed exact original Root/I/Source coverage");
        TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(q.get(1), 2)).requireContent(QueryCodecSupport.bytes(q.get(2), 3));
        material(QueryCodecSupport.bytes(q.get(7), 8));
        final Set<String> identities = new HashSet<>();
        for (int index = 8; index < q.size(); index++) {
            final var domain = decodeDomain(QueryCodecSupport.bytes(q.get(index), 9));
            if (!identities.add(Bytes.hex(domain.producerIdentity())) || !domain.scope().equals(plan.scope())
                    || !domain.closedAt().sameSourceIdentity(coveredThrough.source()) || domain.closedAt().compareTo(coveredThrough.source()) > 0)
                throw new IllegalStateException("closed original domain does not belong to the actual scope/cut");
            if (domain.clause() == Clause.REGISTERED_RETRY_CEILING) requireFence(domain, coveredThrough);
        }
    }

    private static byte[] material(byte[] value) { if (value == null || value.length == 0) throw unavailable("complete original closed-contract raw material is unavailable"); return Bytes.copy(value); }
    private static TargetSourceAuthorityPreparation.InputUnavailable unavailable(String message) { return new TargetSourceAuthorityPreparation.InputUnavailable(message); }
    private static byte[] frame(SourceReplayEntry entry) { return entry instanceof SourceReplayRecord c ? CommandCodec.encodeFrame(c.command()) : ((SourceReplayMutation) entry).mutation().encodeFrame(); }
    private static byte[] sourceDigest(SourceReplayEntry entry) { return entry instanceof SourceReplayRecord c ? c.command().canonicalFrameDigest() : Bytes.sha256(((SourceReplayMutation) entry).mutation().canonicalEnvelope()); }
}
