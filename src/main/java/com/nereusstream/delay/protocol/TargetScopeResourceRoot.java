package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Pure finite shared resource state. Only the original backing owner's durable CAS may issue
 * a child or process loan; constructing/decoding this DTO establishes no such authority.
 * Native exposure/attempt debits remain exclusively in the child's original Scope Root2.
 */
public final class TargetScopeResourceRoot {
    public static final int VERSION = 1;
    public static final int MAX_CANONICAL_BYTES = 3_199_216;
    public static final int FIRST_BIRTH_MARKER_MAX_CANONICAL_BYTES = 160;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-scope-resource-root\0");

    public enum AllocationState {
        OPEN,
        CLOSED
    }

    public enum ChildState {
        PENDING,
        NATIVE_CONFIRMED
    }

    public enum LoanState {
        OPEN,
        CLOSED
    }

    public enum Purpose {
        SCOPE_REGISTRY,
        SOURCE_FEES,
        SDK_ISSUER,
        CAPACITY
    }

    /** A stable acyclic identity; it cannot be reissued by a new process or a missing Root. */
    public record Reference(byte[] deploymentAllocationId, byte[] birthNonce, byte[] policyDigest) {
        public static final int MAX_CANONICAL_BYTES = 104;

        public Reference {
            deploymentAllocationId = assigned32(deploymentAllocationId, "deployment backing allocation ID");
            birthNonce = assigned32(birthNonce, "deployment backing birth nonce");
            policyDigest = assigned32(policyDigest, "resource policy digest");
        }

        @Override
        public byte[] deploymentAllocationId() {
            return Bytes.copy(deploymentAllocationId);
        }

        @Override
        public byte[] birthNonce() {
            return Bytes.copy(birthNonce);
        }

        @Override
        public byte[] policyDigest() {
            return Bytes.copy(policyDigest);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION);
                CanonicalProtobuf.bytes(out, 2, deploymentAllocationId);
                CanonicalProtobuf.bytes(out, 3, birthNonce);
                CanonicalProtobuf.bytes(out, 4, policyDigest);
            });
        }

        public static Reference decode(byte[] encoded) {
            final var f =
                    TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 4, false, "resource Root reference");
            QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3, 4}, "resource Root reference");
            requireVersion(f.getFirst(), "resource Root reference");
            final var value = new Reference(
                    QueryCodecSupport.fixed(f.get(1), 2, 32),
                    QueryCodecSupport.fixed(f.get(2), 3, 32),
                    QueryCodecSupport.fixed(f.get(3), 4, 32));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "resource Root reference");
            return value;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Reference that
                    && Arrays.equals(deploymentAllocationId, that.deploymentAllocationId)
                    && Arrays.equals(birthNonce, that.birthNonce)
                    && Arrays.equals(policyDigest, that.policyDigest);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(canonicalBytes());
        }
    }

    /**
     * The backend first retains this original birth marker and independently establishes its
     * owned initial bootstrap before creating a Root. Marker-present/Root-missing is UNKNOWN,
     * never permission for a second birth. This value supplies no cross-key atomic transaction.
     */
    public record BirthMarker(Reference reference, byte[] originalHistoryDigest) {
        public static final int MAX_CANONICAL_BYTES = FIRST_BIRTH_MARKER_MAX_CANONICAL_BYTES;

        public BirthMarker {
            Objects.requireNonNull(reference);
            originalHistoryDigest = assigned32(originalHistoryDigest, "complete original history digest");
        }

        @Override
        public byte[] originalHistoryDigest() {
            return Bytes.copy(originalHistoryDigest);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION);
                CanonicalProtobuf.bytes(out, 2, reference.canonicalBytes());
                CanonicalProtobuf.bytes(out, 3, originalHistoryDigest);
            });
        }

        public static BirthMarker decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(
                    encoded, MAX_CANONICAL_BYTES, 3, false, "resource first-birth marker");
            QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3}, "resource first-birth marker");
            requireVersion(f.getFirst(), "resource first-birth marker");
            final var value = new BirthMarker(
                    Reference.decode(QueryCodecSupport.bytes(f.get(1), 2)), QueryCodecSupport.fixed(f.get(2), 3, 32));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "resource first-birth marker");
            return value;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof BirthMarker that
                    && reference.equals(that.reference)
                    && Arrays.equals(originalHistoryDigest, that.originalHistoryDigest);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(canonicalBytes());
        }
    }

    /**
     * Projection of actual Native64 funding, independently verified by the Authority. fundedAtDigest
     * is SHA256 of the complete TargetQuotaMutation.canonicalBytes(), including its physical Source
     * position and sequence; the mutation's input-envelope digest alone is insufficient.
     */
    public record NativeConfirmation(long fundedAtSequence, byte[] fundedAtDigest, byte[] fundingFrameDigest) {
        public static final int MAX_CANONICAL_BYTES = 96;

        public NativeConfirmation {
            if (fundedAtSequence == 0) {
                throw new IllegalArgumentException("actual Native64 funding sequence is required");
            }
            fundedAtDigest = assigned32(fundedAtDigest, "complete actual Native64 fundedAt digest");
            fundingFrameDigest = assigned32(fundingFrameDigest, "complete actual Native64 funding frame digest");
        }

        @Override
        public byte[] fundedAtDigest() {
            return Bytes.copy(fundedAtDigest);
        }

        @Override
        public byte[] fundingFrameDigest() {
            return Bytes.copy(fundingFrameDigest);
        }

        public static NativeConfirmation from(TargetQuotaMutation actualFundedAt, byte[] actualFundingFrameDigest) {
            Objects.requireNonNull(actualFundedAt).requireSourceApplied();
            return new NativeConfirmation(
                    actualFundedAt.sequence(), Bytes.sha256(actualFundedAt.canonicalBytes()), actualFundingFrameDigest);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION);
                CanonicalProtobuf.uint64Bits(out, 2, fundedAtSequence);
                CanonicalProtobuf.bytes(out, 3, fundedAtDigest);
                CanonicalProtobuf.bytes(out, 4, fundingFrameDigest);
            });
        }

        public static NativeConfirmation decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(
                    encoded, MAX_CANONICAL_BYTES, 4, false, "Native64 funding confirmation");
            QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3, 4}, "Native64 funding confirmation");
            requireVersion(f.getFirst(), "Native64 funding confirmation");
            final var value = new NativeConfirmation(
                    QueryCodecSupport.uint64Bits(f.get(1), 2),
                    QueryCodecSupport.fixed(f.get(2), 3, 32),
                    QueryCodecSupport.fixed(f.get(3), 4, 32));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Native64 funding confirmation");
            return value;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof NativeConfirmation that
                    && fundedAtSequence == that.fundedAtSequence
                    && Arrays.equals(fundedAtDigest, that.fundedAtDigest)
                    && Arrays.equals(fundingFrameDigest, that.fundingFrameDigest);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(canonicalBytes());
        }
    }

    /**
     * One permanently charged slot. retainedUntil is a minimum obligation horizon, not an expiry
     * or refund clock. The original Scope storage is keyed by scopeId, so another epoch or Header
     * cannot create a second child allocation for that ID.
     */
    public record Child(
            int childIndex,
            TargetSloControlScopeHeader.Reference headerReference,
            byte[] definitionDigest,
            TargetScopeResourcePolicy.Shares fixedShares,
            long retainedUntil,
            ChildState state,
            NativeConfirmation confirmation) {
        public static final int MAX_CANONICAL_BYTES = 464;

        public Child {
            if (childIndex < 0
                    || childIndex >= TargetScopeResourcePolicy.MAX_CHILDREN
                    || retainedUntil < 0
                    || retainedUntil == Long.MAX_VALUE) {
                throw new IllegalArgumentException("finite resource child index and retention horizon required");
            }
            Objects.requireNonNull(headerReference);
            definitionDigest = assigned32(definitionDigest, "original paid child definition/allocation digest");
            Objects.requireNonNull(fixedShares);
            Objects.requireNonNull(state);
            if ((state == ChildState.PENDING) != (confirmation == null)) {
                throw new IllegalArgumentException(
                        "only NATIVE_CONFIRMED children carry actual Native64 funding projection");
            }
        }

        @Override
        public byte[] definitionDigest() {
            return Bytes.copy(definitionDigest);
        }
        /** The existing immutable Definition is the allocation grant; it is not duplicated on wire. */
        public byte[] allocationGrantDigest() {
            return definitionDigest();
        }

        private Child confirmed(NativeConfirmation actual) {
            Objects.requireNonNull(actual);
            if (state == ChildState.NATIVE_CONFIRMED) {
                if (!confirmation.equals(actual)) {
                    throw new IllegalArgumentException("child Native64 confirmation changed");
                }
                return this;
            }
            return new Child(
                    childIndex,
                    headerReference,
                    definitionDigest,
                    fixedShares,
                    retainedUntil,
                    ChildState.NATIVE_CONFIRMED,
                    actual);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION);
                CanonicalProtobuf.uint32(out, 2, childIndex);
                CanonicalProtobuf.bytes(out, 3, headerReference.canonicalBytes());
                CanonicalProtobuf.bytes(out, 4, definitionDigest);
                CanonicalProtobuf.bytes(out, 5, fixedShares.canonicalBytes());
                CanonicalProtobuf.uint64(out, 6, retainedUntil);
                CanonicalProtobuf.uint32(out, 7, state == ChildState.PENDING ? 1 : 2);
                if (confirmation != null) {
                    CanonicalProtobuf.bytes(out, 8, confirmation.canonicalBytes());
                }
            });
        }

        public static Child decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 8, false, "resource child");
            QueryCodecSupport.requireNumbers(
                    f,
                    f.size() == 7 ? new int[] {1, 2, 3, 4, 5, 6, 7} : new int[] {1, 2, 3, 4, 5, 6, 7, 8},
                    "resource child");
            requireVersion(f.getFirst(), "resource child");
            final int state = QueryCodecSupport.uint32(f.get(6), 7);
            if (state != 1 && state != 2) {
                throw new IllegalArgumentException("unknown resource child state");
            }
            final var value = new Child(
                    QueryCodecSupport.uint32(f.get(1), 2),
                    TargetSloControlScopeHeader.Reference.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                    QueryCodecSupport.fixed(f.get(3), 4, 32),
                    TargetScopeResourcePolicy.Shares.decode(QueryCodecSupport.bytes(f.get(4), 5)),
                    QueryCodecSupport.uint64Bits(f.get(5), 6),
                    state == 1 ? ChildState.PENDING : ChildState.NATIVE_CONFIRMED,
                    f.size() == 8 ? NativeConfirmation.decode(QueryCodecSupport.bytes(f.get(7), 8)) : null);
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "resource child");
            return value;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Child that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(canonicalBytes());
        }
    }

    /**
     * One durable process/purpose slot, reusable only as the same still-OPEN loan. Local read
     * references may be issued from it without Source-frame records. Closing a local reference
     * does not close this process slot. Restart, lost lease, timeout and UNKNOWN never close it.
     */
    public record ProcessLoan(
            int loanIndex,
            int childIndex,
            byte[] processIncarnationDigest,
            Purpose purpose,
            LoanState state,
            byte[] closureProofDigest) {
        public static final int MAX_CANONICAL_BYTES = 128;

        public ProcessLoan {
            if (loanIndex < 0
                    || loanIndex >= TargetScopeResourcePolicy.MAX_PROCESS_LOANS
                    || childIndex < 0
                    || childIndex >= TargetScopeResourcePolicy.MAX_CHILDREN) {
                throw new IllegalArgumentException("finite resource process-loan and child indices required");
            }
            processIncarnationDigest = assigned32(processIncarnationDigest, "exact process incarnation digest");
            Objects.requireNonNull(purpose);
            Objects.requireNonNull(state);
            if (state == LoanState.OPEN) {
                if (closureProofDigest != null) {
                    throw new IllegalArgumentException("OPEN process loan has no closure proof");
                }
            } else {
                closureProofDigest = assigned32(closureProofDigest, "actual process/purpose exit proof digest");
            }
        }

        @Override
        public byte[] processIncarnationDigest() {
            return Bytes.copy(processIncarnationDigest);
        }

        @Override
        public byte[] closureProofDigest() {
            return closureProofDigest == null ? null : Bytes.copy(closureProofDigest);
        }

        private boolean sameKey(int child, byte[] process, Purpose use) {
            return childIndex == child && Arrays.equals(processIncarnationDigest, process) && purpose == use;
        }

        private ProcessLoan closed(byte[] proofDigest) {
            final var proof = assigned32(proofDigest, "actual process/purpose exit proof digest");
            if (state == LoanState.CLOSED) {
                if (!Arrays.equals(closureProofDigest, proof)) {
                    throw new IllegalArgumentException("process-loan closure proof changed");
                }
                return this;
            }
            return new ProcessLoan(loanIndex, childIndex, processIncarnationDigest, purpose, LoanState.CLOSED, proof);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION);
                CanonicalProtobuf.uint32(out, 2, loanIndex);
                CanonicalProtobuf.uint32(out, 3, childIndex);
                CanonicalProtobuf.bytes(out, 4, processIncarnationDigest);
                CanonicalProtobuf.uint32(out, 5, purpose.ordinal() + 1);
                CanonicalProtobuf.uint32(out, 6, state == LoanState.OPEN ? 1 : 2);
                if (closureProofDigest != null) {
                    CanonicalProtobuf.bytes(out, 7, closureProofDigest);
                }
            });
        }

        public static ProcessLoan decode(byte[] encoded) {
            final var f =
                    TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 7, false, "resource process loan");
            QueryCodecSupport.requireNumbers(
                    f,
                    f.size() == 6 ? new int[] {1, 2, 3, 4, 5, 6} : new int[] {1, 2, 3, 4, 5, 6, 7},
                    "resource process loan");
            requireVersion(f.getFirst(), "resource process loan");
            final int purpose = QueryCodecSupport.uint32(f.get(4), 5);
            final int state = QueryCodecSupport.uint32(f.get(5), 6);
            if (purpose < 1 || purpose > Purpose.values().length || state != 1 && state != 2) {
                throw new IllegalArgumentException("unknown resource process-loan purpose/state");
            }
            final var value = new ProcessLoan(
                    QueryCodecSupport.uint32(f.get(1), 2),
                    QueryCodecSupport.uint32(f.get(2), 3),
                    QueryCodecSupport.fixed(f.get(3), 4, 32),
                    Purpose.values()[purpose - 1],
                    state == 1 ? LoanState.OPEN : LoanState.CLOSED,
                    f.size() == 7 ? QueryCodecSupport.fixed(f.get(6), 7, 32) : null);
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "resource process loan");
            return value;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ProcessLoan that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(canonicalBytes());
        }
    }

    private final TargetScopeResourcePolicy policy;
    private final long revision;
    private final AllocationState allocationState;
    private final byte[] allocationClosureProofDigest;
    private final TargetScopeResourcePolicy.Shares remainingShares;
    private final List<Child> children;
    private final List<ProcessLoan> processLoans;
    private final byte[] digest;

    private TargetScopeResourceRoot(
            TargetScopeResourcePolicy policy,
            long revision,
            AllocationState allocationState,
            byte[] allocationClosureProofDigest,
            TargetScopeResourcePolicy.Shares remainingShares,
            List<Child> children,
            List<ProcessLoan> processLoans) {
        this.policy = Objects.requireNonNull(policy);
        if (revision < 0 || revision == Long.MAX_VALUE) {
            throw new IllegalArgumentException("finite resource Root revision required");
        }
        this.revision = revision;
        this.allocationState = Objects.requireNonNull(allocationState);
        if (allocationState == AllocationState.OPEN) {
            if (allocationClosureProofDigest != null) {
                throw new IllegalArgumentException("OPEN resource allocation has no closure proof");
            }
            this.allocationClosureProofDigest = null;
        } else {
            this.allocationClosureProofDigest =
                    assigned32(allocationClosureProofDigest, "new-allocation closure proof digest");
        }
        this.remainingShares = Objects.requireNonNull(remainingShares);
        this.children = List.copyOf(children);
        this.processLoans = List.copyOf(processLoans);
        if (this.children.size() > policy.maximumChildren()
                || this.processLoans.size() > policy.maximumProcessLoans()) {
            throw new IllegalArgumentException("resource Root exceeds its configured child/process-loan slots");
        }
        var expectedRemaining = policy.allocatableShares();
        long minimumRevision = allocationState == AllocationState.CLOSED ? 1 : 0;
        for (int n = 0; n < this.children.size(); n++) {
            final var child = this.children.get(n);
            if (child.childIndex() != n) {
                throw new IllegalArgumentException("resource child slots must be contiguous and never reused");
            }
            requireNewChildIdentity(this.children.subList(0, n), child);
            expectedRemaining = expectedRemaining.subtract(child.fixedShares());
            minimumRevision = Math.addExact(minimumRevision, child.state() == ChildState.PENDING ? 1 : 2);
        }
        if (!remainingShares.equals(expectedRemaining)) {
            throw new IllegalArgumentException("resource Root remaining shares disagree with permanent child charges");
        }
        for (int n = 0; n < this.processLoans.size(); n++) {
            final var loan = this.processLoans.get(n);
            if (loan.loanIndex() != n || loan.childIndex() >= this.children.size()) {
                throw new IllegalArgumentException(
                        "process-loan slots must be contiguous and reference an original child");
            }
            if (loan.purpose() == Purpose.SDK_ISSUER
                    && this.children.get(loan.childIndex()).state() != ChildState.NATIVE_CONFIRMED) {
                throw new IllegalArgumentException("SDK issuer requires actual Native64 confirmation");
            }
            for (int p = 0; p < n; p++) {
                if (this.processLoans
                        .get(p)
                        .sameKey(loan.childIndex(), loan.processIncarnationDigest(), loan.purpose())) {
                    throw new IllegalArgumentException("duplicate process/child/purpose loan slot");
                }
            }
            minimumRevision = Math.addExact(minimumRevision, loan.state() == LoanState.OPEN ? 1 : 2);
        }
        if (revision != minimumRevision) {
            throw new IllegalArgumentException(
                    "resource Root revision must count every irreversible transition exactly once");
        }
        digest = Bytes.sha256(DOMAIN, fields());
        if (canonicalBytes().length > MAX_CANONICAL_BYTES
                || canonicalBytes().length > policy.management().maximumRootBytes()) {
            throw new IllegalArgumentException("resource Root exceeds its original prepaid management bytes");
        }
    }

    /** Initial state declaration only; a missing durable Root never authorizes calling this again. */
    public static TargetScopeResourceRoot open(TargetScopeResourcePolicy policy) {
        Objects.requireNonNull(policy);
        return new TargetScopeResourceRoot(
                policy, 0, AllocationState.OPEN, null, policy.allocatableShares(), List.of(), List.of());
    }

    public int schemaVersion() {
        return VERSION;
    }

    public TargetScopeResourcePolicy policy() {
        return policy;
    }

    public Reference reference() {
        return policy.reference();
    }

    public long revision() {
        return revision;
    }

    public AllocationState allocationState() {
        return allocationState;
    }

    public byte[] allocationClosureProofDigest() {
        return allocationClosureProofDigest == null ? null : Bytes.copy(allocationClosureProofDigest);
    }

    public TargetScopeResourcePolicy.Shares remainingShares() {
        return remainingShares;
    }

    public List<Child> children() {
        return children;
    }

    public List<ProcessLoan> processLoans() {
        return processLoans;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public byte[] firstBirthMarker() {
        return new BirthMarker(reference(), policy.binding().originalHistoryDigest()).canonicalBytes();
    }

    /** A reservation must be durably CASed before native funding or handing out any resource. */
    public TargetScopeResourceRoot reserveChild(
            TargetSloControlScopeHeader header,
            byte[] definitionDigest,
            TargetScopeResourcePolicy.Shares fixedShares,
            long retainedUntil) {
        policy.requireHeader(header);
        final var requested = new Child(
                0, header.reference(), definitionDigest, fixedShares, retainedUntil, ChildState.PENDING, null);
        for (var original : children) {
            if (original.headerReference().equals(requested.headerReference())) {
                if (!Arrays.equals(original.definitionDigest(), requested.definitionDigest())
                        || !original.fixedShares().equals(requested.fixedShares())
                        || original.retainedUntil() != requested.retainedUntil()) {
                    throw new IllegalArgumentException(
                            "child retry changed its original allocation definition, fixed shares or retention");
                }
                return this;
            }
        }
        return appendChild(new Child(
                children.size(),
                requested.headerReference(),
                requested.definitionDigest(),
                fixedShares,
                retainedUntil,
                ChildState.PENDING,
                null));
    }

    private TargetScopeResourceRoot appendChild(Child child) {
        if (allocationState != AllocationState.OPEN) {
            throw new IllegalStateException("new child allocations are permanently CLOSED");
        }
        if (children.size() == policy.maximumChildren()
                || child.childIndex() != children.size()
                || child.state() != ChildState.PENDING) {
            throw new IllegalArgumentException("new child requires its next unused PENDING slot");
        }
        requireNewChildIdentity(children, child);
        final var next = new ArrayList<>(children);
        next.add(child);
        return new TargetScopeResourceRoot(
                policy,
                nextRevision(),
                allocationState,
                allocationClosureProofDigest,
                remainingShares.subtract(child.fixedShares()),
                next,
                processLoans);
    }

    /** Confirmation is allowed after new allocations close; it never refunds the pending charge. */
    public TargetScopeResourceRoot confirmChild(int childIndex, NativeConfirmation actualNative64) {
        final var original = child(childIndex);
        final var confirmed = original.confirmed(actualNative64);
        if (original == confirmed) {
            return this;
        }
        final var next = new ArrayList<>(children);
        next.set(childIndex, confirmed);
        return new TargetScopeResourceRoot(
                policy,
                nextRevision(),
                allocationState,
                allocationClosureProofDigest,
                remainingShares,
                next,
                processLoans);
    }

    /** Stops only new child allocations; Root2 issuance closure is a separate original transition. */
    public TargetScopeResourceRoot closeNewAllocations(byte[] actualClosureProofDigest) {
        final var proof = assigned32(actualClosureProofDigest, "new-allocation closure proof digest");
        if (allocationState == AllocationState.CLOSED) {
            if (!Arrays.equals(allocationClosureProofDigest, proof)) {
                throw new IllegalArgumentException("new-allocation closure proof changed");
            }
            return this;
        }
        return new TargetScopeResourceRoot(
                policy, nextRevision(), AllocationState.CLOSED, proof, remainingShares, children, processLoans);
    }

    /**
     * Reuses the same durable OPEN process/purpose slot, without another quota issuance. Pending
     * funding may borrow CAPACITY, registry or Source-fee references; SDK issuance requires Native64.
     * The Authority must independently qualify the actual backing and exact purpose in every case.
     */
    public TargetScopeResourceRoot openProcessLoan(int childIndex, byte[] processIncarnationDigest, Purpose purpose) {
        final var originalChild = child(childIndex);
        final var process = assigned32(processIncarnationDigest, "exact process incarnation digest");
        Objects.requireNonNull(purpose);
        for (var original : processLoans) {
            if (original.sameKey(childIndex, process, purpose)) {
                if (original.state() == LoanState.CLOSED) {
                    throw new IllegalStateException("closed process/purpose loan cannot reopen or reuse its slot");
                }
                return this;
            }
        }
        if (purpose == Purpose.SDK_ISSUER && originalChild.state() != ChildState.NATIVE_CONFIRMED) {
            throw new IllegalStateException("SDK issuer requires actual Native64 confirmation");
        }
        if (processLoans.size() == policy.maximumProcessLoans()) {
            throw new IllegalStateException("finite process-loan slots exhausted");
        }
        final var next = new ArrayList<>(processLoans);
        next.add(new ProcessLoan(next.size(), childIndex, process, purpose, LoanState.OPEN, null));
        return new TargetScopeResourceRoot(
                policy, nextRevision(), allocationState, allocationClosureProofDigest, remainingShares, children, next);
    }

    /**
     * Only an actual manager's opaque proof of exit of this process and all references/native SDK
     * work for this exact purpose may qualify this transition. A caller-supplied digest alone is
     * not that proof. Closure permanently retains the slot and every child's original charge.
     */
    public TargetScopeResourceRoot closeProcessLoan(int loanIndex, byte[] actualExitProofDigest) {
        final var original = processLoan(loanIndex);
        final var closed = original.closed(actualExitProofDigest);
        if (original == closed) {
            return this;
        }
        final var next = new ArrayList<>(processLoans);
        next.set(loanIndex, closed);
        return new TargetScopeResourceRoot(
                policy, nextRevision(), allocationState, allocationClosureProofDigest, remainingShares, children, next);
    }

    public Child child(int childIndex) {
        if (childIndex < 0 || childIndex >= children.size()) {
            throw new IllegalArgumentException("unknown original child slot");
        }
        return children.get(childIndex);
    }

    public ProcessLoan processLoan(int loanIndex) {
        if (loanIndex < 0 || loanIndex >= processLoans.size()) {
            throw new IllegalArgumentException("unknown original process-loan slot");
        }
        return processLoans.get(loanIndex);
    }

    /**
     * Exact one-step CAS guard. Refuses rebirth, ABA, policy replacement, old child/loan edits,
     * replacement proofs, slot reuse and refunds. It validates structure, never actual proof authority.
     */
    public void requireTransitionFrom(TargetScopeResourceRoot original) {
        Objects.requireNonNull(original);
        if (!policy.equals(original.policy) || revision != original.nextRevision()) {
            throw new IllegalArgumentException("resource transition changed its original policy or exact revision");
        }
        final TargetScopeResourceRoot expected;
        if (children.size() == original.children.size() + 1 && processLoans.size() == original.processLoans.size()) {
            expected = original.appendChild(children.getLast());
        } else if (children.size() == original.children.size()
                && processLoans.size() == original.processLoans.size() + 1) {
            final var added = processLoans.getLast();
            if (added.state() != LoanState.OPEN) {
                throw new IllegalArgumentException("new process loan must be OPEN");
            }
            expected = original.openProcessLoan(added.childIndex(), added.processIncarnationDigest(), added.purpose());
        } else if (children.size() == original.children.size() && processLoans.size() == original.processLoans.size()) {
            if (allocationState != original.allocationState) {
                if (allocationState != AllocationState.CLOSED) {
                    throw new IllegalArgumentException("new child allocation cannot reopen");
                }
                expected = original.closeNewAllocations(allocationClosureProofDigest);
            } else {
                TargetScopeResourceRoot changed = null;
                for (int n = 0; n < children.size(); n++) {
                    if (!children.get(n).equals(original.children.get(n))) {
                        if (changed != null || children.get(n).state() != ChildState.NATIVE_CONFIRMED) {
                            throw new IllegalArgumentException(
                                    "resource transition changed multiple or original child declarations");
                        }
                        changed = original.confirmChild(n, children.get(n).confirmation());
                    }
                }
                for (int n = 0; n < processLoans.size(); n++) {
                    if (!processLoans.get(n).equals(original.processLoans.get(n))) {
                        if (changed != null || processLoans.get(n).state() != LoanState.CLOSED) {
                            throw new IllegalArgumentException(
                                    "resource transition changed multiple or original process loans");
                        }
                        changed =
                                original.closeProcessLoan(n, processLoans.get(n).closureProofDigest());
                    }
                }
                if (changed == null) {
                    throw new IllegalArgumentException("resource transition did not change one original state");
                }
                expected = changed;
            }
        } else {
            throw new IllegalArgumentException("resource transition removed/reused slots or made multiple allocations");
        }
        if (!equals(expected)) {
            throw new IllegalArgumentException(
                    "resource transition changed an immutable declaration or original charge");
        }
    }

    private static void requireNewChildIdentity(List<Child> originals, Child candidate) {
        for (var original : originals) {
            if (Arrays.equals(
                    original.headerReference().scopeId(),
                    candidate.headerReference().scopeId())) {
                throw new IllegalArgumentException(
                        "an original child Scope ID cannot be allocated again with another epoch or Header");
            }
        }
    }

    private long nextRevision() {
        final long next = Math.addExact(revision, 1);
        if (next == Long.MAX_VALUE) {
            throw new IllegalStateException("finite resource Root revision exhausted");
        }
        return next;
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, policy.canonicalBytes());
            CanonicalProtobuf.uint64(out, 3, revision);
            CanonicalProtobuf.uint32(out, 4, allocationState == AllocationState.OPEN ? 1 : 2);
            if (allocationClosureProofDigest != null) {
                CanonicalProtobuf.bytes(out, 5, allocationClosureProofDigest);
            }
            CanonicalProtobuf.bytes(out, 6, remainingShares.canonicalBytes());
            for (var child : children) {
                CanonicalProtobuf.bytes(out, 7, child.canonicalBytes());
            }
            for (var loan : processLoans) {
                CanonicalProtobuf.bytes(out, 8, loan.canonicalBytes());
            }
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 9, digest);
        });
    }

    public static TargetScopeResourceRoot decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(
                encoded,
                MAX_CANONICAL_BYTES,
                7 + TargetScopeResourcePolicy.MAX_CHILDREN + TargetScopeResourcePolicy.MAX_PROCESS_LOANS,
                true,
                "resource Root");
        if (f.size() < 6) {
            throw new IllegalArgumentException("incomplete resource Root");
        }
        requireVersion(f.getFirst(), "resource Root");
        final int state = QueryCodecSupport.uint32(f.get(3), 4);
        if (state != 1 && state != 2) {
            throw new IllegalArgumentException("unknown resource allocation state");
        }
        int at = 4;
        final byte[] closure;
        if (f.get(at).number() == 5) {
            closure = QueryCodecSupport.fixed(f.get(at++), 5, 32);
        } else {
            closure = null;
        }
        final var remaining = TargetScopeResourcePolicy.Shares.decode(QueryCodecSupport.bytes(f.get(at++), 6));
        final var children = new ArrayList<Child>();
        while (at < f.size() && f.get(at).number() == 7) {
            if (children.size() == TargetScopeResourcePolicy.MAX_CHILDREN) {
                throw new IllegalArgumentException("resource child bound exceeded");
            }
            children.add(Child.decode(QueryCodecSupport.bytes(f.get(at++), 7)));
        }
        final var loans = new ArrayList<ProcessLoan>();
        while (at < f.size() && f.get(at).number() == 8) {
            if (loans.size() == TargetScopeResourcePolicy.MAX_PROCESS_LOANS) {
                throw new IllegalArgumentException("resource process-loan bound exceeded");
            }
            loans.add(ProcessLoan.decode(QueryCodecSupport.bytes(f.get(at++), 8)));
        }
        if (at + 1 != f.size()) {
            throw new IllegalArgumentException("invalid resource Root suffix");
        }
        final var value = new TargetScopeResourceRoot(
                TargetScopeResourcePolicy.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                QueryCodecSupport.uint64Bits(f.get(2), 3),
                state == 1 ? AllocationState.OPEN : AllocationState.CLOSED,
                closure,
                remaining,
                children,
                loans);
        if (!Arrays.equals(value.digest, QueryCodecSupport.fixed(f.get(at), 9, 32))) {
            throw new IllegalArgumentException("resource Root digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "resource Root");
        return value;
    }

    private static byte[] assigned32(byte[] value, String name) {
        return TargetCompatibilityCodec.assigned(value, 32, name);
    }

    private static void requireVersion(CanonicalProtobuf.Reader.Field field, String name) {
        if (QueryCodecSupport.uint32(field, 1) != VERSION) {
            throw new IllegalArgumentException("unknown " + name + " schema");
        }
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetScopeResourceRoot that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
