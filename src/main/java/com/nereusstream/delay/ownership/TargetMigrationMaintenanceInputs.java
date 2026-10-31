package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.TargetMigrationCatalogBarrierReceipt;
import com.nereusstream.delay.protocol.TargetMigrationCustodyGuard;
import com.nereusstream.delay.protocol.TargetMigrationObjectReference;
import com.nereusstream.delay.protocol.TargetMigrationRequest;
import com.nereusstream.delay.protocol.TargetMigrationRetirementConfirmRequest;
import com.nereusstream.delay.protocol.TargetMigrationRetirementInventory;
import com.nereusstream.delay.protocol.TargetMigrationRetirementResourceReceipt;
import com.nereusstream.delay.protocol.TargetMigrationRoot;
import com.nereusstream.delay.protocol.TargetMigrationSettlementRequest;
import com.nereusstream.delay.runtime.TargetMigrationRetirementStore;
import com.nereusstream.delay.runtime.TargetMigrationStore;
import com.nereusstream.delay.store.BoundedReadBudget;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Original legacy sub-loan: retained raw facts and real owner operations, never a completed-retirement callback. */
public interface TargetMigrationMaintenanceInputs extends CheckpointInputFuture.Cleanup, AutoCloseable {
    final class StaleCursor extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        public StaleCursor(String message) {
            super(message);
        }
    }

    interface Provider {
        /** Must retain this exact loan/partial cleanup on opening before any native acquisition. */
        TargetMigrationMaintenanceInputs open(TargetMigrationMaintenance.Opening opening);
        /**
         * Independently reacquires exact durable Prepared/raw/owner materials during ordinary startup/replay,
         * including before the live Worker exists. This is a real original Source hold, not a caller's hot
         * resource action or a completed-retirement callback. It survives loss of the live maintenance object.
         */
        TargetSourceAuthorityLoader.HeldInputs loadSource(SourceReplayMutation original, Runnable current);
    }

    /** Raw original record and the independently protected complete noFuture object. */
    record Settlement(TargetMigrationSettlementRequest request, byte[] terminalRaw, byte[] noFutureRaw) {
        public Settlement {
            Objects.requireNonNull(request);
            Objects.requireNonNull(terminalRaw);
            Objects.requireNonNull(noFutureRaw);
            if (!Bytes.constantTimeEquals(Bytes.sha256(terminalRaw), request.originalTerminalDigest())) {
                throw new IllegalArgumentException("settlement changed complete original terminal/raw record");
            }
            request.noFutureProof().requireContent(noFutureRaw);
            terminalRaw = Bytes.copy(terminalRaw);
            noFutureRaw = Bytes.copy(noFutureRaw);
        }

        @Override
        public byte[] terminalRaw() {
            return Bytes.copy(terminalRaw);
        }

        @Override
        public byte[] noFutureRaw() {
            return Bytes.copy(noFutureRaw);
        }

        long retainedBytes() {
            return Math.addExact(
                    request.canonicalBytes().length, Math.addExact(terminalRaw.length, noFutureRaw.length));
        }
    }

    record Page(byte[] cursor, List<Settlement> settlements, boolean complete) {
        public Page {
            cursor = cursor == null ? null : Bytes.copy(cursor);
            settlements = List.copyOf(settlements);
            if (!complete && (cursor == null || cursor.length == 0 || cursor.length > 65_536)) {
                throw new IllegalArgumentException("partial settlement page requires its original finite cursor");
            }
        }

        @Override
        public byte[] cursor() {
            return cursor == null ? null : Bytes.copy(cursor);
        }

        long retainedBytes() {
            long bytes = cursor == null ? 0 : cursor.length;
            for (var settlement : settlements) {
                bytes = Math.addExact(bytes, settlement.retainedBytes());
            }
            return bytes;
        }
    }

    void requireCurrent();
    /** The same actual admission/owner token, with no I/O from a Store/SDK monitor. */
    void requireLocalCurrent();

    long retainedBytes();

    TargetMigrationSourceExposure.ManagementHandle originalManagementSource();

    TargetScopeDeploymentAllocation.RegistryHandle originalManagementRegistry();

    ControlTargetRegistrationAuthority registration();
    /** Stable original operation ID/independent CONTROL grant, separately for each exact candidate. */
    TargetMigrationInputs.ControlPreparation controlPreparation(TargetMigrationRequest request);

    TargetMigrationStore.Authority sourceAuthority();

    TargetMigrationCustodyGuard.SettlementAuthority settlementAuthority();

    TargetMigrationRetirementStore.OriginalInputs retirementAuthority();
    /** Real existing old-domain and new-owner loans; numerical limits grant no archive/transfer rights. */
    default Archive archive() {
        throw new IllegalStateException("old logical namespace lacks its actual paid archive/receiver acquisition");
    }

    interface Archive {
        com.nereusstream.delay.runtime.TargetMigrationArchivePreparation.Limits preparationLimits();
        /** One cumulative operation budget, shared by all scan and copy turns. */
        BoundedReadBudget preparationReads();

        com.nereusstream.delay.store.RenewableS3CompatibleCheckpointObjectStoreAdapter objects();

        com.nereusstream.delay.store.TargetMigrationArchiveCustody.NewOwner receiver();

        com.nereusstream.delay.store.TargetMigrationArchiveCustody.Limits receiverLimits();
        /** One independent actual new-owner full raw-chain audit budget, never reset per link/page. */
        BoundedReadBudget receiverAuditReads();

        void requireCurrent();
    }

    /** Finite original ID census; incomplete/failed reads never mean no remaining legacy obligations. */
    Page settlements(byte[] cursor, BoundedReadBudget reads, Runnable current);
    /** Independently authenticates terminal raw, original Source cause and complete noFuture ownership. */
    void authenticateSettlement(Settlement settlement, TargetMigrationRoot actualRoot, Runnable current);
    /** Exact original member cut and dispositions; absence means no qualified whole-domain retirement. */
    Optional<TargetMigrationRetirementInventory> retirementInventory(
            TargetMigrationRoot actualRoot, BoundedReadBudget reads, Runnable current);
    /** Complete fresh reader-set material, protected by the same original independent eligibility owner. */
    TargetMigrationObjectReference eligibleReaders(
            TargetMigrationRoot actualRoot, BoundedReadBudget reads, Runnable current);

    /** Actual Catalog CAS and real resource owners, all still under this original sub-loan. */
    Retirement retirement();

    interface Retirement {
        /** Re-read/reuse the actual durable barrier for this exact Native intent, never caller-made DTOs. */
        Barrier closeAdmissions(TargetMigrationRetirementStore.AppliedIntent actual, Runnable current);
        /**
         * The exact original component/provider starts before a receipt can be produced. Register any
         * partially opened action on opening first. Shared Source/Registry/client/pool must remain owned.
         */
        ResourceAction openResource(
                TargetMigrationMaintenance.Opening opening,
                TargetMigrationRetirementStore.AppliedIntent actual,
                Barrier barrier,
                TargetMigrationRetirementInventory.Resource exact,
                Runnable current);
        /** Complete raw original noFuture/Pin/query/reader/retention/Source/fee evidence after every action. */
        TargetMigrationRetirementConfirmRequest.Closure closure(
                TargetMigrationRetirementStore.AppliedIntent actual,
                Barrier barrier,
                List<TargetMigrationRetirementResourceReceipt> receipts,
                BoundedReadBudget reads,
                Runnable current);
        /** Catalog adopts only actual Source confirmation, retaining its permanent anti-resurrection tombstone. */
        void adoptConfirmed(
                TargetMigrationRetirementStore.AppliedConfirmation actual, Barrier barrier, Runnable current);
        /** Return the specific legacy sub-loan after durable transfer/exit; never close shared Inputs or clients. */
        void releaseLegacySubLoan(TargetMigrationRetirementStore.AppliedConfirmation actual, Runnable current);
    }

    interface Barrier {
        TargetMigrationCatalogBarrierReceipt receipt();

        void requireIntent(TargetMigrationRetirementStore.AppliedIntent actual);

        void requireCurrent();
        /** Actual permanent admission state plus all pre-barrier Pins/readers/queries drained. */
        void requireDrained();
    }

    interface ResourceAction extends CheckpointInputFuture.Cleanup, AutoCloseable {
        void requireBound(
                TargetMigrationRetirementStore.AppliedIntent actual,
                Barrier barrier,
                TargetMigrationRetirementInventory.Resource exact);
        /** One bounded original stop/exit/delete or protected exact transfer step; no new executor/queue. */
        void advance(Runnable current);
        /** Available only after the actual original action/receiver custody and durable operation history. */
        Optional<TargetMigrationRetirementResourceReceipt> terminalReceipt();

        long retainedBytes();

        boolean ioExited();

        void requestStop();

        @Override
        void close();
    }
    /** Stops this acquisition only; old material/fees remain held across UNKNOWN and failed cleanup. */
    void requestStop();

    boolean ioExited();

    @Override
    void close();
}
