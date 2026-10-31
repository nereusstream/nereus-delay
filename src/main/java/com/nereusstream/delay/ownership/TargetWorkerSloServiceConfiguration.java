package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.ShardLogFrame;
import com.nereusstream.delay.protocol.TargetSloCustodyReceipt;
import com.nereusstream.delay.protocol.TargetSloCustodyRecord;
import com.nereusstream.delay.protocol.TargetSloWriterEpoch;
import com.nereusstream.delay.runtime.TargetSloExportPageReader;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.BoundedReadBudget;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.LongSupplier;

/** Finite maintenance envelopes only; signed Source state and original providers grant certification rights. */
public record TargetWorkerSloServiceConfiguration(
        String providersReference,
        long continuationTaskBytes,
        ReadLimits finalReads,
        TargetSloFinalMaintenance.Limits finalPages,
        long finalTaskBytes,
        ReadLimits custodyReads,
        TargetSloExportPageReader.Limits custodyPages,
        long custodyTaskBytes) {
    public static final int MAXIMUM_TASKS = 3;
    public static final long MAXIMUM_CUSTODY_FRAME_BYTES =
            (long) TargetSloCustodyRecord.MAX_INPUT_BYTES + ShardLogFrame.HEADER_LENGTH + ShardLogFrame.TRAILER_LENGTH;

    public record ReadLimits(int records, long bytes, long nanos) {
        public ReadLimits {
            if (records < 1
                    || records == Integer.MAX_VALUE
                    || bytes < 1
                    || bytes == Long.MAX_VALUE
                    || nanos < 1
                    || nanos == Long.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "SLO maintenance reads require finite record, byte and elapsed bounds");
            }
        }

        public BoundedReadBudget newBudget(LongSupplier clock) {
            return new BoundedReadBudget(records, bytes, nanos, Objects.requireNonNull(clock));
        }
    }

    /** The original deployment supplies these providers, including every per-flight resource release. */
    public record Providers(
            String reference,
            TargetSloFinalContinuation.Provider continuation,
            TargetSloFinalMaintenance.Provider finalMaintenance,
            TargetSloCustodyExportMaintenance.Provider custody) {
        public Providers {
            reference = providerReference(reference);
            Objects.requireNonNull(continuation);
            Objects.requireNonNull(finalMaintenance);
            Objects.requireNonNull(custody);
        }
    }

    public TargetWorkerSloServiceConfiguration {
        providersReference = providerReference(providersReference);
        Objects.requireNonNull(finalReads);
        Objects.requireNonNull(finalPages);
        Objects.requireNonNull(custodyReads);
        Objects.requireNonNull(custodyPages);
        finite(continuationTaskBytes);
        finite(finalTaskBytes);
        finite(custodyTaskBytes);
        if (Math.addExact(
                                Math.multiplyExact(2L, finalPages.maximumCanonicalBytes()),
                                TargetSloFinalMaintenance.MAXIMUM_FRAME_BYTES)
                        >= finalTaskBytes
                || Math.addExact(
                                Math.multiplyExact(2L, custodyPages.maximumCanonicalBytes()),
                                Math.addExact(
                                        MAXIMUM_CUSTODY_FRAME_BYTES,
                                        Math.addExact(
                                                TargetSloCustodyReceipt.MAX_CANONICAL_BYTES,
                                                TargetSloWriterEpoch.MAX_CANONICAL_BYTES)))
                        >= custodyTaskBytes
                || finalReads.bytes() < finalPages.maximumCanonicalBytes()
                || custodyReads.bytes() < custodyPages.maximumCanonicalBytes()) {
            throw new IllegalArgumentException(
                    "SLO page/read/task envelopes cannot retain their complete original flight");
        }
        Math.addExact(Math.addExact(continuationTaskBytes, finalTaskBytes), custodyTaskBytes);
        Math.addExact(
                Math.addExact(continuationTaskBytes, finalTaskBytes),
                Math.addExact(custodyTaskBytes, Math.addExact(finalReads.bytes(), custodyReads.bytes())));
    }

    public static TargetWorkerSloServiceConfiguration load(Path file) throws IOException {
        final var f = TargetWorkerRuntimeConfigLoader.loadFields(file);
        if (f.integer("version") != 1) {
            throw new IllegalArgumentException("unsupported SLO service configuration version");
        }
        final var value = new TargetWorkerSloServiceConfiguration(
                f.text("providers.reference"),
                f.number("continuation.maximumTaskBytes"),
                reads(f, "final.read."),
                new TargetSloFinalMaintenance.Limits(
                        f.integer("final.page.maximumRows"), f.number("final.page.maximumCanonicalBytes")),
                f.number("final.maximumTaskBytes"),
                reads(f, "custody.read."),
                new TargetSloExportPageReader.Limits(
                        f.integer("custody.page.maximumRows"),
                        f.number("custody.page.maximumCanonicalBytes"),
                        f.integer("custody.page.maximumEventsPerExport")),
                f.number("custody.maximumTaskBytes"));
        f.requireComplete();
        return value;
    }

    private static ReadLimits reads(TargetWorkerRuntimeConfigLoader.Fields fields, String prefix) {
        return new ReadLimits(
                fields.integer(prefix + "records"), fields.number(prefix + "bytes"), fields.number(prefix + "nanos"));
    }

    public void requireWorker(TargetWorkerRuntimeConfig worker) {
        Objects.requireNonNull(worker);
        if (worker.maximumSourceFrameBytes()
                        < Math.max(TargetSloFinalMaintenance.MAXIMUM_FRAME_BYTES, MAXIMUM_CUSTODY_FRAME_BYTES)
                || continuationTaskBytes
                        <= Math.addExact(
                                worker.maximumSourceFrameBytes(), TargetSloFinalMaintenance.MAXIMUM_FRAME_BYTES)) {
            throw new IllegalArgumentException(
                    "SLO service cannot retain its business Source frame and complete observation frame");
        }
        for (long bytes : new long[] {continuationTaskBytes, finalTaskBytes, custodyTaskBytes}) {
            worker.workClasses()
                    .requireTaskServiceable(
                            new WorkClassTask(WorkClass.OUTCOME_AND_CONTROL, "target-slo-service-envelope", bytes));
        }
    }

    public void requireProviders(Providers providers) {
        if (!providersReference.equals(Objects.requireNonNull(providers).reference())) {
            throw new IllegalArgumentException(
                    "SLO configuration does not reference this Source's original provider bundle");
        }
    }

    public long maximumQueuedBytes() {
        return Math.addExact(Math.addExact(continuationTaskBytes, finalTaskBytes), custodyTaskBytes);
    }

    /** Declared concurrent task/read byte envelopes; this is a configured budget, not a heap measurement. */
    public long retainedHeapBytes() {
        return Math.addExact(maximumQueuedBytes(), Math.addExact(finalReads.bytes(), custodyReads.bytes()));
    }

    private static void finite(long value) {
        if (value < 1 || value == Long.MAX_VALUE) {
            throw new IllegalArgumentException("SLO task envelope must be finite");
        }
    }

    private static String providerReference(String value) {
        if (value == null || !value.matches("[A-Za-z0-9._/-]{1,256}")) {
            throw new IllegalArgumentException("SLO service requires a literal original provider reference");
        }
        return value;
    }
}
