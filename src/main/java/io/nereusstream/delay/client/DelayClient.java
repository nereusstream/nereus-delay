package io.nereusstream.delay.client;

import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.LargeScheduleIntent;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ScheduleIntent;
import io.nereusstream.delay.runtime.CommandResult;

import java.util.List;
import java.util.concurrent.CompletionStage;

/** Client contract separating preparation, queueing and authoritative application. */
public interface DelayClient extends AutoCloseable {
    PreparedCommand prepareSchedule(ScheduleIntent intent, long retryUntilEpochMs);

    PreparedCommand prepareLargeSchedule(LargeScheduleIntent intent, long retryUntilEpochMs);

    PreparedCommand prepareCancel(DelayMessageId messageId, int expectedGeneration, long retryUntilEpochMs);

    PreparedCommand prepareReschedule(DelayMessageId messageId, int expectedGeneration, long deliverAtEpochMs,
                                      long expireAtEpochMs, long retryUntilEpochMs);

    CompletionStage<EnqueueOutcome> enqueue(PreparedCommand command);

    /** Enqueues each prepared command independently and returns outcomes in input order. */
    CompletionStage<List<EnqueueOutcome>> enqueueBatch(List<PreparedCommand> commands);

    CompletionStage<CommandResult> awaitApplied(CommandQueuedReceipt receipt);

    @Override
    void close();
}
