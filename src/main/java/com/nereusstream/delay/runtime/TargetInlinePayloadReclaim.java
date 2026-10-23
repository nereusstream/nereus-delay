package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.*;
import java.util.*;

/** Complete terminal-payload joins shared by the separately admitted INLINE and OBJECT stages. These local facts do not grant reader, Pin or Source-history authority. */
public final class TargetInlinePayloadReclaim {
    private TargetInlinePayloadReclaim() {}
    /** Original Source inputs authenticated by the existing held Source/image authority, never today’s config. */
    public record SourceFacts(com.nereusstream.delay.ownership.SourceReplayMutation fence,
            Map<DelayMessageId,com.nereusstream.delay.ownership.SourceReplayEntry> retained){
        public SourceFacts {
            Objects.requireNonNull(fence);retained=Map.copyOf(retained);
            if(fence.mutation().type()!=SystemMutationType.TIME_FENCE||retained.isEmpty()||retained.size()>32)
                throw new IllegalArgumentException("bounded original inline Source facts required");
            TargetTimeFenceBody.decode(fence.mutation().canonicalBody());
            for(var entry:retained.values())TargetSourcePosition.requireBounded(entry.position());
            TargetSourcePosition.requireBounded(fence.position());
        }
        public long retainedBytes(){
            long bytes=fence.mutation().canonicalEnvelope().length+fence.position().canonicalBytes().length+256L;
            for(var entry:retained.values()){
                final long frame=entry instanceof com.nereusstream.delay.ownership.SourceReplayMutation mutation
                        ?mutation.mutation().canonicalEnvelope().length
                        :CommandCodec.encodeFrame(((com.nereusstream.delay.ownership.SourceReplayRecord)entry).command()).length;
                if(frame>TargetResourceReclaimBody.MAX_CANONICAL_BYTES+4096L)throw new IllegalArgumentException("inline original Source input exceeds bounded frame");
                bytes=Math.addExact(bytes,frame+entry.position().canonicalBytes().length+256L);
            }
            return bytes;
        }
        /** Freezes only already correlated actual Source stamps; the original Native rows remain mandatory dependencies. */
        public TargetInlinePayloadSourceFacts freeze(TargetCheckpointDependencies dependencies,TargetResourceReclaimBody body){
            final var values=new ArrayList<TargetInlinePayloadSourceFacts.Retained>();
            if(!retained.keySet().equals(ids(body)))throw new IllegalArgumentException("SourceFacts changed selected Message group");
            for(var operation:body.payloadOperations()){
                final var owner=operation.expected();
                dependencies.requireRetentionDecision(owner);
                values.add(new TargetInlinePayloadSourceFacts.Retained(owner.messageId(),frame(retained.get(owner.messageId()),owner.mutation())));
            }
            return new TargetInlinePayloadSourceFacts(frame(fence,dependencies.requireTimeFence(fence).mutation()),values);
        }
        private static TargetInlinePayloadSourceFacts.Frame frame(com.nereusstream.delay.ownership.SourceReplayEntry entry,TargetQuotaMutation stamp){
            if(!Arrays.equals(entry.position().canonicalBytes(),stamp.source().canonicalBytes()))throw new IllegalArgumentException("SourceFacts changed actual Source position");
            return entry instanceof com.nereusstream.delay.ownership.SourceReplayMutation mutation
                    ?new TargetInlinePayloadSourceFacts.Frame(2,stamp,mutation.mutation().canonicalEnvelope())
                    :new TargetInlinePayloadSourceFacts.Frame(1,stamp,CommandCodec.encodeFrame(((com.nereusstream.delay.ownership.SourceReplayRecord)entry).command()));
        }
        public static SourceFacts from(TargetInlinePayloadSourceFacts capsule){
            final var values=new HashMap<DelayMessageId,com.nereusstream.delay.ownership.SourceReplayEntry>();
            for(var original:capsule.retained())values.put(original.message(),entry(original.input()));
            return new SourceFacts((com.nereusstream.delay.ownership.SourceReplayMutation)entry(capsule.fence()),values);
        }
        private static com.nereusstream.delay.ownership.SourceReplayEntry entry(TargetInlinePayloadSourceFacts.Frame frame){
            return frame.kind()==2?new com.nereusstream.delay.ownership.SourceReplayMutation(SystemMutation.decodeEnvelope(frame.frame()),frame.appliedAt().source(),null,null)
                    :new com.nereusstream.delay.ownership.SourceReplayRecord(CommandCodec.decodeFrame(frame.frame()),frame.appliedAt().source(),null,null);
        }
    }
    public record Policy(DelayMessageId message,TargetCheckpointDependencies.RetryUse use,
            RetryPolicySemantic retry,TerminalPolicySemantic terminal){
        public Policy { Objects.requireNonNull(message);Objects.requireNonNull(use);Objects.requireNonNull(retry);Objects.requireNonNull(terminal);
            if(!use.reference().matches(retry))throw new IllegalArgumentException("inline policy changed its original Retry reference");
            terminal.requirePrimaryReleasePolicy(retry);
        }
    }
    /** Durable dependency check shared by independent ledger and companion readers; never an authority grant. */
    public static void requireRecordedSourceFacts(TargetCheckpointDependencies dependencies,TargetResourceReclaimBody body){
        if(!body.selfContainedPayloadRelease())throw new IllegalArgumentException("self-contained inline history required");
        final var facts=SourceFacts.from(body.payloadFacts());
        if(!Arrays.equals(body.payloadFacts().canonicalBytes(),facts.freeze(dependencies,body).canonicalBytes()))
            throw new IllegalStateException("retained inline capsule differs from original Native first/POSITION history");
        body.payloadFacts().requireFor(body);
        final var budgets=budgets(dependencies);
        for(var original:body.payloadFacts().retained())requireRetainedIdentity(original.message(),facts.retained().get(original.message()),
                original.input().appliedAt(),id->Objects.requireNonNull(budgets.get(Bytes.hex(id)),"retained Source original Attempt"));
    }
    public static void requireRecordedSourceFacts(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetResourceReclaimBody body,TargetQuotaMutation frontier){
        if(!body.selfContainedPayloadRelease())throw new IllegalArgumentException("self-contained inline history required");
        body.payloadFacts().requireFor(body);final var facts=SourceFacts.from(body.payloadFacts());
        final var fence=first(view,overlay,facts.fence(),body.payloadFacts().fence().appliedAt(),frontier);
        if(SystemMutationResult.decode(fence.typedPayload()).stableCode()!=StableCode.OK)throw new IllegalStateException("original closure TimeFence was not APPLIED OK");
        for(var original:body.payloadFacts().retained()){
            final var entry=facts.retained().get(original.message());first(view,overlay,entry,original.input().appliedAt(),frontier);
            requireRetainedIdentity(original.message(),entry,original.input().appliedAt(),id->budget(view,overlay,id,body.shard()));
        }
    }
    private static Map<String,TargetQuotaAttemptBudget> budgets(TargetCheckpointDependencies dependencies){
        final var budgets=new HashMap<String,TargetQuotaAttemptBudget>();
        for(var admission:dependencies.admissions())budgets.put(Bytes.hex(admission.attempt().publishAttemptId()),admission.attempt());
        return budgets;
    }
    private static TargetQuotaAttemptBudget budget(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,byte[] id,ShardId shard){
        final byte[] key=Bytes.concat(new byte[]{TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG,TargetKeyCodec.KEY_FORMAT},id);
        return TargetQuotaAttemptBudget.decodeForStore(key,TargetValueEnvelope.decode(Objects.requireNonNull(view.projected(ColumnFamily.META,key,overlay),
                "retained Source original Attempt"),TargetQuotaAttemptBudget.VALUE_TYPE).payload(),shard);
    }
    /** An Outcome names an Attempt; its immutable Native locator must identify this selected Message. */
    private static void requireRetainedIdentity(DelayMessageId message,com.nereusstream.delay.ownership.SourceReplayEntry entry,
            TargetQuotaMutation stamp,java.util.function.Function<byte[],TargetQuotaAttemptBudget> budgets){
        if(entry instanceof com.nereusstream.delay.ownership.SourceReplayRecord command){
            if(command.command().type()!=CommandType.CANCEL||!command.command().delayMessageId().equals(message))
                throw new IllegalStateException("original Command is not this INLINE Message's retention transition");return;
        }
        final var mutation=((com.nereusstream.delay.ownership.SourceReplayMutation)entry).mutation();
        if(mutation.type()==SystemMutationType.EXPIRE_GENERATION){
            if(!TargetExpireGenerationBody.decode(mutation.canonicalBody()).messageId().equals(message))throw new IllegalStateException("retained expiry changed Message");return;
        }
        final var outcome=switch(mutation.type()){
            case PUBLISH_OUTCOME->PublishOutcomeBody.decode(mutation.canonicalBody());
            case EVIDENCE_RESOLUTION->PublishOutcomeBody.decodeEvidenceResolution(mutation.canonicalBody());
            default->throw new IllegalStateException("Source operation cannot retain this INLINE primary");
        };
        final var budget=budgets.apply(outcome.publishAttemptId());
        if(!budget.locator().messageId().equals(message)||budget.resolvedAt()==null||!budget.resolvedAt().equals(stamp)
                ||budget.phase()==TargetQuotaAttemptBudget.Phase.ADMITTED||budget.phase()==TargetQuotaAttemptBudget.Phase.UNKNOWN)
            throw new IllegalStateException("retained Outcome differs from selected Message's definitive Attempt");
    }
    /** Policy bytes come from this same fully authenticated companion, at every actual retained binding use. */
    public static List<Policy> policies(TargetCheckpointDependencies dependencies,TargetCheckpointSemanticSnapshot snapshot,
            TargetResourceReclaimBody body,Runnable current){
        final var required=dependencies.retryUsesFor(ids(body),current);
        final var values=new HashMap<String,RetryPolicySemantic>();
        snapshot.retryPolicies().forEach(value->values.put(Bytes.hex(value.ref().canonicalBytes()),value));
        final var contents=new HashMap<String,TerminalPolicySemantic>();
        snapshot.terminalPolicies().forEach(value->contents.put(Bytes.hex(value.use().canonicalBytes()),value.policy()));
        final var result=new ArrayList<Policy>();
        for(var operation:body.payloadOperations()){
            current.run();final var expected=operation.expected();
            final var uses=required.get(expected.messageId());
            if(uses==null||uses.isEmpty())throw unavailable("inline owner has no complete retained binding history");
            for(var use:uses){
                current.run();result.add(new Policy(expected.messageId(),use,
                        Objects.requireNonNull(values.get(Bytes.hex(use.reference().canonicalBytes())),"original Retry content"),
                        Objects.requireNonNull(contents.get(Bytes.hex(use.canonicalBytes())),"original terminal policy content")));
            }
        }
        return List.copyOf(result);
    }
    /**
     * A complete physical/ledger fold has already joined every Claim/index/barrier/open Attempt and alias.
     * Here every generation and original writer of the selected identities must be terminal before the Floor.
     * The authenticated ingress fence is both a trusted-time lower bound and closure of the old writer windows.
     */
    public static TargetQuotaMutation requireClosed(TargetCheckpointDependencies d,TargetResourceReclaimBody body,
            List<Policy> policies,SourceFacts facts,boolean allowReleased,Runnable current){
        final var selected=ids(body);final var root=d.root();final var floor=body.floor();
        final var retainedBudgets=budgets(d);
        if(!root.metadata().shardId().equals(body.shard())||!Arrays.equals(root.root().tenantScope(),body.scope().tenantScope())
                ||!Arrays.equals(root.root().recoveryLineage(),body.lineage()))throw new IllegalArgumentException("inline closure changed original Source scope");
        final var fenceBody=TargetTimeFenceBody.decode(facts.fence().mutation().canonicalBody());
        final var fenceFirst=d.requireTimeFence(facts.fence());fenceFirst.mutation().requireCoveredByFloor(floor);
        if(body.selfContainedPayloadRelease()&&!Arrays.equals(body.payloadFacts().canonicalBytes(),facts.freeze(d,body).canonicalBytes()))
            throw new IllegalStateException("inline Source capsule changed actual original first/POSITION facts");
        if(d.ingressFence().closedThroughEpochMs()<fenceBody.closeThrough())throw unavailable("Native Source has not applied the original closure fence");
        if(facts.retained().size()!=selected.size()||!facts.retained().keySet().equals(selected))throw new IllegalArgumentException("inline retained Source facts changed Message group");
        final var uses=d.retryUsesFor(selected,current);
        final var supplied=new HashMap<DelayMessageId,Map<String,Policy>>();
        for(var policy:policies){current.run();if(!selected.contains(policy.message()))throw new IllegalArgumentException("unrelated inline policy");
            final var prior=supplied.computeIfAbsent(policy.message(),id->new HashMap<>()).put(Bytes.hex(policy.use().canonicalBytes()),policy);
            if(prior!=null)throw new IllegalArgumentException("duplicate inline policy use");
        }
        final var terminals=new HashMap<String,TargetTerminalGenerationRecord>();
        for(var terminal:d.terminalGenerations()){
            current.run();if(!selected.contains(terminal.locator().messageId()))continue;
            terminal.mutation().requireCoveredByFloor(floor);
            if(!terminal.runtime().attemptObligations().isEmpty())throw unavailable("historical inline generation still owns an Attempt");
            terminals.put(Bytes.hex(terminal.key()),terminal);
        }
        TargetQuotaMutation release=null;Boolean released=null;
        for(var operation:body.payloadOperations()){
            current.run();final var inline=operation;
            final var expected=inline.expected();final var actual=Objects.requireNonNull(d.payloadOwner(expected.messageId()),"inline owner missing from complete fold");
            final boolean isReleased=actual.phase()==TargetQuotaPayloadOwner.Phase.RELEASED;
            if(released!=null&&released!=isReleased)throw new IllegalStateException("inline group is only partially released");
            released=isReleased;
            if(isReleased){
                if(!allowReleased)throw unavailable("inline Native first already has released state");
                actual.requireReleasedFrom(expected,floor);
                if(d.messageState(expected.messageId())!=null)throw new IllegalStateException("released inline image retains its primary");
                if(release!=null&&!release.equals(actual.mutation()))throw new IllegalStateException("inline group has different Source decisions");
                release=actual.mutation();
            }else{
                if(!Arrays.equals(actual.canonicalBytes(),expected.canonicalBytes()))throw unavailable("inline owner changed after selection");
                final var message=Objects.requireNonNull(d.messageState(expected.messageId()),"inline primary missing from full fold");
                if(!message.locator().equals(inline.locator())||message.stateVersion()!=inline.stateVersion()
                        ||!Arrays.equals(message.primaryDigest(),inline.primaryDigest())||!message.runtime().terminal()
                        ||!message.runtime().attemptObligations().isEmpty())throw unavailable("inline primary is not the exact closed terminal value");
            }
            final var retainedFirst=d.requireRetentionDecision(expected);expected.mutation().requireCoveredByFloor(floor);
            expected.mutation().requireAtOrBefore(fenceFirst.mutation());
            final var retainedInput=Objects.requireNonNull(facts.retained().get(expected.messageId()),"original RETAINED Source input");
            requireRetainedIdentity(expected.messageId(),retainedInput,expected.mutation(),id->Objects.requireNonNull(retainedBudgets.get(Bytes.hex(id)),"retained Source original Attempt"));
            if(!Arrays.equals(retainedInput.position().canonicalBytes(),expected.mutation().source().canonicalBytes()))
                throw new IllegalStateException("retained Source input changed original canonical persistence timestamp");
            if(retainedInput instanceof com.nereusstream.delay.ownership.SourceReplayMutation mutation){
                if(retainedFirst.kind()!=TargetResultRecord.Kind.SYSTEM)throw new IllegalStateException("retained input kind differs from original Source first");
                final var retainedResult=SystemMutationResult.decode(retainedFirst.typedPayload());
                if(!Arrays.equals(retainedFirst.typedPayload(),SystemMutationResult.from(mutation.mutation(),ApplyStatus.APPLIED,retainedResult.stableCode(),mutation.position().canonicalBytes()).encode())
                        ||!Arrays.equals(expected.mutation().mutationDigest(),Bytes.sha256(mutation.mutation().canonicalEnvelope())))
                    throw new IllegalStateException("retained Source mutation differs from original APPLIED first");
            }else{
                final var command=((com.nereusstream.delay.ownership.SourceReplayRecord)retainedInput).command();
                if(retainedFirst.kind()!=TargetResultRecord.Kind.COMMAND||!command.delayMessageId().equals(expected.messageId())
                        ||!Arrays.equals(retainedFirst.logicalId(),command.commandId().bytes())
                        ||!Arrays.equals(CommandDedupeRecord.decode(retainedFirst.typedPayload()).commandHash(),command.commandHash())
                        ||!Arrays.equals(expected.mutation().mutationDigest(),command.canonicalFrameDigest()))
                    throw new IllegalStateException("retained Source Command differs from original APPLIED first");
            }
            final var terminal=Objects.requireNonNull(terminals.get(Bytes.hex(TargetTerminalGenerationRecord.key(inline.locator()))),"inline current terminal metadata");
            terminal.requireOwner(expected);
            if(terminal.stateVersion()!=inline.stateVersion()||!Arrays.equals(Bytes.sha256(terminal.canonicalBytes()),inline.terminalDigest()))
                throw unavailable("inline terminal metadata changed after selection");
            if(!isReleased&&!Arrays.equals(terminal.runtime().canonicalBytes(),d.messageState(expected.messageId()).runtime().canonicalBytes()))
                throw new IllegalStateException("terminal metadata differs from current primary runtime");
            final var needed=uses.get(expected.messageId());final var actualPolicies=supplied.get(expected.messageId());
            if(needed==null||needed.isEmpty()||actualPolicies==null||actualPolicies.size()!=needed.size())throw unavailable("inline policy history is incomplete");
            for(var use:needed){
                current.run();final var policy=Objects.requireNonNull(actualPolicies.get(Bytes.hex(use.canonicalBytes())),"original inline policy use");
                policy.terminal().requirePrimaryReleasePolicy(policy.retry());
                if(fenceBody.closeThrough()<policy.terminal().retentionDeadline(expected.mutation()))
                    throw unavailable("authenticated Source time/fence has not closed inline retention");
            }
        }
        for(var terminal:terminals.values()){
            current.run();final var operation=body.payloadOperations().stream()
                    .filter(o->o.expected().messageId().equals(terminal.locator().messageId())).findFirst().orElseThrow();
            terminal.requireOwner(operation.expected());
            if(Integer.compareUnsigned(terminal.locator().generation(),operation.locator().generation())>0)
                throw new IllegalStateException("inline history has a future generation");
        }
        final var outcomes=new HashMap<String,TargetResultRecord>();
        for(var first:d.outcomeResults())outcomes.put(Bytes.hex(Bytes.sha256(first.mutation().canonicalBytes())),first);
        for(var admission:d.admissions()){
            current.run();final var budget=admission.attempt();if(!selected.contains(budget.locator().messageId()))continue;
            if(budget.phase()==TargetQuotaAttemptBudget.Phase.ADMITTED||budget.phase()==TargetQuotaAttemptBudget.Phase.UNKNOWN||budget.resolvedAt()==null)
                throw unavailable("inline payload still has an unresolved writer");
            budget.resolvedAt().requireCoveredByFloor(floor);admission.first().mutation().requireCoveredByFloor(floor);
            final var outcome=Objects.requireNonNull(outcomes.get(Bytes.hex(Bytes.sha256(budget.resolvedAt().canonicalBytes()))),"inline writer lost definitive Source first");
            if(!outcome.mutation().equals(budget.resolvedAt())||!outcome.appliedFirst())throw new IllegalStateException("inline writer changed definitive Source resolution");
            d.requireSystemPosition(admission.first());d.requireSystemPosition(outcome);
            final long deadline=Math.max(SystemMutationResult.decode(admission.first().typedPayload()).retryUntilEpochMs(),
                    SystemMutationResult.decode(outcome.typedPayload()).retryUntilEpochMs());
            if(fenceBody.closeThrough()<deadline)throw unavailable("Source fence has not closed original writer retry windows");
        }
        current.run();return release;
    }
    private static Set<DelayMessageId> ids(TargetResourceReclaimBody body){
        if(!body.inlinePayloadRelease()&&!body.objectPayloadReclaim())throw new IllegalArgumentException("inline operation group required");
        final var ids=new HashSet<DelayMessageId>();
        for(var operation:body.payloadOperations())ids.add(operation.expected().messageId());
        return Set.copyOf(ids);
    }
    /**
     * Source-time point checks against an opaque, fully audited Floor closure. Legal Claim/Admission/replay
     * creation changes the selected primary/runtime/owner; definitive historical writers are already closed
     * by the original fence. The independently held writer/reader admission forbids untracked aliases through
     * commit. Unrelated Native writes therefore do not require another whole-Shard fold on the Source path.
     */
    public static void requireCurrent(TargetStoreBackend.Reader reader,TargetResourceReclaimBody body,List<Policy> policies,Runnable current){
        if(!body.selfContainedPayloadRelease())throw unavailable("legacy body10 has no durable original SourceFacts protection");
        final var facts=SourceFacts.from(body.payloadFacts());
        if(!TargetResourceReclaimStore.matchesExpected(reader,body))throw unavailable("inline current Native values changed after full Floor closure");
        final var fenceBody=TargetTimeFenceBody.decode(facts.fence().mutation().canonicalBody());
        final var fenceFirst=first(reader,facts.fence(),body.payloadFacts().fence().appliedAt());
        fenceFirst.mutation().requireCoveredByFloor(body.floor());
        if(SystemMutationResult.decode(fenceFirst.typedPayload()).stableCode()!=StableCode.OK)throw new IllegalStateException("original closure TimeFence was not APPLIED OK");
        if(reader.closedIngressDeadlineThrough()<fenceBody.closeThrough())throw unavailable("Native prefix lacks the original committed closure fence");
        final var byMessage=new HashMap<DelayMessageId,List<Policy>>();for(var policy:policies)byMessage.computeIfAbsent(policy.message(),id->new ArrayList<>()).add(policy);
        if(!facts.retained().keySet().equals(ids(body)))throw new IllegalArgumentException("inline Source facts changed exact selected group");
        for(var operation:body.payloadOperations()){
            current.run();final var inline=operation;final var owner=inline.expected();
            final var original=Objects.requireNonNull(facts.retained().get(owner.messageId()),"original retained Source");
            first(reader,original,owner.mutation());
            requireRetainedIdentity(owner.messageId(),original,owner.mutation(),id->budget(reader,List.of(),id,body.shard()));
            final byte[] primary=reader.get(ColumnFamily.ID,TargetKeyCodec.message(owner.messageId()));
            final var message=TargetMessageRecord.decodeForStore(TargetKeyCodec.message(owner.messageId()),TargetValueEnvelope.decode(primary,TargetMessageRecord.VALUE_TYPE).payload(),body.shard());
            owner.requireMessagePayload(message);
            final byte[] terminalKey=TargetTerminalGenerationRecord.key(inline.locator());
            final var terminal=TargetTerminalGenerationRecord.decode(TargetValueEnvelope.decode(reader.get(ColumnFamily.TERMINAL,terminalKey),TargetTerminalGenerationRecord.VALUE_TYPE).payload());
            terminal.requireOwner(owner);terminal.mutation().requireCoveredByFloor(body.floor());
            if(!message.runtime().terminal()||!message.runtime().attemptObligations().isEmpty()||!terminal.runtime().attemptObligations().isEmpty()
                    ||!Arrays.equals(message.runtime().canonicalBytes(),terminal.runtime().canonicalBytes()))throw new IllegalStateException("inline Native terminal closure changed");
            final var originalPolicies=Objects.requireNonNull(byMessage.get(owner.messageId()),"complete original inline policies");
            for(var policy:originalPolicies){
                current.run();policy.terminal().requirePrimaryReleasePolicy(policy.retry());
                if(fenceBody.closeThrough()<policy.terminal().retentionDeadline(owner.mutation()))throw unavailable("inline retention is not closed by original Source fence");
            }
        }
        current.run();
    }
    private static TargetResultRecord first(TargetStoreBackend.Reader reader,com.nereusstream.delay.ownership.SourceReplayEntry original,TargetQuotaMutation expected){
        return first(reader,List.of(),original,expected,reader.aggregate().mutation());
    }
    private static TargetResultRecord first(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            com.nereusstream.delay.ownership.SourceReplayEntry original,TargetQuotaMutation expected,TargetQuotaMutation frontier){
        final boolean system=original instanceof com.nereusstream.delay.ownership.SourceReplayMutation;
        final byte[] id=system?((com.nereusstream.delay.ownership.SourceReplayMutation)original).mutation().systemMutationId()
                :((com.nereusstream.delay.ownership.SourceReplayRecord)original).command().commandId().bytes();
        final byte[] key=Bytes.concat(new byte[]{(byte)(system?TargetKeyCodec.RESULT_SYSTEM_TAG:TargetKeyCodec.RESULT_COMMAND_TAG),TargetKeyCodec.KEY_FORMAT},id);
        final var first=TargetResultRecord.decode(TargetValueEnvelope.decode(Objects.requireNonNull(view.projected(ColumnFamily.DEDUPE,key,overlay),"original inline Source first"),TargetResultRecord.VALUE_TYPE).payload());
        first.requireStored(key,TargetResultRecord.VALUE_TYPE,first.canonicalBytes());
        if(!first.appliedFirst()||!Arrays.equals(first.mutation().source().canonicalBytes(),original.position().canonicalBytes())
                ||expected!=null&&!first.mutation().equals(expected))throw new IllegalStateException("inline Source fact differs from actual original first");
        final byte[] digest=system?Bytes.sha256(((com.nereusstream.delay.ownership.SourceReplayMutation)original).mutation().canonicalEnvelope())
                :((com.nereusstream.delay.ownership.SourceReplayRecord)original).command().canonicalFrameDigest();
        if(!Arrays.equals(first.mutation().mutationDigest(),digest))throw new IllegalStateException("inline original Source frame digest changed");
        if(system){
            final var actual=SystemMutationResult.decode(first.typedPayload());final var input=((com.nereusstream.delay.ownership.SourceReplayMutation)original).mutation();
            if(!Arrays.equals(first.typedPayload(),SystemMutationResult.from(input,ApplyStatus.APPLIED,actual.stableCode(),original.position().canonicalBytes()).encode()))
                throw new IllegalStateException("inline original System result changed");
        }else if(!Arrays.equals(CommandDedupeRecord.decode(first.typedPayload()).commandHash(),((com.nereusstream.delay.ownership.SourceReplayRecord)original).command().commandHash()))
            throw new IllegalStateException("inline original Command hash changed");
        final byte[] posKey=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG,TargetKeyCodec.KEY_FORMAT},original.position().canonicalBytes());
        final var position=TargetResultRecord.decode(TargetValueEnvelope.decode(Objects.requireNonNull(view.projected(ColumnFamily.DEDUPE,posKey,overlay),"original inline Source POSITION"),TargetResultRecord.VALUE_TYPE).payload());
        position.requireStored(posKey,TargetResultRecord.VALUE_TYPE,position.canonicalBytes());position.requireFirst(first);
        if(!position.mutation().equals(first.mutation()))throw new IllegalStateException("inline original POSITION stamp changed");
        first.mutation().requireAtOrBefore(frontier);return first;
    }
    private static IllegalStateException unavailable(String message){return new IllegalStateException(message);}
}
