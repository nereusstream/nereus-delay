package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.*;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Local Native joins for the two OBJECT stages. Actual provider/admission and original trust remain independent. */
public final class TargetObjectPayloadReclaim {
    private TargetObjectPayloadReclaim() {}

    public static TargetQuotaMutation requireClosed(TargetCheckpointDependencies d,SystemMutation input,
            List<TargetInlinePayloadReclaim.Policy> policies,boolean allowApplied,Runnable current) {
        final var body=TargetResourceReclaimBody.decode(input.canonicalBody());
        if(!body.objectPayloadReclaim())throw new IllegalArgumentException("OBJECT lifecycle body required");
        if(body.uncommittedObjectReclaim())return TargetUncommittedPayloadReclaim.requireClosed(d,input,allowApplied,current);
        TargetInlinePayloadReclaim.requireRecordedSourceFacts(d,body);
        if(body.objectOperation().nativeOrigin()!=null)TargetObjectPayloadAcceptance.requireActual(d,body.objectOperation().expected(),body.objectOperation().nativeOrigin());
        final var first=d.resultRecord(systemKey(input));
        final boolean applied=first!=null&&first.appliedFirst();
        if(applied){
            if(!allowApplied)throw new IllegalStateException("OBJECT stage already has a Source decision");
            requireFirst(d,input,first.mutation());
        }
        final var owner=Objects.requireNonNull(d.payloadOwner(body.objectOperation().expected().messageId()),"OBJECT owner");
        TargetResourceReclaimBody closure=body;
        if(owner.phase()==TargetQuotaPayloadOwner.Phase.RELEASED){
            if(!applied)throw new IllegalStateException("OBJECT changed to released before this Source decision");
            final var history=d.sourceHistories().stream().filter(h->h.decidedAt().equals(owner.mutation())).findFirst().orElseThrow();
            closure=TargetResourceReclaimBody.decode(history.input().canonicalBody());
            if(!(closure.operations().getFirst() instanceof TargetResourceReclaimBody.ObjectPayloadConfirm confirmation))
                throw new IllegalStateException("OBJECT owner released without a dedicated confirmation");
            requireSameIntent(body.operations().getFirst() instanceof TargetResourceReclaimBody.ObjectPayloadConfirm selected?selected.intent()
                    :new TargetPayloadObjectRetiringRecord(input,first.mutation()),confirmation.intent());
            requireFirst(d,history.input(),history.decidedAt());
        }
        TargetInlinePayloadReclaim.requireClosed(d,closure,policies,
                TargetInlinePayloadReclaim.SourceFacts.from(closure.payloadFacts()),allowApplied,current);
        final var pending=d.payloadObjectIntent(body.objectOperation().expected().messageId());
        if(owner.phase()==TargetQuotaPayloadOwner.Phase.RELEASED){
            if(pending!=null)throw new IllegalStateException("released OBJECT retains its pending intent");
        }else if(body.operations().getFirst() instanceof TargetResourceReclaimBody.ObjectPayloadConfirm confirmation){
            requireSameIntent(confirmation.intent(),Objects.requireNonNull(pending,"OBJECT confirmation lost pending intent"));
            requireFirst(d,pending.intent(),pending.appliedAt());
        }else if(applied){
            requireSameIntent(new TargetPayloadObjectRetiringRecord(input,first.mutation()),Objects.requireNonNull(pending,"APPLIED OBJECT intent missing"));
        }else if(pending!=null)throw new IllegalStateException("OBJECT already has a different retirement intent");
        current.run();return applied?first.mutation():null;
    }

    /** Caller must first complete Inline.RetirementCursor for this exact ledger and input. */
    public static TargetQuotaMutation requirePreparedClosed(TargetRetirementLedgerCursor ledger,SystemMutation input,
            boolean allowApplied,Runnable current){
        final var d=Objects.requireNonNull(ledger).dependenciesForClosure();
        final var body=TargetResourceReclaimBody.decode(input.canonicalBody());
        if(!body.objectPayloadReclaim())throw new IllegalArgumentException("OBJECT lifecycle body required");
        if(body.uncommittedObjectReclaim())return TargetUncommittedPayloadReclaim.requirePreparedClosed(ledger,input,allowApplied,current);
        TargetInlinePayloadReclaim.requireRecordedSourceFacts(d,body);
        if(body.objectOperation().nativeOrigin()!=null)TargetObjectPayloadAcceptance.requireActual(d,body.objectOperation().expected(),body.objectOperation().nativeOrigin());
        final var first=d.resultRecord(systemKey(input));final boolean applied=first!=null&&first.appliedFirst();
        if(applied){
            if(!allowApplied)throw new IllegalStateException("OBJECT stage already has a Source decision");
            requirePreparedFirst(d,input,first.mutation());
        }
        final var owner=Objects.requireNonNull(d.payloadOwner(body.objectOperation().expected().messageId()),"OBJECT owner");
        if(owner.phase()==TargetQuotaPayloadOwner.Phase.RELEASED){
            if(!applied)throw new IllegalStateException("OBJECT changed to released before this Source decision");
            final var release=Objects.requireNonNull(ledger.resultAt(owner.mutation()),"OBJECT release Source first");
            if(release.kind()!=TargetResultRecord.Kind.SYSTEM||!release.appliedFirst()||!release.mutation().equals(owner.mutation()))
                throw new IllegalStateException("OBJECT release changed actual Source first");
            final var history=Objects.requireNonNull(d.sourceHistory(release.logicalId()),"OBJECT release Source history");
            if(!history.decidedAt().equals(owner.mutation()))throw new IllegalStateException("OBJECT release changed actual Source history");
            final var closure=TargetResourceReclaimBody.decode(history.input().canonicalBody());
            if(!(closure.operations().getFirst() instanceof TargetResourceReclaimBody.ObjectPayloadConfirm confirmation))
                throw new IllegalStateException("OBJECT owner released without a dedicated confirmation");
            requireSameIntent(body.operations().getFirst() instanceof TargetResourceReclaimBody.ObjectPayloadConfirm selected?selected.intent()
                    :new TargetPayloadObjectRetiringRecord(input,first.mutation()),confirmation.intent());
            requirePreparedFirst(d,history.input(),history.decidedAt());
            TargetInlinePayloadReclaim.requireRecordedSourceFacts(d,closure);
        }
        final var pending=d.payloadObjectIntent(body.objectOperation().expected().messageId());
        if(owner.phase()==TargetQuotaPayloadOwner.Phase.RELEASED){
            if(pending!=null)throw new IllegalStateException("released OBJECT retains its pending intent");
        }else if(body.operations().getFirst() instanceof TargetResourceReclaimBody.ObjectPayloadConfirm confirmation){
            requireSameIntent(confirmation.intent(),Objects.requireNonNull(pending,"OBJECT confirmation lost pending intent"));
            requirePreparedFirst(d,pending.intent(),pending.appliedAt());
        }else if(applied){
            requireSameIntent(new TargetPayloadObjectRetiringRecord(input,first.mutation()),Objects.requireNonNull(pending,"APPLIED OBJECT intent missing"));
        }else if(pending!=null)throw new IllegalStateException("OBJECT already has a different retirement intent");
        current.run();return applied?first.mutation():null;
    }

    public static void requireCurrent(TargetStoreBackend.Reader reader,SystemMutation input,
            List<TargetInlinePayloadReclaim.Policy> policies,Runnable current){
        final var body=TargetResourceReclaimBody.decode(input.canonicalBody());
        if(!body.objectPayloadReclaim())throw new IllegalArgumentException("OBJECT stage required");
        if(body.uncommittedObjectReclaim()){TargetUncommittedPayloadReclaim.requireCurrent(reader,input,current);return;}
        TargetInlinePayloadReclaim.requireCurrent(reader,body,policies,current);
        if(body.objectOperation().nativeOrigin()!=null)TargetObjectPayloadAcceptance.requireActual(reader,List.of(),body.objectOperation().expected(),body.objectOperation().nativeOrigin(),reader.aggregate().mutation());
        final var operation=body.objectOperation();
        final byte[] key=TargetPayloadObjectRetiringRecord.key(operation.expected().messageId());
        final byte[] raw=reader.get(ColumnFamily.META,key);
        if(body.operations().getFirst() instanceof TargetResourceReclaimBody.ObjectPayloadConfirm confirmation){
            final var pending=TargetPayloadObjectRetiringRecord.decodeForStore(key,
                    TargetValueEnvelope.decode(Objects.requireNonNull(raw,"pending OBJECT intent"),TargetPayloadObjectRetiringRecord.VALUE_TYPE).payload(),body.scope(),body.lineage());
            requireSameIntent(confirmation.intent(),pending);
            requireFirst(reader,List.of(),pending.intent(),pending.appliedAt(),reader.aggregate().mutation());
        }else if(raw!=null)throw new IllegalStateException("OBJECT intent already exists");
        current.run();
    }

    /** Checks a charged pending row without recursing through Native54/55 accounting. */
    public static void requirePending(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetPayloadObjectRetiringRecord record,TargetQuotaMutation frontier){
        final var body=record.body();
        if(body.uncommittedObjectReclaim()){TargetUncommittedPayloadReclaim.requirePending(view,overlay,record,frontier);return;}
        record.appliedAt().requireAtOrBefore(frontier);
        requireFirst(view,overlay,record.intent(),record.appliedAt(),frontier);
        TargetInlinePayloadReclaim.requireRecordedSourceFacts(view,overlay,body,frontier);
        if(body.objectOperation().nativeOrigin()!=null)TargetObjectPayloadAcceptance.requireActual(view,overlay,body.objectOperation().expected(),body.objectOperation().nativeOrigin(),frontier);
        requirePrimary(view,overlay,body.objectOperation(),frontier);
        requireTerminal(view,overlay,body.objectOperation(),frontier).mutation().requireCoveredByFloor(body.floor());
    }

    public static void requireAppliedDecision(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            SystemMutation input,TargetQuotaMutation decided,TargetQuotaMutation frontier){
        final var body=TargetResourceReclaimBody.decode(input.canonicalBody());
        if(!body.objectPayloadReclaim())throw new IllegalArgumentException("OBJECT Source decision required");
        if(body.uncommittedObjectReclaim()){TargetUncommittedPayloadReclaim.requireAppliedDecision(view,overlay,input,decided,frontier);return;}
        TargetInlinePayloadReclaim.requireRecordedSourceFacts(view,overlay,body,frontier);
        if(body.objectOperation().nativeOrigin()!=null)TargetObjectPayloadAcceptance.requireActual(view,overlay,body.objectOperation().expected(),body.objectOperation().nativeOrigin(),frontier);
        final var selected=body.objectOperation();
        requireTerminal(view,overlay,selected,frontier).mutation().requireCoveredByFloor(body.floor());
        final var expected=selected.expected();
        final var owner=TargetQuotaPayloadOwner.decodeForStore(expected.key(),payload(view,overlay,ColumnFamily.META,expected.key(),TargetQuotaPayloadOwner.VALUE_TYPE),body.shard(),body.scope().tenantScope());
        final var original=body.operations().getFirst() instanceof TargetResourceReclaimBody.ObjectPayloadConfirm confirmation
                ?confirmation.intent():new TargetPayloadObjectRetiringRecord(input,decided);
        requireFirst(view,overlay,original.intent(),original.appliedAt(),frontier);
        final byte[] pending=view.projected(ColumnFamily.META,original.key(),overlay);
        if(owner.phase()==TargetQuotaPayloadOwner.Phase.RETAINED){
            if(body.operations().getFirst() instanceof TargetResourceReclaimBody.ObjectPayloadConfirm)
                throw new IllegalStateException("APPLIED OBJECT confirmation retains the owner charge");
            requireSameIntent(original,TargetPayloadObjectRetiringRecord.decodeForStore(original.key(),
                    TargetValueEnvelope.decode(Objects.requireNonNull(pending,"pending OBJECT row"),TargetPayloadObjectRetiringRecord.VALUE_TYPE).payload(),body.scope(),body.lineage()));
            requirePrimary(view,overlay,selected,frontier);return;
        }
        if(owner.phase()!=TargetQuotaPayloadOwner.Phase.RELEASED||pending!=null
                ||view.projected(ColumnFamily.ID,TargetKeyCodec.message(expected.messageId()),overlay)!=null)
            throw new IllegalStateException("OBJECT final state is neither retained intent nor exact released confirmation");
        final var history=releaseHistory(view,overlay,body.scope(),body.lineage(),owner,frontier);
        final var confirmationBody=TargetResourceReclaimBody.decode(history.input().canonicalBody());
        if(!(confirmationBody.operations().getFirst() instanceof TargetResourceReclaimBody.ObjectPayloadConfirm confirmation))
            throw new IllegalStateException("released OBJECT lacks its dedicated Source confirmation");
        requireSameIntent(original,confirmation.intent());
        if(body.operations().getFirst() instanceof TargetResourceReclaimBody.ObjectPayloadConfirm
                &&(!Arrays.equals(input.canonicalEnvelope(),history.input().canonicalEnvelope())||!decided.equals(owner.mutation())))
            throw new IllegalStateException("OBJECT confirmation changed actual released Source decision");
        owner.requireReleasedFrom(expected,confirmationBody.floor());
        requireTerminal(view,overlay,selected,frontier);
    }

    public static TargetRetirementSourceHistoryRecord releaseHistory(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,byte[] lineage,TargetQuotaPayloadOwner owner,TargetQuotaMutation frontier){
        if(owner.kind()!=TargetQuotaPayloadOwner.Kind.OBJECT||owner.phase()!=TargetQuotaPayloadOwner.Phase.RELEASED)
            throw new IllegalArgumentException("released OBJECT owner required");
        if(owner.committedPayload()==null)return TargetUncommittedPayloadReclaim.releaseHistory(view,overlay,scope,lineage,owner,frontier);
        final byte[] positionKey=positionKey(owner.mutation());
        final var position=TargetResultRecord.decode(payload(view,overlay,ColumnFamily.DEDUPE,positionKey,TargetResultRecord.VALUE_TYPE));
        position.requireStored(positionKey,TargetResultRecord.VALUE_TYPE,position.canonicalBytes());
        if(position.kind()!=TargetResultRecord.Kind.POSITION_SYSTEM||!position.mutation().equals(owner.mutation()))
            throw new IllegalStateException("OBJECT release lost actual Source POSITION");
        final byte[] key=TargetRetirementSourceHistoryRecord.key(position.logicalId());
        final var history=TargetRetirementSourceHistoryRecord.decodeForStore(key,payload(view,overlay,ColumnFamily.META,key,TargetRetirementSourceHistoryRecord.VALUE_TYPE),scope,lineage);
        if(!history.decidedAt().equals(owner.mutation())||!history.authorizationTuple().equals(ProtocolTuple.targetObjectPayloadReclaim()))
            throw new IllegalStateException("OBJECT release changed exact Native history");
        requireFirst(view,overlay,history.input(),history.decidedAt(),frontier);return history;
    }
    private static void requirePrimary(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetResourceReclaimBody.ObjectPayloadRetire selected,TargetQuotaMutation frontier){
        final var expected=selected.expected();
        if(!Arrays.equals(expected.canonicalBytes(),payload(view,overlay,ColumnFamily.META,expected.key(),TargetQuotaPayloadOwner.VALUE_TYPE)))
            throw new IllegalStateException("pending OBJECT changed its frozen owner/fees");
        final byte[] key=TargetKeyCodec.message(expected.messageId());
        final byte[] primary=payload(view,overlay,ColumnFamily.ID,key,TargetMessageRecord.VALUE_TYPE);
        final var message=TargetMessageRecord.decodeForStore(key,primary,expected.primaryIdentity().shard());expected.requireMessagePayload(message);
        if(!message.locator().equals(selected.locator())||message.stateVersion()!=selected.stateVersion()
                ||!Arrays.equals(Bytes.sha256(primary),selected.primaryDigest())||!message.runtime().terminal()||!message.runtime().attemptObligations().isEmpty())
            throw new IllegalStateException("pending OBJECT changed its exact terminal primary");
        final var terminal=requireTerminal(view,overlay,selected,frontier);
        if(!Arrays.equals(terminal.runtime().canonicalBytes(),message.runtime().canonicalBytes()))throw new IllegalStateException("OBJECT terminal and primary differ");
    }
    private static TargetTerminalGenerationRecord requireTerminal(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetResourceReclaimBody.ObjectPayloadRetire selected,TargetQuotaMutation frontier){
        final byte[] key=TargetTerminalGenerationRecord.key(selected.locator());
        final byte[] raw=payload(view,overlay,ColumnFamily.TERMINAL,key,TargetTerminalGenerationRecord.VALUE_TYPE);
        final var terminal=TargetTerminalGenerationRecord.decode(raw);terminal.requireOwner(selected.expected());terminal.mutation().requireAtOrBefore(frontier);
        if(!Arrays.equals(key,terminal.key())||terminal.stateVersion()!=selected.stateVersion()||!Arrays.equals(Bytes.sha256(raw),selected.terminalDigest())
                ||!terminal.runtime().terminal()||!terminal.runtime().attemptObligations().isEmpty())throw new IllegalStateException("OBJECT lost original terminal metadata");return terminal;
    }
    private static void requireFirst(TargetCheckpointDependencies d,SystemMutation input,TargetQuotaMutation stamp){
        final var first=Objects.requireNonNull(d.resultRecord(systemKey(input)),"OBJECT actual Source first");
        if(!first.mutation().equals(stamp)||!Arrays.equals(first.typedPayload(),SystemMutationResult.from(input,ApplyStatus.APPLIED,StableCode.OK,stamp.source().canonicalBytes()).encode()))
            throw new IllegalStateException("OBJECT input differs from APPLIED/OK Source first");d.requireSystemPosition(first);
        final var history=d.sourceHistories().stream().filter(h->h.decidedAt().equals(stamp)).findFirst().orElseThrow();
        if(!Arrays.equals(history.input().canonicalEnvelope(),input.canonicalEnvelope()))throw new IllegalStateException("OBJECT input differs from retained exact history");
    }
    private static void requirePreparedFirst(TargetCheckpointDependencies d,SystemMutation input,TargetQuotaMutation stamp){
        final var first=Objects.requireNonNull(d.resultRecord(systemKey(input)),"OBJECT actual Source first");
        if(!first.mutation().equals(stamp)||!Arrays.equals(first.typedPayload(),SystemMutationResult.from(input,ApplyStatus.APPLIED,StableCode.OK,stamp.source().canonicalBytes()).encode()))
            throw new IllegalStateException("OBJECT input differs from APPLIED/OK Source first");d.requireSystemPosition(first);
        final var history=Objects.requireNonNull(d.sourceHistory(input.systemMutationId()),"OBJECT retained exact Source history");
        if(!history.decidedAt().equals(stamp)||!Arrays.equals(history.input().canonicalEnvelope(),input.canonicalEnvelope()))
            throw new IllegalStateException("OBJECT input differs from retained exact history");
    }
    private static void requireFirst(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            SystemMutation input,TargetQuotaMutation stamp,TargetQuotaMutation frontier){
        stamp.requireAtOrBefore(frontier);
        final byte[] key=systemKey(input);
        final var first=TargetResultRecord.decode(payload(view,overlay,ColumnFamily.DEDUPE,key,TargetResultRecord.VALUE_TYPE));
        first.requireStored(key,TargetResultRecord.VALUE_TYPE,first.canonicalBytes());
        if(!first.mutation().equals(stamp)||!Arrays.equals(first.typedPayload(),SystemMutationResult.from(input,ApplyStatus.APPLIED,StableCode.OK,stamp.source().canonicalBytes()).encode()))
            throw new IllegalStateException("OBJECT lost its exact APPLIED/OK Source first");
        final byte[] positionKey=positionKey(stamp);
        final var position=TargetResultRecord.decode(payload(view,overlay,ColumnFamily.DEDUPE,positionKey,TargetResultRecord.VALUE_TYPE));
        position.requireStored(positionKey,TargetResultRecord.VALUE_TYPE,position.canonicalBytes());position.requireFirst(first);
        if(!position.mutation().equals(stamp))throw new IllegalStateException("OBJECT lost actual Source POSITION stamp");
        final byte[] historyKey=TargetRetirementSourceHistoryRecord.key(input.systemMutationId());
        final var history=TargetRetirementSourceHistoryRecord.decode(payload(view,overlay,ColumnFamily.META,historyKey,TargetRetirementSourceHistoryRecord.VALUE_TYPE));
        if(!history.decidedAt().equals(stamp)||!Arrays.equals(history.input().canonicalEnvelope(),input.canonicalEnvelope()))
            throw new IllegalStateException("OBJECT lost exact original Source history");
    }
    private static void requireSameIntent(TargetPayloadObjectRetiringRecord expected,TargetPayloadObjectRetiringRecord actual){
        if(!Arrays.equals(expected.canonicalBytes(),actual.canonicalBytes()))throw new IllegalStateException("OBJECT lifecycle changed its original exact intent");
    }
    private static byte[] systemKey(SystemMutation input){return Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG,TargetKeyCodec.KEY_FORMAT},input.systemMutationId());}
    private static byte[] positionKey(TargetQuotaMutation stamp){return Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG,TargetKeyCodec.KEY_FORMAT},stamp.source().canonicalBytes());}
    private static byte[] payload(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,ColumnFamily family,byte[] key,int type){
        return TargetValueEnvelope.decode(Objects.requireNonNull(view.projected(family,key,overlay),"OBJECT lifecycle dependency"),type).payload();
    }
}
