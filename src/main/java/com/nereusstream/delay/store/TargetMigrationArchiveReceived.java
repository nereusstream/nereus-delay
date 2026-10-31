package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.TargetMigrationArchivePreparation;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Actual exact immutable receiving result. Only the repository's put/readback path can mint one. */
public final class TargetMigrationArchiveReceived {
    final TargetMigrationArchiveCustody custody;
    final TargetMigrationArchiveInventory inventory;
    final TargetMigrationObjectReference inventoryObject;
    private final TargetMigrationArchiveReceipt receipt;
    private final byte[] packetDigest;
    private final int packetBytes;
    private TargetMigrationArchiveHeld audited;
    private TargetMigrationArchiveReceived(TargetMigrationArchiveCustody custody,TargetMigrationArchiveInventory inventory,
            TargetMigrationObjectReference inventoryObject,byte[] packetDigest,int packetBytes){
        this.custody=custody;this.inventory=inventory;this.inventoryObject=inventoryObject;
        this.packetDigest=Bytes.copy(packetDigest);this.packetBytes=packetBytes;
        receipt=inventory.coverage().complete()?new TargetMigrationArchiveReceipt(inventory.intentDigest(),inventory.intentAt(),
                inventory.shadowCut(),inventory.coveredThrough(),inventory.owner(),inventory.allocation(),inventory.archiveRoot(),
                inventory.coverage(),List.of(inventoryObject),inventoryObject,inventory.sourceCoverageDigest(),inventory.tailRows(),inventory.tailRawBytes()):null;
    }
    static TargetMigrationArchiveReceived receive(TargetMigrationArchiveCustody custody,TargetMigrationArchivePreparation.TransferMaterial material){
        material.requireCurrent();custody.requireCurrent();
        if(!material.intent().intention().equals(custody.intent.intention())||!material.intent().intentAt().equals(custody.intent.intentAt())
                ||!material.plan().equals(custody.intent.migration().plan()))throw new IllegalArgumentException("archive receive changed its actual Source intent/plan");
        final byte[] raw=material.packetBytes();final byte[] rawDigest=Bytes.sha256(raw);
        final String prefix="archive/"+Bytes.hex(custody.intent.intention().digest())+"/";
        final TargetMigrationObjectReference prior=material.priorInventory();TargetMigrationArchiveInventory previous=null;
        if(prior!=null){
            custody.owner.requireObject(prior.resource());previous=TargetMigrationArchiveInventory.decode(custody.objects.readMigrationObject(prior.resource(),custody.limits.maximumReceiptBytes()));
            if(!Arrays.equals(previous.archiveRoot(),material.priorArchiveRoot())
                    ||!Arrays.equals(previous.intentDigest(),custody.intent.intention().digest())||!previous.intentAt().equals(custody.intent.intentAt())
                    ||!previous.owner().equals(custody.owner.owner())||!previous.allocation().equals(custody.owner.allocation()))
                throw new IllegalStateException("previous exact archive custody belongs to another root/owner/intent");
        }else if(!Arrays.equals(material.priorArchiveRoot(),TargetMigrationArchiveRequest.genesisRoot(custody.intent.intention().digest()))){
            throw new IllegalStateException("non-genesis archive has no actual Source-bound previous inventory reference");
        }
        final int batches=previous==null?1:Math.incrementExact(previous.packets());
        final long reserved=Math.addExact(previous==null?0:previous.reservedBytes(),Math.addExact(raw.length,custody.limits.maximumReceiptBytes()));
        custody.reserve(raw.length,reserved,batches);
        final var packet=new TargetMigrationObjectReference(custody.put(prefix+"packets/"+Bytes.hex(rawDigest)+".pb",raw,custody.limits.maximumPacketBytes()));
        final byte[] archiveRoot=material.archiveRoot(packet);
        final var next=new TargetMigrationArchiveInventory(custody.intent.intention().digest(),custody.intent.intentAt(),material.shadowCut(),
                material.coveredThrough(),custody.owner.owner(),custody.owner.allocation(),material.priorArchiveRoot(),prior,packet,archiveRoot,
                material.coverage(),material.sourceCoverageDigest(),material.recordCount(),batches,reserved,custody.owner.originalObligation(),
                custody.owner.retention(),material.tailRows(),material.tailRawBytes());
        final byte[] encoded=next.canonicalBytes();
        if(encoded.length>custody.limits.maximumReceiptBytes())throw new IllegalStateException("complete original receiver fee/retention evidence exceeds its actual object reservation");
        final byte[] inventoryIdentity=Bytes.sha256(rawDigest,archiveRoot,prior==null?new byte[0]:prior.canonicalBytes());
        final var durable=new TargetMigrationObjectReference(custody.put(prefix+"inventory/"+Bytes.hex(inventoryIdentity)+".pb",encoded,custody.limits.maximumReceiptBytes()));
        material.requireCurrent();custody.requireCurrent();return new TargetMigrationArchiveReceived(custody,next,durable,rawDigest,raw.length);
    }
    public TargetMigrationRecordReference record(long ordinal){
        requireLocalCurrent();if(ordinal<0||ordinal>=inventory.records())throw new IllegalArgumentException("archive record is outside the actual complete received packet");
        return new TargetMigrationRecordReference(inventory.packet(),ordinal);
    }
    public TargetMigrationObjectReference packet(){requireLocalCurrent();return inventory.packet();}
    public TargetMigrationArchiveReceipt receipt(){requireCurrent();return Objects.requireNonNull(receipt,"archive base/range coverage is not complete");}
    boolean matchesMaterial(TargetMigrationArchivePreparation.TransferMaterial material){
        return inventory.intentAt().equals(material.intent().intentAt())&&Arrays.equals(inventory.intentDigest(),material.intent().intention().digest())
                &&inventory.shadowCut().equals(material.shadowCut())&&inventory.coveredThrough().equals(material.coveredThrough())
                &&Arrays.equals(packetDigest,Bytes.sha256(material.packetBytes()));
    }
    public void requireMaterial(TargetMigrationArchivePreparation.TransferMaterial material){
        material.requireCurrent();requireCurrent();
        if(!matchesMaterial(material)||packetBytes!=material.packetBytes().length||inventory.records()!=material.recordCount()
                ||!inventory.coverage().equals(material.coverage())||!Arrays.equals(inventory.sourceCoverageDigest(),material.sourceCoverageDigest())
                ||!Arrays.equals(inventory.archiveRoot(),material.archiveRoot(inventory.packet())))
            throw new IllegalStateException("received immutable archive differs from the privately audited original material");
    }
    public void requireTakeover(TargetMigrationRoot root,TargetMigrationRetirementRecord retirement,TargetMigrationArchiveRequest request,SourcePosition source){
        custody.requireCurrent();requireTakeoverMaterial(root,retirement,request);
        if(!request.coveredThrough().source().sameSourceIdentity(source)||source.compareTo(request.coveredThrough().source())<=0)
            throw new IllegalStateException("archive TAKEOVER changed its actual received root/coverage/Source");
    }
    /** Material qualification before signing T; it never fabricates T's still-unknown actual Source position. */
    public void requireTakeoverMaterial(TargetMigrationRoot root,TargetMigrationRetirementRecord retirement,TargetMigrationArchiveRequest request){
        requireCurrent();request.requireRetirement(root,retirement);
        if(!(request.action() instanceof TargetMigrationArchiveRequest.Takeover takeover)||!takeover.receipt().equals(receipt()))
            throw new IllegalStateException("archive TAKEOVER changed its actual complete received material");
        Objects.requireNonNull(audited,"complete exact new-owner inventory/packet audit is not finished").requireReceipt(receipt());
    }
    public void requireCompleteAudit(TargetMigrationArchiveHeld held){
        Objects.requireNonNull(held).requireReceipt(receipt());audited=held;
    }
    public TargetMigrationArchiveHeld.RuntimeHold acquireRuntime(com.nereusstream.delay.runtime.TargetMigrationArchiveStore.AppliedTakeover actual,
            TargetMigrationArchiveHeld.RuntimeOpening opening){
        return Objects.requireNonNull(audited,"complete original archive raw receiving audit").acquireRuntime(actual,opening);
    }
    public TargetMigrationObjectReference inventory(){requireLocalCurrent();return inventoryObject;}
    /** Source's Native guard consumes only the same local receiver loan; remote reads occur during preparation. */
    public void requireCurrent(){requireLocalCurrent();}
    public void requireLocalCurrent(){custody.requireLocalCurrent();}
    /** This batch's metadata only; the custody parent already owns and counts its actual receiver loan. */
    public long retainedBytes(){return Math.addExact(32L,Math.addExact(inventory.canonicalBytes().length,Math.addExact(inventoryObject.canonicalBytes().length,receipt==null?0:receipt.canonicalBytes().length)));}
    public boolean ioExited(){return custody.ioExited();}
}
