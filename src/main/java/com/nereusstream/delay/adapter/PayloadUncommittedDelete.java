package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord;
import com.nereusstream.delay.protocol.TargetResourceReclaimBody;
import com.nereusstream.delay.protocol.TargetUncommittedObjectClosure;
import java.util.Arrays;
import java.util.Objects;

/** Original remote closure/terminal custody. Serialized public material cannot mint this hot witness. */
public final class PayloadUncommittedDelete {
    private PayloadUncommittedDelete() {}
    public static final class Observation {
        private final TargetPayloadObjectRetiringRecord intent;
        private final TargetUncommittedObjectClosure closure;
        private Observation(TargetPayloadObjectRetiringRecord intent,TargetUncommittedObjectClosure closure){
            this.intent=Objects.requireNonNull(intent);this.closure=Objects.requireNonNull(closure);closure.requireFor(intent);
            if(!closure.complete())throw new IllegalArgumentException("complete actual closed provider terminal required");
        }
        public TargetPayloadObjectRetiringRecord intent(){return intent;}
        public TargetUncommittedObjectClosure closure(){return closure;}
        public long retainedBytes(){return Math.addExact(intent.canonicalBytes().length,closure.canonicalBytes().length);}
        public void requireConfirmation(TargetResourceReclaimBody body){
            if(!(body.operations().getFirst() instanceof TargetResourceReclaimBody.UncommittedObjectConfirm confirmation)
                    ||!Arrays.equals(intent.canonicalBytes(),confirmation.intent().canonicalBytes())
                    ||!Arrays.equals(closure.canonicalBytes(),confirmation.closure().canonicalBytes()))throw new IllegalArgumentException("uncommitted confirmation changed actual original provider terminal");
        }
    }
    static Observation retained(TargetPayloadObjectRetiringRecord intent,TargetUncommittedObjectClosure closure){return new Observation(intent,closure);}
}
