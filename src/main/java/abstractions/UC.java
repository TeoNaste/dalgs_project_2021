package abstractions;

import proto.CommunicationProtocol;
import system.MySystem;
import utils.Utils;

//Leader-Driven Consensus alg 5.7 pag 243
public class UC extends Abstraction {
    private String topic;
    private boolean proposed, decided;
    private CommunicationProtocol.Value value;
    private CommunicationProtocol.ProcessId l, newl;
    private int ets, newts;
    private EP ep;

    public UC(CommunicationProtocol.ProcessId processId, MySystem system, String abstractionId, String parentId,String topic) {
        super(processId, system, abstractionId, parentId);

        this.topic = topic;
        value = CommunicationProtocol.Value.newBuilder().setDefined(false).setV(-1).build();
        proposed = false;
        decided = false;
        ets = 0;
        l = processId;
        newts = 0;
        newl = null;

        CommunicationProtocol.EpInternalState internalState = CommunicationProtocol.EpInternalState.newBuilder()
                .setValue(value)
                .setValueTimestamp(0)
                .build();
        ep = new EP(processId,system,abstractionId+".ep["+ets+"]",abstractionId,internalState,ets);

        system.addAbstraction(ep);
        system.addAbstraction(abstractionId,".ec");
    }

    @Override
    public void handleMessage(CommunicationProtocol.Message message) {
        switch (message.getType()){
            case UC_PROPOSE:
                uponUcPropose(message);
                break;
            case EC_START_EPOCH:
                uponEcStartEpoch(message);
                break;
            case EP_ABORTED:
                uponEpAborted(message);
                break;
            case EP_DECIDE:
                uponEpDecide(message);
                break;
        }
    }


    private void uponUcPropose(CommunicationProtocol.Message message) {
        value = message.getUcPropose().getValue();

        if (checkInternalCondition())
            triggerEpPropose();
    }

    private void uponEcStartEpoch(CommunicationProtocol.Message message) {
        newts = message.getEcStartEpoch().getNewTimestamp();
        newl = message.getEcStartEpoch().getNewLeader();

        CommunicationProtocol.EpAbort abort = CommunicationProtocol.EpAbort.newBuilder().build();

        CommunicationProtocol.Message abortMessage = CommunicationProtocol.Message.newBuilder()
                .setEpAbort(abort)
                .setType(CommunicationProtocol.Message.Type.EP_ABORT)
                .setSystemId(system.getSystemId())
                .setFromAbstractionId(abstractionId)
                .setToAbstractionId(abstractionId+".ep["+ets+"]")
                .build();

        system.receiveEvent(abortMessage);
    }

    private void uponEpAborted(CommunicationProtocol.Message message) {
        if(ets != message.getEpAborted().getEts()){
            system.receiveEvent(message);
            return;
        }

        ets = newts;
        l = newl;
        proposed = false;
        CommunicationProtocol.EpInternalState state = CommunicationProtocol.EpInternalState.newBuilder()
                .setValue(message.getEpAborted().getValue())
                .setValueTimestamp(message.getEpAborted().getValueTimestamp())
                .buildPartial();

        system.addAbstraction(abstractionId,".ep["+ets+"]",state,ets);

        if (checkInternalCondition())
            triggerEpPropose();
    }

    private boolean checkInternalCondition() {
        if (!Utils.ProcessEquals(l, processId))
            return false;
        if (!value.getDefined())
            return false;
        if (proposed)
            return false;
        return true;
    }

    private void triggerEpPropose() {
        proposed = true;

        CommunicationProtocol.EpPropose propose = CommunicationProtocol.EpPropose.newBuilder()
                .setValue(value)
                .build();

        CommunicationProtocol.Message prposeMessage = CommunicationProtocol.Message.newBuilder()
                .setEpPropose(propose)
                .setType(CommunicationProtocol.Message.Type.EP_PROPOSE)
                .setSystemId(system.getSystemId())
                .setFromAbstractionId(abstractionId)
                .setToAbstractionId(abstractionId+".ep["+ets+"]")
                .build();

        system.receiveEvent(prposeMessage);
    }

    private void uponEpDecide(CommunicationProtocol.Message message) {
        if(ets != message.getEpDecide().getEts()){
            system.receiveEvent(message);
            return;
        }

        if(!decided){
            decided = true;

            CommunicationProtocol.UcDecide decide = CommunicationProtocol.UcDecide.newBuilder()
                    .setValue(message.getEpDecide().getValue())
                    .build();

            CommunicationProtocol.Message udecideMessage = CommunicationProtocol.Message.newBuilder()
                    .setUcDecide(decide)
                    .setType(CommunicationProtocol.Message.Type.UC_DECIDE)
                    .setSystemId(system.getSystemId())
                    .setFromAbstractionId(abstractionId)
                    .setToAbstractionId(abstractionId)
                    .build();

            CommunicationProtocol.Message toSend = Utils.constructNetworkMessage(udecideMessage,processId.getPort(),processId.getHost(),parentId,abstractionId);
            system.receiveEvent(toSend);
        }
    }

}
