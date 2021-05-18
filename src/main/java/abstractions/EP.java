package abstractions;

import proto.CommunicationProtocol;
import system.MySystem;
import utils.Utils;

import java.sql.SQLOutput;
import java.util.HashMap;
import java.util.Map;

//Read/Write Epoch Consensu alg 5.6 pag 241
public class EP extends Abstraction {
    private CommunicationProtocol.EpInternalState state;
    private CommunicationProtocol.Value tmpval;
    private Map<proto.CommunicationProtocol.ProcessId,CommunicationProtocol.EpInternalState> states;
    private int accepted;
    private int ets;
    private boolean aborted;


    public EP(CommunicationProtocol.ProcessId processId, MySystem system, String abstractionId, String parentId, CommunicationProtocol.EpInternalState state, int ets) {
        super(processId, system, abstractionId, parentId);

        this.state = state;
        tmpval = CommunicationProtocol.Value.newBuilder().setDefined(false).setV(-1).build();
        states = new HashMap<>();
        accepted = 0;
        this.ets = ets;
        aborted = false;

        system.addAbstraction(abstractionId,".beb");
        system.addAbstraction(abstractionId+".beb",".pl");
        system.addAbstraction(abstractionId,".pl");
    }

    @Override
    public void handleMessage(CommunicationProtocol.Message message) {
        if(aborted){
            system.receiveEvent(message);
            return;
        }
        switch (message.getType()){
            case EP_PROPOSE:
                uponEpPropose(message);
                break;
            case BEB_DELIVER:{
                if(message.getBebDeliver().getMessage().getType() == CommunicationProtocol.Message.Type.EP_INTERNAL_READ)
                    uponBebDeliverRead(message);
                if(message.getBebDeliver().getMessage().getType() == CommunicationProtocol.Message.Type.EP_INTERNAL_WRITE)
                    uponBebDeliverWrite(message);
                if(message.getBebDeliver().getMessage().getType() == CommunicationProtocol.Message.Type.EP_INTERNAL_DECIDED)
                    uponBebDeliverDecided(message);
                break;
            }
            case PL_DELIVER:{
                if(message.getPlDeliver().getMessage().getType() == CommunicationProtocol.Message.Type.EP_INTERNAL_STATE)
                    uponPlDeliverState(message);
                if(message.getPlDeliver().getMessage().getType() == CommunicationProtocol.Message.Type.EP_INTERNAL_ACCEPT)
                    uponPlDeliverAccept(message);
                break;
            }case EP_ABORT:{
                uponEpAbort();
                break;
            }
        }
    }

    private void uponEpPropose(CommunicationProtocol.Message message) {
        tmpval = message.getEpPropose().getValue();

        CommunicationProtocol.EpInternalRead read = CommunicationProtocol.EpInternalRead.newBuilder().build();

        CommunicationProtocol.Message readMessage = CommunicationProtocol.Message.newBuilder()
                .setEpInternalRead(read)
                .setType(CommunicationProtocol.Message.Type.EP_INTERNAL_READ)
                .setSystemId(system.getSystemId())
                .setFromAbstractionId(abstractionId)
                .setToAbstractionId(abstractionId)
                .build();

        CommunicationProtocol.Message toSend = Utils.constructBebBroadcast(readMessage,abstractionId+".beb",abstractionId);

        system.addAbstraction(abstractionId,".beb");
        system.receiveEvent(toSend);
    }

    private void uponBebDeliverRead(CommunicationProtocol.Message message) {
        CommunicationProtocol.Message stateMessage = CommunicationProtocol.Message.newBuilder()
                .setEpInternalState(state)
                .setType(CommunicationProtocol.Message.Type.EP_INTERNAL_STATE)
                .setSystemId(system.getSystemId())
                .setFromAbstractionId(abstractionId)
                .setToAbstractionId(abstractionId)
                .build();

        System.out.println(processId.getOwner()+"-"+processId.getIndex()+" sends "+stateMessage.getType().name());

        CommunicationProtocol.Message toSend = Utils.constructPlSend(stateMessage,message.getBebDeliver().getSender(),abstractionId+".pl",abstractionId);
        system.addAbstraction(abstractionId,".pl");
        system.receiveEvent(toSend);
    }

    private void uponPlDeliverState(CommunicationProtocol.Message message) {
        CommunicationProtocol.EpInternalState state = CommunicationProtocol.EpInternalState.newBuilder()
                .setValue(message.getPlDeliver().getMessage().getEpInternalState().getValue())
                .setValueTimestamp(message.getPlDeliver().getMessage().getEpInternalState().getValueTimestamp())
                .build();

        CommunicationProtocol.ProcessId q = Utils.find(system.getProcessIds(),message.getPlDeliver().getSender());
        states.put(q,state);

        if(checkStatesReceived())
            uponEnoughStates();
    }

    private void uponEnoughStates(){
        CommunicationProtocol.EpInternalState state = highest();

        if(state.getValue().getDefined())
            tmpval = state.getValue();
        states.clear();

        CommunicationProtocol.EpInternalWrite internalWrite = CommunicationProtocol.EpInternalWrite.newBuilder()
                .setValue(tmpval)
                .build();

        CommunicationProtocol.Message writeMessage = CommunicationProtocol.Message.newBuilder()
                .setEpInternalWrite(internalWrite)
                .setType(CommunicationProtocol.Message.Type.EP_INTERNAL_WRITE)
                .setSystemId(system.getSystemId())
                .setFromAbstractionId(abstractionId)
                .setToAbstractionId(abstractionId)
                .build();

        CommunicationProtocol.Message toSend = Utils.constructBebBroadcast(writeMessage,abstractionId+".beb",abstractionId);

        system.addAbstraction(abstractionId,".beb");
        system.receiveEvent(toSend);
    }


    private void uponBebDeliverWrite(CommunicationProtocol.Message message) {
        state = CommunicationProtocol.EpInternalState.newBuilder()
                .setValue(message.getBebDeliver().getMessage().getEpInternalWrite().getValue())
                .setValueTimestamp(ets)
                .build();

        CommunicationProtocol.EpInternalAccept internalAccept = CommunicationProtocol.EpInternalAccept.newBuilder().build();

        CommunicationProtocol.Message acceptMssage = CommunicationProtocol.Message.newBuilder()
                .setEpInternalAccept(internalAccept)
                .setType(CommunicationProtocol.Message.Type.EP_INTERNAL_ACCEPT)
                .setSystemId(system.getSystemId())
                .setFromAbstractionId(abstractionId)
                .setToAbstractionId(abstractionId)
                .build();

        CommunicationProtocol.Message toSend = Utils.constructPlSend(acceptMssage,message.getBebDeliver().getSender(),abstractionId+".pl",abstractionId);

        system.addAbstraction(abstractionId,".pl");
        system.receiveEvent(toSend);
    }

    private void uponPlDeliverAccept(CommunicationProtocol.Message message) {
        accepted++;

        if(checkAcceptsReceived()){
            uponEnoughAccepts();
        }
    }

    private void uponEnoughAccepts(){
        accepted = 0;

        CommunicationProtocol.EpInternalDecided decided = CommunicationProtocol.EpInternalDecided.newBuilder()
                .setValue(tmpval)
                .build();

        CommunicationProtocol.Message decidedMessage = CommunicationProtocol.Message.newBuilder()
                .setEpInternalDecided(decided)
                .setType(CommunicationProtocol.Message.Type.EP_INTERNAL_DECIDED)
                .setSystemId(system.getSystemId())
                .setFromAbstractionId(abstractionId)
                .setToAbstractionId(abstractionId)
                .build();

        CommunicationProtocol.Message toSend = Utils.constructBebBroadcast(decidedMessage,abstractionId+".beb",abstractionId);

        system.addAbstraction(abstractionId,".beb");
        system.receiveEvent(toSend);
    }


    private void uponBebDeliverDecided(CommunicationProtocol.Message message) {
        CommunicationProtocol.EpDecide epDecide = CommunicationProtocol.EpDecide.newBuilder()
                .setValue(message.getBebDeliver().getMessage().getEpInternalDecided().getValue())
                .setEts(state.getValueTimestamp())
                .build();

        CommunicationProtocol.Message decideMessage = CommunicationProtocol.Message.newBuilder()
                .setEpDecide(epDecide)
                .setType(CommunicationProtocol.Message.Type.EP_DECIDE)
                .setSystemId(system.getSystemId())
                .setFromAbstractionId(abstractionId)
                .setToAbstractionId(parentId)
                .build();

        System.out.println(processId.getOwner()+"-"+processId.getIndex()+" received EpDecided with value: "+message.getBebDeliver().getMessage().getEpInternalDecided().getValue().getV());
        system.receiveEvent(decideMessage);
    }

    private void uponEpAbort() {
        CommunicationProtocol.EpAborted epAborted = CommunicationProtocol.EpAborted.newBuilder()
                .setEts(state.getValueTimestamp())
                .setValue(state.getValue())
                .build();

        CommunicationProtocol.Message abortedMessage = CommunicationProtocol.Message.newBuilder()
                .setEpAborted(epAborted)
                .setType(CommunicationProtocol.Message.Type.EP_ABORTED)
                .setSystemId(system.getSystemId())
                .setFromAbstractionId(abstractionId)
                .setToAbstractionId(parentId)
                .build();

        system.receiveEvent(abortedMessage);

        aborted = true;
//        system.removeAbstraction(this);
    }

    private boolean checkStatesReceived(){
        if(states.size() > system.getProcessIds().size()/2)
            return true;
        return false;
    }

    private boolean checkAcceptsReceived(){
        if(accepted > system.getProcessIds().size()/2)
            return true;
        return false;
    }

    private CommunicationProtocol.EpInternalState highest(){
        CommunicationProtocol.EpInternalState highest = CommunicationProtocol.EpInternalState.newBuilder()
                .setValueTimestamp(-1)
                .setValue(CommunicationProtocol.Value.newBuilder().setV(-1).build())
                .build();

        for(CommunicationProtocol.ProcessId p : states.keySet()){
            CommunicationProtocol.EpInternalState s = states.get(p);
            if(s.getValueTimestamp() > highest.getValueTimestamp())
                highest = s;
        }
        return highest;
    }
}
