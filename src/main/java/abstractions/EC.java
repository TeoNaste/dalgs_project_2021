package abstractions;

import proto.CommunicationProtocol;
import system.MySystem;
import utils.Utils;

//LeaderBased Epoch Change alg 5.5 pag 237
public class EC extends Abstraction {
    private CommunicationProtocol.ProcessId trusted;
    private int lasts;
    private int ts;

    public EC(CommunicationProtocol.ProcessId processId, MySystem system, String abstractionId, String parentId) {
        super(processId, system, abstractionId, parentId);

        trusted = system.getProcessIds().get(0);
        lasts = 0;
        ts = Utils.find(system.getProcessIds(),processId).getRank();

        system.addAbstraction(abstractionId,".eld");
        system.addAbstraction(abstractionId,".beb");
        system.addAbstraction(abstractionId+".beb",".pl");
        system.addAbstraction(abstractionId,".pl");
    }

    @Override
    public void handleMessage(CommunicationProtocol.Message message) {
        switch (message.getType()){
            case ELD_TRUST:
                uponEldTrust(message);
                break;
            case BEB_DELIVER:
                uponBebDeliverNewEpoch(message);
                break;
            case PL_DELIVER:
                uponPlDeliverNack(message);
                break;
        }

    }

    private void uponEldTrust(CommunicationProtocol.Message message) {
        trusted = message.getEldTrust().getProcess();
        if(Utils.ProcessEquals(trusted,processId)){
            ts++;

            CommunicationProtocol.EcInternalNewEpoch newEpoch = CommunicationProtocol.EcInternalNewEpoch.newBuilder()
                    .setTimestamp(ts)
                    .build();

            CommunicationProtocol.Message newMessage = CommunicationProtocol.Message.newBuilder()
                    .setEcInternalNewEpoch(newEpoch)
                    .setType(CommunicationProtocol.Message.Type.EC_INTERNAL_NEW_EPOCH)
                    .setSystemId(system.getSystemId())
                    .setFromAbstractionId(abstractionId)
                    .setToAbstractionId(abstractionId)
                    .build();

            CommunicationProtocol.Message toSend = Utils.constructBebBroadcast(newMessage,abstractionId+".beb",abstractionId);
            system.addAbstraction(abstractionId,".beb");
            system.receiveEvent(toSend);
        }
    }

    private void uponBebDeliverNewEpoch(CommunicationProtocol.Message message) {
        CommunicationProtocol.ProcessId leader = message.getBebDeliver().getSender();
        CommunicationProtocol.EcInternalNewEpoch newEpoch = message.getBebDeliver().getMessage().getEcInternalNewEpoch();

        if(Utils.ProcessEquals(leader,trusted) && newEpoch.getTimestamp() > lasts){
            lasts = newEpoch.getTimestamp();

            CommunicationProtocol.EcStartEpoch startEpoch = CommunicationProtocol.EcStartEpoch.newBuilder()
                    .setNewTimestamp(newEpoch.getTimestamp())
                    .setNewLeader(leader)
                    .build();

            CommunicationProtocol.Message startMessage = CommunicationProtocol.Message.newBuilder()
                    .setEcStartEpoch(startEpoch)
                    .setType(CommunicationProtocol.Message.Type.EC_START_EPOCH)
                    .setSystemId(system.getSystemId())
                    .setFromAbstractionId(abstractionId)
                    .setToAbstractionId(parentId)
                    .build();

            system.receiveEvent(startMessage);
        }else {
            CommunicationProtocol.EcInternalNack nack = CommunicationProtocol.EcInternalNack.newBuilder().build();

            CommunicationProtocol.Message nackMessage = CommunicationProtocol.Message.newBuilder()
                    .setEcInternalNack(nack)
                    .setType(CommunicationProtocol.Message.Type.EC_INTERNAL_NACK)
                    .setSystemId(system.getSystemId())
                    .setFromAbstractionId(abstractionId)
                    .setToAbstractionId(abstractionId)
                    .build();

            CommunicationProtocol.Message toSend = Utils.constructPlSend(nackMessage,leader,abstractionId+".pl",abstractionId);
            system.addAbstraction(abstractionId,".pl");
            system.receiveEvent(toSend);
        }
    }

    private void uponPlDeliverNack(CommunicationProtocol.Message message) {
        if(Utils.ProcessEquals(trusted,processId)){
            ts += system.getProcessIds().size();

            CommunicationProtocol.EcInternalNewEpoch newEpoch = CommunicationProtocol.EcInternalNewEpoch.newBuilder()
                    .setTimestamp(ts)
                    .build();

            CommunicationProtocol.Message newMessage = CommunicationProtocol.Message.newBuilder()
                    .setEcInternalNewEpoch(newEpoch)
                    .setType(CommunicationProtocol.Message.Type.EC_INTERNAL_NEW_EPOCH)
                    .setSystemId(system.getSystemId())
                    .setFromAbstractionId(abstractionId)
                    .setToAbstractionId(abstractionId)
                    .build();

            CommunicationProtocol.Message toSend = Utils.constructBebBroadcast(newMessage,abstractionId+".beb",abstractionId);
            system.addAbstraction(abstractionId,".beb");
            system.receiveEvent(toSend);
        }
    }
}
