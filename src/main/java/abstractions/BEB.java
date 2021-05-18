package abstractions;

import proto.CommunicationProtocol;
import system.MySystem;
import utils.Utils;


//Basic Broadcast alg 3.1 pag 95
public class BEB extends Abstraction {

    public BEB(CommunicationProtocol.ProcessId processId, MySystem system, String abstractionId,String parentId) {
        super(processId,  system, abstractionId, parentId);
    }

    public void uponBebBroadcast(CommunicationProtocol.Message message){
        System.out.println(processId.getOwner()+"-"+processId.getIndex()+" is broadcasting "+message.getBebBroadcast().getMessage().getType().name()+"...");

        system.addAbstraction(abstractionId, ".pl");

        for(CommunicationProtocol.ProcessId p : system.getProcessIds()){

            CommunicationProtocol.Message valueMess = CommunicationProtocol.Message.newBuilder(message.getBebBroadcast().getMessage()).build();

            CommunicationProtocol.Message toSend = Utils.constructPlSend(valueMess,p,abstractionId+".pl",abstractionId);

            system.receiveEvent(toSend);
        }
    }

    public void uponPlDeliver(CommunicationProtocol.Message message){
//        CommunicationProtocol.AppValue appValue = message.getPlDeliver().getMessage().getAppValue();
//
//        CommunicationProtocol.Message.Builder builder = CommunicationProtocol.Message.newBuilder();
//        builder.setAppValue(appValue);
//        builder.setToAbstractionId(parentId);
//        builder.setFromAbstractionId(abstractionId);
//        builder.setType(CommunicationProtocol.Message.Type.APP_VALUE);
//        builder.setSystemId(system.getSystemId());

        CommunicationProtocol.Message deliver = CommunicationProtocol.Message.newBuilder(message.getPlDeliver().getMessage())
                .setToAbstractionId(parentId)
                .setFromAbstractionId(abstractionId)
                .build();

        CommunicationProtocol.Message toDeliver = Utils.constructBebDeliver(deliver,message.getPlDeliver().getSender());

        system.receiveEvent(toDeliver);
    }

    @Override
    public void handleMessage(CommunicationProtocol.Message message) {
        switch (message.getType()){
            case BEB_BROADCAST:
                uponBebBroadcast(message);
                break;
            case PL_DELIVER:
                uponPlDeliver(message);
                break;
        }

    }
}
