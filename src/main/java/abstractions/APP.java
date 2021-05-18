package abstractions;

import proto.CommunicationProtocol;
import system.MySystem;
import utils.Utils;

public class APP extends Abstraction {

    public APP(CommunicationProtocol.ProcessId processId, MySystem system, String abstractionId, String parentId) {
        super(processId, system, abstractionId, parentId);

        this.system.addAbstraction(abstractionId,".beb");
        this.system.addAbstraction(abstractionId+".beb",".pl");
    }

    @Override
    public void handleMessage(CommunicationProtocol.Message message) {
        switch (message.getType()){
            case PL_DELIVER:
                uponPlDeliver(message);
                break;
            case BEB_DELIVER:
                uponBebDeliver(message);
                break;
            case NETWORK_MESSAGE:
                uponOtherRequests(message);
                break;
        }

    }

    //Receive Message of type PL DELIVER and cread message BEB Broadcast
    public void uponPlDeliver(CommunicationProtocol.Message message){
        CommunicationProtocol.Message received = message.getPlDeliver().getMessage();
        CommunicationProtocol.Message toSend = CommunicationProtocol.Message.newBuilder().build();

        switch (received.getType()){
            case APP_BROADCAST:{
                CommunicationProtocol.Value value = received.getAppBroadcast().getValue();

                CommunicationProtocol.AppValue.Builder valueBuilder = CommunicationProtocol.AppValue.newBuilder();
                valueBuilder.setValue(value);
                CommunicationProtocol.AppValue appValue = valueBuilder.build();


                CommunicationProtocol.Message.Builder builder = CommunicationProtocol.Message.newBuilder();
                builder.setAppValue(appValue);
//                builder.setToAbstractionId(abstractionId +".beb");
                builder.setToAbstractionId(abstractionId);
                builder.setFromAbstractionId(abstractionId);
                builder.setType(CommunicationProtocol.Message.Type.APP_VALUE);
                builder.setSystemId(system.getSystemId());
                CommunicationProtocol.Message bebMessage = builder.build();

                toSend = Utils.constructBebBroadcast(bebMessage,abstractionId+".beb", abstractionId);

                system.addAbstraction(getAbstractionId(),".beb");
                break;
            }
            case APP_WRITE:{
                String register = received.getAppWrite().getRegister();

                CommunicationProtocol.NnarWrite writeMes = CommunicationProtocol.NnarWrite.newBuilder()
                        .setValue(received.getAppWrite().getValue())
                        .build();

                CommunicationProtocol.Message toNnar = CommunicationProtocol.Message.newBuilder()
                        .setNnarWrite(writeMes)
                        .setToAbstractionId(abstractionId+".nnar["+register+"]")
                        .setFromAbstractionId(abstractionId)
                        .setSystemId(system.getSystemId())
                        .setType(CommunicationProtocol.Message.Type.NNAR_WRITE)
                        .build();

                toSend = Utils.constructNetworkMessage(toNnar,processId.getPort(),processId.getHost(),abstractionId+".nnar["+register+"]",abstractionId);

                system.addAbstraction(abstractionId,".nnar["+register+"]",register);
                break;
            }
            case APP_READ:{
                String register = received.getAppRead().getRegister();

                CommunicationProtocol.NnarRead nnarRead = CommunicationProtocol.NnarRead.newBuilder().build();

                CommunicationProtocol.Message readMessage = CommunicationProtocol.Message.newBuilder()
                        .setNnarRead(nnarRead)
                        .setType(CommunicationProtocol.Message.Type.NNAR_READ)
                        .setSystemId(system.getSystemId())
                        .setToAbstractionId(abstractionId+".nnar["+register+"]")
                        .setFromAbstractionId(abstractionId)
                        .build();

                toSend = Utils.constructNetworkMessage(readMessage,processId.getPort(),processId.getHost(),abstractionId+".nnar["+register+"]",abstractionId);

                system.addAbstraction(abstractionId,".nnar["+register+"]",register);
                break;
            }
            case APP_PROPOSE:{
                CommunicationProtocol.UcPropose ucPropose = CommunicationProtocol.UcPropose.newBuilder()
                        .setValue(received.getAppPropose().getValue())
                        .build();

                CommunicationProtocol.Message proposeMessage = CommunicationProtocol.Message.newBuilder()
                        .setUcPropose(ucPropose)
                        .setType(CommunicationProtocol.Message.Type.UC_PROPOSE)
                        .setSystemId(system.getSystemId())
                        .setFromAbstractionId(abstractionId)
                        .setToAbstractionId(abstractionId+".uc["+received.getAppPropose().getTopic()+"]")
                        .build();

                system.addAbstraction(abstractionId,".uc["+received.getAppPropose().getTopic()+"]",received.getAppPropose().getTopic());

                toSend = proposeMessage;
                break;
            }
        }

        system.receiveEvent(toSend);
    }

    public void uponBebDeliver(CommunicationProtocol.Message message){
        CommunicationProtocol.AppValue appValue = message.getBebDeliver().getMessage().getAppValue();

        CommunicationProtocol.Message.Builder builder = CommunicationProtocol.Message.newBuilder();
        builder.setAppValue(appValue);
//        builder.setToAbstractionId(abstractionId+".pl");
        builder.setToAbstractionId(abstractionId);
        builder.setFromAbstractionId(abstractionId);
        builder.setType(CommunicationProtocol.Message.Type.APP_VALUE);
        builder.setSystemId(system.getSystemId());
        CommunicationProtocol.Message plMessage = builder.build();

        CommunicationProtocol.Message toSend = Utils.constructPlSend(plMessage,system.getHub(),abstractionId+".pl",abstractionId);

        System.out.println(processId.getOwner()+"-"+processId.getIndex()+" BebDeliver value "+appValue.getValue().getV());
        system.receiveEvent(toSend);
    }

    private void uponOtherRequests(CommunicationProtocol.Message message) {
        CommunicationProtocol.Message innerMessage = message.getNetworkMessage().getMessage();

        switch (innerMessage.getType()){
            case APP_READ_RETURN:{
//                CommunicationProtocol.AppReadReturn readReturn = CommunicationProtocol.AppReadReturn.newBuilder()
//                        .setValue(innerMessage.getNnarReadReturn().getValue())
//                        .setRegister("x")
//                        .build();
//
//                CommunicationProtocol.Message readMessage = CommunicationProtocol.Message.newBuilder()
//                        .setAppReadReturn(readReturn)
//                        .setType(CommunicationProtocol.Message.Type.APP_READ_RETURN)
//                        .setSystemId(system.getSystemId())
//                        .setToAbstractionId(abstractionId)
//                        .setFromAbstractionId(abstractionId)
//                        .build();

                CommunicationProtocol.Message toSend = Utils.constructPlSend(innerMessage,system.getHub(),abstractionId+".pl",abstractionId);
                system.receiveEvent(toSend);
                break;
            }
            case APP_WRITE_RETURN:{
//                CommunicationProtocol.AppWriteReturn writeReturn = CommunicationProtocol.AppWriteReturn.newBuilder()
//                        .setRegister("x").build();
//
//                CommunicationProtocol.Message readMessage = CommunicationProtocol.Message.newBuilder()
//                        .setAppWriteReturn(writeReturn)
//                        .setType(CommunicationProtocol.Message.Type.APP_WRITE_RETURN)
//                        .setSystemId(system.getSystemId())
//                        .setToAbstractionId(abstractionId)
//                        .setFromAbstractionId(abstractionId)
//                        .build();

                CommunicationProtocol.Message toSend = Utils.constructPlSend(innerMessage,system.getHub(),abstractionId+".pl",abstractionId);
                system.receiveEvent(toSend);
                break;
            }case UC_DECIDE:{
                CommunicationProtocol.AppDecide appDecide = CommunicationProtocol.AppDecide.newBuilder()
                        .setValue(message.getNetworkMessage().getMessage().getUcDecide().getValue())
                        .build();

                CommunicationProtocol.Message decideMessage = CommunicationProtocol.Message.newBuilder()
                        .setAppDecide(appDecide)
                        .setType(CommunicationProtocol.Message.Type.APP_DECIDE)
                        .setSystemId(system.getSystemId())
                        .setFromAbstractionId(abstractionId)
                        .setToAbstractionId(abstractionId)
                        .build();

                CommunicationProtocol.Message toSend = Utils.constructPlSend(decideMessage,system.getHub(),abstractionId+".pl",abstractionId);
                system.receiveEvent(toSend);
            }

        }
    }
}
