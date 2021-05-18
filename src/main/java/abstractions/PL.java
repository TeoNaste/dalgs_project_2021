package abstractions;

import proto.CommunicationProtocol;
import system.MySystem;
import utils.Utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

public class PL extends Abstraction {

    public PL(CommunicationProtocol.ProcessId processId, MySystem system, String abstractionId,String parentId) {
        super(processId, system, abstractionId,parentId);
    }

    public void uponPlSend(CommunicationProtocol.Message message){
        CommunicationProtocol.Message innerMessage = message.getNetworkMessage().getMessage();
        CommunicationProtocol.Message newMessage = CommunicationProtocol.Message.newBuilder(innerMessage).build();

        CommunicationProtocol.ProcessId sender = Utils.find(system.getProcessIds(),message.getNetworkMessage().getSenderListeningPort());

        if(sender == null)
            sender = system.getHub();

        CommunicationProtocol.Message toSend = Utils.constrctPlDeliver(newMessage,sender,parentId,abstractionId);

        system.receiveEvent(toSend);
    }

    public void uponBebPlSend(CommunicationProtocol.Message message){
        CommunicationProtocol.AppValue appValue = message.getNetworkMessage().getMessage().getAppValue();

        CommunicationProtocol.Message.Builder builder = CommunicationProtocol.Message.newBuilder();
        builder.setAppValue(appValue);
//        builder.setToAbstractionId(parentId);
//        builder.setFromAbstractionId(abstractionId);
        builder.setType(CommunicationProtocol.Message.Type.APP_VALUE);
        builder.setSystemId(system.getSystemId());
        CommunicationProtocol.Message appMessage = builder.build();

        CommunicationProtocol.Message toSend = Utils.constrctPlDeliver(appMessage,processId,parentId,abstractionId);

//        system.addBeb("app",".beb");
        system.receiveEvent(toSend);
    }

    public void triggerPlSend(CommunicationProtocol.Message message) {
        CommunicationProtocol.Message message1 = message.getPlSend().getMessage();

        CommunicationProtocol.Message innerMessage = CommunicationProtocol.Message.newBuilder(message1).build();

        CommunicationProtocol.ProcessId dest = message.getPlSend().getDestination();

        CommunicationProtocol.Message netMessage = Utils.constructNetworkMessage(innerMessage, processId.getPort(),processId.getHost(), abstractionId, abstractionId);

        try {
            byte[] response = netMessage.toByteArray();

            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + response.length);
            buffer.putInt(response.length);
            buffer.put(response);

            Thread.sleep(10);
            Socket socket = new Socket(dest.getHost(), dest.getPort());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            out.write(buffer.array());
            out.close();
            socket.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handleMessage(CommunicationProtocol.Message message) {

        switch (message.getType()){
            case NETWORK_MESSAGE:
                uponPlSend(message);
                break;
            case PL_SEND:
                triggerPlSend(message);
                break;

        }
    }
}
