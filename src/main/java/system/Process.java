package system;

import proto.CommunicationProtocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Process extends Thread {
    private int port;
    private String owner;
    private int uuid;
    private List<CommunicationProtocol.ProcessId> processIds;
    private MySystem system;
    private CommunicationProtocol.ProcessId processId;

    public Process(int port, String owner,int uuid){
        this.port = port;
        this.owner = owner;
        this.uuid = uuid;
        this.processIds = new ArrayList<>();
        processId = createProcessId();
    }

    //Send ProcRegistration
    public void register() throws IOException {
        CommunicationProtocol.ProcRegistration.Builder ProcRegistration = CommunicationProtocol.ProcRegistration.newBuilder();
        ProcRegistration.setIndex(uuid);
        ProcRegistration.setOwner(owner);
        CommunicationProtocol.ProcRegistration reg = ProcRegistration.build();

        CommunicationProtocol.Message.Builder message = CommunicationProtocol.Message.newBuilder();
        message.setProcRegistration(reg);
        message.setSystemId("sys-1");
        message.setType(CommunicationProtocol.Message.Type.PROC_REGISTRATION);
        CommunicationProtocol.Message regMes = message.build();

        CommunicationProtocol.NetworkMessage.Builder networkMessage = CommunicationProtocol.NetworkMessage.newBuilder();
        networkMessage.setMessage(regMes);
        networkMessage.setSenderHost("127.0.0.1");
        networkMessage.setSenderListeningPort(port);
        CommunicationProtocol.NetworkMessage netMes = networkMessage.build();

        CommunicationProtocol.Message.Builder completeMessage = CommunicationProtocol.Message.newBuilder();
        completeMessage.setNetworkMessage(netMes);
        completeMessage.setSystemId(regMes.getSystemId());
        completeMessage.setType(CommunicationProtocol.Message.Type.NETWORK_MESSAGE);
        CommunicationProtocol.Message regRequest = completeMessage.build();

        byte[] appreg = regRequest.toByteArray();

        Socket socket = new Socket("127.0.0.1", 5000);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());

        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + appreg.length);
        buffer.putInt(appreg.length);
        buffer.put(appreg);

        out.write(buffer.array());
        out.close();
        socket.close();
    }

    //Receive ProcInitializeSystem
    public void receiveResponse() throws IOException {

        ServerSocket serverSocket = new ServerSocket(port);
        Socket recvSocket = serverSocket.accept();

        DataInputStream in = new DataInputStream(recvSocket.getInputStream());
        int rez = -1;
        rez = in.readInt();
        byte[] response = new byte[rez];
        in.readFully(response);

        recvSocket.close();
        serverSocket.close();

        CommunicationProtocol.Message messageR = CommunicationProtocol.Message.parseFrom(response);
        CommunicationProtocol.ProcInitializeSystem initializeSystem = messageR.getNetworkMessage().getMessage().getProcInitializeSystem();
        processIds = initializeSystem.getProcessesList();

        //initialize system
        System.out.println(owner+"-"+uuid+" is creating system...");
        this.system = new MySystem(messageR.getSystemId(),processIds,this);
    }

    private CommunicationProtocol.ProcessId createProcessId(){
        CommunicationProtocol.ProcessId.Builder builder = CommunicationProtocol.ProcessId.newBuilder();
        builder.setHost("127.0.0.1");
        builder.setPort(port);
        builder.setOwner(owner);
        builder.setIndex(uuid);

        CommunicationProtocol.ProcessId processId = builder.build();

        return processId;
    }


    @Override
    public void run() {
        try {
            register();
            receiveResponse();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getPort() {
        return port;
    }

    public CommunicationProtocol.ProcessId getProcessId() {
        return processId;
    }

    public String getOwner() {
        return owner;
    }

    public int getUuid() {
        return uuid;
    }


}
