package system;

import proto.CommunicationProtocol;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class EventLoop extends Thread{
    private MySystem system;
    private boolean stop = false;
    private ServerSocket serverSocket;

    public EventLoop(MySystem system) {
        this.system = system;
    }

    public void setStop(boolean stop) {
        this.stop = stop;
    }

    //Listen on messages and put them in the event queue
    public void listenEvents() throws IOException {
        serverSocket = new ServerSocket(system.getPort());
        while (!stop) {
            try {
                Socket recvSocket = serverSocket.accept();
                DataInputStream in = new DataInputStream(recvSocket.getInputStream());

                int size = in.readInt();
                byte[] message = new byte[size];
                in.readFully(message);

                CommunicationProtocol.Message event = CommunicationProtocol.Message.parseFrom(message);

                //send to system to put it in the queue
                if(event.getNetworkMessage().getMessage().getType() == CommunicationProtocol.Message.Type.PROC_DESTROY_SYSTEM){
                    setStop(true);
                    system.stopSystem();
                }
                system.receiveEvent(event);
            } catch (IOException e) {
                e.printStackTrace();
            }catch (Exception e){
                System.out.println("Program ended.");
            }
        }
        serverSocket.close();
    }

    @Override
    public void run() {
        try {
            listenEvents();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
