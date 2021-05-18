package system;

import abstractions.Abstraction;
import proto.CommunicationProtocol;

import java.util.Optional;

public class EventHandler extends Thread{
    private MySystem system;
    private boolean stop = false;

    public EventHandler(MySystem system){
        this.system = system;
    }

    public void setStop(boolean stop) {
        this.stop = stop;
    }

    @Override
    public void run() {
        while (!stop) {
            if(!system.getEventQueue().isEmpty()) {
                CommunicationProtocol.Message message = system.getEventQueue().remove();

                Optional<Abstraction> optional = system.getAbstractions().stream().filter(a -> a.getAbstractionId().equals(message.getToAbstractionId())).findFirst();
                if(optional.isPresent()) {
                    optional.get().handleMessage(message);
                }else {
                    system.receiveEvent(message);
                }
            }
        }
    }
}



