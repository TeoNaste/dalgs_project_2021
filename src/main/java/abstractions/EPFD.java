package abstractions;

import proto.CommunicationProtocol;
import system.MySystem;
import utils.Utils;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArrayList;

//Eventually Perfect Link Detector - Increase timeout - 2.7 pg 74
public class EPFD extends Abstraction {
    private List<CommunicationProtocol.ProcessId> alive;
    private List<CommunicationProtocol.ProcessId> suspected;
    private int delay;
    private Timer timer;

    public EPFD(CommunicationProtocol.ProcessId processId, MySystem system, String abstractionId, String parentId) {
        super(processId, system, abstractionId, parentId);

        alive = new CopyOnWriteArrayList<>();
        alive.addAll(system.getProcessIds());
        suspected = new CopyOnWriteArrayList<>();
        delay = 100;
        timer = new Timer();

        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                triggerTimeout();
            }
        };
        timer.schedule(task,delay);
    }

    private void triggerTimeout() {
        CommunicationProtocol.EpfdTimeout timeout = CommunicationProtocol.EpfdTimeout.newBuilder().build();

        CommunicationProtocol.Message timeoueMessage = CommunicationProtocol.Message.newBuilder()
                .setEpfdTimeout(timeout)
                .setType(CommunicationProtocol.Message.Type.EPFD_TIMEOUT)
                .setSystemId(system.getSystemId())
                .setFromAbstractionId(abstractionId)
                .setToAbstractionId(abstractionId)
                .build();

        system.receiveEvent(timeoueMessage);
    }

    @Override
    public void handleMessage(CommunicationProtocol.Message message) {
        switch (message.getType()){
            case EPFD_TIMEOUT:
                //M(Timeout)
                uponTimeout();
                break;
            case PL_DELIVER:{
                //M(PlDeliver(M(Heartbeat)))
                if(message.getPlDeliver().getMessage().getType() == CommunicationProtocol.Message.Type.EPFD_INTERNAL_HEARTBEAT_REPLY)
                    uponPlDeliverReply(message);
                if(message.getPlDeliver().getMessage().getType() == CommunicationProtocol.Message.Type.EPFD_INTERNAL_HEARTBEAT_REQUEST)
                    uponPlDeliverRequest(message);
            }
        }
    }

    private void uponPlDeliverRequest(CommunicationProtocol.Message message) {
        CommunicationProtocol.ProcessId sender = message.getPlDeliver().getSender();

        CommunicationProtocol.EpfdInternalHeartbeatRequest request = CommunicationProtocol.EpfdInternalHeartbeatRequest.newBuilder().build();

        CommunicationProtocol.Message requestMessage = CommunicationProtocol.Message.newBuilder()
                .setEpfdInternalHeartbeatRequest(request)
                .setType(CommunicationProtocol.Message.Type.EPFD_INTERNAL_HEARTBEAT_REQUEST)
                .setSystemId(system.getSystemId())
                .setFromAbstractionId(abstractionId)
                .setToAbstractionId(abstractionId)
                .build();

        CommunicationProtocol.Message toSend = Utils.constructPlSend(requestMessage,sender,abstractionId+".pl",abstractionId);

        system.addAbstraction(abstractionId,".pl");

        system.receiveEvent(toSend);
    }

    private void uponPlDeliverReply(CommunicationProtocol.Message message) {
        CommunicationProtocol.ProcessId sender = message.getPlDeliver().getSender();
        CommunicationProtocol.ProcessId p = Utils.find(system.getProcessIds(),sender.getPort());

        alive.add(p);
    }

    private void uponTimeout() {
        if(Utils.checkIfIntersect(alive,suspected)){
            delay += 100;
        }

        for(CommunicationProtocol.ProcessId p: system.getProcessIds()){
            if(Utils.find(alive,p) == null && Utils.find(suspected,p) == null){
                suspected.add(p);
                triggerSuspect(p);
            }else if(Utils.find(alive,p) != null && Utils.find(suspected,p) != null){
                suspected.remove(p);
                triggerRestore(p);
            }

            CommunicationProtocol.EpfdInternalHeartbeatRequest request = CommunicationProtocol.EpfdInternalHeartbeatRequest.newBuilder().build();

            CommunicationProtocol.Message requestMessage = CommunicationProtocol.Message.newBuilder()
                    .setEpfdInternalHeartbeatRequest(request)
                    .setType(CommunicationProtocol.Message.Type.EPFD_INTERNAL_HEARTBEAT_REQUEST)
                    .setSystemId(system.getSystemId())
                    .setFromAbstractionId(abstractionId)
                    .setToAbstractionId(abstractionId)
                    .build();

            CommunicationProtocol.Message toSend = Utils.constructPlSend(requestMessage,p,abstractionId+".pl",abstractionId);

            system.addAbstraction(abstractionId,".pl");
            system.receiveEvent(toSend);
        }

        alive.clear();

        timer.cancel();
        timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                triggerTimeout();
            }
        };
        timer.schedule(task, delay);
    }

    private void triggerRestore(CommunicationProtocol.ProcessId p) {
        CommunicationProtocol.EpfdRestore epfdRestore = CommunicationProtocol.EpfdRestore.newBuilder()
                .setProcess(p)
                .build();

        CommunicationProtocol.Message restoreMessage = CommunicationProtocol.Message.newBuilder()
                .setEpfdRestore(epfdRestore)
                .setType(CommunicationProtocol.Message.Type.EPFD_RESTORE)
                .setSystemId(system.getSystemId())
                .setFromAbstractionId(abstractionId)
                .setToAbstractionId(parentId)
                .build();

        system.receiveEvent(restoreMessage);
    }

    private void triggerSuspect(CommunicationProtocol.ProcessId p) {
        CommunicationProtocol.EpfdSuspect epfdSuspect = CommunicationProtocol.EpfdSuspect.newBuilder()
                .setProcess(p)
                .build();

        CommunicationProtocol.Message suspectMessage = CommunicationProtocol.Message.newBuilder()
                .setEpfdSuspect(epfdSuspect)
                .setType(CommunicationProtocol.Message.Type.EPFD_SUSPECT)
                .setSystemId(system.getSystemId())
                .setFromAbstractionId(abstractionId)
                .setToAbstractionId(parentId)
                .build();

        system.receiveEvent(suspectMessage);
    }
}
