package system;

import abstractions.*;
import proto.CommunicationProtocol;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;

public class MySystem {
    private List<CommunicationProtocol.ProcessId> processIds;
    private Queue<CommunicationProtocol.Message> eventQueue;
    private String systemId;
    private Process process;
    private List<Abstraction> abstractions;
    private EventHandler eventHandler;
    private EventLoop eventLoop;

    public MySystem() {
    }

    public MySystem(String systemId, List<CommunicationProtocol.ProcessId> processIds, Process process) {
        this.processIds = processIds;
        this.systemId = systemId;
        this.process = process;
        eventQueue = new ArrayBlockingQueue<>(10000);
        abstractions = new CopyOnWriteArrayList<>();

        //start event loop
        System.out.println(process.getOwner() + "-" + process.getUuid() + " starting event loop...");
        eventLoop = new EventLoop(this);
        eventLoop.start();

        //register APP and PL
        abstractions.add(new APP(process.getProcessId(), this, "app", ""));
        abstractions.add(new PL(process.getProcessId(), this, "app.pl", "app"));
        abstractions.add(new PL(process.getProcessId(),this, "app.beb.pl","app.beb"));

        System.out.println(process.getOwner() + "-" + process.getUuid() + " starting handling events...");
        eventHandler = new EventHandler(this);
        eventHandler.start();
    }

    public void receiveEvent(CommunicationProtocol.Message message) {
        eventQueue.add(message);
    }

    public int getPort() {
        return process.getPort();
    }

    public Queue<CommunicationProtocol.Message> getEventQueue() {
        return eventQueue;
    }

    public List<Abstraction> getAbstractions() {
        return abstractions;
    }

    public String getSystemId() {
        return systemId;
    }

    public List<CommunicationProtocol.ProcessId> getProcessIds() {
        return processIds;
    }

    public void addAbstraction(String parentId, String abstractionId, Object... parameters) {
        if (!existsAbstraction(parentId+abstractionId)) {

            if(abstractionId.matches(".*\\.pl")) {
                System.out.println(process.getOwner() + "-" + process.getUuid() + " is adding " + parentId + abstractionId + "...");
                abstractions.add(new PL(process.getProcessId(), this, parentId + abstractionId, parentId));
            }
            else if(abstractionId.matches(".*\\.beb")) {
                System.out.println(process.getOwner() + "-" + process.getUuid() + " is adding " + parentId + abstractionId + "...");
                abstractions.add(new BEB(process.getProcessId(), this, parentId + abstractionId, parentId));
            }

            else if(abstractionId.matches(".*\\.nnar\\[.]")) {
                System.out.println(process.getOwner() + "-" + process.getUuid() + " is adding " + parentId + abstractionId + "...");
                abstractions.add(new NNAR(process.getProcessId(),this, parentId + abstractionId, parentId, (String)parameters[0]));
            }
            else if(abstractionId.matches(".*\\.uc\\[.*]")){
                System.out.println(process.getOwner() + "-" + process.getUuid() + " is adding " + parentId + abstractionId + "...");
                abstractions.add(new UC(process.getProcessId(),this, parentId + abstractionId, parentId, (String)parameters[0]));
            }
            else if(abstractionId.matches(".*\\.ep\\[.*]")){
                System.out.println(process.getOwner() + "-" + process.getUuid() + " is adding " + parentId + abstractionId + "...");
                abstractions.add(new EP(process.getProcessId(), this, parentId + abstractionId, parentId, (CommunicationProtocol.EpInternalState) parameters[0], (int) parameters[1]));
            }
            else if(abstractionId.matches(".*\\.ec")){
                System.out.println(process.getOwner() + "-" + process.getUuid() + " is adding " + parentId + abstractionId + "...");
                abstractions.add(new EC(process.getProcessId(), this, parentId + abstractionId, parentId));
            }
            else if(abstractionId.matches(".*\\.eld")){
                System.out.println(process.getOwner() + "-" + process.getUuid() + " is adding " + parentId + abstractionId + "...");
                abstractions.add(new ELD(process.getProcessId(), this, parentId + abstractionId, parentId));
            }
            else if(abstractionId.matches(".*\\.epfd")){
                System.out.println(process.getOwner() + "-" + process.getUuid() + " is adding " + parentId + abstractionId + "...");
                abstractions.add(new EPFD(process.getProcessId(), this, parentId + abstractionId, parentId));
            }
        }
    }

    public void addAbstraction(Abstraction abstraction){
        System.out.println(process.getOwner() + "-" + process.getUuid() + " is adding " + abstraction.getAbstractionId() + "...");
        abstractions.add(abstraction);
    }

    public void removeAbstraction(Abstraction abstraction){
        abstractions.remove(abstraction);
    }

    private boolean existsAbstraction(String abstractionId) {
        return abstractions.stream().anyMatch(a -> a.getAbstractionId().equals(abstractionId));
    }

    public CommunicationProtocol.ProcessId getHub() {
        return CommunicationProtocol.ProcessId.newBuilder()
                .setHost("127.0.0.1")
                .setPort(5000)
                .build();
    }

    public void stopSystem(){
        System.out.println("Stopping process "+process.getOwner()+"-"+process.getUuid());
        eventHandler.setStop(true);
        eventQueue = new ArrayBlockingQueue<>(10000);
    }
}
