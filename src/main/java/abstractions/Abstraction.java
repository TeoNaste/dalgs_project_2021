package abstractions;

import proto.CommunicationProtocol;
import system.MySystem;

public abstract class Abstraction {
    protected CommunicationProtocol.ProcessId processId;
    protected MySystem system;
    protected String abstractionId;
    protected String parentId;

    public Abstraction() { }

    public Abstraction(CommunicationProtocol.ProcessId processId, MySystem system, String abstractionId,String parentId) {
        this.processId = processId;
        this.system = system;
        this.abstractionId = abstractionId;
        this.parentId = parentId;
    }

    public Abstraction(MySystem system, String abstractionId,String parentId) {
        this.system = system;
        this.abstractionId = abstractionId;
        this.parentId = parentId;
    }

    public String getAbstractionId() {
        return abstractionId;
    }

    public abstract void handleMessage(CommunicationProtocol.Message message);
}
