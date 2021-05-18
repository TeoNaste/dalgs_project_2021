package abstractions;

import proto.CommunicationProtocol;
import system.MySystem;
import utils.Utils;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

//Monarchial Eventual Leader Detection alg 2.8 pag 76
public class ELD extends Abstraction {
    private List<CommunicationProtocol.ProcessId> suspected;
    private CommunicationProtocol.ProcessId leader;

    public ELD(CommunicationProtocol.ProcessId processId, MySystem system, String abstractionId, String parentId) {
        super(processId, system, abstractionId, parentId);

        suspected = new CopyOnWriteArrayList<>();
        leader = processId;

        system.addAbstraction(abstractionId,".epfd");
    }

    @Override
    public void handleMessage(CommunicationProtocol.Message message) {
        switch (message.getType()){
            case EPFD_RESTORE:{
                uponEpfdRestore(message);
                break;
            }
            case EPFD_SUSPECT:{
                uponEpfdSuspect(message);
                break;
            }
        }
    }

    private void uponEpfdSuspect(CommunicationProtocol.Message message) {
        CommunicationProtocol.ProcessId p = message.getEpfdSuspect().getProcess();

        suspected.add(p);
        if(leaderNeedsChange())
            uponChangeLeader();
    }

    private void uponEpfdRestore(CommunicationProtocol.Message message) {
        CommunicationProtocol.ProcessId p = message.getEpfdRestore().getProcess();

        suspected.remove(p);
        if(leaderNeedsChange())
            uponChangeLeader();
    }

    public void uponChangeLeader(){
        leader = Utils.maxrank(Utils.getListDifference(system.getProcessIds(),suspected));

        CommunicationProtocol.EldTrust eldTrust = CommunicationProtocol.EldTrust.newBuilder().setProcess(leader).build();

        CommunicationProtocol.Message trustMessage = CommunicationProtocol.Message.newBuilder()
                .setEldTrust(eldTrust)
                .setType(CommunicationProtocol.Message.Type.ELD_TRUST)
                .setSystemId(system.getSystemId())
                .setFromAbstractionId(abstractionId)
                .setToAbstractionId(parentId)
                .build();

        System.out.println(processId.getOwner()+"-"+processId.getIndex()+": new leader is "+leader.getOwner()+"-"+leader.getIndex());
        system.receiveEvent(trustMessage);
    }

    private boolean leaderNeedsChange(){
        CommunicationProtocol.ProcessId supposedLeader = Utils.maxrank(Utils.getListDifference(system.getProcessIds(),suspected));
        if(supposedLeader == null)
            return false;
        else if(leader.getRank() != supposedLeader.getRank())
            return true;
        else
            return false;
    }
}
