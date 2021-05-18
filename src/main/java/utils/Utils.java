package utils;

import proto.CommunicationProtocol;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class Utils {
    public static CommunicationProtocol.Message constrctPlDeliver(CommunicationProtocol.Message toDeliver, CommunicationProtocol.ProcessId source, String toAbs, String fromAbs){
        CommunicationProtocol.PlDeliver.Builder deliverBuilder = CommunicationProtocol.PlDeliver.newBuilder();
        deliverBuilder.setMessage(toDeliver);
        deliverBuilder.setSender(source);
        CommunicationProtocol.PlDeliver plDeliver = deliverBuilder.build();

        CommunicationProtocol.Message.Builder mesBuilder = CommunicationProtocol.Message.newBuilder();
        mesBuilder.setPlDeliver(plDeliver);
        mesBuilder.setSystemId(toDeliver.getSystemId());
        mesBuilder.setType(CommunicationProtocol.Message.Type.PL_DELIVER);
        mesBuilder.setToAbstractionId(toAbs);
        mesBuilder.setFromAbstractionId(fromAbs);
        CommunicationProtocol.Message deliverMes = mesBuilder.build();

        return deliverMes;
    }

    public static CommunicationProtocol.Message constructNetworkMessage(CommunicationProtocol.Message message,int listeningPort,String host, String toAbs, String fromAbs){
        CommunicationProtocol.NetworkMessage.Builder netBuilder = CommunicationProtocol.NetworkMessage.newBuilder();
        netBuilder.setMessage(message);
        netBuilder.setSenderListeningPort(listeningPort);
        netBuilder.setSenderHost(host);

        CommunicationProtocol.NetworkMessage netMes = netBuilder.build();

        CommunicationProtocol.Message.Builder completeMessage = CommunicationProtocol.Message.newBuilder();
        completeMessage.setNetworkMessage(netMes);
        completeMessage.setToAbstractionId(toAbs);
        completeMessage.setFromAbstractionId(fromAbs);
        completeMessage.setSystemId(message.getSystemId());
        completeMessage.setType(CommunicationProtocol.Message.Type.NETWORK_MESSAGE);

        return completeMessage.build();
    }

    public static CommunicationProtocol.Message constructBebBroadcast(CommunicationProtocol.Message toSend, String toAbs, String fromAbs){
        CommunicationProtocol.BebBroadcast.Builder bcastBuilder = CommunicationProtocol.BebBroadcast.newBuilder();
        bcastBuilder.setMessage(toSend);
        CommunicationProtocol.BebBroadcast bebcast = bcastBuilder.build();

        CommunicationProtocol.Message.Builder messBuilder = CommunicationProtocol.Message.newBuilder();
        messBuilder.setBebBroadcast(bebcast);
        messBuilder.setToAbstractionId(toAbs);
        messBuilder.setFromAbstractionId(fromAbs);
        messBuilder.setSystemId(toSend.getSystemId());
        messBuilder.setType(CommunicationProtocol.Message.Type.BEB_BROADCAST);
        CommunicationProtocol.Message bebmsg = messBuilder.build();

        return bebmsg;
    }


    public static CommunicationProtocol.Message constructPlSend(CommunicationProtocol.Message toSend, CommunicationProtocol.ProcessId dest, String toAbs, String fromAbs){

        CommunicationProtocol.PlSend.Builder sendBuilder = CommunicationProtocol.PlSend.newBuilder();
        sendBuilder.setMessage(toSend);
        sendBuilder.setDestination(dest);
        CommunicationProtocol.PlSend plSend = sendBuilder.build();

        CommunicationProtocol.Message.Builder mesBuilder = CommunicationProtocol.Message.newBuilder();
        mesBuilder.setPlSend(plSend);
        mesBuilder.setToAbstractionId(toAbs);
        mesBuilder.setFromAbstractionId(fromAbs);
        mesBuilder.setSystemId(toSend.getSystemId());
        mesBuilder.setType(CommunicationProtocol.Message.Type.PL_SEND);
        CommunicationProtocol.Message sendMes = mesBuilder.build();

        return sendMes;
    }

    public static CommunicationProtocol.Message constructBebDeliver(CommunicationProtocol.Message toSend, CommunicationProtocol.ProcessId sender){
        CommunicationProtocol.BebDeliver.Builder bcastBuilder = CommunicationProtocol.BebDeliver.newBuilder();
        bcastBuilder.setMessage(toSend);
        bcastBuilder.setSender(sender);
        CommunicationProtocol.BebDeliver bebcast = bcastBuilder.build();

        CommunicationProtocol.Message.Builder messBuilder = CommunicationProtocol.Message.newBuilder();
        messBuilder.setBebDeliver(bebcast);
        messBuilder.setFromAbstractionId(toSend.getFromAbstractionId());
        messBuilder.setToAbstractionId(toSend.getToAbstractionId());
        messBuilder.setSystemId(toSend.getSystemId());
        messBuilder.setType(CommunicationProtocol.Message.Type.BEB_DELIVER);
        CommunicationProtocol.Message bebmsg = messBuilder.build();

        return bebmsg;

    }

    public static boolean ProcessEquals(CommunicationProtocol.ProcessId a, CommunicationProtocol.ProcessId b){
        if(a.getHost().equals(b.getHost()) &&
                a.getPort() == b.getPort() &&
                a.getIndex() == b.getIndex() &&
                a.getOwner().equals(b.getOwner()))
            return true;

        return false;
    }

    public static CommunicationProtocol.ProcessId find(List<CommunicationProtocol.ProcessId> processes, CommunicationProtocol.ProcessId processId){
        for(CommunicationProtocol.ProcessId p : processes) {
            if (ProcessEquals(p, processId))
                return p;
        }
        return null;
    }

    public static CommunicationProtocol.ProcessId find(List<CommunicationProtocol.ProcessId> processes, int listeningPort){
        CommunicationProtocol.ProcessId process = null;
        for(CommunicationProtocol.ProcessId p : processes)
            if(p.getPort() == listeningPort)
                process = p;
        return process;
    }

    public static int findRank(List<CommunicationProtocol.ProcessId> processes, CommunicationProtocol.ProcessId processId){
        return find(processes,processId).getRank();
    }

    public static CommunicationProtocol.ProcessId maxrank(List<CommunicationProtocol.ProcessId> list){
        if(list.size()==0)
            return null;
        CommunicationProtocol.ProcessId leader = list.get(0);
        for(CommunicationProtocol.ProcessId p : list)
            if(p.getRank() > leader.getRank())
                leader = p;

        return leader;
    }

    public static boolean checkIfIntersect(List<CommunicationProtocol.ProcessId> a, List<CommunicationProtocol.ProcessId> b){
        for(CommunicationProtocol.ProcessId p : a){
            if(find(b,p)!=null)
                return true;
        }
        return false;
    }

    public static List<CommunicationProtocol.ProcessId> getListDifference(List<CommunicationProtocol.ProcessId> a, List<CommunicationProtocol.ProcessId> b){
        List<CommunicationProtocol.ProcessId> result = new CopyOnWriteArrayList<>();
        for(CommunicationProtocol.ProcessId p : a)
            if(find(b,p)==null)
                result.add(p);

        return result;
    }

}
