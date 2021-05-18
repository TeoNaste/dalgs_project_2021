package abstractions;

import com.google.protobuf.Descriptors;
import proto.CommunicationProtocol;
import system.MySystem;
import utils.NnarTriple;
import utils.Utils;

import java.util.HashMap;
import java.util.Map;

//Alg 4.10, 4.11 pag 186
public class NNAR extends Abstraction {
    private String register;
    private NnarTriple triple;
    private int acks;
    private CommunicationProtocol.Value writeval;
    private int rid;
    private Map<CommunicationProtocol.ProcessId,NnarTriple> readlist;
    private CommunicationProtocol.Value readval;
    private boolean reading;

    public NNAR(CommunicationProtocol.ProcessId processId, MySystem system, String abstractionId, String parentId, String register) {
        super(processId,system, abstractionId, parentId);
        this.register = register;

        triple = new NnarTriple(0,0,CommunicationProtocol.Value.newBuilder().setDefined(false).build());
        acks = 0;
        writeval = CommunicationProtocol.Value.newBuilder().setDefined(false).build();
        rid = 0;
        readlist = new HashMap<>();
        readval = CommunicationProtocol.Value.newBuilder().setDefined(false).build();
        reading = false;
    }

    @Override
    public void handleMessage(CommunicationProtocol.Message message) {
        switch (message.getType()){
            case NETWORK_MESSAGE:{
                if(message.getNetworkMessage().getMessage().getType() == CommunicationProtocol.Message.Type.NNAR_WRITE)
                    uponNnarWrite(message);
                else
                    uponNnarRead(message);
                break;
            }
            case BEB_DELIVER:{
                if(message.getBebDeliver().getMessage().getType() == CommunicationProtocol.Message.Type.NNAR_INTERNAL_READ)
                    uponBebDeliverRead(message);
                if(message.getBebDeliver().getMessage().getType() == CommunicationProtocol.Message.Type.NNAR_INTERNAL_WRITE)
                    uponBebDeliverWrite(message);
                break;
            }
            case PL_DELIVER:{
                if(message.getPlDeliver().getMessage().getType() == CommunicationProtocol.Message.Type.NNAR_INTERNAL_VALUE)
                    uponPlDeliverInternalValue(message);
                if(message.getPlDeliver().getMessage().getType() == CommunicationProtocol.Message.Type.NNAR_INTERNAL_ACK)
                    uponPlDeliverAck(message);
                break;
            }

        }

    }

    private void uponPlDeliverAck(CommunicationProtocol.Message message) {
        if(rid != message.getPlDeliver().getMessage().getNnarInternalAck().getReadId()) {
            system.receiveEvent(message);
            return;
        }

        acks++;

        if(acks > system.getProcessIds().size()/2){
            acks = 0;
            if(reading){
                reading = false;
//                CommunicationProtocol.NnarReadReturn readReturn = CommunicationProtocol.NnarReadReturn.newBuilder()
//                        .setValue(readval)
//                        .build();

                CommunicationProtocol.AppReadReturn readReturn = CommunicationProtocol.AppReadReturn.newBuilder()
                        .setValue(readval)
                        .setRegister(register)
                        .build();

                CommunicationProtocol.Message readReturnMessage = CommunicationProtocol.Message.newBuilder()
                        .setAppReadReturn(readReturn)
                        .setType(CommunicationProtocol.Message.Type.APP_READ_RETURN)
                        .setSystemId(system.getSystemId())
                        .setFromAbstractionId(abstractionId)
                        .setToAbstractionId(abstractionId)
                        .build();

                System.out.println(processId.getOwner()+'-'+processId.getIndex()+" read returns value "+readval.getV()+"...");
                CommunicationProtocol.Message toApp = Utils.constructNetworkMessage(readReturnMessage,processId.getPort(),processId.getHost(),parentId,abstractionId);

                system.receiveEvent(toApp);
            }
            else{
//                CommunicationProtocol.NnarWriteReturn writeReturn = CommunicationProtocol.NnarWriteReturn.newBuilder().build();

                CommunicationProtocol.AppWriteReturn writeReturn = CommunicationProtocol.AppWriteReturn.newBuilder()
                        .setRegister(register).build();

                CommunicationProtocol.Message writeReturnMessage = CommunicationProtocol.Message.newBuilder()
                        .setAppWriteReturn(writeReturn)
                        .setType(CommunicationProtocol.Message.Type.APP_WRITE_RETURN)
                        .setSystemId(system.getSystemId())
                        .setFromAbstractionId(abstractionId)
                        .setToAbstractionId(abstractionId)
                        .build();

                System.out.println(processId.getOwner()+'-'+processId.getIndex()+" write returns value "+writeval.getV()+"...");
                CommunicationProtocol.Message toApp = Utils.constructNetworkMessage(writeReturnMessage,processId.getPort(),processId.getHost(),parentId,abstractionId);

                system.receiveEvent(toApp);
            }
        }


    }


    private void uponPlDeliverInternalValue(CommunicationProtocol.Message message) {
        if(rid != message.getPlDeliver().getMessage().getNnarInternalValue().getReadId()) {
            system.receiveEvent(message);
            return;
        }

        CommunicationProtocol.ProcessId q = Utils.find(system.getProcessIds(),message.getPlDeliver().getSender());
        CommunicationProtocol.NnarInternalValue value = message.getPlDeliver().getMessage().getNnarInternalValue();
        NnarTriple received = new NnarTriple(value.getTimestamp(),value.getWriterRank(),value.getValue());

        readlist.put(q,received);

        if(readlist.size() > system.getProcessIds().size() / 2){
            NnarTriple maxTs = highest();
            readval = maxTs.getVal();
            readlist = new HashMap<>();

            if(reading){
                CommunicationProtocol.NnarInternalWrite internalWrite = CommunicationProtocol.NnarInternalWrite.newBuilder()
                        .setReadId(rid)
                        .setTimestamp(maxTs.getTs())
                        .setWriterRank(maxTs.getWr())
                        .setValue(readval)
                        .build();

                CommunicationProtocol.Message writeMessage = CommunicationProtocol.Message.newBuilder()
                        .setNnarInternalWrite(internalWrite)
                        .setType(CommunicationProtocol.Message.Type.NNAR_INTERNAL_WRITE)
                        .setSystemId(system.getSystemId())
                        .setFromAbstractionId(abstractionId)
                        .setToAbstractionId(abstractionId)
                        .build();

                CommunicationProtocol.Message toSend = Utils.constructBebBroadcast(writeMessage,abstractionId+".beb",abstractionId);

                system.receiveEvent(toSend);
            }
            else {
                CommunicationProtocol.NnarInternalWrite internalWrite = CommunicationProtocol.NnarInternalWrite.newBuilder()
                        .setReadId(rid)
                        .setTimestamp(maxTs.getTs()+1)
                        .setWriterRank(Utils.findRank(system.getProcessIds(),processId))
                        .setValue(writeval)
                        .build();

                CommunicationProtocol.Message writeMessage = CommunicationProtocol.Message.newBuilder()
                        .setNnarInternalWrite(internalWrite)
                        .setType(CommunicationProtocol.Message.Type.NNAR_INTERNAL_WRITE)
                        .setSystemId(system.getSystemId())
                        .setFromAbstractionId(abstractionId)
                        .setToAbstractionId(abstractionId)
                        .build();

                CommunicationProtocol.Message toSend = Utils.constructBebBroadcast(writeMessage,abstractionId+".beb",abstractionId);

                system.receiveEvent(toSend);
            }
        }
    }

    private void uponBebDeliverWrite(CommunicationProtocol.Message message) {
        CommunicationProtocol.NnarInternalWrite internalWrite = message.getBebDeliver().getMessage().getNnarInternalWrite();

        if(internalWrite.getWriterRank() > triple.getWr() && internalWrite.getTimestamp() > triple.getTs()){
            triple.setTs(internalWrite.getTimestamp());
            triple.setWr(internalWrite.getWriterRank());
            triple.setVal(internalWrite.getValue());

            System.out.println(processId.getOwner()+'-'+processId.getIndex()+" trigger Ack with value "+triple.getVal().getV()+" and ts "+triple.getTs()+"...");
        }

        CommunicationProtocol.NnarInternalAck ack = CommunicationProtocol.NnarInternalAck.newBuilder()
                .setReadId(internalWrite.getReadId())
                .build();

        CommunicationProtocol.Message ackMessage = CommunicationProtocol.Message.newBuilder()
                .setNnarInternalAck(ack)
                .setType(CommunicationProtocol.Message.Type.NNAR_INTERNAL_ACK)
                .setSystemId(system.getSystemId())
                .setFromAbstractionId(abstractionId)
                .setToAbstractionId(abstractionId)
                .build();

        CommunicationProtocol.Message toSend = Utils.constructPlSend(ackMessage,message.getBebDeliver().getSender(),abstractionId+".pl",abstractionId);

        system.receiveEvent(toSend);
    }

    private void uponBebDeliverRead(CommunicationProtocol.Message message) {
        //send internal value
        System.out.println(processId.getOwner()+'-'+processId.getIndex()+" sends value "+writeval.getV()+"...");

        CommunicationProtocol.BebDeliver bebDeliver = message.getBebDeliver();
        CommunicationProtocol.NnarInternalRead internalRead = message.getBebDeliver().getMessage().getNnarInternalRead();

        CommunicationProtocol.NnarInternalValue value = CommunicationProtocol.NnarInternalValue.newBuilder()
                .setTimestamp(triple.getTs())
                .setWriterRank(triple.getWr())
                .setValue(triple.getVal())
                .setReadId(internalRead.getReadId())
                .build();

        CommunicationProtocol.Message valueMessage = CommunicationProtocol.Message.newBuilder()
                .setNnarInternalValue(value)
                .setType(CommunicationProtocol.Message.Type.NNAR_INTERNAL_VALUE)
                .setSystemId(system.getSystemId())
                .setToAbstractionId(abstractionId)
                .setFromAbstractionId(abstractionId)
                .build();

        CommunicationProtocol.Message toSend = Utils.constructPlSend(valueMessage,bebDeliver.getSender(),abstractionId+".pl",abstractionId);

        system.addAbstraction(abstractionId, ".pl");

        system.receiveEvent(toSend);

    }

    private void uponNnarRead(CommunicationProtocol.Message message) {
        rid++;
        acks = 0;
        readlist = new HashMap<>();
        reading = true;

        CommunicationProtocol.NnarInternalRead internalRead = CommunicationProtocol.NnarInternalRead.newBuilder()
                .setReadId(rid)
                .build();

        CommunicationProtocol.Message readMes = CommunicationProtocol.Message.newBuilder()
                .setNnarInternalRead(internalRead)
                .setToAbstractionId(abstractionId)
                .setFromAbstractionId(abstractionId)
                .setSystemId(system.getSystemId())
                .setType(CommunicationProtocol.Message.Type.NNAR_INTERNAL_READ)
                .build();

        CommunicationProtocol.Message toSend = Utils.constructBebBroadcast(readMes,abstractionId+".beb",abstractionId);

        system.addAbstraction(abstractionId,".beb");

        System.out.println(processId.getOwner()+'-'+processId.getIndex()+" starts reading on register "+register+"...");
        system.receiveEvent(toSend);
    }

    private void uponNnarWrite(CommunicationProtocol.Message message) {
        CommunicationProtocol.NnarWrite write = message.getNetworkMessage().getMessage().getNnarWrite();
        rid++;
        writeval = write.getValue();
        acks = 0;
        readlist = new HashMap<>();

        CommunicationProtocol.NnarInternalRead internalRead = CommunicationProtocol.NnarInternalRead.newBuilder()
                .setReadId(rid)
                .build();

        CommunicationProtocol.Message readMes = CommunicationProtocol.Message.newBuilder()
                .setNnarInternalRead(internalRead)
                .setToAbstractionId(abstractionId)
                .setFromAbstractionId(abstractionId)
                .setSystemId(system.getSystemId())
                .setType(CommunicationProtocol.Message.Type.NNAR_INTERNAL_READ)
                .build();

        CommunicationProtocol.Message toSend = Utils.constructBebBroadcast(readMes,abstractionId+".beb",abstractionId);

        system.addAbstraction(abstractionId,".beb");

        System.out.println(processId.getOwner()+'-'+processId.getIndex()+" is writing value "+writeval.getV()+" on register "+register+"...");
        system.receiveEvent(toSend);
    }

    private NnarTriple highest(){
        NnarTriple highest = new NnarTriple(-1,-1, CommunicationProtocol.Value.newBuilder().build());
        for (CommunicationProtocol.ProcessId p: readlist.keySet()) {
            NnarTriple value = readlist.get(p);
            if( value.getTs() > highest.getTs()) {
                highest = value;
            }
        }
        return highest;
    }

}
