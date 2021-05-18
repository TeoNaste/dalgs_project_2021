package utils;

import proto.CommunicationProtocol;

public class NnarTriple {
    private int ts;
    private int wr;
    private CommunicationProtocol.Value val;

    public NnarTriple(int ts, int wr, CommunicationProtocol.Value val) {
        this.ts = ts;
        this.wr = wr;
        this.val = val;
    }

    public int getTs() {
        return ts;
    }

    public int getWr() {
        return wr;
    }

    public CommunicationProtocol.Value getVal() {
        return val;
    }

    public void setTs(int ts) {
        this.ts = ts;
    }

    public void setWr(int wr) {
        this.wr = wr;
    }

    public void setVal(CommunicationProtocol.Value val) {
        this.val = val;
    }
}
