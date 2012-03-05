package edu.ucsc.dbtune.seq.bip.def;

public class SeqInumPlan {
    public SeqInumQuery query;
    public int id;
    public double internalCost;
    public SeqInumSlot[] slots;

    public SeqInumPlan(SeqInumQuery query,int id) {
        this.query = query;
        this.id=id;
    }
}
