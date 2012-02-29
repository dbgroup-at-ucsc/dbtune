package edu.ucsc.dbtune.seq.bip.def;

import java.util.Vector;

public class SeqInumSlot {
    public SeqInumPlan plan;
    public double fullTableScanCost;
    public Vector<SeqInumSlotIndexCost> costs = new Vector<SeqInumSlotIndexCost>();

    public SeqInumSlot(SeqInumPlan plan) {
        this.plan = plan;
    }
}
