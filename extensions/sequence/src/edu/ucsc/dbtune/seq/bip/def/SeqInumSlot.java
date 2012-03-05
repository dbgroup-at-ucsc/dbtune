package edu.ucsc.dbtune.seq.bip.def;

import java.util.Vector;

public class SeqInumSlot {
    public SeqInumPlan plan;
    public double fullTableScanCost;
    public Vector<SeqInumSlotIndexCost> costs = new Vector<SeqInumSlotIndexCost>();
    
    //the following variables are only for debugging purposes 
    //and should be removed when the system works correctly
    public SeqInumSlotIndexCost selectedIndex;

    public SeqInumSlot(SeqInumPlan plan) {
        this.plan = plan;
    }
}
