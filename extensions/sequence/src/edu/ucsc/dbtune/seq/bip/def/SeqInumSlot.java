package edu.ucsc.dbtune.seq.bip.def;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;

public class SeqInumSlot implements Serializable {
    public SeqInumPlan plan;
    public int id;
    public double fullTableScanCost;
    boolean valid=true;
    public Vector<SeqInumSlotIndexCost> costs = new Vector<SeqInumSlotIndexCost>();

    // the following variables are only for debugging purposes
    // and should be removed when the system works correctly
    public SeqInumSlotIndexCost selectedIndex;

    public SeqInumSlot(SeqInumPlan plan, int id) {
        this.plan = plan;
        this.id = id;
    }

    public double getCost(SeqInumIndex index) {
        if (index == null)
            return fullTableScanCost;
        for (SeqInumSlotIndexCost c : costs) {
            if (c.index == index)
                return c.cost;
        }
        throw new Error();
    }

    public void save(Rx rx) {
        rx.setAttribute("fullTableScanCost", fullTableScanCost);
        for (SeqInumSlotIndexCost cost : costs) {
            cost.save(rx.createChild("index"));
        }
    }

    public SeqInumSlot(Hashtable<String, SeqInumIndex> indexHash,
            SeqInumPlan plan, Rx rx, int id) {
        this.plan = plan;
        this.id = id;
        fullTableScanCost = rx.getDoubleAttribute("fullTableScanCost");
        for (Rx r : rx.findChilds("index")) {
            SeqInumSlotIndexCost cost=new SeqInumSlotIndexCost(indexHash, r);
            if (cost.cost>1E100) {
                Rt.error(cost.cost);
                continue;
            }
            costs.add(cost);
        }
        if (fullTableScanCost>1E100&&costs.size()==0) {
            Rt.error(fullTableScanCost);
            valid=false;
        }
    }
}
