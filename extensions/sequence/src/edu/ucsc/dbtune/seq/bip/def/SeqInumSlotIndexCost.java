package edu.ucsc.dbtune.seq.bip.def;

import java.io.Serializable;
import java.util.Hashtable;

import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.util.Rx;

public class SeqInumSlotIndexCost implements Serializable {
    public SeqInumIndex index;
    public double cost;
    public double updateCost;

    public SeqInumSlotIndexCost() {
    }

    public void times(double d) {
        cost *= d;
//        updateCost *= d;
    }

    public boolean equals(SeqInumSlotIndexCost s2) {
        if (index != s2.index)
            return false;
        if (Math.abs(cost - s2.cost) > SeqInumPlan.maxAllowedError)
            return false;
        if (Math.abs(updateCost - s2.updateCost) > SeqInumPlan.maxAllowedError)
            return false;
        return true;
    }

    public boolean isCoveredBy(SeqInumSlotIndexCost s2) {
        if (index != s2.index)
            return false;
        if (cost < s2.cost - SeqInumPlan.maxAllowedError)
            return false;
        if (updateCost < s2.updateCost - SeqInumPlan.maxAllowedError)
            return false;
        return true;
    }

    public void save(Rx rx) {
        rx.setAttribute("indexId", index.id);
        rx.setAttribute("indexName", index.name);
        rx.setAttribute("cost", cost);
        rx.setAttribute("updateCost", updateCost);
    }

    public SeqInumSlotIndexCost(Hashtable<String, SeqInumIndex> indexHash, Rx rx) {
        String indexName = rx.getAttribute("indexName");
        index = indexHash.get(indexName);
        if (index == null)
            throw new Error();
        cost = rx.getDoubleAttribute("cost");
        updateCost = rx.getDoubleAttribute("updateCost");
    }
}
