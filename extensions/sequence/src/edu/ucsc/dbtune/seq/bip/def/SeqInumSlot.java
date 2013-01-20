package edu.ucsc.dbtune.seq.bip.def;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Vector;

import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;

public class SeqInumSlot implements Serializable {
    public SeqInumPlan plan;
    public int id;
    public double fullTableScanCost;
    boolean valid = true;
    public Vector<SeqInumSlotIndexCost> costs = new Vector<SeqInumSlotIndexCost>();

    // the following variables are only for debugging purposes
    // and should be removed when the system works correctly
    public SeqInumSlotIndexCost selectedIndex;

    public SeqInumSlot(SeqInumPlan plan, int id) {
        this.plan = plan;
        this.id = id;
    }

    public void removeIndex(HashSet<SeqInumIndex> remove) {
        Vector<SeqInumSlotIndexCost> costs2 = new Vector<SeqInumSlotIndexCost>();
        for (SeqInumSlotIndexCost cost : costs) {
            if (!remove.contains(cost.index))
                costs2.add(cost);
        }
        this.costs = costs2;
    }

    public double cost(SeqInumIndex[] indexes) {
        double min = fullTableScanCost;
        for (SeqInumSlotIndexCost cost : costs) {
            double c = cost.cost(indexes);
            if (c < min)
                min = c;
        }
        return min;
    }

    public void times(double d) {
        fullTableScanCost *= d;
        for (SeqInumSlotIndexCost cost : costs) {
            cost.times(d);
        }
        // updateCost *= d;
    }

    public void removeDupIndexes() {
        Vector<SeqInumSlotIndexCost> v = new Vector<SeqInumSlotIndexCost>();
        for (SeqInumSlotIndexCost cost : costs) {
            if (cost.cost > fullTableScanCost - 0.1)
                continue;
            v.add(cost);
        }
        costs = v;
    }

    public boolean equals(SeqInumSlot s2) {
        if (Math.abs(fullTableScanCost - s2.fullTableScanCost) > SeqInumPlan.maxAllowedError)
            return false;
        SeqInumSlotIndexCost[] c1 = costs.toArray(new SeqInumSlotIndexCost[costs.size()]);
        SeqInumSlotIndexCost[] c2 = s2.costs.toArray(new SeqInumSlotIndexCost[s2.costs.size()]);
        if (c1.length != c2.length)
            return false;
        Comparator<SeqInumSlotIndexCost> c = new Comparator<SeqInumSlotIndexCost>() {
            @Override
            public int compare(SeqInumSlotIndexCost o1, SeqInumSlotIndexCost o2) {
                return o1.index.id - o2.index.id;
            }
        };
        Arrays.sort(c1, c);
        Arrays.sort(c2, c);
        for (int i = 0; i < c1.length; i++) {
            if (!c1[i].equals(c2[i]))
                return false;
        }
        return true;
    }

    public boolean isCoveredBy(SeqInumSlot s2) {
        if (fullTableScanCost < s2.fullTableScanCost - SeqInumPlan.maxAllowedError)
            return false;
        SeqInumSlotIndexCost[] c1 = costs.toArray(new SeqInumSlotIndexCost[costs.size()]);
        SeqInumSlotIndexCost[] c2 = s2.costs.toArray(new SeqInumSlotIndexCost[s2.costs.size()]);
        if (c1.length > c2.length)
            return false;
        Comparator<SeqInumSlotIndexCost> c = new Comparator<SeqInumSlotIndexCost>() {
            @Override
            public int compare(SeqInumSlotIndexCost o1, SeqInumSlotIndexCost o2) {
                return o1.index.id - o2.index.id;
            }
        };
        Arrays.sort(c1, c);
        Arrays.sort(c2, c);
        int p = 0;
        for (int i = 0; i < c1.length; i++) {
            boolean found = false;
            while (p < c2.length) {
                if (c1[i].isCoveredBy(c2[p++])) {
                    found = true;
                    break;
                }
            }
            if (!found)
                return false;
        }
        return true;
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

    public SeqInumSlot(Hashtable<String, SeqInumIndex> indexHash, SeqInumPlan plan, Rx rx, int id) {
        this.plan = plan;
        this.id = id;
        fullTableScanCost = rx.getDoubleAttribute("fullTableScanCost");
        for (Rx r : rx.findChilds("index")) {
            SeqInumSlotIndexCost cost = new SeqInumSlotIndexCost(indexHash, r);
            if (cost.cost > 1E100) {
                Rt.error(cost.cost);
                continue;
            }
            costs.add(cost);
        }
        if (fullTableScanCost > 1E100 && costs.size() == 0) {
            Rt.error(fullTableScanCost);
            valid = false;
        }
    }
}
