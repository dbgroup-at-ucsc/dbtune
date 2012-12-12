package edu.ucsc.dbtune.seq.bip.def;

import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Vector;

import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;

public class SeqInumPlan implements Serializable {
    public SeqInumQuery query;
    public String plan;
    public int id;
    public double internalCost;
    public SeqInumSlot[] slots;
    public boolean valid = true;

    public SeqInumPlan(SeqInumQuery query, int id) {
        this.query = query;
        this.id = id;
    }

    public double cost(SeqInumIndex[] indexes) {
        double cost = internalCost;
        for (SeqInumSlot slot : slots) {
            cost += slot.cost(indexes);
        }
        return cost;
    }

    public int totalIndexes() {
        int n = 0;
        for (SeqInumSlot slot : slots)
            n += slot.costs.size();
        return n;
    }

    public void removeDupIndexes() {
        Vector<SeqInumSlot> v = new Vector<SeqInumSlot>();
        for (SeqInumSlot slot : slots) {
            slot.removeDupIndexes();
            if (slot.costs.size() == 0)
                internalCost += slot.fullTableScanCost;
            else
                v.add(slot);
        }
        slots = v.toArray(new SeqInumSlot[v.size()]);
        v.clear();
        BitSet bs = new BitSet();
        for (int i = 0; i < slots.length; i++) {
            if (bs.get(i))
                continue;
            int dup = 1;
            for (int j = i + 1; j < slots.length; j++) {
                if (slots[i].equals(slots[j])) {
                    dup++;
                    bs.set(j);
                    break;
                }
            }
            if (dup > 1)
                slots[i].times(dup);
            v.add(slots[i]);
        }
        slots = v.toArray(new SeqInumSlot[v.size()]);
    }

    public void sortSlots() {
        Arrays.sort(slots, new Comparator<SeqInumSlot>() {
            @Override
            public int compare(SeqInumSlot o1, SeqInumSlot o2) {
                if (o1.costs.size() == 0 && o2.costs.size() == 0)
                    return 0;
                if (o1.costs.size() == 0)
                    return -1;
                if (o2.costs.size() == 0)
                    return 1;
                int t = o1.costs.get(0).index.id - o2.costs.get(0).index.id;
                if (t != 0)
                    return t;
                if (o1.fullTableScanCost > o2.fullTableScanCost)
                    return 1;
                if (o1.fullTableScanCost < o2.fullTableScanCost)
                    return -1;
                return 0;
            }
        });
    }

    public boolean equalSlots(SeqInumPlan p2) {
        if (slots.length != p2.slots.length)
            return false;
        HashSet<SeqInumSlot> used = new HashSet<SeqInumSlot>();
        for (SeqInumSlot slot1 : slots) {
            // Rt.p(slot1.id);
            boolean find = false;
            for (SeqInumSlot slot2 : p2.slots) {
                if (used.contains(slot2))
                    continue;
                if (slot1.equals(slot2)) {
                    used.add(slot2);
                    // Rt.p(slot1.id+" "+slot2.id);
                    find = true;
                    break;
                }
            }
            if (!find)
                return false;
        }
        return true;
    }

    public static double maxAllowedError = 0.1;

    public boolean isCoveredBy(SeqInumPlan p2) {
        if (slots.length != p2.slots.length)
            return false;
        if (internalCost < p2.internalCost - maxAllowedError)
            return false;
        HashSet<SeqInumSlot> used = new HashSet<SeqInumSlot>();
        for (SeqInumSlot slot1 : slots) {
            // Rt.p(slot1.id);
            boolean find = false;
            for (SeqInumSlot slot2 : p2.slots) {
                if (used.contains(slot2))
                    continue;
                if (slot1.isCoveredBy(slot2)) {
                    used.add(slot2);
                    // Rt.p(slot1.id+" "+slot2.id);
                    find = true;
                    break;
                }
            }
            if (!find)
                return false;
        }
        return true;
    }

    public void save(Rx rx) {
        rx.setAttribute("id", id);
        rx.setAttribute("internalCost", internalCost);
        rx.createChild("plan", plan);
        for (SeqInumSlot slot : slots) {
            slot.save(rx.createChild("slot"));
        }
    }

    public SeqInumPlan(Hashtable<String, SeqInumIndex> indexHash,
            SeqInumQuery query, Rx rx) {
        this.query = query;
        id = rx.getIntAttribute("id");
        internalCost = rx.getDoubleAttribute("internalCost");
        plan = rx.getChildText("plan");
        Rx[] rs = rx.findChilds("slot");
        slots = new SeqInumSlot[rs.length];
        for (int i = 0; i < rs.length; i++) {
            slots[i] = new SeqInumSlot(indexHash, this, rs[i], i);
            if (!slots[i].valid)
                valid = false;
        }
    }
}
