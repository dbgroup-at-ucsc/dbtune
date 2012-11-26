package edu.ucsc.dbtune.inum.prune;

import static edu.ucsc.dbtune.inum.FullTableScanIndex.getFullTableScanIndexInstance;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.optimizer.plan.TableAccessSlot;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;

/**
 * 
 * @author wangrui
 * 
 */
public class PrunableInumPlan extends InumPlan {
    Vector<PrunableInumSlot> slots = new Vector<PrunableInumSlot>();
    HashSet<PrunableInumSlot> hash = new HashSet<PrunableInumSlot>();
    public double internalCost;
    public double minCost;
    public double maxCost;
    public boolean usable = true;
    public boolean redundant = false;

    public PrunableInumPlan(InumPlan plan, Set<Index> candidates)
            throws SQLException {
        super(plan);
        minCost = maxCost = internalCost = plan.getInternalCost();
        for (TableAccessSlot slot : plan.getSlots()) {
            PrunableInumSlot s = new PrunableInumSlot(slot, plan, candidates);
            slots.add(s);
            hash.add(s);

            if (!s.usable) {
                usable = false;
                break;
            }
            minCost += s.minCost;
            maxCost += s.maxCost;
            // Rt.p(s.maxCost);
            // Rt.p(maxCost);
        }
        // Rt.p(maxCost);
    }

    /**
     * save everything to a xml node
     * 
     * @param rx
     */
    public void save(Rx rx) {
        rx.createChild("internalCost", internalCost);
        rx.createChild("minCost", minCost);
        rx.createChild("maxCost", maxCost);
        Rx s = rx.createChild("slots");
        for (PrunableInumSlot slot : slots) {
            slot.save(s.createChild("slot"));
        }
        super.save(rx.createChild("inumPlan"));
    }

    public PrunableInumPlan(Catalog catalog, Rx rx, Map<String, Index> map)
            throws SQLException {
        super(catalog, rx.findChild("inumPlan"));
        internalCost = rx.getChildDoubleContent("internalCost");
        minCost = rx.getChildDoubleContent("minCost");
        maxCost = rx.getChildDoubleContent("maxCost");
        Rx[] rs = rx.findChild("slots").findChilds("slot");
        for (int i = 0; i < rs.length; i++) {
            PrunableInumSlot slot = new PrunableInumSlot(this, rs[i], map);
            slots.add(slot);
            hash.add(slot);
        }
    }

    public TableAccessSlot getSlotById(int id) {
        return slotsById.get(id);
    }

    public void removeDupIndexes() {
        removeUselessIndexAccessCosts();
        mergeSameSlots();
    }

    public void removeUselessIndexAccessCosts() {
        Vector<PrunableInumSlot> v = new Vector<PrunableInumSlot>();
        for (PrunableInumSlot slot : slots) {
            slot.removeDupIndexes();
            if (slot.accessCost.size() == 0)
                internalCost += slot.ftsCost;
            else
                v.add(slot);
        }
        slots.clear();
        slots.addAll(v);
    }

    public void mergeSameSlots() {
        Vector<PrunableInumSlot> v = new Vector<PrunableInumSlot>();
        BitSet bs = new BitSet();
        for (int i = 0; i < slots.size(); i++) {
            if (bs.get(i))
                continue;
            int dup = 1;
            for (int j = i + 1; j < slots.size(); j++) {
                if (slots.get(i).equalsIndexAndCost(slots.get(j))) {
                    dup++;
                    bs.set(j);
                    break;
                }
            }
            if (dup > 1)
                slots.get(i).times(dup);
            v.add(slots.get(i));
        }
        slots = v;
    }

    public void mergeSameOrderSlots() {
        Vector<PrunableInumSlot> v = new Vector<PrunableInumSlot>();
        BitSet bs = new BitSet();
        for (int slotId = 0; slotId < slots.size(); slotId++) {
            if (bs.get(slotId))
                continue;
            PrunableInumSlot src = slots.get(slotId);
            Vector<PrunableInumSlot> sameOrder = new Vector<PrunableInumSlot>();
            for (int j = slotId + 1; j < slots.size(); j++) {
                if (src.sameOrder(slots.get(j))) {
                    sameOrder.add(slots.get(j));
                    bs.set(j);
                    break;
                }
            }
            if (sameOrder.size() > 0) {
                for (PrunableInumSlot s : sameOrder) {
                    src.ftsCost += s.ftsCost;
                }
                for (Index index : src.indexes) {
                    double d = src.accessCost.get(index);
                    for (PrunableInumSlot s : sameOrder) {
                        d += s.accessCost.get(index);
                    }
                    src.accessCost.put(index, d);
                }
            }
            v.add(src);
        }
        slots = v;
    }

    /**
     * @param p2
     * @return
     */
    public boolean isCoveredBy(PrunableInumPlan p2) {
        if (slots.size() < p2.slots.size())
            return false;
        if (internalCost < p2.internalCost - 1E-5)
            return false;
        HashSet<PrunableInumSlot> used = new HashSet<PrunableInumSlot>();
        for (PrunableInumSlot slot2 : p2.slots) {
            // Rt.p(slot1.id);
            boolean find = false;
            for (PrunableInumSlot slot1 : slots) {
                if (used.contains(slot1))
                    continue;
                if (slot1.isCoveredBy(slot2)) {
                    used.add(slot1);
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

    public int numOfSlots() {
        return slots.size();
    }

    public int numOfIndex() {
        int sum = 0;
        for (PrunableInumSlot slot : slots) {
            sum += slot.numOfIndex();
        }
        return sum;
    }

    public void sortSlots() {
        Collections.sort(slots, new Comparator<PrunableInumSlot>() {
            @Override
            public int compare(PrunableInumSlot o1, PrunableInumSlot o2) {
                return o1.name.compareTo(o2.name);
            }
        });
    }

    public void print() {
        Rt.np("internal=%,.0f  min=%,.0f max=%,.0f", internalCost, minCost,
                maxCost);
        sortSlots();
        for (PrunableInumSlot slot : slots) {
            Rt.np("\t%,.0f %s", slot.ftsCost, slot.name);
            Index[] indexs = slot.accessCost.keySet().toArray(
                    new Index[slot.accessCost.size()]);
            Arrays.sort(indexs, new Comparator<Index>() {
                @Override
                public int compare(Index o1, Index o2) {
                    return o1.toString().compareTo(o2.toString());
                }
            });
            for (Index index : indexs) {
                Rt.np("\t\t%,.0f %s", slot.accessCost.get(index), index
                        .toString());
            }
        }
    }

    @Override
    public double getInternalCost() {
        return internalCost;
    }

    @Override
    public Collection<TableAccessSlot> getSlots() {
        return new Vector<TableAccessSlot>(slots);
    }

    public Operator instantiate2(TableAccessSlot slot, Index index)
            throws SQLException {
        double cost = plug(slot, index);
        Operator operator = slot.duplicate();
        operator.slot = slot;
        operator.scanCost = operator.accumulatedCost = cost;
        return operator;
    }

    @Override
    public double plug(TableAccessSlot slot, Index index) throws SQLException {
        if (!(slot instanceof PrunableInumSlot))
            throw new SQLException("slot is not returned by this class");
        if (!hash.contains(slot))
            throw new SQLException("slot is not returned by this class");
        return ((PrunableInumSlot) slot).plug(slot, index);
    }
}
