package edu.ucsc.dbtune.inum.prune;

import static edu.ucsc.dbtune.inum.FullTableScanIndex.getFullTableScanIndexInstance;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.optimizer.plan.TableAccessSlot;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;
import edu.ucsc.dbtune.util.Tree.Entry;

public class PrunableInumSlot extends TableAccessSlot {
    public double minCost;
    public double maxCost;
    public boolean usable = true;
    public double ftsCost;
    public String name;
    List<Index> indexes = new Vector<Index>();
    Hashtable<Index, Double> accessCost = new Hashtable<Index, Double>();

    public PrunableInumSlot(TableAccessSlot slot, InumPlan plan,
            Set<Index> candidates) throws SQLException {
        super(slot);
        usable = false;
        minCost = Double.POSITIVE_INFINITY;
        maxCost = Double.NEGATIVE_INFINITY;
        for (Index index : candidates) {
            if (index.getTable().equals(slot.getTable())
                    && !(index instanceof FullTableScanIndex)) {

                double cost = plan.plug(slot, index);

                // if the cost is INF ==> the index
                // is not used in this slot of this particular
                // template plan
                if (!Double.isInfinite(cost)) {
                    indexes.add(index);
                    accessCost.put(index, cost);
                    usable = true;
                    if (cost < minCost)
                        minCost = cost;
                    if (cost > maxCost)
                        maxCost = cost;
                }
            }
        }
        // add the Full Table Scan Index at the last position in this slot
        FullTableScanIndex ftsIdx = getFullTableScanIndexInstance(slot
                .getTable());
        ftsCost = plan.plug(slot, ftsIdx);

        if (!Double.isInfinite(ftsCost))
            usable = true;
        
        if (ftsCost < minCost)
            minCost = ftsCost;
        if (ftsCost > maxCost)
            maxCost = ftsCost;
//        Rt.p(ftsCost);
//        Rt.p(maxCost);
        name=getIndex().toString();
        if ("[]".equals(name))
            name=getTable().toString();
    }

    public PrunableInumSlot(PrunableInumPlan plan,Rx rx, Map<String, Index> map) throws SQLException {
        super(plan.getSlotById(rx.getIntAttribute("id")));
        minCost = rx.getChildDoubleContent("minCost");
        maxCost = rx.getChildDoubleContent("maxCost");
        ftsCost = rx.getChildDoubleContent("ftsCost");
        ftsCost = rx.getChildDoubleContent("ftsCost");
        Rx[] rs = rx.findChilds("index");
        for (int i = 0; i < rs.length; i++) {
            Index index = map.get(rs[i].getText());
            if (index == null)
                throw new SQLException("Can't find index");
            double cost = rs[i].getDoubleAttribute("cost");
            indexes.add(index);
            accessCost.put(index, cost);
        }
        name=getIndex().toString();
        if ("[]".equals(name))
            name=getTable().toString();
    }

    public int numOfIndex() {
        return accessCost.size();
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TableAccessSlot))
            return false;

        TableAccessSlot op = (TableAccessSlot) o;

        return id == op.id;
    }

    public boolean equalsIndexAndCost(PrunableInumSlot s2) {
        if (Math.abs(ftsCost - s2.ftsCost) > 1E-5)
            return false;
        if (accessCost.size() != s2.accessCost.size())
            return false;
        for (Index index : accessCost.keySet()) {
            Double c1 = accessCost.get(index);
            Double c2 = s2.accessCost.get(index);
            if (c2 == null)
                return false;
            if (Math.abs(c1 - c2) > 1E-5)
                return false;
        }
        return true;
    }

    public void times(double d) {
        ftsCost *= d;
        for (Index index : accessCost.keySet()) {
            Double c1 = accessCost.get(index);
            accessCost.put(index, c1 * d);
        }
        // updateCost *= d;
    }

    public boolean isCoveredBy(PrunableInumSlot s2) {
        if (ftsCost < s2.ftsCost - 1E-5)
            return false;
        if (accessCost.size() > s2.accessCost.size())
            return false;
        for (Index index : accessCost.keySet()) {
            Double c1 = accessCost.get(index);
            Double c2 = s2.accessCost.get(index);
            if (c2 == null)
                return false;
            if (c1 < c2 - 1E-5)
                return false;
        }
        return true;
    }

    public void save(Rx rx) {
        rx.setAttribute("id", id);
        rx.createChild("minCost", minCost);
        rx.createChild("maxCost", maxCost);
        rx.createChild("ftsCost", ftsCost);
        for (Map.Entry<Index, Double> entry : accessCost.entrySet()) {
            Rx index = rx.createChild("index", entry.getKey().toString());
            index.setAttribute("cost", entry.getValue());
        }
    }

    public void removeDupIndexes() {
        Vector<Index> indexes2 = new Vector<Index>();
        Hashtable<Index, Double> accessCost2 = new Hashtable<Index, Double>();
        for (Map.Entry<Index, Double> entry : accessCost.entrySet()) {
            Index index = entry.getKey();
            double cost = entry.getValue();
            if (cost > ftsCost - 1E-5)
                continue;
            indexes2.add(index);
            accessCost2.put(index, cost);
        }
        indexes = indexes2;
        accessCost = accessCost2;
    }

    public double plug(TableAccessSlot slot, Index index) throws SQLException {
        if (index instanceof FullTableScanIndex)
            return ftsCost;
        Double cost = accessCost.get(index);
        if (cost == null) {
//            for (Index index2 : accessCost.keySet()) {
//                Rt.p(index2.toString());
//            }
            return ftsCost;
//            throw new SQLException("Haven't seen " + index + " before");
        }
        return cost;
    }
}
