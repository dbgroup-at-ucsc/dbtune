package edu.ucsc.dbtune.seq.bip;

import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import edu.ucsc.dbtune.bip.core.InumQueryPlanDesc;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.seq.SeqCost;
import edu.ucsc.dbtune.seq.bip.def.*;
import edu.ucsc.dbtune.seq.utils.RTimerN;
import edu.ucsc.dbtune.seq.utils.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

public class SeqInumCost {
    public Hashtable<String, SeqInumQuery> queryHash = new Hashtable<String, SeqInumQuery>();
    public Hashtable<String, SeqInumIndex> indexHash = new Hashtable<String, SeqInumIndex>();
    public Vector<SeqInumIndex> indices = new Vector<SeqInumIndex>();
    public Vector<SeqInumQuery> queries = new Vector<SeqInumQuery>();
    public double storageConstraint = Double.MAX_VALUE;
    InumOptimizer optimizer;
    Set<Index> inumIndices;
    Hashtable<Index, SeqInumIndex> indexToInumIndex = new Hashtable<Index, SeqInumIndex>();

    public SeqInumQuery addQuery(SQLStatement statement) throws SQLException {
        int id = queries.size();
        SeqInumQuery q = new SeqInumQuery(id);
        q.name = "Q" + id;
        q.sql = statement;
        queryHash.put(q.name, q);
        queries.add(q);

        RTimerN timer = new RTimerN();
        QueryPlanDesc desc = InumQueryPlanDesc
                .getQueryPlanDescInstance(statement);
        // Populate the INUM space
        desc.generateQueryPlanDesc(optimizer, inumIndices);
        // Rt.p("BIP INUM populate time: " + timer.getSecondElapse());
        q.plans = new SeqInumPlan[desc.getNumberOfTemplatePlans()];
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
            SeqInumPlan plan = new SeqInumPlan(q, k);
            plan.internalCost = desc.getInternalPlanCost(k);
            plan.slots = new SeqInumSlot[desc.getNumberOfSlots()];
            for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                SeqInumSlot slot = new SeqInumSlot(plan);
                slot.fullTableScanCost = Double.MAX_VALUE;
                for (Index index : desc.getIndexesAtSlot(i)) {
                    timer = new RTimerN();
                    if (index instanceof FullTableScanIndex) {
                        slot.fullTableScanCost = desc.getAccessCost(k, index);
                        // Rt.p(index);
                        plugInFCount++;
                    } else {
                        SeqInumSlotIndexCost c = new SeqInumSlotIndexCost();
                        c.index = indexToInumIndex.get(index);
                        if (c.index == null)
                            throw new SQLException("Can't map index "
                                    + index.toString());
                        c.cost = desc.getAccessCost(k, index);
                        // Rt.p(index);
                        slot.costs.add(c);
                        // Rt
                        // .p("BIP INUM plugin time: "
                        // + timer.getSecondElapse());
                    }
                    plugInTime += timer.getSecondElapse();
                    plugInCount++;
                    // Rt.p("%f %f",timer.getSecondElapse(),plugInTime);
                }
                plan.slots[i] = slot;
            }
            q.plans[k] = plan;
        }
        return q;
    }

    public static SeqInumCost fromInum(InumOptimizer optimizer,
            Workload workload, Set<Index> indexes) throws SQLException {
        SeqInumCost cost = new SeqInumCost();
        cost.optimizer = optimizer;
        cost.inumIndices = indexes;
        Index[] indexs = indexes.toArray(new Index[indexes.size()]);
        for (int i = 0; i < indexs.length; i++) {
            SeqInumIndex q = new SeqInumIndex(i);
            q.name = "I" + i;
            q.index = indexs[i];
            cost.indexToInumIndex.put(indexs[i], q);
            cost.indexHash.put(q.name, q);
            cost.indices.add(q);
            q.createCost = SeqCost.getCreateIndexCost(optimizer, q.index);
            q.dropCost = 0;
            q.storageCost = 0; // TODO add storage cost
        }
        if (workload != null) {
            for (int i = 0; i < workload.size(); i++) {
                cost.addQuery(workload.get(i));
            }
        }
        return cost;
    }

    public static double plugInTime = 0;
    public static int plugInCount = 0;
    public static int plugInFCount = 0;

    public static SeqInumCost fromFile(String table) {
        SeqInumCost cost = new SeqInumCost();
        boolean readingPlan = false;
        boolean readingSlot = false;
        int queryId = 0;
        for (String line : table.split("\r?\n")) {
            line = line.trim();
            if (line.length() == 0)
                continue;
            if (line.startsWith("#"))
                continue;
            // Rt.p(line);
            String[] ss = line.split("[ |\t]+");
            if (!readingPlan && !readingSlot) {
                if (ss[0].startsWith("I")) {
                    SeqInumIndex index = new SeqInumIndex(cost.indices.size());
                    index.name = ss[0];
                    index.createCost = Double.parseDouble(ss[1]);
                    index.dropCost = Double.parseDouble(ss[2]);
                    index.storageCost = Double.parseDouble(ss[3]);
                    if (cost.indexHash.put(index.name, index) != null)
                        throw new Error("duplicate index");
                    cost.indices.add(index);
                } else if (ss[0].startsWith("Q") || ss[0].startsWith("U")) {
                    SeqInumQuery q = new SeqInumQuery(queryId++);
                    q.name = ss[0];
                    q.plans = new SeqInumPlan[Integer.parseInt(ss[1])];
                    String[] rs = ss[2].split(",");
                    q.relevantIndices = new SeqInumIndex[rs.length];
                    for (int i = 0; i < rs.length; i++) {
                        SeqInumIndex index = cost.indexHash.get(rs[i]);
                        if (index == null)
                            throw new Error("index not found");
                        q.relevantIndices[i] = index;
                    }
                    cost.queryHash.put(q.name, q);
                } else if ("SEQ".equals(ss[0])) {
                    String[] rs = ss[1].split(",");
                    cost.queries = new Vector<SeqInumQuery>();
                    for (int i = 0; i < rs.length; i++) {
                        SeqInumQuery q = cost.queryHash.get(rs[i]);
                        if (q == null)
                            throw new Error("query not found");
                        cost.queries.add(q);
                    }
                } else if ("STORAGE-CONSTRIANT".equals(ss[0])) {
                    cost.storageConstraint = Double.parseDouble(ss[1]);
                } else if ("PLAN".equals(ss[0])) {
                    readingPlan = true;
                }
            } else if (!readingSlot) {
                if ("SLOT".equals(ss[0])) {
                    readingSlot = true;
                    continue;
                }
                SeqInumQuery q = cost.queryHash.get(ss[0]);
                if (q == null)
                    throw new Error("query not found");
                int planId = Integer.parseInt(ss[1]);
                if (q.plans[planId] != null)
                    throw new Error("plan exist " + q.name + " " + planId);
                SeqInumPlan plan = new SeqInumPlan(q, planId);
                plan.internalCost = Double.parseDouble(ss[2]);
                plan.slots = new SeqInumSlot[Integer.parseInt(ss[3])];
                q.plans[planId] = plan;
                // Rt.p(q.name + " " + planId);
            } else {
                SeqInumQuery q = cost.queryHash.get(ss[0]);
                if (q == null)
                    throw new Error("query not found");
                SeqInumPlan plan = q.plans[Integer.parseInt(ss[1])];
                int slotId = Integer.parseInt(ss[2]);
                if (plan.slots[slotId] != null)
                    throw new Error("slot exist");
                SeqInumSlot slot = new SeqInumSlot(plan);
                slot.fullTableScanCost = Double.parseDouble(ss[3]);
                for (int i = 4; i < ss.length; i++) {
                    SeqInumSlotIndexCost queryCost = new SeqInumSlotIndexCost();
                    String[] rs = ss[i].split(",");
                    queryCost.index = cost.indexHash.get(rs[0]);
                    if (queryCost.index == null)
                        throw new Error("index " + rs[0] + " not found");
                    queryCost.cost = Double.parseDouble(rs[1]);
                    slot.costs.add(queryCost);
                }
                plan.slots[slotId] = slot;
            }
        }
        return cost;
    }
}
