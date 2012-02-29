package edu.ucsc.dbtune.seq.bip;

import java.util.Hashtable;
import java.util.Vector;

import edu.ucsc.dbtune.seq.bip.def.*;
import edu.ucsc.dbtune.seq.utils.Rt;

public class SeqInumCost {
    public Hashtable<String, SeqInumQuery> queries = new Hashtable<String, SeqInumQuery>();
    public Hashtable<String, SeqInumIndex> indices = new Hashtable<String, SeqInumIndex>();
    public Vector<SeqInumIndex> indicesV = new Vector<SeqInumIndex>();
    public SeqInumQuery[] sequence;

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
//            Rt.p(line);
            String[] ss = line.split("[ |\t]+");
            if (!readingPlan && !readingSlot) {
                if (ss[0].startsWith("I")) {
                    SeqInumIndex index = new SeqInumIndex();
                    index.name = ss[0];
                    index.createCost = Integer.parseInt(ss[1]);
                    index.dropCost = Integer.parseInt(ss[2]);
                    index.storageCost = Integer.parseInt(ss[3]);
                    if (cost.indices.put(index.name, index) != null)
                        throw new Error("duplicate index");
                    cost.indicesV.add(index);
                } else if (ss[0].startsWith("Q") || ss[0].startsWith("U")) {
                    SeqInumQuery q = new SeqInumQuery(queryId++);
                    q.name = ss[0];
                    q.plans = new SeqInumPlan[Integer.parseInt(ss[1])];
                    String[] rs = ss[2].split(",");
                    q.relevantIndices = new SeqInumIndex[rs.length];
                    for (int i = 0; i < rs.length; i++) {
                        SeqInumIndex index = cost.indices.get(rs[i]);
                        if (index == null)
                            throw new Error("index not found");
                        q.relevantIndices[i] = index;
                    }
                    cost.queries.put(q.name, q);
                } else if ("SEQ".equals(ss[0])) {
                    String[] rs = ss[1].split(",");
                    cost.sequence = new SeqInumQuery[rs.length];
                    for (int i = 0; i < rs.length; i++) {
                        SeqInumQuery q = cost.queries.get(rs[i]);
                        if (q == null)
                            throw new Error("query not found");
                        cost.sequence[i] = q;
                    }
                } else if ("PLAN".equals(ss[0])) {
                    readingPlan = true;
                }
            } else if (!readingSlot) {
                if ("SLOT".equals(ss[0])) {
                    readingSlot = true;
                    continue;
                }
                SeqInumQuery q = cost.queries.get(ss[0]);
                if (q == null)
                    throw new Error("query not found");
                int planId = Integer.parseInt(ss[1]);
                if (q.plans[planId] != null)
                    throw new Error("plan exist " + q.name + " " + planId);
                SeqInumPlan plan = new SeqInumPlan(q, planId);
                plan.baseCost = Double.parseDouble(ss[2]);
                plan.slots = new SeqInumSlot[Integer.parseInt(ss[3])];
                q.plans[planId] = plan;
//                Rt.p(q.name + " " + planId);
            } else {
                SeqInumQuery q = cost.queries.get(ss[0]);
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
                    queryCost.index = cost.indices.get(rs[0]);
                    if (queryCost.index == null)
                        throw new Error("index " + rs[0] + " not found");
                    queryCost.cost = Double.parseDouble(rs[1]);
                    slot.costs.add(queryCost);
                }
                plan.slots[slotId]=slot;
            }
        }
        return cost;
    }
}
