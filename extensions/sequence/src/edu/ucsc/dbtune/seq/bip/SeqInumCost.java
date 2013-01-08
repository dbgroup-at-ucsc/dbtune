package edu.ucsc.dbtune.seq.bip;

import static com.google.common.collect.Iterables.get;
import static edu.ucsc.dbtune.util.MetadataUtils.getIndexesPerTable;
import static edu.ucsc.dbtune.util.MetadataUtils.getIndexesReferencingTables;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.bip.core.InumQueryPlanDesc;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.optimizer.plan.TableAccessSlot;
import edu.ucsc.dbtune.seq.SeqCost;
import edu.ucsc.dbtune.seq.bip.def.*;
import edu.ucsc.dbtune.seq.utils.PerfTest;
import edu.ucsc.dbtune.seq.utils.RTimer;
import edu.ucsc.dbtune.seq.utils.RTimerN;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

public class SeqInumCost implements Serializable {
    public Hashtable<String, SeqInumQuery> queryHash = new Hashtable<String, SeqInumQuery>();
    public Hashtable<String, SeqInumIndex> indexHash = new Hashtable<String, SeqInumIndex>();
    public Vector<SeqInumIndex> indices = new Vector<SeqInumIndex>();
    public Vector<SeqInumQuery> queries = new Vector<SeqInumQuery>();
    public double storageConstraint = Double.MAX_VALUE;
    public InumOptimizer optimizer;
    public Set<Index> inumIndices;
    public Hashtable<Index, SeqInumIndex> indexToInumIndex = new Hashtable<Index, SeqInumIndex>();
    public boolean complete = true; // whether all queries have been added.
    public double costWithoutIndex;
    public double costWithAllIndex;
    public boolean addTransitionCostToObjective=false;
    public boolean eachWindowContainsOneQuery=false;

    public SeqInumCost dup(int times) {
        SeqInumCost cost = new SeqInumCost();
        cost.queryHash = this.queryHash;
        cost.indexHash = this.indexHash;
        cost.indices = this.indices;
        cost.storageConstraint = this.storageConstraint;
        cost.optimizer = this.optimizer;
        cost.inumIndices = this.inumIndices;
        cost.indexToInumIndex = this.indexToInumIndex;
        for (int i = 0; i < times; i++) {
            cost.queries.addAll(this.queries);
        }
        return cost;
    }

    public int indexCount() {
        return indices.size();
    }

    public int totalIndexAccessCosts() {
        int n = 0;
        for (SeqInumQuery p : queries)
            n += p.totalIndexes();
        return n;
    }

    public int totalSlots() {
        int n = 0;
        for (SeqInumQuery p : queries)
            n += p.totalSlots();
        return n;
    }

    public int totalPlans() {
        int n = 0;
        for (SeqInumQuery p : queries)
            n += p.plans.length;
        return n;
    }

    public void save(File file) throws Exception {
        Rx root = new Rx("workload");
        save(root);
        String xml = root.getXml();
        Rt.write(file, xml);
    }

    public void save(Rx rx) {
        rx.setAttribute("complete", complete);
        rx.setAttribute("costWithoutIndex", costWithoutIndex);
        rx.setAttribute("costWithAllIndex", costWithAllIndex);
        for (SeqInumQuery query : queries) {
            query.save(rx.createChild("query"));
        }
        for (SeqInumIndex index : indices) {
            index.save(rx.createChild("index"));
        }
    }

    public static SeqInumCost loadFromXml(Rx rx, DatabaseSystem db)
            throws SQLException {
        SeqInumCost cost = new SeqInumCost();
        int queryId = 0;
        cost.complete = !"false".equals(rx.getAttribute("complete"));
        cost.costWithoutIndex = rx.getDoubleAttribute("costWithoutIndex");
        cost.costWithAllIndex = rx.getDoubleAttribute("costWithAllIndex");
        for (Rx r : rx.findChilds("index")) {
            SeqInumIndex index = new SeqInumIndex(r, db);
            if (cost.indexHash.put(index.name, index) != null)
                throw new Error("duplicate index");
            cost.indices.add(index);
        }
        cost.queries = new Vector<SeqInumQuery>();
        for (Rx r : rx.findChilds("query")) {
            SeqInumQuery q = new SeqInumQuery(cost.indexHash, r);
            cost.queryHash.put(q.name, q);
            cost.queries.add(q);
        }
        return cost;
    }

    public SeqInumQuery addQuery(SQLStatement statement) throws SQLException {
        int id = queries.size();
        SeqInumQuery q = new SeqInumQuery(id);
        q.name = "Q" + id;
        q.sql = statement;
        queryHash.put(q.name, q);
        queries.add(q);

        RTimerN timer = new RTimerN();
        PerfTest.startTimer();
        InumQueryPlanDesc desc = (InumQueryPlanDesc) InumQueryPlanDesc
                .getQueryPlanDescInstance(statement);
        // Rt.p(id);
        // Rt.p(statement.getSQL());
        // Populate the INUM space
        desc.generateQueryPlanDesc(optimizer, inumIndices);
        PerfTest.addTimer("inum");
        PerfTest.startTimer();
        plugInTime += desc.pluginTime / 1000000000.0;
        populateTime += desc.populateTime / 1000000000.0;
        // Rt.p("BIP INUM populate time: " + timer.getSecondElapse());
        q.plans = new SeqInumPlan[desc.getNumberOfTemplatePlans()];
        q.baseTableUpdateCost = desc.getBaseTableUpdateCost();
        for (int planId = 0; planId < desc.getNumberOfTemplatePlans(); planId++) {
            SeqInumPlan plan = new SeqInumPlan(q, planId);
            plan.plan = "";
            plan.internalCost = desc.getInternalPlanCost(planId);
            plan.slots = new SeqInumSlot[desc.getNumberOfSlots(planId)];
            for (int slotId = 0; slotId < desc.getNumberOfSlots(planId); slotId++) {
                SeqInumSlot slot = new SeqInumSlot(plan, slotId);
                slot.fullTableScanCost = Double.MAX_VALUE;
                for (Index index : desc.getIndexesAtSlot(planId, slotId)) {
                    // timer = new RTimerN();
                    if (index instanceof FullTableScanIndex) {
                        slot.fullTableScanCost = desc.getAccessCost(planId,
                                slotId, index);
                        // Rt.p(index);
                        plugInFCount++;
                    } else {
                        SeqInumSlotIndexCost c = new SeqInumSlotIndexCost();
                        c.index = indexToInumIndex.get(index);
                        if (c.index == null)
                            throw new SQLException("Can't map index "
                                    + index.toString());
                        c.cost = desc.getAccessCost(planId, slotId, index);
                        c.updateCost = desc.getUpdateCost(index);
                        // Rt.p(index);
                        slot.costs.add(c);
                        // Rt
                        // .p("BIP INUM plugin time: "
                        // + timer.getSecondElapse());
                    }
                    // plugInTime += timer.getSecondElapse();
                    plugInCount++;
                    // Rt.p("%f %f",timer.getSecondElapse(),plugInTime);
                }
                Vector<SeqInumSlotIndexCost> costs = new Vector<SeqInumSlotIndexCost>();
                for (SeqInumSlotIndexCost cost : slot.costs) {
                    if (cost.cost < slot.fullTableScanCost)
                        costs.add(cost);
                }
                slot.costs = costs;
                plan.slots[slotId] = slot;
            }
            q.plans[planId] = plan;
        }
//        Rt.p(q.totalIndexes());
        PerfTest.addTimer("inumRest");
        return q;
    }

    public SeqInumQuery addQuery2(SQLStatement statement) throws SQLException {
        int id = queries.size();
        SeqInumQuery q = new SeqInumQuery(id);
        q.name = "Q" + id;
        q.sql = statement;
        queryHash.put(q.name, q);
        queries.add(q);

        RTimerN timer = new RTimerN();
        PerfTest.startTimer();
        InumPreparedSQLStatement space = (InumPreparedSQLStatement) optimizer
                .prepareExplain(statement);
        InumPlan[] plans = space.getTemplatePlans().toArray(new InumPlan[0]);

        Hashtable<Integer, Double> indexUpdateCosts = new Hashtable<Integer, Double>();
        ExplainedSQLStatement inumExplain = space.explain(inumIndices);
        for (Index index : inumIndices) {
            double cost = inumExplain.getUpdateCost(index);
            indexUpdateCosts.put(index.getId(), cost);
        }
        // InumQueryPlanDesc desc = (InumQueryPlanDesc) InumQueryPlanDesc
        // .getQueryPlanDescInstance(statement);
        // Rt.p(id);
        // Rt.p(statement.getSQL());
        // Populate the INUM space
        // desc.generateQueryPlanDesc(optimizer, inumIndices);
        PerfTest.addTimer("inum");
        PerfTest.startTimer();

        // plugInTime += desc.pluginTime / 1000000000.0;
        // populateTime += desc.populateTime / 1000000000.0;
        // Rt.p("BIP INUM populate time: " + timer.getSecondElapse());
        q.plans = new SeqInumPlan[space.getTemplatePlans().size()];
        q.baseTableUpdateCost = plans[0].getBaseTableUpdateCost();
        for (int k = 0; k < plans.length; k++) {
            SeqInumPlan plan = new SeqInumPlan(q, k);
            InumPlan iplan = plans[k];

            Set<Index> indexes = getIndexesReferencingTables(inumIndices, iplan
                    .getTables());
            for (Table table : iplan.getTables())
                indexes.add(FullTableScanIndex
                        .getFullTableScanIndexInstance(table));

            plan.plan = iplan.toString();
            plan.internalCost = iplan.getInternalCost();
            TableAccessSlot[] slots = iplan.getSlots().toArray(
                    new TableAccessSlot[0]);

            Map<Table, Set<Index>> indexesPerTable = getIndexesPerTable(indexes);

            plan.slots = new SeqInumSlot[slots.length];
            for (int i = 0; i < slots.length; i++) {
                SeqInumSlot slot = new SeqInumSlot(plan, i);
                TableAccessSlot islot = slots[i];
                Set<Index> indexesForTable = indexesPerTable.get(islot
                        .getTable());

                slot.fullTableScanCost = Double.MAX_VALUE;
                for (Index index : indexesForTable) {
                    double costForIndex = iplan.plug(islot, index);
                    if (Double.isInfinite(costForIndex))
                        continue;
                    costForIndex *= islot.coefficient;
                    // plugInTime += timer.getSecondElapse();
                    if (index instanceof FullTableScanIndex) {
                        slot.fullTableScanCost = costForIndex;
                        plugInFCount++;
                    } else {
                        SeqInumSlotIndexCost c = new SeqInumSlotIndexCost();
                        c.index = indexToInumIndex.get(index);
                        if (c.index == null)
                            throw new SQLException("Can't map index "
                                    + index.toString());
                        c.cost = costForIndex;
                        c.updateCost = indexUpdateCosts.get(index.getId());
                        slot.costs.add(c);
                    }
                    plugInCount++;
                }
                plan.slots[i] = slot;
            }
            q.plans[k] = plan;
        }
        q.timeUsedToLoad = timer.getSecondElapse();
        PerfTest.addTimer("inumRest");
        return q;
    }

//    public SeqInumIndex[] getIndexes(HashSet<Index> indexes) {
//        SeqInumIndex[] a2=new SeqInumIndex[indexes.size()];
//        int n=0;
//        for (Index index : indexes) {
//            for (SeqInumIndex a : indices) {
//                if 
//            }
//        }
//    }
    public static SeqInumCost fromInum(DatabaseSystem db,
            InumOptimizer optimizer, Workload workload, Set<Index> indexes)
            throws SQLException {
        SeqInumCost cost = new SeqInumCost();
        cost.optimizer = optimizer;
        cost.inumIndices = indexes;
        Index[] indexs = indexes.toArray(new Index[indexes.size()]);
        for (int i = 0; i < indexs.length; i++) {
            SeqInumIndex q = new SeqInumIndex(i);
            q.name = "I" + i;
            q.index = indexs[i];
            q.indexStr = q.index.toString();
            q.indexStr = q.indexStr.substring(1, q.indexStr.length() - 1);
            cost.indexToInumIndex.put(indexs[i], q);
            cost.indexHash.put(q.name, q);
            cost.indices.add(q);
            if (q.index.getCreationCost() > 0.01)
                q.createCost = q.index.getCreationCost();
            else {
                PerfTest.startTimer();
                q.createCost = SeqCost
                        .getCreateIndexCost(optimizer, q.index, q);
                PerfTest.addTimer("calculate create index cost");
            }
            q.dropCost = 0;
            // Statement st = db.getConnection().createStatement();
            q.storageCost = q.index.getBytes();// SeqCost.getIndexSize(st,
            // q.index); // TODO add
            // storage cost
            // st.close();
        }
        if (workload != null) {
            for (int i = 0; i < workload.size(); i++) {
                cost.addQuery(workload.get(i));
            }
        }
        return cost;
    }

    public static double plugInTime = 0;
    public static double populateTime = 0;
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
                SeqInumSlot slot = new SeqInumSlot(plan, slotId);
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
