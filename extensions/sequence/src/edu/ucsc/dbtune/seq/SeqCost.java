package edu.ucsc.dbtune.seq;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.inum.DerbyInterestingOrdersExtractor;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.seq.def.*;
import edu.ucsc.dbtune.seq.utils.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

public class SeqCost {
    public Hashtable<String, SeqQuery> queries = new Hashtable<String, SeqQuery>();
    public Hashtable<String, SeqIndex> indices = new Hashtable<String, SeqIndex>();
    public Vector<SeqIndex> indicesV = new Vector<SeqIndex>();
    public SeqQuery[] sequence;
    public SeqIndex[] source;
    public SeqIndex[] destination;
    public int storageConstraint;

    private SeqCost() {
    }

    public static SeqCost fromFile(String table) {
        SeqCost cost = new SeqCost();
        boolean readingCost = false;
        int queryId = 0;
        for (String line : table.split("\r?\n")) {
            line = line.trim();
            if (line.length() == 0)
                continue;
            if (line.startsWith("#"))
                continue;
            String[] ss = line.split("[ |\t]+");
            if (!readingCost) {
                if (ss[0].startsWith("I")) {
                    SeqIndex index = new SeqIndex();
                    index.name = ss[0];
                    index.createCost = Integer.parseInt(ss[1]);
                    index.dropCost = Integer.parseInt(ss[2]);
                    index.storageCost = Integer.parseInt(ss[3]);
                    if (cost.indices.put(index.name, index) != null)
                        throw new Error("duplicate index");
                    cost.indicesV.add(index);
                } else if (ss[0].startsWith("Q") || ss[0].startsWith("U")) {
                    SeqQuery q = new SeqQuery(queryId++);
                    q.name = ss[0];
                    q.costWithoutIndex = Integer.parseInt(ss[1]);
                    String[] rs = ss[2].split(",");
                    q.relevantIndices = new SeqIndex[rs.length];
                    for (int i = 0; i < rs.length; i++) {
                        SeqIndex index = cost.indices.get(rs[i]);
                        if (index == null)
                            throw new Error("index not found");
                        q.relevantIndices[i] = index;
                    }
                    cost.queries.put(q.name, q);
                } else if ("SEQ".equals(ss[0])) {
                    String[] rs = ss[1].split(",");
                    cost.sequence = new SeqQuery[rs.length];
                    for (int i = 0; i < rs.length; i++) {
                        SeqQuery q = cost.queries.get(rs[i]);
                        if (q == null)
                            throw new Error("query not found");
                        cost.sequence[i] = q;
                    }
                } else if ("SOURCE".equals(ss[0])) {
                    if (ss.length < 2) {
                        cost.source = new SeqIndex[0];
                    } else {
                        String[] rs = ss[1].split(",");
                        cost.source = new SeqIndex[rs.length];
                        for (int i = 0; i < rs.length; i++) {
                            SeqIndex index = cost.indices.get(rs[i]);
                            if (index == null)
                                throw new Error("index not found");
                            cost.source[i] = index;
                        }
                    }
                } else if ("DESTINATION".equals(ss[0])) {
                    if (ss.length < 2) {
                        cost.destination = new SeqIndex[0];
                    } else {
                        String[] rs = ss[1].split(",");
                        cost.destination = new SeqIndex[rs.length];
                        for (int i = 0; i < rs.length; i++) {
                            SeqIndex index = cost.indices.get(rs[i]);
                            if (index == null)
                                throw new Error("index not found");
                            cost.destination[i] = index;
                        }
                    }
                } else if ("STORAGE-CONSTRIANT".equals(ss[0])) {
                    cost.storageConstraint = Integer.parseInt(ss[1]);
                } else if ("COST".equals(ss[0])) {
                    readingCost = true;
                }
            } else {
                SeqQuery q = cost.queries.get(ss[0]);
                if (q == null)
                    throw new Error("query not found");
                SeqQueryCostWithIndex queryCost = new SeqQueryCostWithIndex();
                String[] rs = ss[1].split(",");
                queryCost.indices = new SeqIndex[rs.length];
                for (int i = 0; i < rs.length; i++) {
                    SeqIndex index = cost.indices.get(rs[i]);
                    if (index == null)
                        throw new Error("index " + rs[i] + " not found");
                    queryCost.indices[i] = index;
                }
                queryCost.cost = Integer.parseInt(ss[2]);
                q.costsWithIndices.add(queryCost);
            }
        }
        return cost;
    }

    Optimizer optimizer;

    public static double getCreateIndexCost(Optimizer optimizer, Index a)
            throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("select * from " + a.getTable().getName() + " order by");
        for (Column col : a) {
            sql.append(" " + col.getName()
                    + (a.isAscending(col) ? " asc" : "desc"));
            break;
        }
        SQLStatementPlan sqlPlan = optimizer.explain(sql.toString()).getPlan();
        return sqlPlan.getRootOperator().getAccumulatedCost();
    }

    public static SeqCost fromOptimizer(DatabaseSystem db, Optimizer optimizer,
            Workload workload, Index[] indices) throws SQLException {
        SeqCost costModel = new SeqCost();
        costModel.optimizer = optimizer;

        int indexId = 0;
        for (Index a : indices) {
            SeqIndex index = new SeqIndex();
            index.name = "I" + indexId;
            index.index = a;
            index.createCost = getCreateIndexCost(optimizer, a);
            index.dropCost = 0;
            index.storageCost = 0;
            if (costModel.indices.put(index.name, index) != null)
                throw new Error("duplicate index");
            costModel.indicesV.add(index);
            indexId++;
        }

        costModel.sequence = new SeqQuery[workload.size()];
        for (int queryId = 0; queryId < workload.size(); queryId++) {
            SeqQuery q = new SeqQuery(queryId);
            q.name = "Q" + queryId;
            q.sql = workload.get(queryId);
            // Rt.np(sql);
            SQLStatementPlan sqlPlan = optimizer.explain(q.sql).getPlan();
            q.costWithoutIndex = sqlPlan.getRootOperator().getAccumulatedCost();

            DerbyInterestingOrdersExtractor interestingOrdersExtractor = new DerbyInterestingOrdersExtractor(
                    db.getCatalog(), true);
            List<Set<Index>> indexesPerTable = interestingOrdersExtractor
                    .extract(q.sql);
            HashSet<SeqIndex> allIndex = new HashSet<SeqIndex>();
            for (Set<Index> set : indexesPerTable) {
                for (Index index : set) {
                    SeqIndex seqIndex = costModel.indices.get(index.toString());
                    if (seqIndex != null)
                        allIndex.add(seqIndex);
                }
            }

            q.relevantIndices = allIndex.toArray(new SeqIndex[allIndex.size()]);
            // for (int i = 0; i < rs.length; i++) {
            // SeqIndex index = costModel.indices.get(rs[i]);
            // if (index == null)
            // throw new Error("index not found");
            // q.relevantIndices[i] = index;
            // }
            costModel.queries.put(q.name, q);
            costModel.sequence[queryId] = q;
        }

        costModel.source = new SeqIndex[0];
        costModel.destination = new SeqIndex[0];
        return costModel;
    }

    public SeqConfiguration[] getAllConfigurations(Vector<SeqIndex> indices) {
        return getAllConfigurations(indices
                .toArray(new SeqIndex[indices.size()]));
    }

    private SeqConfiguration empty = new SeqConfiguration(new SeqIndex[0]);

    public SeqConfiguration[] getAllConfigurations(SeqIndex[] indices) {
        int count = indices.length;
        if (count > 31)
            throw new Error("overflow");
        int n = 1;
        for (int i = 0; i < count; i++)
            n *= 2;
        Vector<SeqConfiguration> cs = new Vector<SeqConfiguration>();
        Vector<SeqIndex> vs = new Vector<SeqIndex>();
        for (int i = 0; i < n; i++) {
            vs.clear();
            int storageCost = 0;
            for (int j = 0; j < count; j++) {
                if ((i & (1 << j)) != 0) {
                    vs.add(indices[j]);
                    storageCost += indices[j].storageCost;
                }
            }
            if (storageConstraint <= 0 || storageCost <= storageConstraint) {
                if (vs.size() == 0)
                    cs.add(empty);
                else
                    cs.add(new SeqConfiguration(vs.toArray(new SeqIndex[vs
                            .size()])));
            }
        }
        return cs.toArray(new SeqConfiguration[cs.size()]);
    }

    public double getCost(SeqConfiguration from, SeqConfiguration to) {
        Double d = from.transitionCostCache.get(to);
        if (d != null)
            return d;
        double cost = 0;
        HashSet<SeqIndex> h = new HashSet<SeqIndex>();
        for (SeqIndex i : from.indices)
            h.add(i);
        for (SeqIndex i : to.indices)
            if (!h.contains(i))
                cost += i.createCost;
        h.clear();
        for (SeqIndex i : to.indices)
            h.add(i);
        for (SeqIndex i : from.indices)
            if (!h.contains(i))
                cost += i.dropCost;
        from.transitionCostCache.put(to, cost);
        return cost;
    }

    public double getCost(SeqQuery q, SeqConfiguration conf)
            throws SQLException {
        Double d = q.costCache.get(conf);
        if (d != null)
            return d;
        double cost = 0;
        if (optimizer != null) {
            HashSet<Index> allIndexes = new HashSet<Index>();
            for (SeqIndex i : conf.indices)
                allIndexes.add(i.index);
            ExplainedSQLStatement explain = optimizer
                    .explain(q.sql, allIndexes);
            cost = explain.getTotalCost();
            // Rt.p(allIndexes.size() + " " + cost);
        } else {
            cost = q.costWithoutIndex;
            HashSet<SeqIndex> h = new HashSet<SeqIndex>();
            for (SeqIndex i : conf.indices)
                h.add(i);
            next: for (SeqQueryCostWithIndex c : q.costsWithIndices) {
                for (SeqIndex i : c.indices)
                    if (!h.contains(i))
                        continue next;
                if (c.cost < cost)
                    cost = c.cost;
            }
        }
        q.costCache.put(conf, cost);
        return cost;
    }
}
