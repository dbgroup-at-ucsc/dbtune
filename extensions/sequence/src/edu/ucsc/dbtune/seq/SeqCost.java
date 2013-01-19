package edu.ucsc.dbtune.seq;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.deployAware.DATQuery;
import edu.ucsc.dbtune.inum.DerbyInterestingOrdersExtractor;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.seq.bip.def.SeqInumQuery;
import edu.ucsc.dbtune.seq.def.*;
import edu.ucsc.dbtune.seq.utils.RTimer;
import edu.ucsc.dbtune.seq.utils.RTimerN;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

public class SeqCost {
    public Hashtable<String, SeqQuerySet> queries = new Hashtable<String, SeqQuerySet>();
    public Hashtable<String, SeqIndex> indices = new Hashtable<String, SeqIndex>();
    public Vector<SeqIndex> indicesV = new Vector<SeqIndex>();
    public SeqQuerySet[] sequence;
    public SeqIndex[] source;
    public SeqIndex[] destination;
    public double storageConstraint = Double.MAX_VALUE;
    public double maxTransitionCost = Double.MAX_VALUE;
    public int maxIndexesWindow = Integer.MAX_VALUE;
    public double[] stepBoost;
    public boolean addTransitionCostToObjective = true;
    public boolean useDB2Optimizer = false;
    public DatabaseSystem db;
    public Optimizer optimizer;
    public int whatIfCount = 0;

    private SeqCost() {
    }

    public SeqCost dupQuery(int times) {
        SeqCost c2 = new SeqCost();
        Vector<SeqQuerySet> queries = new Vector<SeqQuerySet>();
        for (int i = 0; i < times; i++) {
            for (SeqQuerySet q : sequence) {
                queries.add(q);
            }
        }
        c2.indices = new Hashtable<String, SeqIndex>(indices);
        c2.indicesV = new Vector<SeqIndex>(indicesV);
        c2.source = source.clone();
        c2.destination = destination.clone();
        c2.storageConstraint = storageConstraint;
        c2.sequence = queries.toArray(new SeqQuerySet[queries.size()]);
        for (int i = 0; i < c2.sequence.length; i++) {
            c2.queries.put("Q" + i, c2.sequence[i]);
        }
        return c2;
    }

    public SeqCost copy(int size) {
        SeqCost c2 = new SeqCost();
        Vector<SeqQuerySet> queries = new Vector<SeqQuerySet>();
        for (int i = 0; i < size; i++) {
            queries.add(sequence[i]);
        }
        c2.indices = new Hashtable<String, SeqIndex>(indices);
        c2.indicesV = new Vector<SeqIndex>(indicesV);
        c2.source = source.clone();
        c2.destination = destination.clone();
        c2.storageConstraint = storageConstraint;
        c2.sequence = queries.toArray(new SeqQuerySet[queries.size()]);
        for (int i = 0; i < c2.sequence.length; i++) {
            c2.queries.put("Q" + i, c2.sequence[i]);
        }
        return c2;
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
                    SeqQuerySet set = new SeqQuerySet();
                    set.name = ss[0];
                    set.queries = new SeqQuery[] { q };
                    cost.queries.put(set.name, set);
                } else if ("SEQ".equals(ss[0])) {
                    String[] rs = ss[1].split(",");
                    cost.sequence = new SeqQuerySet[rs.length];
                    for (int i = 0; i < rs.length; i++) {
                        SeqQuerySet q = cost.queries.get(rs[i]);
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
                SeqQuerySet q = cost.queries.get(ss[0]);
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
                q.queries[0].costsWithIndices.add(queryCost);
            }
        }
        return cost;
    }

    public static double getCreateIndexCost(Optimizer optimizer, Index a, SeqInumIndex q) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("select ");
        boolean first = true;
        for (Column col : a) {
            if (first)
                first = false;
            else
                sql.append(",");
            sql.append(col.getName());
        }
        sql.append(" from " + a.getSchema().getName() + "." + a.getTable().getName() + " order by");
        first = true;
        for (Column col : a) {
            if (first)
                first = false;
            else
                sql.append(",");
            sql.append(" " + col.getName() + (a.isAscending(col) ? " asc" : " desc"));
        }
        RTimerN timer = new RTimerN();
        if (q != null)
            q.createCostSQL = sql.toString();
        // Rt.p(sql);
        SQLStatementPlan sqlPlan = optimizer.explain(sql.toString()).getPlan();
        totalCreateIndexNanoTime += timer.get();
        if (q != null && optimizer instanceof InumOptimizer) {
            DB2Optimizer o2 = (DB2Optimizer) optimizer.getDelegate();
            q.createCostDB2 = o2.explain(sql.toString()).getTotalCost();
        }
        // Rt.p("%,d",totalCreateIndexNanoTime);
        return sqlPlan.getRootOperator().getAccumulatedCost();
    }

    public static long getIndexSize(Statement st, Index a) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("select sum(");
        boolean first = true;
        for (Column col : a) {
            if (first)
                first = false;
            else
                sql.append("+");
            sql.append("length(" + col.getName() + ")");
        }
        sql.append(") from " + a.getSchema().getName() + "." + a.getTable().getName());
        RTimerN timer = new RTimerN();
        Rt.p(sql);
        st.execute(sql.toString());
        totalCreateIndexNanoTime += timer.get();
        ResultSet rs = st.getResultSet();
        if (rs.next())
            return rs.getLong(1);
        throw new Error();
    }

    public static double populateTime = 0;
    public static long totalWhatIfNanoTime = 0;
    public static long totalCreateIndexNanoTime = 0;
    public static long plugInTime = 0;
    public static int plugInCount = 0;

    public static SeqCost fromOptimizer(DatabaseSystem db, Optimizer optimizer, Workload workload, Index[] indices)
            throws SQLException {
        SeqCost costModel = new SeqCost();
        costModel.optimizer = optimizer;

        int indexId = 0;
        for (Index a : indices) {
            SeqIndex index = new SeqIndex();
            index.name = "I" + indexId;
            index.index = a;
            index.createCost = getCreateIndexCost(optimizer, a, null);
            index.dropCost = 0;
            Statement st = db.getConnection().createStatement();
            index.storageCost = getIndexSize(st, a);
            st.close();
            if (costModel.indices.put(index.name, index) != null)
                throw new Error("duplicate index");
            costModel.indicesV.add(index);
            indexId++;
        }

        costModel.sequence = new SeqQuerySet[workload.size()];
        for (int queryId = 0; queryId < workload.size(); queryId++) {
            SeqQuery q = new SeqQuery(queryId);
            q.name = "Q" + queryId;
            q.sql = workload.get(queryId);
            // Rt.np(sql);
            RTimerN timer = new RTimerN();
            if (optimizer instanceof edu.ucsc.dbtune.optimizer.InumOptimizer) {
                q.inum = (InumPreparedSQLStatement) optimizer.prepareExplain(q.sql);
                populateTime += timer.getSecondElapse();
                Rt.p("GREEDY INUM populate time: " + timer.getSecondElapse());
                totalWhatIfNanoTime += timer.get();
                timer.reset();
                q.costWithoutIndex = q.inum.explain(new HashSet<Index>()).getTotalCost();
            } else {
                SQLStatementPlan sqlPlan = optimizer.explain(q.sql).getPlan();
                q.costWithoutIndex = sqlPlan.getRootOperator().getAccumulatedCost();
                if (true)
                    throw new Error();
            }
            Rt.p("GREEDY INUM plugin time: " + timer.getSecondElapse());
            totalWhatIfNanoTime += timer.get();
            plugInTime += timer.get();

            // DerbyInterestingOrdersExtractor interestingOrdersExtractor = new
            // DerbyInterestingOrdersExtractor(
            // db.getCatalog(), true);
            // List<Set<Index>> indexesPerTable = interestingOrdersExtractor
            // .extract(q.sql);
            // HashSet<SeqIndex> allIndex = new HashSet<SeqIndex>();
            // for (Set<Index> set : indexesPerTable) {
            // for (Index index : set) {
            // SeqIndex seqIndex = costModel.indices.get(index.toString());
            // if (seqIndex != null)
            // allIndex.add(seqIndex);
            // }
            // }

            // q.relevantIndices = allIndex.toArray(new
            // SeqIndex[allIndex.size()]);
            // for (int i = 0; i < rs.length; i++) {
            // SeqIndex index = costModel.indices.get(rs[i]);
            // if (index == null)
            // throw new Error("index not found");
            // q.relevantIndices[i] = index;
            // }
            SeqQuerySet set = new SeqQuerySet();
            set.name = q.name;
            set.queries = new SeqQuery[] { q };
            costModel.queries.put(set.name, set);
            costModel.sequence[queryId] = set;
        }

        costModel.source = new SeqIndex[0];
        costModel.destination = new SeqIndex[0];
        return costModel;
    }

    public static SeqCost fromInum(SeqInumCost cost) throws SQLException {
        SeqCost costModel = new SeqCost();
        costModel.addTransitionCostToObjective = cost.addTransitionCostToObjective;
        int indexId = 0;
        for (SeqInumIndex a : cost.indices) {
            SeqIndex index = new SeqIndex();
            index.name = "I" + indexId;
            // index.index = a.loadIndex(db);
            index.createCost = a.createCost;
            index.dropCost = 0;
            index.storageCost = a.storageCost;
            index.inumIndex = a;
            if (costModel.indices.put(index.name, index) != null)
                throw new Error("duplicate index");
            costModel.indicesV.add(index);
            indexId++;
        }

        costModel.sequence = new SeqQuerySet[cost.queries.size()];
        for (int queryId = 0; queryId < cost.queries.size(); queryId++) {
            SeqQuery q = new SeqQuery(queryId);
            q.name = "Q" + queryId;
            q.inumCached = cost.queries.get(queryId);
            q.sql = q.inumCached.sql;
            q.weight = q.inumCached.weight;
            q.costWithoutIndex = q.inumCached.cost(new SeqInumIndex[0]);
            SeqQuerySet set = new SeqQuerySet();
            set.name = q.name;
            set.queries = new SeqQuery[] { q };
            costModel.queries.put(set.name, set);
            costModel.sequence[queryId] = set;
        }

        costModel.source = new SeqIndex[0];
        costModel.destination = new SeqIndex[0];
        return costModel;
    }

    public static SeqCost multiWindows(SeqInumCost cost, int windows) throws SQLException {
        SeqCost costModel = new SeqCost();
        costModel.addTransitionCostToObjective = cost.addTransitionCostToObjective;
        int indexId = 0;
        for (SeqInumIndex a : cost.indices) {
            SeqIndex index = new SeqIndex();
            index.id = indexId;
            index.name = "I" + indexId;
            // index.index = a.loadIndex(db);
            index.createCost = a.createCost;
            index.dropCost = 0;
            index.storageCost = a.storageCost;
            index.inumIndex = a;
            if (costModel.indices.put(index.name, index) != null)
                throw new Error("duplicate index");
            costModel.indicesV.add(index);
            indexId++;
        }

        costModel.sequence = new SeqQuerySet[windows];
        for (int windowId = 0; windowId < windows; windowId++) {
            SeqQuerySet set = new SeqQuerySet();
            set.name = "W" + windowId;
            set.queries = new SeqQuery[cost.queries.size()];
            for (int queryId = 0; queryId < cost.queries.size(); queryId++) {
                SeqQuery q = new SeqQuery(queryId);
                q.name = "Q" + queryId;
                q.inumCached = cost.queries.get(queryId);
                q.sql = q.inumCached.sql;
                q.weight = q.inumCached.weight;
                q.costWithoutIndex = q.inumCached.cost(new SeqInumIndex[0]);
                set.queries[queryId] = q;
            }
            costModel.queries.put(set.name, set);
            costModel.sequence[windowId] = set;
        }

        costModel.source = new SeqIndex[0];
        costModel.destination = new SeqIndex[0];
        return costModel;
    }

    public SeqConfiguration[] getAllConfigurations(Vector<SeqIndex> indices) {
        return getAllConfigurations(indices.toArray(new SeqIndex[indices.size()]));
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
            // int numOfIndexes = 0;
            for (int j = 0; j < count; j++) {
                if ((i & (1 << j)) != 0) {
                    vs.add(indices[j]);
                    storageCost += indices[j].storageCost;
                    // numOfIndexes++;
                }
            }
            if ((storageConstraint <= 0 || storageCost <= storageConstraint)
            // && (maxIndexes <= 0 || numOfIndexes <= maxIndexes)
            ) {
                if (vs.size() == 0)
                    cs.add(empty);
                else
                    cs.add(new SeqConfiguration(vs.toArray(new SeqIndex[vs.size()])));
            }
        }
        return cs.toArray(new SeqConfiguration[cs.size()]);
    }

    public Double getCost(SeqConfiguration from, SeqConfiguration to) {
        Double d = from.transitionCostCache.get(to);
        if (d != null)
            return d;
        double cost = 0;
        int createdIndexes = 0;
        HashSet<SeqIndex> h = new HashSet<SeqIndex>();
        for (SeqIndex i : from.indices)
            h.add(i);
        for (SeqIndex i : to.indices) {
            if (!h.contains(i)) {
                cost += i.createCost;
                createdIndexes++;
            }
        }
        if (maxIndexesWindow > 0 && createdIndexes > maxIndexesWindow)
            return null;
        h.clear();
        for (SeqIndex i : to.indices)
            h.add(i);
        for (SeqIndex i : from.indices)
            if (!h.contains(i))
                cost += i.dropCost;
        from.transitionCostCache.put(to, cost);
        return cost;
    }

    public double verifyCost(int stepId, DatabaseSystem db, DB2Optimizer optimizer, SeqStepConf conf) throws Exception {
        double cost = 0;
        HashSet<Index> indexes = conf.configuration.getIndexes(db, optimizer);
        for (SeqQuery query : sequence[stepId].queries) {
            ExplainedSQLStatement explain = optimizer.explain(query.sql, indexes);
            double qcost = explain.getTotalCost();
            cost += qcost;
        }
        return cost;
    }

    public double getCost(SeqQuerySet queries, SeqConfiguration conf) throws SQLException {
        double cost = 0;
        for (SeqQuery q : queries.queries)
            cost += getCost(q, conf);
        return cost;
    }

    public double getCost(SeqQuery q, SeqConfiguration conf) throws SQLException {
        Double d = q.costCache.get(conf);
        if (d != null)
            return d * q.weight;
        whatIfCount++;
        double cost = 0;
        if (useDB2Optimizer) {
            HashSet<Index> allIndexes = new HashSet<Index>();
            for (SeqIndex i : conf.indices)
                allIndexes.add(i.inumIndex.loadIndex(db));
            ExplainedSQLStatement explain = optimizer.explain(q.sql, allIndexes);
            cost = explain.getTotalCost();
        } else if (q.inumCached != null) {
            SeqInumIndex[] indexes = new SeqInumIndex[conf.indices.length];
            for (int i = 0; i < conf.indices.length; i++)
                indexes[i] = conf.indices[i].inumIndex;
            cost = q.inumCached.cost(indexes);
        } else if (optimizer != null) {
            HashSet<Index> allIndexes = new HashSet<Index>();
            for (SeqIndex i : conf.indices)
                allIndexes.add(i.index);
            try {
                RTimerN timer = new RTimerN();
                if (q.inum != null) {
                    cost = q.inum.explain(allIndexes).getTotalCost();
                    // Rt.p("GREEDY INUM plugin time: " +
                    // timer.getSecondElapse());
                } else {
                    ExplainedSQLStatement explain = optimizer.explain(q.sql, allIndexes);
                    cost = explain.getTotalCost();
                }
                totalWhatIfNanoTime += timer.get();
                plugInTime += timer.get();
                plugInCount++;
            } catch (Exception e) {
                Rt.p(q.sql.getSQL());
                for (Index index : allIndexes) {
                    Rt.p(index);
                }
                e.printStackTrace();
                RTimerN timer = new RTimerN();
                ExplainedSQLStatement explain = optimizer.explain(q.sql, allIndexes);
                totalWhatIfNanoTime += timer.get();
                cost = explain.getTotalCost();
            }
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
        return cost * q.weight;
    }
}
