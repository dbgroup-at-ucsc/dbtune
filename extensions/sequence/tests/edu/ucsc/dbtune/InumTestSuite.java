package edu.ucsc.dbtune;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.PowerSetOptimalCandidateGenerator;
import edu.ucsc.dbtune.inum.AbstractSpaceComputation;
import edu.ucsc.dbtune.inum.IBGSpaceComputation;
import edu.ucsc.dbtune.inum.InumInterestingOrder;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.ExplainTables;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;
import edu.ucsc.dbtune.workload.SQLStatement;

public class InumTestSuite {
    static class Result {
        int id;
        boolean timeout = false;
        double db2fts;
        double inumfts;
        double db2Index;
        double inumIndex;

        public Result(int id) {
            this.id = id;
        }
    }

    static class IndexSet {
        String name;
        String[] indexNames;
        Index[] index;
        Result[] results;

        public IndexSet(Workload w, String name) {
            this.name = name;
            results = new Result[w.workload.size()];
        }

        public IndexSet(Workload w, Rx rx) throws SQLException {
            this.name = rx.getAttribute("name");
            Rx[] rxs = rx.findChilds("index");
            indexNames = new String[rxs.length];
            index = new Index[rxs.length];
            // Rt.p("loading " + name);
            for (int i = 0; i < index.length; i++) {
                indexNames[i] = rxs[i].getText();
            }
            results = new Result[w.workload.size()];
            for (Rx rx2 : rx.findChilds("result")) {
                int id = rx2.getIntAttribute("id");
                if (results[id] != null)
                    throw new Error();
                results[id] = new Result(id);
                results[id].timeout = rx2.getBooleanAttribute("timeout");
                results[id].db2fts = rx2.getChildDoubleContent("db2fts");
                results[id].inumfts = rx2.getChildDoubleContent("inumfts");
                results[id].db2Index = rx2.getChildDoubleContent("db2Index");
                results[id].inumIndex = rx2.getChildDoubleContent("inumIndex");
            }
        }

        public Index getIndex(int id, DatabaseSystem db) throws SQLException {
            if (index[id] == null)
                index[id] = InumTest2.createIndex(db, indexNames[id]);
            return index[id];
        }

        public void save(Rx rx) {
            rx.setAttribute("name", name);
            if (indexNames != null) {
                for (int i = 0; i < indexNames.length; i++) {
                    rx.createChild("index", indexNames[i]);
                }
            }
            if (results != null) {
                for (int i = 0; i < results.length; i++) {
                    if (results[i] != null) {
                        Rx rx2 = rx.createChild("result");
                        rx2.setAttribute("id", results[i].id);
                        rx2.setAttribute("timeout", results[i].timeout);
                        rx2.createChild("db2fts", results[i].db2fts);
                        rx2.createChild("inumfts", results[i].inumfts);
                        rx2.createChild("db2Index", results[i].db2Index);
                        rx2.createChild("inumIndex", results[i].inumIndex);
                    }
                }
            }
        }
    }

    static class Workload {
        String dbName;
        DatabaseSystem db;
        String name;
        String file;
        edu.ucsc.dbtune.workload.Workload workload;
        IndexSet[] indexSets;

        public Workload(String dbName, String name, String file)
                throws IOException, SQLException {
            this.dbName = dbName;
            this.name = name;
            this.file = file;
            String queries = Rt.readFile(new File(file));
            workload = new edu.ucsc.dbtune.workload.Workload("",
                    new StringReader(queries));
        }

        public void load(Rx rx, Environment en) throws SQLException {
            if (!this.name.equals(rx.getAttribute("name")))
                throw new Error(this.name + " " + rx.getChildText("name"));
            // if (!this.file.equals(rx.getChildText("file")))
            // throw new Error(this.file + " " + rx.getChildText("file"));
            // if (!this.dbName.equals(rx.getChildText("dbName")))
            // throw new Error(this.dbName + " " + rx.getChildText("dbName"));
            Rx[] rxs = rx.findChilds("indexSet");
            indexSets = new IndexSet[rxs.length];
            for (int i = 0; i < rxs.length; i++) {
                indexSets[i] = new IndexSet(this, rxs[i]);
            }

        }

        DatabaseSystem getDb(Environment en) throws SQLException {
            if (db == null) {
                en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/"
                        + dbName);
                db = newDatabaseSystem(en);
            }
            return db;
        }

        public void createIndex(Environment en) throws SQLException {
            indexSets = new IndexSet[3];
            indexSets[0] = new IndexSet(this, "U-OPT");
            indexSets[1] = new IndexSet(this, "powerset");
            indexSets[2] = new IndexSet(this, "one-column-index");
        }

        public void save(Rx rx) {
            rx.setAttribute("name", name);
            rx.setAttribute("dbName", dbName);
            rx.createChild("file", file);
            if (indexSets != null) {
                for (IndexSet set : indexSets) {
                    set.save(rx.createChild("indexSet"));
                }
            }
        }
    }

    Workload[] workloads = {
            new Workload("tpch10g", "TPC-H",
                    "resources/workloads/db2/tpch/complete.sql"),
            // new Workload("test", "OTAB",
            // "resources/workloads/db2/online-benchmark-100/workload.sql"),
            new Workload("test", "TPCDS",
                    "resources/workloads/db2/tpcds/db2.sql"), };

    File resultFile;
    Environment en = Environment.getInstance();

    public InumTestSuite() throws Exception {
        en.setProperty("username", "db2inst1");
        en.setProperty("password", "db2inst1admin");
        en.setProperty("workloads.dir", "resources/workloads/db2");

        ExplainTables.optimizationLevel = 1;
        resultFile = new File(
                "extensions/sequence/tests/edu/ucsc/dbtune/inumTestSuite1.xml");
        if (!resultFile.exists())
            save();

        Rx root = Rx.findRoot(Rt.readFile(resultFile));
        Hashtable<String, Rx> hash = new Hashtable<String, Rx>();
        for (Rx rx : root.findChilds("workload")) {
            hash.put(rx.getAttribute("name"), rx);
        }
        for (int i = 0; i < workloads.length; i++) {
            Rx rx = hash.get(workloads[i].name);
            if (rx != null)
                workloads[i].load(rx, en);
        }

        for (Workload workload : workloads) {
            if (workload.indexSets.length == 0) {
                Rt.np("Generating index for " + workload.name);
                workload.createIndex(en);
            }

            for (IndexSet set : workload.indexSets) {
                if (set.index != null && set.index.length > 0)
                    continue;
                Set<Index> indexes;
                DatabaseSystem db = workload.getDb(en);
                InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
                DB2Optimizer db2optimizer = (DB2Optimizer) optimizer
                        .getDelegate();

                Rt.np("creating " + set.name);
                if ("U-OPT".equals(set.name)) {
                    CandidateGenerator candGen = new OptimizerCandidateGenerator(
                            getBaseOptimizer(db.getOptimizer()));
                    indexes = candGen.generate(workload.workload);
                } else if ("powerset".equals(set.name)) {
                    indexes = new HashSet<Index>();
                    CandidateGenerator candGen = new PowerSetOptimalCandidateGenerator(
                            db.getOptimizer(), new OptimizerCandidateGenerator(
                                    getBaseOptimizer(db.getOptimizer())), 2);
                    for (int i = 0; i < workload.workload.size(); i++) {
                        Rt.p(i);
                        Set<Index> orders = candGen.generate(workload.workload
                                .get(i));
                        for (Index order : orders) {
                            indexes.add(order);
                        }
                    }
                } else if ("one-column-index".equals(set.name)) {
                    indexes = new HashSet<Index>();
                    for (int i = 0; i < workload.workload.size(); i++) {
                        Set<InumInterestingOrder> orders = AbstractSpaceComputation
                                .extractInterestingOrderFromDB(
                                        workload.workload.get(i), db2optimizer);
                        for (InumInterestingOrder order : orders) {
                            indexes.add(order);
                        }
                    }
                } else {
                    throw new Error();
                }
                set.index = indexes.toArray(new Index[indexes.size()]);
                set.indexNames = new String[set.index.length];
                for (int i = 0; i < set.index.length; i++) {
                    set.indexNames[i] = set.index[i].toString();
                }
                save();
            }
        }

        for (Workload workload : workloads) {
            Rt.np("Workload:\t" + workload.name);
            Rt.np("query count:\t" + workload.workload.size());
            for (IndexSet set : workload.indexSets) {
                Rt.np(set.name + " index count:\t" + set.indexNames.length);
                compareWorkload(workload, set);
                double mean = 0;
                double variance = 0;
                int n = 0;
                for (int i = 0; i < workload.workload.size(); i++) {
                    Result r = set.results[i];
                    if (set.results[i].timeout)
                        Rt.np(i + " timeout");
                    else
                        Rt.np(i + "\t"
                                // + r.db2fts + "\t" + r.inumfts + "\t"
                                + r.db2Index + "\t" + r.inumIndex + "\t"
                                + (r.inumIndex / r.db2Index));
                    if (!set.results[i].timeout) {
                        mean += r.inumIndex / r.db2Index;
                        n++;
                    }
                }
                mean /= n;
                for (int i = 0; i < workload.workload.size(); i++) {
                    Result r = set.results[i];
                    if (!set.results[i].timeout) {
                        double t = r.inumIndex / r.db2Index - mean;
                        variance += t * t;
                    }
                }
                variance /= n;
                Rt.np("mean:\t" + mean);
                Rt.np("variance:\t" + variance);
            }
        }
    }

    void compareWorkload(Workload workload, IndexSet set) throws Exception {
        IBGSpaceComputation.maxTime = 15000;
        for (int i = 0; i < workload.workload.size(); i++) {
            if (set.results[i] != null)
                continue;
            Rt.p(workload.name + " " + set.name + " " + i);
            HashSet<Index> indexes = new HashSet<Index>();
            for (int j = 0; j < set.index.length; j++) {
                indexes.add(set.getIndex(j, workload.getDb(en)));
            }
            DatabaseSystem db = workload.getDb(en);
            InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
            DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();

            SQLStatement statement = workload.workload.get(i);
            ExplainedSQLStatement db2plan = db2optimizer.explain(statement,
                    indexes);
            double db2index = db2plan.getTotalCost();
            db2plan = db2optimizer.explain(statement);
            double db2fts = db2plan.getTotalCost();

            set.results[i] = new Result(i);
            try {
                InumPreparedSQLStatement space;
                space = (InumPreparedSQLStatement) optimizer
                        .prepareExplain(statement);
                ExplainedSQLStatement inumPlan = space.explain(indexes);
                double inumIndex = inumPlan.getTotalCost();
                SQLStatementPlan plan = inumPlan.getPlan();
                InumPlan inumPlan2 = (InumPlan) plan.templatePlan;
                inumPlan = space.explain(new HashSet<Index>());
                double inumFts = inumPlan.getTotalCost();
                set.results[i].inumfts = inumFts;
                set.results[i].inumIndex = inumIndex;
                set.results[i].timeout = false;
            } catch (Exception e) {
                Rt.error(e.getMessage());
                set.results[i].timeout = true;
            }
            set.results[i].db2fts = db2fts;
            set.results[i].db2Index = db2index;
            save();
        }
    }

    public void save() throws Exception {
        Rx rx = new Rx("inumTest");
        for (Workload workload : workloads) {
            workload.save(rx.createChild("workload"));
        }
        Rt.write(resultFile, rx.getXml().getBytes());
    }

    public static void main(String[] args) throws Exception {
        new InumTestSuite();
    }
}