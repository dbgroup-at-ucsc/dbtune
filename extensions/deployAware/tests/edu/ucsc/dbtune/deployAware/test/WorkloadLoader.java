package edu.ucsc.dbtune.deployAware.test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.PowerSetOptimalCandidateGenerator;
import edu.ucsc.dbtune.deployAware.DATBaselines;
import edu.ucsc.dbtune.deployAware.DATWindow;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.seq.utils.PerfTest;
import edu.ucsc.dbtune.seq.utils.Rt;
import edu.ucsc.dbtune.seq.utils.Rx;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.Workload;

public class WorkloadLoader {
    public DatabaseSystem db;
    public Environment en;
    public int querySize = 0;
    public int indexSize = 0;
    public String dbName = "test";
    public String workloadName = "tpch-500-counts";
    public String generateIndexMethod = "recommend";

    public WorkloadLoader(String dbName, String workloadName,
            String generateIndexMethod) {
        this.dbName = dbName;
        this.workloadName = workloadName;
        this.generateIndexMethod = generateIndexMethod;
    }

    public WorkloadLoader(String dbName, String workloadName,
            String generateIndexMethod, int maxQuerySize, int maxIndexSize) {
        this.dbName = dbName;
        this.workloadName = workloadName;
        this.generateIndexMethod = generateIndexMethod;
        this.querySize = maxQuerySize;
        this.indexSize = maxIndexSize;
    }

    public void close() throws SQLException {
        if (db != null)
            db.getConnection().close();
    }

    public Workload getWorkload(Environment en) throws IOException,
            SQLException {
        String file = "/tpch/workload_bip_seq.sql";
        file = "/tpch-small/workload.sql";
        file = "/tpcds-small/workload.sql";
        file = "/tpch-500-counts/workload.sql";
        file = "/" + workloadName + "/workload.sql";
        Workload workload = new Workload("", new FileReader(en
                .getWorkloadsFoldername()
                + file));
        Rt.np("workload size: " + workload.size());
        StringBuilder sb = new StringBuilder();
        int count = 0;
        all: while (true) {
            for (int i = 0; i < workload.size(); i++) {
                sb.append(workload.get(i).getSQL() + ";\r\n");
                count++;
                if (querySize != 0 && count >= querySize)
                    break all;
            }
            if (querySize == 0)
                break;
        }
        return new Workload("", new StringReader(sb.toString()));
    }

    public Set<Index> getIndexes(Workload workload, DatabaseSystem db)
            throws Exception {
        // Rt.runAndShowCommand("db2 connect to test");
        // HashSet<Index> set=new HashSet<Index>();
        // for (int i = 0; i <= 9; i++) {
        // Rt.runAndShowCommand("db2 set current query optimization = "+i);
        File dir = new File("/home/wangrui/workspace/cache/index/" + dbName
                + "/" + workloadName);
        dir.mkdirs();
        File cacheFile = new File(dir, generateIndexMethod + ".xml");
        Set<Index> indexes = new HashSet<Index>();
        if (cacheFile.exists()) {
            Rx root = Rx.findRoot(Rt.readFile(cacheFile));
            for (Rx rx2 : root.findChilds("index")) {
                Rx[] columns = rx2.findChilds("column");
                Vector<Column> v = new Vector<Column>();
                HashMap<Column, Boolean> map = new HashMap<Column, Boolean>();
                for (int i = 0; i < columns.length; i++) {
                    String s = columns[i].getText();
                    String cname = s.substring(0, s.indexOf('('));
                    Column c = (Column) db.getCatalog().findByQualifiedName(
                            cname);
                    if (c == null)
                        throw new Error(cname);
                    v.add(c);
                    map.put(c, "(A)".equals(s.substring(s.indexOf('('))));
                }
                Index index = new Index(v, map);
                indexes.add(index);
            }
            Rt.np("Load index from cache: " + indexes.size());
        } else {
            if ("recommend".equals(generateIndexMethod)) {
                CandidateGenerator candGen = new OptimizerCandidateGenerator(
                        getBaseOptimizer(db.getOptimizer()));
                indexes = candGen.generate(workload);
            } else if (generateIndexMethod.startsWith("powerset")) {
                int size = Integer.parseInt(generateIndexMethod.substring(
                        "powerset".length()).trim());
                CandidateGenerator candGen = new PowerSetOptimalCandidateGenerator(
                        db.getOptimizer(), new OptimizerCandidateGenerator(
                                getBaseOptimizer(db.getOptimizer())), size);
                indexes = candGen.generate(workload);
            } else {
                throw new Error(generateIndexMethod);
            }
            Rt.np("Index size: " + indexes.size());
            Rx rx = new Rx("dataset");
            for (Index index : indexes) {
                Rx rx2 = rx.createChild("index");
                for (Column col : index) {
                    rx2.createChild("column", col
                            + (index.isAscending(col) ? "(A)" : "(D)"));
                }
            }
            Rt.write(cacheFile, rx.getXml().getBytes());
        }
        // set.addAll(indexes);
        // Rt.p(indexes.size());
        // }
        // System.exit(0);

        int size = indexSize;
        if (size > 0 && indexes.size() >= size) {
            Rt.np("Reduce index size from " + indexes.size() + " to " + size);
            Set<Index> temp = new HashSet<Index>();
            int count = 0;
            for (Index index : indexes) {
                temp.add(index);
                count++;
                if (count >= size)
                    break;
            }
            indexes = temp;
        }
        return indexes;
    }

    public Workload getWorkload() throws Exception {
        en = Environment.getInstance();
        en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/" + dbName);
        en.setProperty("username", "db2inst1");
        en.setProperty("password", "db2inst1admin");
        en.setProperty("workloads.dir", "resources/workloads/db2");
        return getWorkload(en);
    }

    public Set<Index> getIndexes() throws Exception {
        Workload workload = getWorkload();
        if (db == null)
            db = newDatabaseSystem(en);
        return getIndexes(workload, db);
    }

    public SeqInumCost loadCost() throws Exception {
        File dir = new File("/home/wangrui/workspace/cache/"
                + generateIndexMethod + "/" + dbName + "/" + workloadName);
        dir.mkdirs();
        File file = new File(dir, querySize + "_" + indexSize + ".xml");
        SeqInumCost cost = null;
        Workload workload = getWorkload();
        if (file.exists()) {
            // Rt.p("loading from cache " + file.getAbsolutePath());
            Rx rx = Rx.findRoot(Rt.readFile(file));
            cost = SeqInumCost.loadFromXml(rx, null);
        } else {
            if (db == null)
                db = newDatabaseSystem(en);

            Set<Index> indexes = getIndexes(workload, db);

            InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
            DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();

            cost = SeqInumCost.fromInum(db, optimizer, null, indexes);
            cost.complete = false;
            cost.save(file);
        }
        if (!cost.complete) {
            if (db == null) {
                db = newDatabaseSystem(en);
                InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
                DB2Optimizer db2optimizer = (DB2Optimizer) optimizer
                        .getDelegate();
                Rx rx = Rx.findRoot(Rt.readFile(file));
                cost = SeqInumCost.loadFromXml(rx, db);
                cost.optimizer = optimizer;
                cost.inumIndices = new HashSet<Index>();
                for (SeqInumIndex index : cost.indices) {
                    cost.inumIndices.add(index.index);
                    cost.indexToInumIndex.put(index.index, index);
                }
            }
            for (int i = cost.queries.size(); i < workload.size(); i++) {
                Rt.p("building inum space for query " + i);
                cost.addQuery(workload.get(i));
                cost.save(file);
            }

            for (int i = 0; i < cost.queries.size(); i++) {
                // cost.queries.get(i).sql = null;
            }
            for (int i = 0; i < cost.indices.size(); i++) {
                // cost.indices.get(i).index = null;
            }

            // DAT dat = new DAT(cost, new double[3], 1, 0, 0);
            // dat.setOptimizer(optimizer);
            PerfTest.startTimer();
            // Rt.p("get index benefit 1");
            double costWithoutIndex = DATWindow.costWithIndex(cost,
                    new boolean[cost.indexCount()]);
            cost.costWithoutIndex = costWithoutIndex;
            for (SeqInumIndex index : cost.indices) {
                Rt.p("index benefit " + index.id);
                if (index.indexBenefit < 0.01) {
                    index.indexBenefit = costWithoutIndex
                            - DATWindow.costWithIndex(cost, index.id);
                    cost.save(file);
                }
            }
            PerfTest.addTimer("calculate index benefit");
            cost.complete = true;
            cost.save(file);
        }
        if (cost.costWithoutIndex < 0.1) {
            double costWithoutIndex = DATWindow.costWithIndex(cost,
                    new boolean[cost.indexCount()]);
            cost.costWithoutIndex = costWithoutIndex;
        }
        if (cost.costWithAllIndex < 0.1) {
            boolean[] bs = new boolean[cost.indexCount()];
            Arrays.fill(bs, true);
            double costWithAllIndex = DATWindow.costWithIndex(cost, bs);
            cost.costWithAllIndex = costWithAllIndex;
        }
        return cost;
    }

}
