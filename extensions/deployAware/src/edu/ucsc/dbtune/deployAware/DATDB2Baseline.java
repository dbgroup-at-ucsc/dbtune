package edu.ucsc.dbtune.deployAware;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;

import java.io.File;
import java.io.StringReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.InumTest2;
import edu.ucsc.dbtune.advisor.db2.DB2Advisor;
import edu.ucsc.dbtune.deployAware.test.DATPaperParams;
import edu.ucsc.dbtune.deployAware.test.DATPaper.TestSet;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.seq.utils.RRange;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;
import edu.ucsc.dbtune.workload.Workload;

public class DATDB2Baseline {
    static class I {
        public static Comparator<I> comparator = new Comparator<I>() {
            @Override
            public int compare(I o1, I o2) {
                if (o1.ratio > o2.ratio)
                    return -1;
                if (o1.ratio < o2.ratio)
                    return 1;
                return 0;
            }
        };
        String name;
        long bytes;
        double benefit;
        double createCost;
        double ratio;
    }

    Vector<I> indexes = new Vector<I>();

    public void save(File output) throws Exception {
        Rx rx = new Rx("db2advisor");
        for (I i : indexes) {
            Rx rx2 = rx.createChild("index", i.name);
            rx2.createChild("size", i.bytes);
            rx2.createChild("benefit", i.benefit);
            rx2.createChild("createCost", i.createCost);
        }
        Rt.write(output, rx.getXml().getBytes());
    }

    public void load(File output) throws Exception {
        Rx rx = Rx.findRoot(Rt.readFile(output));
        indexes.clear();
        for (Rx rx2 : rx.findChilds("index")) {
            I i = new I();
            i.name = rx2.getText().trim();
            i.name = i.name.substring(0, i.name.indexOf(']') + 1);
            i.bytes = rx2.getChildLongContent("size");
            i.benefit = rx2.getChildDoubleContent("benefit");
            i.createCost = rx2.getChildDoubleContent("createCost");
            indexes.add(i);
        }
    }

    public DATDB2Baseline(String dbName, File file, long spaceBudget,
            File output, File output2) throws Exception {
        if (!output.exists()) {
            Environment en = Environment.getInstance();
            en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/" + dbName);
            en.setProperty("username", "db2inst1");
            en.setProperty("password", "db2inst1admin");
            en.setProperty("workloads.dir", "resources/workloads/db2");
            DatabaseSystem db = newDatabaseSystem(en);
            Rt.np(file.getName() + " " + spaceBudget);
            String tpch = Rt.readFile(file);
            DB2Advisor db2Advis = new DB2Advisor(db);
            Workload workload = new Workload("", new StringReader(tpch));
            db2Advis.process(workload);
            Set<Index> indexes = db2Advis
                    .getRecommendation((int) (spaceBudget / (1024 * 1024)));
            for (Index index : indexes) {
                I a = new I();
                a.bytes = index.getBytes();
                a.benefit = index.benefit;
                a.createCost = index.getCreationCost();
                a.name = index.toString();
                this.indexes.add(a);
            }
            save(output);
        }
        load(output);
        for (I i : indexes) {
            i.ratio = i.benefit / i.createCost;
        }
        Collections.sort(indexes, I.comparator);
        RRange<int[]> bestSplit = new RRange<int[]>();
        for (int i = 1; i < indexes.size(); i++) {
            for (int j = i + 1; j < indexes.size(); j++) {
                RRange range = new RRange();
                range.add(totalCost(0, i));
                range.add(totalCost(i, j));
                range.add(totalCost(j, indexes.size()));
                bestSplit.add(range.max, new int[] { i, j });
            }
        }
        int[] bestSplit2 = bestSplit.minObject;
        int p1 = bestSplit2[0];
        int p2 = bestSplit2[1];
        double w1 = totalCost(0, p1);
        double w2 = totalCost(p1, p2);
        double w3 = totalCost(p2, indexes.size());
        double windowSize = Math.max(w1, Math.max(w2, w3));
        HashSet<Index>[] windows = new HashSet[3];
        for (int i = 0; i < windows.length; i++) {
            windows[i] = new HashSet<Index>();
        }
        if (true||!output2.exists()) {
            Environment en = Environment.getInstance();
            en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/" + dbName);
            en.setProperty("username", "db2inst1");
            en.setProperty("password", "db2inst1admin");
            en.setProperty("workloads.dir", "resources/workloads/db2");
            DatabaseSystem db = newDatabaseSystem(en);
            Rt.np(file.getName() + " " + spaceBudget);
            String tpch = Rt.readFile(file);
            Workload workload = new Workload("", new StringReader(tpch));

            for (int i = 0; i < p1; i++)
                windows[0].add(InumTest2.createIndex(db, indexes.get(i).name));
            for (int i = 0; i < p2; i++)
                windows[1].add(InumTest2.createIndex(db, indexes.get(i).name));
            for (int i = 0; i < indexes.size(); i++)
                windows[2].add(InumTest2.createIndex(db, indexes.get(i).name));

            InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
            DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();

            Rx rx = new Rx("testResult");
            double[] total = new double[windows.length];
            for (int i = 0; i < workload.size(); i++) {
                Rx a = rx.createChild("query");
                a.setAttribute("id", i);
                for (int w = 0; w < windows.length; w++) {
                    ExplainedSQLStatement db2plan = db2optimizer.explain(
                            workload.get(i), windows[w]);
                    double db2index = db2plan.getTotalCost();
                    total[w] += db2index;
                    Rx b = a.createChild("window", db2index);
                    b.setAttribute("id", w);
                }
            }
            Rx a = rx.createChild("total");
            for (int w = 0; w < windows.length; w++) {
                Rx b = a.createChild("window", total[w]);
                b.setAttribute("id", w);
            }
            Rt.write(output2, rx.getXml().getBytes());
        }
    }

    double totalCost(int from, int to) {
        double cost = 0;
        for (int i = from; i < to; i++) {
            cost += indexes.get(i).createCost;
        }
        return cost;
    }

    public static void main(String[] args) throws Exception {
        long gigbytes = 1024L * 1024L * 1024L;
        TestSet[] sets = {
                new TestSet("16 TPC-H queries", "tpch10g", "deployAware",
                        "TPCH16.sql", 10 * gigbytes, "TPCH16", 9 * 3600 * 3000),
                new TestSet("63 TPCDS queries", "test", "deployAware",
                        "TPCDS63.sql", 10 * gigbytes, "TPCDS63", 0), };
        DATPaperParams params = new DATPaperParams();
        for (TestSet set : sets) {
            for (double spaceFactor : params.spaceFactor_set) {
                long space = (long) (set.size * spaceFactor);
                File file = new File("resources/workloads/db2/"
                        + set.workloadName + "/" + set.fileName);
                File output = new File("/home/wangrui/dbtune/paper/cache/db2/"
                        + set.workloadName + "/" + set.fileName + spaceFactor
                        + ".xml");
                File output2 = new File("/home/wangrui/dbtune/paper/cache/db2/"
                        + set.workloadName + "/" + set.fileName + spaceFactor
                        + "result.xml");
                output.getParentFile().mkdirs();
                new DATDB2Baseline(set.dbName, file, space, output, output2);
            }
        }
    }
}
