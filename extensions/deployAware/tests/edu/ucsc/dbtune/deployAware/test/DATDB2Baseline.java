package edu.ucsc.dbtune.deployAware.test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;

import java.io.File;
import java.io.PrintStream;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.InumTest2;
import edu.ucsc.dbtune.advisor.db2.DB2Advisor;
import edu.ucsc.dbtune.deployAware.DAT;
import edu.ucsc.dbtune.deployAware.DATOutput;
import edu.ucsc.dbtune.deployAware.DATParameter;
import edu.ucsc.dbtune.deployAware.test.DATPaper.TestSet;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.WorkloadLoader;
import edu.ucsc.dbtune.seq.bip.WorkloadLoaderSettings;
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
    GnuPlot2 plot;

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

    static Hashtable<String, DatabaseSystem> dbs = new Hashtable<String, DatabaseSystem>();

    public static DatabaseSystem getDb(String dbName) throws Exception {
        DatabaseSystem db = dbs.get(dbName);
        if (db == null) {
            Environment en = Environment.getInstance();
            en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/" + dbName);
            en.setProperty("username", "db2inst1");
            en.setProperty("password", "db2inst1admin");
            en.setProperty("workloads.dir", "resources/workloads/db2");
            db = newDatabaseSystem(en);
            dbs.put(dbName, db);
        }
        return db;
    }

    void verify(DatabaseSystem db, Workload workload, HashSet<Index>[] windows,
            File output2) throws Exception {
        InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
        DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();
        Rx rx = new Rx("testResult");
        for (int i = 0; i < windows.length; i++) {
            Rx win = rx.createChild("window");
            win.setAttribute("id", i);
            win.setAttribute("indexCount", windows[i].size());
            for (Index index : windows[i]) {
                win.createChild("index", index.toString());
            }
        }
        double[] total = new double[windows.length + 1];
        for (int i = 0; i < workload.size(); i++) {
            Rx a = rx.createChild("query");
            a.setAttribute("id", i);
            for (int w = 0; w < windows.length; w++) {
                ExplainedSQLStatement db2plan = db2optimizer.explain(workload
                        .get(i), windows[w]);
                double db2index = db2plan.getTotalCost();
                total[w] += db2index;
                Rx b = a.createChild("window", db2index);
                b.setAttribute("id", w);
            }
            ExplainedSQLStatement db2plan = db2optimizer.explain(workload
                    .get(i));
            double db2index = db2plan.getTotalCost();
            a.createChild("fts", db2index);
            total[total.length - 1] += db2index;
        }

        Rx a = rx.createChild("total");
        for (int w = 0; w < windows.length; w++) {
            Rx b = a.createChild("window", total[w]);
            b.setAttribute("id", w);
            Rt.np("verify window %d:\t%d\t%,.0f", w, windows[w].size(),
                    total[w]);
        }
        Rt.np("fts %,.0f", total[total.length - 1]);
        System.exit(0);
        Rt.write(output2, rx.getXml().getBytes());
    }

    double[] loadTotalCosts(File file) throws Exception {
        Rx root = Rx.findRoot(Rt.readFile(file));
        Rx total = root.findChild("total");
        Rx[] win = total.findChilds("window");
        double[] ds = new double[win.length];
        for (int i = 0; i < ds.length; i++) {
            ds[i] = Double.parseDouble(win[i].getText());
        }
        return ds;
    }

    void runDat(String dbName, File file, long loaderSpaceBudget,
            long spaceBudget, int m, double windowSize, double alpha,
            double beta, File output, Set<Index> indexes) throws Exception {
        HashSet<Index>[] windows = new HashSet[m];
        for (int i = 0; i < windows.length; i++) {
            windows[i] = new HashSet<Index>();
        }
        DatabaseSystem db = getDb(dbName);
        WorkloadLoader loader = new WorkloadLoader(dbName, file.getParentFile()
                .getName(), file.getName(), "recommend");
        if (loaderSpaceBudget > 0)
            loader.spaceMB = (int) (loaderSpaceBudget / 1024 / 1024);
        loader.overrideIndexes = indexes;
        SeqInumCost cost = loader.loadCost();
        Rt.p("total index: " + cost.indexCount());
        cost.storageConstraint = spaceBudget * 1000;
        double[] windowConstraints = new double[m];
        for (int i = 0; i < windowConstraints.length; i++)
            windowConstraints[i] = windowSize * 100;
        Rt.np("windowSize: " + windowSize);
        int l = 1000;
        DATParameter params = new DATParameter(cost, windowConstraints, alpha,
                beta, l);
        DAT dat = new DAT();
        // DAT.showFormulas=true;
        DATOutput datOutput = dat.runDAT(params);
        for (int i = 0; i < windows.length; i++) {
            windows[i].clear();
            // if (i < 2)
            // continue;
            // else {
            // for (int j = 0; j < cost.indexCount(); j++) {
            // windows[i].add(cost.indices.get(j).loadIndex(db));
            // }
            // }
            double size = 0;
            for (int j = 0; j < datOutput.ws[i].indexUsed.length; j++) {
                if (datOutput.ws[i].indexUsed[j]) {
                    System.out.print(j + ",");
                    windows[i].add(cost.indices.get(j).loadIndex(db));
                    if (i == 0 || !datOutput.ws[i - 1].indexUsed[j])
                        size += cost.indices.get(j).createCost;
                }
            }
            System.out.println();
            Rt.np("window %d: %d %,.0f", i, windows[i].size(), size);
        }
        for (int j = 0; j < datOutput.ws[m - 1].indexUsed.length; j++) {
            if (!datOutput.ws[m - 1].indexUsed[j]) {
                Rt.np("%d create=%,.0f storage=%,.0f", j,
                        cost.indices.get(j).createCost,
                        cost.indices.get(j).storageCost);
            }
        }
        Rt.np("windowSize: %,.0f", windowSize);
        Rt.p("spaceBudget: %,d", spaceBudget);
        long size = 0;
        for (int i = 0; i < cost.indexCount(); i++) {
            size += cost.indices.get(i).storageCost;
        }
        Rt.p("index size: %,d", size);
        // System.exit(0);
        String tpch = Rt.readFile(file);
        Workload workload = new Workload("", new StringReader(tpch));
        verify(db, workload, windows, output);
    }

    public DATDB2Baseline(double alpha, String dbName, File file,
            long spaceBudget, String spaceName, String outputPrefix)
            throws Exception {
        File output = new File(outputPrefix + ".xml");
        File outputDb2Result = new File(outputPrefix + "result.xml");
        double beta = 1 - alpha;
        File datResultDir = new File(new File(outputPrefix).getParentFile(),
                alpha + "");
        String outputNamePrefix = new File(outputPrefix).getName();
        File outputDatResult = new File(datResultDir, outputNamePrefix
                + "dat.xml");
        File outputDatResultS = new File(datResultDir, outputNamePrefix
                + "datSpace.xml");
        output.getParentFile().mkdirs();
        outputDatResult.getParentFile().mkdirs();
        if (!output.exists()) {
            DatabaseSystem db = getDb(dbName);
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
        int m = 3;
        HashSet<Index>[] windows = new HashSet[m];
        for (int i = 0; i < windows.length; i++) {
            windows[i] = new HashSet<Index>();
        }
        Rt.np(file.getName() + " " + spaceBudget);
        String tpch = Rt.readFile(file);
        Workload workload = new Workload("", new StringReader(tpch));
        if (!outputDb2Result.exists()) {
            DatabaseSystem db = getDb(dbName);
            for (int i = 0; i < p1; i++)
                windows[0].add(InumTest2.createIndex(db, indexes.get(i).name));
            for (int i = 0; i < p2; i++)
                windows[1].add(InumTest2.createIndex(db, indexes.get(i).name));
            for (int i = 0; i < indexes.size(); i++)
                windows[2].add(InumTest2.createIndex(db, indexes.get(i).name));

            verify(db, workload, windows, outputDb2Result);
        }
        if (!outputDatResult.exists()) {
            runDat(dbName, file, 0, spaceBudget, m, windowSize, alpha, beta,
                    outputDatResult, null);
        }
        if (true || !outputDatResultS.exists()) {
            HashSet<Index> hash = new HashSet<Index>();
            for (I i : indexes) {
                Index index = InumTest2.createIndex(getDb(dbName), i.name);
                index.benefit = i.benefit;
                index.setCreationCost(i.createCost);
                index.setBytes(i.bytes);
                hash.add(index);
            }
            runDat(dbName, file, spaceBudget, spaceBudget, m, windowSize,
                    alpha, beta, outputDatResultS, hash);
        }
        double[] db2 = loadTotalCosts(outputDb2Result);
        double[] dat = loadTotalCosts(outputDatResult);
        double[] datSpace = loadTotalCosts(outputDatResultS);
        plot = new GnuPlot2(new File(WorkloadLoaderSettings.dataRoot
                + "/paper/db2/figs"), dbName + "X" + spaceName, "cost",
                "window");
        plot.setPlotNames(new String[] { "DB2Advisor", "datOptimalIndexes",
                "datContraintIndexes" });
        plot.setXtics(new int[] { 1, 2, 3 });
        for (int i = 0; i < db2.length; i++) {
            plot.add(i + 1, db2[i]);
            plot.add(i + 1, dat[i]);
            plot.add(i + 1, datSpace[i]);
            plot.addLine();
        }
        plot.finish();
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
        double alpha = 0.01;
        TestSet[] sets = {
                new TestSet("16 TPC-H queries", "tpch10g", "deployAware",
                        "TPCH16.sql", 10 * gigbytes, "TPCH16", 9 * 3600 * 3000),
                new TestSet("63 TPCDS queries", "test", "deployAware",
                        "TPCDS63.sql", 10 * gigbytes, "TPCDS63", 0), };
        DATPaperParams params = new DATPaperParams();
        File texFile = new File(WorkloadLoaderSettings.dataRoot + "/paper/db2/"
                + alpha + ".tex");
        PrintStream ps = new PrintStream(texFile);
        ps.println("\\documentclass{vldb}\n" + "\n"
                + "\\usepackage{graphicx}   % need for figures\n" + "\n"
                + "\\usepackage{subfigure}\n" + "\n" + "\\begin{document}\n"
                + "" + "");
        params.scale = "0.6";
//        for (TestSet set : sets) {
//            for (int i = 3; i < params.spaceFactor_set.length; i++) {
//                double spaceFactor = params.spaceFactor_set[i];
//                String spaceName = params.spaceFactor_names[i];
//                // spaceFactor =
//                // params.spaceFactor_set[params.spaceFactor_set.length - 1];
//                long space = (long) (set.size * spaceFactor);
//                File file = new File("resources/workloads/db2/"
//                        + set.workloadName + "/" + set.fileName);
//                String outputPrefix = WorkloadLoaderSettings.dataRoot+"/paper/cache/db2/"
//                        + set.workloadName + "/" + set.fileName + spaceFactor;
//                DATDB2Baseline db2 = new DATDB2Baseline(alpha, set.dbName,
//                        file, space, spaceName, outputPrefix);
//                ps.println("\\begin{figure}\n" + "\\centering\n"
//                        + "\\includegraphics[scale=" + params.scale + "]{figs/"
//                        + db2.plot.name + ".eps}\n" + "\\caption{" + set.name
//                        + " space " + spaceName + "}\n"
//                        // + "\\label{" + plot.name + "}\n"
//                        + "\\end{figure}\n" + "");
//            }
//        }
        ps.println("\\end{document}\n");
        ps.close();
        Rt.runAndShowCommand(params.pdflatex + " -interaction=nonstopmode "
                + texFile.getName(), texFile.getParentFile());
    }
}
