package edu.ucsc.dbtune.deployAware.test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.deployAware.DATBaselines;
import edu.ucsc.dbtune.deployAware.DATParameter;
import edu.ucsc.dbtune.deployAware.test.DATPaper.TestSet;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.WorkloadLoader;
import edu.ucsc.dbtune.seq.bip.WorkloadLoaderSettings;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.seq.utils.RTimerN;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.Rt;

public class DATPaperOthers {
    public static void generateIndexes(TestSet[] sets) throws Exception {
        for (String method : new String[] { "recommend", "powerset 2" }) {
            for (TestSet set : sets) {
                WorkloadLoader loader = new WorkloadLoader(set.dbName, set.workloadName, set.fileName, method);
                loader.getIndexes();
            }
        }
        System.exit(0);
    }

    public static void testBipAccuracy(TestSet[] sets, String generateIndexMethod) throws Exception {
        for (TestSet set : sets) {
            Rt.p(set.dbName + " " + set.workloadName + " " + set.fileName);
            PrintStream ps = System.out;
            WorkloadLoader loader = new WorkloadLoader(set.dbName, set.workloadName, set.fileName, generateIndexMethod);
            SeqInumCost cost = loader.loadCost();
            DATParameter param = new DATParameter();
            param.spaceConstraint = Double.MAX_VALUE;
            param.costModel = cost;
            ps.format("fts_cost: %,.2f\n", cost.costWithoutIndex);
            boolean[] bs = new boolean[cost.indexCount()];
            Arrays.fill(bs, true);
            double[] ds = DATBaselines.getQueryCost(cost, bs);
            double[] fts = DATBaselines.getQueryCost(cost, new boolean[cost.indexCount()]);

            Environment en = Environment.getInstance();
            String dbName = set.dbName;
            en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/" + dbName);
            en.setProperty("username", "db2inst1");
            en.setProperty("password", "db2inst1admin");
            en.setProperty("workloads.dir", "resources/workloads/db2");
            DatabaseSystem db = newDatabaseSystem(en);
            InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
            DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();
            HashSet<Index> indexes = new HashSet<Index>();
            for (SeqInumIndex index : cost.indices) {
                indexes.add(index.loadIndex(db));
            }
            Rt.np(set.dbName + " " + set.workloadName);
            Rt.p("queries: " + cost.queries.size());
            Rt.p("indexes: " + indexes.size());
            double db2indexAll = 0;
            double db2ftsAll = 0;
            double inumIndexAll = 0;
            double inumFtsAll = 0;
            for (int i = 0; i < ds.length; i++) {
                ExplainedSQLStatement db2plan = db2optimizer.explain(cost.queries.get(i).sql, indexes);
                double db2index = db2plan.getTotalCost();

                db2plan = db2optimizer.explain(cost.queries.get(i).sql);
                double db2fts = db2plan.getTotalCost();
                db2indexAll += db2index;
                db2ftsAll += db2fts;
                inumIndexAll += ds[i];
                inumFtsAll += fts[i];

                Rt.np(
                        "query=%d\tDB2(FTS)=%,.0f\tBIP(FTS)=%,.0f"
                                + "\tDB2(Index)=%,.0f\tBIP(Index)=%,.0f\tBIP/DB2=%.2f", i, db2fts, fts[i], db2index,
                        ds[i], ds[i] / db2index);
            }
            Rt.np("DB2(FTS)=%,.0f", db2ftsAll);
            Rt.np("INUM(FTS)=%,.0f", inumFtsAll);
            Rt.np("DB2(Index)=%,.0f", db2indexAll);
            Rt.np("INUM(Index)=%,.0f", inumIndexAll);
            Rt.np("fts_cost: %,.2f\n", cost.costWithoutIndex);
            Rt.np("optimal_cost: %,.2f\n", cost.costWithAllIndex);

            // for (int i = 0; i < indices.size(); i++) {
            // SeqInumIndex index = indices.get(i);
            // ps.format("%.2f\t%.2f\t%s\n", index.createCost / 1000000,
            // index.indexBenefit / 1000000, index
            // .getColumnNames());
            // Rt.np(cost.indices.get(i));
            // }
            // ps.close();
        }
        System.exit(0);
    }

    public static void generatePdf(File latexFile, DATPaperParams params, TestSet[] sets) throws IOException {
        {
            PrintStream ps = new PrintStream(latexFile);
            ps.println("\\documentclass{vldb}}\n" + "\n" + "\\usepackage{graphicx}   % need for figures\n" + "\n"
                    + "\\usepackage{subfigure}\n" + "\n" + "\\begin{document}\n" + "" + "");
            ps.println("\\textbf{default values:}\\\\");
            ps.println("windowFactor=" + params.winFactor.def + "\\\\");
            ps.println("spaceFactor=" + params.spaceFactor.def + "\\\\");
            ps.println("l=" + params.l.def + "\\\\");
            ps.println("m=" + params.m.def + "\\\\");
            ps.println("(1-a)/a=" + params._1mada.def + "\\\\");
            ps.println();
            for (int i = 0; i < sets[0].plotNames.size(); i++) {
                File outputDir2 = new File(WorkloadLoaderSettings.dataRoot
                        + "/dbtune.papers/deployment_aware_tuning/figs");
                ps.println("\\begin{figure}[htb]\n" + "\\centering\n");
                for (TestSet set : sets) {
                    ps.println("        \\begin{subfigure}[" + set.shortName + "]\n" + "                \\centering\n"
                            + "                \\includegraphics[scale=0.3]{figs/" + set.plotNames.get(i) + ".eps}\n"
                            + "        \\end{subfigure}");
                    // ps.println("\\begin{figure}\n" + "\\centering\n"
                    // + "\\includegraphics[scale=" + scale + "]{figs/"
                    // + set.plotNames.get(i) + ".eps}\n" + "\\caption{"
                    // + set.figureNames.get(i) + "}\n"
                    // // + "\\label{" + plot.name + "}\n"
                    // + "\\end{figure}\n" + "");
                    if (params.copyEps)
                        Rt.copyFile(new File(params.figsDir, set.plotNames.get(i) + ".eps"), new File(outputDir2,
                                set.plotNames.get(i) + ".eps"));
                }
                ps.println("\\caption{" + sets[0].figureNames.get(i) + "}\n" + "\\end{figure}");
            }
            ps.println("\\end{document}\n");
            ps.close();
            Rt.runAndShowCommand(params.latex + " -interaction=nonstopmode " + latexFile.getName(), params.outputDir);
        }
        for (int i = 0; i < sets[0].plotNames.size(); i++) {
            latexFile = new File(params.outputDir, "_exp_" + (i + 1) + ".tex");
            // for (TestSet set : sets) {
            // File latexFile = new File(outputDir, "_" + set.workloadName
            // + ".tex");
            // Rt.runAndShowCommand(latex + " -interaction=nonstopmode "
            // + latexFile.getName(), outputDir);
        }
    }

    //    public static void generateSkylinePdf(File latexFile,
    //            DATPaperParams params, TestSet[] sets) throws Exception {
    //        PrintStream ps = new PrintStream(params.skylineLatexFile);
    //        ps.println("\\documentclass{vldb}}\n" + "\n"
    //                + "\\usepackage{graphicx}   % need for figures\n" + "\n"
//                + "\\usepackage{subfigure}\n" + "\n" + "\\begin{document}\n"
//                + "" + "");
    //        for (TestSet set : sets) {
    //            Rt.p(set.dbName + " " + set.workloadName);
    //            long spaceBudge = (long) (set.size * params.spaceFactor.def);
    //            GnuPlot2.uniform = true;
    //            GnuPlot2 plot = new GnuPlot2(params.outputDir, set.shortName
    //                    + "Xskyline", "r", "cost");
    //            plot.setXtics(null, params.tau_set);
    //            plot.setPlotNames(new String[] { "DAT" });
    //            ps.println("\\begin{figure}\n" + "\\centering\n"
    //                    + "\\includegraphics[scale=" + params.scale + "]{"
    //                    + plot.name + ".eps}\n" + "\\caption{" + set.name
    //                    + " skyline}\n"
    //                    // + "\\label{" + plot.name + "}\n"
    //                    + "\\end{figure}\n" + "");
    //            if (params.exp5) {
    //                double alpha = 0.000001;
    //                double beta = 1 - alpha;
    //                DATSeparateProcess dsp = new DATSeparateProcess(set.dbName,
    //                        set.workloadName, set.fileName,
    //                        params.generateIndexMethod, alpha, beta, params.m.def,
    //                        params.l.def, spaceBudge, params.windowSize, 0);
    //                dsp.run();
    //                double p0int = dsp.datIntermediate;
    //                for (double tau : params.tau_set) {
    //                    dsp = new DATSeparateProcess(set.dbName, set.workloadName,
    //                            set.fileName, params.generateIndexMethod, alpha,
    //                            beta, params.m.def, params.l.def, spaceBudge,
    //                            params.windowSize, p0int * tau);
    //                    dsp.run();
    //                    plot.add(tau, dsp.dat);
    //                    plot.addLine();
    //                }
    //            }
    //            plot.finish();
    //        }
    //        // ps.println("\\end{multicols}");
    //        ps.println("\\end{document}\n");
    //        ps.close();
    //        Rt.runAndShowCommand(params.latex + " -interaction=nonstopmode "
    //                + params.skylineLatexFile.getName(), params.outputDir);
    //    }
    //
    //    public static void runPerfTest(DATPaperParams params, TestSet[] sets)
    //            throws Exception {
    //        TestSet perfTest = sets[3];
    //        DATSeparateProcess dsp = new DATSeparateProcess(perfTest.dbName,
    //                perfTest.workloadName, perfTest.fileName,
    //                params.generateIndexMethod, 0.5, 0.5, params.m_def,
    //                params.l_def, Double.MAX_VALUE, params.windowSize, 0);
    //        dsp.generatePerfReport = true;
    //        dsp.run();
    //    }

    //    public static void runScalabilityTest(DATPaperParams params, TestSet[] sets)
    //            throws Exception {
    //        // 0 3
    //        TestSet perfTest = sets[3];
    //        WorkloadLoader loader = new WorkloadLoader(perfTest.dbName,
    //                perfTest.workloadName, perfTest.fileName,
    //                params.generateIndexMethod);
    //        SeqInumCost cost = loader.loadCost();
    //
    //        PrintStream ps2 = new PrintStream(
    //                WorkloadLoaderSettings.dataRoot+"/scalability.txt");
    //        int start = 1;
    //        int step = 1;
    //        int end = start + 100 * step;
    //        for (int i = start; i <= end; i += step) {
    //            DATSeparateProcess dsp = new DATSeparateProcess(perfTest.dbName,
    //                    perfTest.workloadName, perfTest.fileName,
    //                    params.generateIndexMethod, 0.5, 0.5, params.m_def,
    //                    params.l_def, Double.MAX_VALUE, params.windowSize, 0);
    //            dsp.runGreedy = false;
    //            dsp.runMKP = false;
    //            dsp.dupWorkloadNTimes = i;
    //            // dsp.generatePerfReport = true;
    //            RTimerN timer = new RTimerN();
    //            dsp.run();
    //            ps2.format(cost.queries.size() * i + "\t%.3f\t%d\t%s\t%s\n", timer
    //                    .getSecondElapse(), dsp.memoryUsed, perfTest.shortName,
    //                    new Date().toString());
    //            ps2.flush();
    //        }
    //        ps2.close();
    //    }

    public static void main(String[] args) {

    }

}
