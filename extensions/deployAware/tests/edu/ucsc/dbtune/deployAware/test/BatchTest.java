package edu.ucsc.dbtune.deployAware.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

import edu.ucsc.dbtune.deployAware.test.DATPaper.TestSet;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.seq.utils.Rt;
import edu.ucsc.dbtune.seq.utils.Rx;

public class BatchTest {
    public static void main(String[] args) throws Exception {
        String generateIndexMethod = "recommend";
        generateIndexMethod = "powerset 2";

        long gigbytes = 1024L * 1024L * 1024L;
        File outputFile = new File("/home/wangrui/dbtune/batch.txt");
        PrintStream ps = new PrintStream(outputFile);
        TestSet[] sets = {
        // new TestSet("170 OST queries", "test", "OST", 10 * gigbytes),
        new TestSet("12 TPC-H queries", "tpch10g", "tpch-inum", 10 * gigbytes),
        // new TestSet("12 TPC-H queries  \\& update stream RF1 and RF2",
        // "tpch10g", "tpch-benchmark-mix", 10 * gigbytes),
        // new TestSet("100 OTAB [5] queries", "test",
        // "online-benchmark-100", 10 * gigbytes),
        // new TestSet("100 OTAB [5] queries and 10 updates", "test",
        // "online-benchmark-update-100", 10 * gigbytes),
        };

        for (TestSet set : sets) {
            Rt.np(set.dbName + " " + set.workloadName);
            WorkloadLoader loader = new WorkloadLoader(set.dbName,
                    set.workloadName, generateIndexMethod);
            SeqInumCost cost = loader.loadCost();
            Rt
                    .np("index\tcreate cost\tstorage cost\tbenefit(with-none)\tbenefit(all-without)");
            for (int i = 0; i < cost.indices.size(); i++) {
                SeqInumIndex index = cost.indices.get(i);
                Rt.np("%d\t%,.0f\t%,.0f\t%,.0f\t%,.0f", i, index.createCost,
                        index.storageCost, index.indexBenefit,
                        index.indexBenefit2);
            }
        }
        // System.exit(0);
        int[] _1mada_set = { 1,
        // 2, 4, 16
        };
        int[] m_set = { 3,
        // 2, 4, 5, 6
        };
        int[] l_set = { 20 };// 100, 5, 10, 20, 50, };
        double[] spaceFactor_set = { 2, 5, 10, 20, 50, 100, 200 };// 0.05, 0.1,
                                                                  // 0.25, 0.5,
        // 1, 2,
        // 5, 10, 20, 50,
        // 100, 1000 };
        double[] winFactor_set = { 0.5, 0.6, 0.7, 0.8, 0.9, 1, 1.1, 1.2, 1.3,
                1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0, 5, 10, 100,
        // 1000, 2000, 5000, 10000, 20000, 50000,
        // 100000
        };
        winFactor_set = new double[30];
        double start = 0.1;
        for (int i = 0; i < winFactor_set.length; i++) {
            // winFactor_set[i]=0.5+i*0.1;
            winFactor_set[i] = start;
            start *= 1.5;
        }
        for (int _1mada : _1mada_set) {
            double alpha = DATPaper.getAlpha(_1mada);
            double beta = 1 - alpha;
            for (int m : m_set) {
                for (int l : l_set) {
                    for (TestSet set : sets) {
                        Rt.p(set.dbName + " " + set.workloadName);
                        WorkloadLoader loader = new WorkloadLoader(set.dbName,
                                set.workloadName, generateIndexMethod);
                        SeqInumCost cost = loader.loadCost();
                        long totalCost = 0;
                        for (int i = 0; i < cost.indices.size(); i++) {
                            SeqInumIndex index = cost.indices.get(i);
                            totalCost += index.createCost;
                        }
                        long avgCost = totalCost / cost.indices.size();
                        Rt.p("avgCost=" + totalCost / cost.indices.size());
                        ps.println(set.dbName + " " + set.workloadName);
                        ps.println("alpha=" + alpha + "\tbeta=" + beta);
                        ps.println("(1-a)/a=" + _1mada + "\tm=" + m + "\tl="
                                + l + "\twinFactor=" + avgCost);
                        ps.format("winF\\spaceF\t");
                        for (double spaceFactor : spaceFactor_set) {
                            ps.format("%.2f\t", spaceFactor);
                        }
                        ps.println();
                        for (double winFactor : winFactor_set) {
                            ps.format("%.2f\t", winFactor);
                            for (double spaceFactor : spaceFactor_set) {
                                long space = (long) (set.size * spaceFactor);
                                int windowSize = (int) (winFactor * avgCost);

                                DATSeparateProcess dsp = new DATSeparateProcess(
                                        set.dbName, set.workloadName,
                                        generateIndexMethod, alpha, beta, m, l,
                                        space, windowSize, 0);
                                dsp.runMKP = false;
                                dsp.run();
                                double dat = dsp.dat;
                                // double mkp = dsp.bip;
                                double greedyRatio = dsp.greedy;
                                double result = dat / greedyRatio * 100;
                                int percent = (int) result;
                                ps.format("%d%%\t", percent);
                                ps.flush();
                            }
                            ps.println();
                        }
                    }
                }
            }
        }
        ps.close();
    }

}
