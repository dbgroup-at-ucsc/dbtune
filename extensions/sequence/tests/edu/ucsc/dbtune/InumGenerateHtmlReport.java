package edu.ucsc.dbtune;

import java.io.File;
import java.io.PrintStream;

import edu.ucsc.dbtune.InumTestSuite.Workload;
import edu.ucsc.dbtune.InumTestSuite.IndexSet;

public class InumGenerateHtmlReport {
    static void clearPreviousResult(Workload[] workloads) throws Exception {
        InumTestSuite suite = new InumTestSuite(workloads, true);
        for (Workload workload : workloads) {
            for (IndexSet set : workload.indexSets) {
                for (int i = 0; i < set.results.length; i++) {
                    // if (i==7)
                    set.results[i] = null;
                }
            }
            workload.save();
        }
    }

    public static void main(String[] args) throws Exception {
        Workload[] workloads = {
//                new Workload("tpch10g_22", "tpch10g", "TPC-H",
//                        "resources/workloads/db2/tpch/complete.sql"),
//                new Workload("tpcds10g_99", "test", "TPCDS",
//                        "resources/workloads/db2/tpcds/db2.sql"),
                new Workload("tpcds10g_40", "test", "TPCDS40",
                        "resources/workloads/db2/deployAware/tpcds_40.sql"),
//                new Workload("online-benchmark-100", "test", "OTAB",
//                        "resources/workloads/db2/online-benchmark-100/workload.sql"), //
        };
//         clearPreviousResult(workloads);
        InumTestSuite.IBGTIMEOUT=30000;
        InumTestSuite suite = new InumTestSuite(workloads, true);
        suite.compare();
        for (Workload workload : workloads) {
            PrintStream ps = new PrintStream(new File(suite.resultDir,
                    workload.uuid + ".html"));
            suite.showHtmlResult(workload, ps);
            ps.close();
            ps = new PrintStream(new File(suite.resultDir, workload.uuid
                    + ".txt"));
            suite.showCompactResult(workload, ps, true);
            ps.close();
            ps = new PrintStream(new File(suite.resultDir, workload.uuid
                    + ".ratio.txt"));
            suite.showCompactResult(workload, ps, false);
            ps.close();
        }
    }
}
