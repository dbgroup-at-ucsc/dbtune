package edu.ucsc.dbtune;

import java.io.File;
import java.io.PrintStream;

import edu.ucsc.dbtune.InumTestSuite.Workload;

public class InumGenerateHtmlReport {
    public static void main(String[] args) throws Exception {
        Workload[] workloads = {
                new Workload("tpch10g_22", "tpch10g", "TPC-H",
                        "resources/workloads/db2/tpch/complete.sql"),
                // new Workload("test", "OTAB",
                // "resources/workloads/db2/online-benchmark-100/workload.sql"),
                new Workload("tpcds10g_99", "test", "TPCDS",
                        "resources/workloads/db2/tpcds/db2.sql"),
        // new Workload("test", "TPCDS 39",
        // "resources/workloads/db2/tpcds/39.sql"),
        };
        boolean continueFromLastTest = true;
//        continueFromLastTest = false;
        InumTestSuite suite = new InumTestSuite(workloads, continueFromLastTest);
        for (Workload workload : workloads) {
            PrintStream ps = new PrintStream(new File(suite.resultDir,
                    workload.uuid + ".html"));
            // suite.showCompactResult(workload);
            // suite.showResultWithCosts(workload);
            suite.showHtmlResult(workload, ps);
            ps.close();
        }
    }
}
