package edu.ucsc.dbtune.deployAware.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

import edu.ucsc.dbtune.deployAware.test.DATPaper.TestSet;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.seq.utils.Rt;

public class BatchTest {
    public static void main(String[] args) throws Exception {
        DATTest2.querySize = 0;
        DATTest2.indexSize = 0;
        File tmpFile = new File("/home/wangrui/dbtune/tmp.txt");
        String cmd = "/usr/lib/jvm/java-6-openjdk/bin/java"
                + " -Djava.library.path=lib"
                + " -Dfile.encoding=UTF-8"
                + " -classpath"
                + " /home/wangrui/workspace/dbtune/bin"
                + ":/home/wangrui/workspace/dbtune/lib/caliper-0.0.jar"
                + ":/home/wangrui/workspace/dbtune/lib/ant-contrib-1.0b3.jar"
                + ":/home/wangrui/workspace/dbtune/lib/cglib-nodep-2.2.jar"
                + ":/home/wangrui/workspace/dbtune/lib/db2jcc4-9.7.5.jar"
                + ":/home/wangrui/workspace/dbtune/lib/guava-11.0.1.jar"
                + ":/home/wangrui/workspace/dbtune/lib/jackson-core-1.8.1.jar"
                + ":/home/wangrui/workspace/dbtune/lib/jackson-mapper-1.8.1.jar"
                + ":/home/wangrui/workspace/dbtune/lib/jarjar-snapshot.jar"
                + ":/home/wangrui/workspace/dbtune/lib/javassist-3.14.0-GA.jar"
                + ":/home/wangrui/workspace/dbtune/lib/junit-4.9.jar"
                + ":/home/wangrui/workspace/dbtune/lib/LaTeXlet-1.1.jar"
                + ":/home/wangrui/workspace/dbtune/lib/mockito-all-1.8.5.jar"
                + ":/home/wangrui/workspace/dbtune/lib/mysql-5.1.17.jar"
                + ":/home/wangrui/workspace/dbtune/lib/objenesis-1.2.jar"
                + ":/home/wangrui/workspace/dbtune/lib/postgresql-9.0-801.jdbc4.jar"
                + ":/home/wangrui/workspace/dbtune/lib/powermock-mockito-1.4.9-full.jar"
                + ":/home/wangrui/workspace/dbtune/extensions/inum/lib/derby-10.8.2.2.jar"
                + ":/home/wangrui/workspace/dbtune/extensions/bip/lib/cplex-12.2.jar"
                + " edu.ucsc.dbtune.deployAware.test.DATTest2";
        long gigbytes = 1024L * 1024L * 1024L;
        File outputFile = new File("/home/wangrui/dbtune/batch.txt");
        PrintStream ps = new PrintStream(outputFile);
        TestSet[] sets = {
                new TestSet("12 TPC-H queries", "tpch10g", "tpch-inum",
                        10 * gigbytes),
                new TestSet("12 TPC-H queries  \\& update stream RF1 and RF2",
                        "tpch10g", "tpch-benchmark-mix", 10 * gigbytes),
                new TestSet("100 OTAB [5] queries", "test",
                        "online-benchmark-100", 10 * gigbytes),
                new TestSet("100 OTAB [5] queries and 10 updates", "test",
                        "online-benchmark-update-100", 10 * gigbytes), };
        int[] _1mada_set = { 1, 2, 4, 16 };
        int[] m_set = { 2, 3, 4, 5, 6 };
        int[] l_set = { 5, 10, 20, 50, 100 };
        double[] spaceFactor_set = { 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 50,
                100, 1000 };
        double[] winFactor_set = { 10, 100, 1000, 10000, 100000, 1000000 };
        for (TestSet set : sets) {
            Rt.p(set.dbName + " " + set.workloadName);
            DATTest2.dbName = set.dbName;
            DATTest2.workloadName = set.workloadName;
            SeqInumCost cost = DATTest2.loadCost();
            long totalCost = 0;
            for (int i = 0; i < cost.indices.size(); i++) {
                SeqInumIndex index = cost.indices.get(i);
                totalCost += index.createCost;
            }
            Rt.p("avgCost=" + totalCost / cost.indices.size());
            for (int _1mada : _1mada_set) {
                double alpha = DATPaper.getAlpha(_1mada);
                double beta = 1 - alpha;
                for (int m : m_set) {
                    for (int l : l_set) {
                        ps.println(set.dbName + " " + set.workloadName);
                        ps.println("alpha=" + alpha + "\tbeta=" + beta);
                        ps.println("(1-a)/a=" + _1mada + "\tm=" + m + "\tl="
                                + l + "\twinFactor=" + l
                                * (totalCost / cost.indices.size()));
                        ps.format("winF\\spaceF\t");
                        for (double spaceFactor : spaceFactor_set) {
                            ps.format("%.2f\t", spaceFactor);
                        }
                        ps.println();
                        for (double winFactor : winFactor_set) {
                            ps.format("%.2f\t", winFactor);
                            for (double spaceFactor : spaceFactor_set) {
                                long space = (long) (set.size * spaceFactor);
                                int windowSize = (int) (winFactor * l * (totalCost / cost.indices
                                        .size()));

                                StringBuilder sb = new StringBuilder();
                                sb.append(cmd);
                                sb.append(" " + set.dbName);
                                sb.append(" " + set.workloadName);
                                sb.append(" " + alpha);
                                sb.append(" " + beta);
                                sb.append(" " + m);
                                sb.append(" " + l);
                                sb.append(" " + space);
                                sb.append(" " + windowSize);
                                sb.append(" " + tmpFile.getAbsolutePath());
                                tmpFile.delete();
                                Rt.p(sb.toString());
                                Rt
                                        .runAndShowCommand(
                                                sb.toString(),
                                                new String[] {
                                                        "ILOG_LICENSE_FILE=/data/cplex/access.ilm",
                                                        "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/home/db2inst1/sqllib/bin:/home/db2inst1/sqllib/adm:/home/db2inst1/sqllib/misc", },
                                                new File("."));
                                int percent = Integer.parseInt(Rt
                                        .readFile(tmpFile));
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
