package edu.ucsc.dbtune;

import java.io.File;
import java.io.IOException;

import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;

public class BipVsGreedyTest2 {
    String cmdline = "/usr/lib/jvm/java-6-openjdk/bin/java"
            + " -Djava.library.path=lib"
            + " -Dfile.encoding=UTF-8"
            + " -classpath /home/wangrui/workspace/deployAware/bin:/home/wangrui/workspace/deployAware/lib/caliper-0.0.jar:/home/wangrui/workspace/deployAware/lib/ant-contrib-1.0b3.jar:/home/wangrui/workspace/deployAware/lib/cglib-nodep-2.2.jar:/home/wangrui/workspace/deployAware/lib/db2jcc4-9.7.5.jar:/home/wangrui/workspace/deployAware/lib/guava-11.0.1.jar:/home/wangrui/workspace/deployAware/lib/jackson-core-1.8.1.jar:/home/wangrui/workspace/deployAware/lib/jackson-mapper-1.8.1.jar:/home/wangrui/workspace/deployAware/lib/jarjar-snapshot.jar:/home/wangrui/workspace/deployAware/lib/javassist-3.14.0-GA.jar:/home/wangrui/workspace/deployAware/lib/junit-4.9.jar:/home/wangrui/workspace/deployAware/lib/LaTeXlet-1.1.jar:/home/wangrui/workspace/deployAware/lib/mockito-all-1.8.5.jar:/home/wangrui/workspace/deployAware/lib/mysql-5.1.17.jar:/home/wangrui/workspace/deployAware/lib/objenesis-1.2.jar:/home/wangrui/workspace/deployAware/lib/postgresql-9.0-801.jdbc4.jar:/home/wangrui/workspace/deployAware/lib/powermock-mockito-1.4.9-full.jar:/home/wangrui/workspace/deployAware/extensions/inum/lib/derby-10.8.2.2.jar:/home/wangrui/workspace/deployAware/extensions/bip/lib/cplex-12.2.jar"
            + " edu.ucsc.dbtune.BipTest2";
    int queryCount;
    int indexCount;
    double inumCreateIndexCostTime;
    double inumPluginTime;
    double inumPopulateTime;
    double allTime;
    double bipTime;
    double bipCost;

    void run(boolean bip, int queryTimes, int indexSize) throws Exception {
        Process process = Runtime
                .getRuntime()
                .exec(
                        cmdline + " " + queryTimes + " " + indexSize + " "
                                + (bip ? "bip" : "greedy"),
                        new String[] {
                                "ILOG_LICENSE_FILE=/data/cplex/access.ilm",
                                "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/home/db2inst1/sqllib/bin:/home/db2inst1/sqllib/adm:/home/db2inst1/sqllib/misc", });
        StringBuilder sb = new StringBuilder();
        Rt.showInputStream(process.getInputStream(), sb);
        Rt.showInputStream(process.getErrorStream(), sb);
        try {
            process.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Rx rx = Rx.findRoot(Rt.readFile(new File("/tmp/t.xml")));
        queryCount = rx.getChildIntContent("queryCount");
        indexCount = rx.getChildIntContent("indexCount");
        inumCreateIndexCostTime = rx
                .getChildDoubleContent("createIndexCostTime");
        inumPluginTime = rx.getChildDoubleContent("inumPluginTime");
        inumPopulateTime = rx.getChildDoubleContent("inumPopulateTime");
        allTime = rx.getChildDoubleContent("allTime");
        bipTime = rx.getChildDoubleContent("time");
        bipCost = rx.getChildDoubleContent("cost");
    }

    public BipVsGreedyTest2() throws Exception {
        int queryTimes = 1;
        int indexSize = 10;
        StringBuilder sb = new StringBuilder();
        for (indexSize = 10; indexSize <= 60; indexSize += 10) {
            run(true, queryTimes, indexSize);
            String log = String.format(
                    "%d x %d,BIP,\t\t%.3f,\t%.3f,\t%.3f,\t%.3f,\t%.3f,\t \"%,.0f\"",
                    queryCount, indexCount,//
                    inumCreateIndexCostTime, //
                    inumPopulateTime, //
                    inumPluginTime, //
                    allTime, //
                    bipTime,//
                    bipCost);
            sb.append(log + "\n");
            Rt
                    .np("query x index,\talgorithm,\t createCostTime,\t populateTime,\t pluginTime,\t "
                            + "allTime,\t algorithmTime,\t cost");
            Rt.np(sb);
            run(false, queryTimes, indexSize);
            log = String
                    .format(
                            "%d x %d,GREEDY,\t\t%.3f,\t%.3f,\t%.3f,\t%.3f,\t%.3f,\t \"%,.0f\"",
                            queryCount, indexCount,//
                            inumCreateIndexCostTime, //
                             inumPopulateTime, //
                            inumPluginTime, //
                            allTime, //
                            bipTime,//
                            bipCost);
            sb.append(log + "\n");
            Rt
                    .np("query x index,\talgorithm\t createCostTime,\t pluginTime,\t "
                            + "inumTime,\t time,\t cost");
            Rt.np(sb);
        }
    }

    public static void main(String[] args) throws Exception {
        new BipVsGreedyTest2();
    }
}