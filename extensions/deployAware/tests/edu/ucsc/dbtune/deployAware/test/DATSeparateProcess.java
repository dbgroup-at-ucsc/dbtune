package edu.ucsc.dbtune.deployAware.test;

import java.io.File;
import java.io.IOException;

import javax.xml.transform.TransformerException;

import edu.ucsc.dbtune.seq.utils.Rt;
import edu.ucsc.dbtune.seq.utils.Rx;

public class DATSeparateProcess {
    String dbName;
    String workloadName;
    double alpha;
    double beta;
    int m;
    int l;
    double spaceBudge;
    double windowSize;
    double intermediateConstraint;
    public boolean generatePerfReport = false;
    public boolean runDAT = true;
    public boolean runGreedy = true;
    public boolean runMKP = true;
    public int dupWorkloadNTimes=1;

    double dat;
    double datIntermediate;
    double bip;
    double greedy;

    public DATSeparateProcess(String dbName, String workloadName, double alpha,
            double beta, int m, int l, double spaceBudge, double windowSize,
            double intermediateConstraint) {
        this.dbName = dbName;
        this.workloadName = workloadName;
        this.alpha = alpha;
        this.beta = beta;
        this.m = m;
        this.l = m;
        this.spaceBudge = spaceBudge;
        this.windowSize = windowSize;
        this.intermediateConstraint = intermediateConstraint;
    }

    public void run() throws Exception {
        File tmpInputFile = new File("/home/wangrui/dbtune/tmpInput.txt");
        File tmpFile = new File("/home/wangrui/dbtune/tmp.txt");
        File tmpPerfFile = new File("/home/wangrui/dbtune/tmpPerf.txt");
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
        Rx rx = new Rx("dat");
        rx.createChild("dbName", dbName);
        rx.createChild("workloadName", workloadName);
        rx.createChild("alpha", alpha);
        rx.createChild("beta", beta);
        rx.createChild("m", m);
        rx.createChild("l", l);
        rx.createChild("spaceBudge", spaceBudge);
        rx.createChild("windowSize", windowSize);
        rx.createChild("intermediateConstraint", intermediateConstraint);
        if (generatePerfReport)
            rx.createChild("perfReportFile", tmpPerfFile.getAbsolutePath());
        rx.createChild("runDAT", runDAT);
        rx.createChild("runGreedy", runGreedy);
        rx.createChild("runMKP", runMKP);
        rx.createChild("dupWorkloadNTimes", dupWorkloadNTimes);
        Rt.write(tmpInputFile, rx.getXml());
        StringBuilder sb = new StringBuilder();
        sb.append(cmd);
        sb.append(" " + tmpInputFile.getAbsolutePath());
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
        Rx root = Rx.findRoot(Rt.readFile(tmpFile));
        dat = root.getChildDoubleContent("dat");
        datIntermediate = root.getChildDoubleContent("datIntermediate");
        bip = root.getChildDoubleContent("bip");
        greedy = root.getChildDoubleContent("greedyRatio");
    }

}
