package edu.ucsc.dbtune.deployAware.test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.transform.TransformerException;

import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;

public class DATSeparateProcess {
    String dbName;
    String workloadName;
    String fileName;
    String generateIndexMethod;
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
    public int dupWorkloadNTimes = 1;
    File debugFile;

    double dat;
    double datIntermediate;
    double bip;
    double greedy;

    double[] datWindowCosts;
    double[] greedyWindowCosts;
    double[] mkpWindowCosts;

    boolean exitFlag = false;
    Object sync = new Object();
    long memoryUsed = 0; // in KB

    public DATSeparateProcess(String dbName, String workloadName,
            String fileName, String generateIndexMethod, double alpha,
            double beta, int m, int l, double spaceBudge, double windowSize,
            double intermediateConstraint) {
        this.dbName = dbName;
        this.workloadName = workloadName;
        this.fileName = fileName;
        this.generateIndexMethod = generateIndexMethod;
        this.alpha = alpha;
        this.beta = beta;
        this.m = m;
        this.l = l;
        this.spaceBudge = spaceBudge;
        this.windowSize = windowSize;
        this.intermediateConstraint = intermediateConstraint;
    }

    public Thread showInputStream(final InputStream is, final StringBuilder sb) {
        Thread ts = new Thread() {
            @Override
            public void run() {
                byte[] bs = new byte[1024];
                try {
                    while (true) {
                        int len = is.read(bs);
                        if (len < 0)
                            break;
                        String s = new String(bs, 0, len);
                        System.out.print(s);
                        synchronized (sb) {
                            sb.append(s);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                exitFlag = true;
                synchronized (sync) {
                    sync.notifyAll();
                }
            }
        };
        ts.start();
        return ts;
    }

    public static long getProcessMemory(String pattern) throws Exception {
        String cmd = "ps -eo rss,vsz,cmd";
        String result = Rt.runCommand(cmd);
        for (String line : result.split("\n")) {
            if (line.contains(pattern)) {
                line = line.trim();
                String[] ss = line.split(" +");
                return Long.parseLong(ss[0]);
            }
        }
        return 0;
    }

    public void run() throws Exception {
        File tmpInputFile = new File("/home/wangrui/dbtune/tmpInput.txt");
        File tmpFile = new File("/home/wangrui/dbtune/tmp.txt");
        File tmpPerfFile = new File("/home/wangrui/dbtune/tmpPerf.txt");
        final String cmd = "/usr/lib/jvm/java-6-openjdk/bin/java"
                + " -Djava.library.path=lib"
                + " -Dfile.encoding=UTF-8"
                + " -classpath"
                + " /home/wangrui/workspace/deployAware/bin"
                + ":/home/wangrui/workspace/deployAware/lib/caliper-0.0.jar"
                + ":/home/wangrui/workspace/deployAware/lib/ant-contrib-1.0b3.jar"
                + ":/home/wangrui/workspace/deployAware/lib/cglib-nodep-2.2.jar"
                + ":/home/wangrui/workspace/deployAware/lib/db2jcc4-9.7.5.jar"
                + ":/home/wangrui/workspace/deployAware/lib/guava-11.0.1.jar"
                + ":/home/wangrui/workspace/deployAware/lib/jackson-core-1.8.1.jar"
                + ":/home/wangrui/workspace/deployAware/lib/jackson-mapper-1.8.1.jar"
                + ":/home/wangrui/workspace/deployAware/lib/jarjar-snapshot.jar"
                + ":/home/wangrui/workspace/deployAware/lib/javassist-3.14.0-GA.jar"
                + ":/home/wangrui/workspace/deployAware/lib/junit-4.9.jar"
                + ":/home/wangrui/workspace/deployAware/lib/LaTeXlet-1.1.jar"
                + ":/home/wangrui/workspace/deployAware/lib/mockito-all-1.8.5.jar"
                + ":/home/wangrui/workspace/deployAware/lib/mysql-5.1.17.jar"
                + ":/home/wangrui/workspace/deployAware/lib/objenesis-1.2.jar"
                + ":/home/wangrui/workspace/deployAware/lib/postgresql-9.0-801.jdbc4.jar"
                + ":/home/wangrui/workspace/deployAware/lib/powermock-mockito-1.4.9-full.jar"
                + ":/home/wangrui/workspace/deployAware/extensions/inum/lib/derby-10.8.2.2.jar"
                + ":/home/wangrui/workspace/deployAware/extensions/bip/lib/cplex-12.2.jar"
                + " edu.ucsc.dbtune.deployAware.test.DATTest2";
        Rx rx = new Rx("dat");
        rx.createChild("dbName", dbName);
        rx.createChild("workloadName", workloadName);
        rx.createChild("fileName", fileName);
        rx.createChild("generateIndexMethod", generateIndexMethod);
        rx.createChild("alpha", alpha);
        rx.createChild("beta", beta);
        rx.createChild("m", m);
        rx.createChild("l", l);
        rx.createChild("spaceBudge", spaceBudge);
        rx.createChild("windowSize", windowSize);
        rx.createChild("intermediateConstraint", intermediateConstraint);
        if (generatePerfReport)
            rx.createChild("perfReportFile", tmpPerfFile.getAbsolutePath());
        if (debugFile != null)
            rx.createChild("debugFile", debugFile.getAbsolutePath());
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

        String[] envp = new String[] {
                "ILOG_LICENSE_FILE=/data/b/cplex/access.ilm",
                "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/home/db2inst1/sqllib/bin:/home/db2inst1/sqllib/adm:/home/db2inst1/sqllib/misc", };
        String cmdLine=sb.toString();
        for (int ti = 0; ti < 20; ti++) {
            Process process = Runtime.getRuntime().exec(cmdLine, envp,
                    new File("."));
            sb = new StringBuilder();
            new Thread("memory checker") {
                public void run() {
                    try {
                        while (!exitFlag) {
                            long mem = getProcessMemory(cmd);
                            if (mem > memoryUsed)
                                memoryUsed = mem;
                            synchronized (sync) {
                                sync.wait(500);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                };
            }.start();
            showInputStream(process.getInputStream(), sb);
            showInputStream(process.getErrorStream(), sb);
            try {
                process.waitFor();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (tmpFile.exists())
                break;
        }
        Rx root = Rx.findRoot(Rt.readFile(tmpFile));
        dat = root.getChildDoubleContent("dat");
        datIntermediate = root.getChildDoubleContent("datIntermediate");
        bip = root.getChildDoubleContent("bip");
        greedy = root.getChildDoubleContent("greedyRatio");
        Rx datWs = root.findChild("datWindows");
        if (datWs != null) {
            Rx[] datW = datWs.findChilds("window");
            datWindowCosts = new double[datW.length];
            for (int i = 0; i < datWindowCosts.length; i++) {
                datWindowCosts[i] = datW[i].getDoubleAttribute("cost");
            }
        }
        Rx greedyWs = root.findChild("greedyWindows");
        if (greedyWs != null) {
            Rx[] datW = greedyWs.findChilds("window");
            greedyWindowCosts = new double[datW.length];
            for (int i = 0; i < greedyWindowCosts.length; i++) {
                greedyWindowCosts[i] = datW[i].getDoubleAttribute("cost");
            }
        }
        Rx mkpWs = root.findChild("mkpWindows");
        if (mkpWs != null) {
            Rx[] datW = mkpWs.findChilds("window");
            mkpWindowCosts = new double[datW.length];
            for (int i = 0; i < mkpWindowCosts.length; i++) {
                mkpWindowCosts[i] = datW[i].getDoubleAttribute("cost");
            }
        }
    }

}
