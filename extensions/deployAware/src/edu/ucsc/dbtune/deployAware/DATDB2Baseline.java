package edu.ucsc.dbtune.deployAware;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;

import java.io.File;
import java.io.StringReader;
import java.util.Set;
import java.util.Vector;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.db2.DB2Advisor;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;
import edu.ucsc.dbtune.workload.Workload;

public class DATDB2Baseline {
    static class I {
        String name;
        long bytes;
    }

    static class S {
        int limitMB;
        Vector<I> indexes = new Vector<I>();
    }

    Vector<S> sets = new Vector<S>();

    File file = new File("/home/wangrui/dbtune/paper/cache/db2advisor.xml");

    public void save() throws Exception {
        Rx rx = new Rx("db2advisor");
        for (S s : sets) {
            Rx set = rx.createChild("set");
            set.setAttribute("size", s.limitMB);
            for (I i : s.indexes) {
                Rx rx2 = set.createChild("index", i.name);
                rx2.setAttribute("size", i.bytes);
            }
        }
        Rt.write(file, rx.getXml().getBytes());
    }

    public DATDB2Baseline() throws Exception {
        Environment en = Environment.getInstance();
        String dbName = "tpch10g";
        en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/" + dbName);
        en.setProperty("username", "db2inst1");
        en.setProperty("password", "db2inst1admin");
        en.setProperty("workloads.dir", "resources/workloads/db2");
        DatabaseSystem db = newDatabaseSystem(en);
        String tpch = Rt.readFile(new File(
                "resources/workloads/db2/tpch/complete.sql"));
        DB2Advisor db2Advis = new DB2Advisor(db);
        Workload workload = new Workload("", new StringReader(tpch));
        db2Advis.process(workload);
        Set<Index> indexes = db2Advis.getRecommendation(-1);
        long n = 0;
        for (Index index : indexes) {
            Rt.np(index + " " + index.getBytes());
            n += index.getBytes();
        }
        Rt.np("%,d", n);
        n = n / (1024 * 1024) + 1024;
        for (int i = 512; i < n; i += 512) {
            db2Advis.process(workload);
            Rt.p(i);
            indexes = db2Advis.getRecommendation(i);
            S s = new S();
            s.limitMB = i;
            long n2 = 0;
            for (Index index : indexes) {
                n2 += index.getBytes();
                I a = new I();
                a.bytes = index.getBytes();
                a.name = index.toString();
                s.indexes.add(a);
            }
            sets.add(s);
            Rt.np("%,d %,d", i, n2);
            save();
        }
    }

    public static void main(String[] args) throws Exception {
        new DATDB2Baseline();
    }
}
