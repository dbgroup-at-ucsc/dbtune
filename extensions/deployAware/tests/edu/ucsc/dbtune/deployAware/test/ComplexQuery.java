package edu.ucsc.dbtune.deployAware.test;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import edu.ucsc.dbtune.util.Rt;

public class ComplexQuery {
    public static void main(String[] args) throws Exception {
        File inputFile = new File(
                "resources/workloads/db2/OST/workload_COMPLEX_QUERIES_p50_w1_1234.txt");
        File outputFile = new File("resources/workloads/db2/OST/workload.sql");
        PrintStream ps = new PrintStream(outputFile);
        for (String line : Rt.readFileAsLines(inputFile)) {
            int b = line.indexOf("FROM");
            int e = line.indexOf("WHERE");
            String s1=line.substring(0,b);
            String where=line.substring(e);
            String[] tables = line.substring(b + 4, e).split(",");
            StringBuilder sb=new StringBuilder();
            for (String table : tables) {
                table = table.trim();
                String[] ss = table.split(" ");
                Rt.p(ss[1] + " " + ss[0]);
                if (sb.length()>0)
                    sb.append(",");
                sb.append(ss[0]);
                where = where.replaceAll(ss[1], ss[0]);
            }
            ps.println(s1+"FROM "+sb+" "+where);
        }
        ps.close();
    }
}
