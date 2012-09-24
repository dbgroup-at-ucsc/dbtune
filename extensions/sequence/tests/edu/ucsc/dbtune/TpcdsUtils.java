package edu.ucsc.dbtune;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.regex.Pattern;

import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.Workload;

/**
 * This file is used to compare the difference between two tpcds workload, can
 * be removed.
 * 
 * @author wangrui
 * 
 */
public class TpcdsUtils {
    public static void main(String[] args) throws Exception {
        String tpcdsNew = Rt.readFile(new File(
                "resources/workloads/db2/tpcds/queriesWithTemplateNumber.sql"));
        String tpcdsOld = Rt.readFile(new File(
                "resources/workloads/db2/tpcds/db2.sql"));
        Workload workload = new Workload("", new StringReader(tpcdsOld));
        int n = 0;
        for (String line2 : tpcdsNew.split("\n")) {
            String templateId=line2.substring(0,line2.indexOf(' '));
            line2 = line2.substring(line2.indexOf(' ') + 1).trim();
            String[] ss = line2.split(";");
            for (String line : ss) {
                line = line.trim();
                String sql = workload.get(n++).getSQL();
                String original=sql;
                sql=sql.trim();
                sql = sql.replaceAll(Pattern.quote("tpcds."), "");
                line = line.replaceAll("\\s+", " ");
                sql = sql.replaceAll("\\s+", " ");
                line = line.replaceAll(" ,", ",");
                sql = sql.replaceAll(" ,", ",");
                line = line.replaceAll(", ", ",");
                sql = sql.replaceAll(", ", ",");
                line = line.replaceAll("\\( ", "(");
                sql = sql.replaceAll("\\( ", "(");
                line = line.replaceAll(" \\+ ", "+");
                sql = sql.replaceAll(" \\+ ", "+");
                line = line.replaceAll(" \\= ", "=");
                sql = sql.replaceAll(" \\= ", "=");
                if (!line.equalsIgnoreCase(sql)) {
                    line = line.toLowerCase();
                    sql = sql.toLowerCase();
                    for (int i = 0; i < line.length() && i < sql.length(); i++) {
                        char c1 = line.charAt(i);
                        char c2 = sql.charAt(i);
                        if (c1 != c2) {
                            line = line.substring(Math.max(0, i - 10));
                            sql = sql.substring(Math.max(0, i - 10));
                            break;
                        }
                    }
                    Rt.np(line);
                    Rt.np(sql);
                }
//                Rt.np("-- "+ templateId);
//                Rt.np(original+";");
            }
        }
    }
}
