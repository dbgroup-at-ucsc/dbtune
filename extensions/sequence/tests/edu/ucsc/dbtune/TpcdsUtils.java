package edu.ucsc.dbtune;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.regex.Pattern;

import edu.ucsc.dbtune.optimizer.ExplainTables;
import edu.ucsc.dbtune.util.ResultTable;
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
    public static void dumpTable(Connection connection, String table,
            String columns) throws SQLException {
        Statement st = connection.createStatement();
        st.execute("select " + columns + " from " + table
                + " fetch first 5 rows only");
        ResultSet rs = st.getResultSet();
        Rt.np(table);
        ExplainTables.dumpResult(rs);
        st.close();
    }

    public static void dumpResult(ResultSet rs) throws SQLException {
        ResultSetMetaData m = rs.getMetaData();
        String[] names = new String[m.getColumnCount()];
        int max = 0;
        for (int i = 0; i < names.length; i++) {
            String s = m.getColumnName(i + 1);
            if (s.length() > max)
                max = s.length();
            names[i] = s;
        }
        Rt.np("(");
        for (int i = 0; i < names.length; i++) {
            System.out.format("\t%s", names[i]);
            if (i < names.length - 1)
                System.out.print(",");
            System.out.println();
        }
        Rt.np(") values (");
        if (rs.next()) {
            for (int i = 0; i < names.length; i++) {
                int type = m.getColumnType(i + 1);
                if (type == Types.TINYINT || type == Types.SMALLINT
                        || type == Types.INTEGER || type == Types.BIGINT
                        || type == Types.FLOAT || type == Types.REAL
                        || type == Types.DOUBLE || type == Types.NUMERIC
                        || type == Types.DECIMAL)
                    System.out.format("\t%s", rs.getString(i + 1));
                else
                    System.out.format("\t\'%s\'", rs.getString(i + 1));
                if (i < names.length - 1)
                    System.out.print(",");
                System.out.println();
            }
        }
        Rt.np(");");
    }

    static void test() throws Exception {
        Class.forName("com.ibm.db2.jcc.DB2Driver");
        Connection conn = DriverManager.getConnection(
                "jdbc:db2://localhost:50000/test", "db2inst1", "db2inst1admin");
        // dumpTable(conn, "tpcds.customer", "*");
        Statement st = conn.createStatement();
        // st.execute("select * from tpcds.customer_address "
        // + "where ca_address_sk=32946");
        String tableName = "web_sales";
        tableName="tpcds."+ tableName;
        String columnName = "ws_order_number";
        st.execute("select max(" + columnName + ") from " + tableName);
        ResultSet rs = st.getResultSet();
        rs.next();
        int n = rs.getInt(1);
        Rt.p(n);
        st.execute("select * from " + tableName + " where " + columnName + "="
                + n);
        Rt.np("insert into "+ tableName);
        dumpResult(st.getResultSet());
        conn.close();
        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        test();
        String tpcdsNew = Rt.readFile(new File(
                "resources/workloads/db2/tpcds/queriesWithTemplateNumber.sql"));
        String tpcdsOld = Rt.readFile(new File(
                "resources/workloads/db2/tpcds/db2.sql"));
        Workload workload = new Workload("", new StringReader(tpcdsOld));
        int n = 0;
        for (String line2 : tpcdsNew.split("\n")) {
            String templateId = line2.substring(0, line2.indexOf(' '));
            line2 = line2.substring(line2.indexOf(' ') + 1).trim();
            String[] ss = line2.split(";");
            for (String line : ss) {
                line = line.trim();
                String sql = workload.get(n++).getSQL();
                String original = sql;
                sql = sql.trim();
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
                // Rt.np("-- "+ templateId);
                // Rt.np(original+";");
            }
        }
    }
}
