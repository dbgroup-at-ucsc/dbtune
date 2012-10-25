package edu.ucsc.dbtune.advisor.db2;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.SQLTypes;
import edu.ucsc.dbtune.optimizer.ExplainTables;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;

public class DB2IndexInfo {
    /**
     * benefit of this index to the input workload
     */
    public double benefit;

    /**
     * update cost of this index to the input workload
     */
    public double updateCost;

    /**
     * index size
     */
    public double sizeMB;

    public static DB2IndexInfo getInfo(DatabaseSystem dbms, Index index,
            SQLStatement sql) throws SQLException {
        Statement stmt = dbms.getConnection().createStatement();
        int freq = 100000000;
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        for (Column c : index.columns())
            sb.append(c.getName()).append(",");
        sb.delete(sb.length() - 1, sb.length());
        sb.append("  FROM ").append(index.getTable().getFullyQualifiedName());
        sb.append("  WHERE ");
        for (Column c : index.columns()) {
            sb.append(c.getName());
            switch (c.getDataType()) {
            case SQLTypes.BIGINT:
            case SQLTypes.CHAR:
            case SQLTypes.DECIMAL:
            case SQLTypes.DOUBLE:
            case SQLTypes.FLOAT:
            case SQLTypes.INTEGER:
            case SQLTypes.NUMERIC:
            case SQLTypes.REAL:
            case SQLTypes.SMALLINT:
                sb.append("=0");
                break;
            case SQLTypes.DATE:
            case SQLTypes.TIME:
            case SQLTypes.TIMESTAMP:
            case SQLTypes.TINYINT:
            case SQLTypes.VARCHAR:
                sb.append("=''");
                break;
            default:
                throw new SQLException("Unknow data type " + c.getDataType());
            }
            break;
        }

        stmt.execute("DELETE FROM systools.advise_index");
        stmt.execute("DELETE FROM systools.advise_workload");
        stmt.execute("INSERT INTO systools.advise_workload ("
                + "   WORKLOAD_NAME," //
                + "   STATEMENT_NO,"//
                + "   STATEMENT_TEXT," //
                + "   STATEMENT_TAG," //
                + "   FREQUENCY,"//
                + "   IMPORTANCE," //
                + "   WEIGHT," //
                + "   COST_BEFORE,"//
                + "   COST_AFTER,"//
                + "   COMPILABLE" //
                + ") VALUES ("//
                + "   'dbtuneworkload',"//
                + "    0, " //
                + "   '" + sb.toString().replace("'", "''") + "'," //
                + "   ''," + freq + ",0,0,0,0,'')");
        stmt.execute("INSERT INTO systools.advise_workload ("
                + "   WORKLOAD_NAME," //
                + "   STATEMENT_NO,"//
                + "   STATEMENT_TEXT," //
                + "   STATEMENT_TAG," //
                + "   FREQUENCY,"//
                + "   IMPORTANCE," //
                + "   WEIGHT," //
                + "   COST_BEFORE,"//
                + "   COST_AFTER,"//
                + "   COMPILABLE" //
                + ") VALUES ("//
                + "   'dbtuneworkload',"//
                + "    1, " //
                + "   '" + sql.getSQL().replace("'", "''") + "'," //
                + "   '',1,0,0,0,0,'')");
        stmt.close();
        CallableStatement cstmt = dbms.getConnection().prepareCall(
                "CALL SYSPROC.DESIGN_ADVISOR(" + "   ?, ?, ?, blob(' "
                        + "      <?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                        + "      <plist version=\"1.0\"> "
                        + "         <dict> "
                        + "            <key>CMD_OPTIONS</key>"
                        + "            <string>"
                        + "               -workload  dbtuneworkload "
                        + "               -disklimit -1"
                        + "               -type      I "
                        + "               -compress  OFF "
                        +
                        // "               -qualifier tpcds " +
                        "               -drop" + "            </string>"
                        + "         </dict>" + "      </plist>'), "
                        + "   NULL, ?, ?)");

        cstmt.setInt(1, 1);
        cstmt.setInt(2, 0);
        cstmt.setString(3, "en_US");
        cstmt.registerOutParameter(4, Types.BLOB);
        cstmt.registerOutParameter(5, Types.BLOB);

        ResultSet rs = cstmt.executeQuery();
        if (ExplainTables.dump) {
            ExplainTables.dumpResult(rs);
            rs = cstmt.executeQuery();
        }

        DB2IndexInfo info = null;
        while (rs.next()) {
            int STATEMENT_NO = rs.getInt("STATEMENT_NO");
            if (STATEMENT_NO != 0)
                continue;
            String diskuse = rs.getString("DISKUSE");
            if (diskuse == null)
                continue;
            if (info != null)
                throw new SQLException("Multiple index created");
            info = new DB2IndexInfo();
            info.sizeMB = rs.getDouble("DISKUSE");
            info.updateCost = rs.getDouble("OVERHEAD");
            info.benefit = rs.getDouble("BENEFIT");
        }
        if (info == null)
            throw new SQLException(
                    "Index was not used. Need to increase frequency.");
        return info;
    }
}

/*
+----------+------------------+--------+----------------+---------+-------------------+--------------+------------+
| SCHEMA   | NAME             | EXISTS | RECOMMENDATION | BENEFIT | OVERHEAD          | STATEMENT_NO | DISKUSE    |
+----------+------------------+--------+----------------+---------+-------------------+--------------+------------+
| DB2INST1 | IDX1210242207160 | N      | I              | 14075.0 | 7.569217892731669 | 2            | 2.704125   |
| DB2INST1 | IDX1210242207270 | N      | I              | 4431.0  | 7.569217892731669 | 10           | 0.64553125 |
| DB2INST1 | IDX1210242207270 | N      | I              | 4431.0  | 7.569217892731669 | 11           | 0.64553125 |
| DB2INST1 | IDX1210242207270 | N      | I              | 4431.0  | 7.569217892731669 | 12           | 0.64553125 |
| DB2INST1 | IDX1210242207270 | N      | I              | 4431.0  | 7.569217892731669 | 13           | 0.64553125 |
| DB2INST1 | IDX1210242207270 | N      | I              | 4431.0  | 7.569217892731669 | 14           | 0.64553125 |
| DB2INST1 | IDX1210242207270 | N      | I              | 4431.0  | 7.569217892731669 | 15           | 0.64553125 |
| TPCDS    | INDEXR1          | Y      | I              |         |                   |              |            |
| TPCDS    | INDEXR3          | Y      | ID             |         |                   |              |            |
| TPCDS    | INDEXR2          | Y      | ID             |         |                   |              |            |
+----------+------------------+--------+----------------+---------+-------------------+--------------+------------+

+----------+------------------+--------+----------------+-----------------+---------------------+--------------+------------+
| SCHEMA   | NAME             | EXISTS | RECOMMENDATION | BENEFIT         | OVERHEAD            | STATEMENT_NO | DISKUSE    |
+----------+------------------+--------+----------------+-----------------+---------------------+--------------+------------+
| DB2INST1 | IDX1210242232140 | N      | I              | 3.9486864488E10 | 1.816769191146419E8 | 0            | 68.7275625 |
| DB2INST1 | IDX1210242232140 | N      | I              | 3.9486864488E10 | 1.816769191146419E8 | 1            | 68.7275625 |
| DB2INST1 | IDX1210242232200 | N      | I              | 3164488.0       | 0.0                 | 1            | 0.64553125 |
+----------+------------------+--------+----------------+-----------------+---------------------+--------------+------------+

+----------+------------------+--------+----------------+------------+----------+--------------+------------+
| SCHEMA   | NAME             | EXISTS | RECOMMENDATION | BENEFIT    | OVERHEAD | STATEMENT_NO | DISKUSE    |
+----------+------------------+--------+----------------+------------+----------+--------------+------------+
| DB2INST1 | IDX1210242234430 | N      | I              | 3.94837E10 | 0.0      | 0            | 68.7275625 |
+----------+------------------+--------+----------------+------------+----------+--------------+------------+

+----------+------------------+--------+----------------+-----------------+---------------------+--------------+------------+
| SCHEMA   | NAME             | EXISTS | RECOMMENDATION | BENEFIT         | OVERHEAD            | STATEMENT_NO | DISKUSE    |
+----------+------------------+--------+----------------+-----------------+---------------------+--------------+------------+
| DB2INST1 | IDX1210242237390 | N      | I              | 3.9486866448E10 | 3.843987141902928E8 | 0            | 68.7275625 |
| DB2INST1 | IDX1210242237390 | N      | I              | 3.9486866448E10 | 3.843987141902928E8 | 1            | 68.7275625 |
| DB2INST1 | IDX1210242237430 | N      | I              | 3166448.0       | 0.0                 | 1            | 0.64553125 |
+----------+------------------+--------+----------------+-----------------+---------------------+--------------+------------+
+----------+------------------+--------+----------------+------------------+---------------------+--------------+------------+
| SCHEMA   | NAME             | EXISTS | RECOMMENDATION | BENEFIT          | OVERHEAD            | STATEMENT_NO | DISKUSE    |
+----------+------------------+--------+----------------+------------------+---------------------+--------------+------------+
| DB2INST1 | IDX1210242241510 | N      | I              | 3.94840166448E11 | 3.843987141902928E8 | 0            | 68.7275625 |
| DB2INST1 | IDX1210242241510 | N      | I              | 3.94840166448E11 | 3.843987141902928E8 | 1            | 68.7275625 |
| DB2INST1 | IDX1210242241550 | N      | I              | 3166448.0        | 0.0                 | 1            | 0.64553125 |
+----------+------------------+--------+----------------+------------------+---------------------+--------------+------------+
*/