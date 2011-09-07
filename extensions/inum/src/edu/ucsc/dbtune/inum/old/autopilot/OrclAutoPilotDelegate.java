package edu.ucsc.dbtune.inum.old.autopilot;

import com.google.common.base.Joiner;
import edu.ucsc.dbtune.inum.old.model.Configuration;
import edu.ucsc.dbtune.inum.old.model.Index;
import edu.ucsc.dbtune.inum.old.model.PhysicalConfiguration;
import edu.ucsc.dbtune.inum.old.model.Plan;
import edu.ucsc.dbtune.inum.old.model.QueryDesc;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by IntelliJ IDEA.
 * User: Dash
 * Date: Jan 1, 2008
 * Time: 12:24:08 AM
 * To change this template use File | Settings | File Templates.
 */
public class OrclAutoPilotDelegate extends AutoPilotDelegate {
    public static final String EXPLAIN_SQL =
            "select id, parent_id, operation, object_name, object_alias, cost from plan_table where statement_id = ";

    private int count;

    public OrclAutoPilotDelegate() {
    }

    public Plan getExecutionPlan(Connection conn, String Q) throws SQLException {
        String threadId = Thread.currentThread().getName();
        Statement stmt = conn.createStatement();
        stmt.execute("delete from plan_table where statement_id = '"+threadId+"'");
        stmt.execute("alter session set \"_use_nosegment_indexes\" = true");
        try {
            stmt.execute("explain plan set statement_id = '"+threadId+"'for " + Q);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        ResultSet rs = stmt.executeQuery(EXPLAIN_SQL + "'" + threadId + "'");
        OrclPlan plan = new OrclPlan();
        while(rs.next()) {
            int rowId = rs.getInt(1);
            int parent = rs.getInt(2);
            String operator = rs.getString(3);
            String target = rs.getString(4);
            String alias = rs.getString(5);
            float cost = rs.getFloat(6);

            plan.add(new OrclPlanDesc(rowId, parent, operator, target, alias, cost));
        }
        rs.close();
        stmt.close();

        plan.analyzePlan();
        return plan;
    }

    public void implement_configuration(autopilot ap, PhysicalConfiguration configuration, Connection conn) {
        try {
            Statement stmt = conn.createStatement();
            Statement stmt1 = conn.createStatement();
            int i = 0;
            for (String tableName : configuration.getIndexedTableNames()) {
                for (Index idx: configuration.getIndexesForTable(tableName)) {
                    int count;
                    if(idx.isImplemented()) continue;
                    synchronized (this) {
                        count = this.count++;
                    }
                    String indexName = "idxdesigner" + (100 + count);
                    String colNames = Joiner.on(", ").join(idx.getColumns());


                    final String q = String.format("create index %s on %s(%s) nosegment", indexName, tableName, colNames);
                    stmt.addBatch(q);
                    //stmt1.addBatch(String.format("call dbms_stats.gather_index_stats('%s','%s')",
                    //        Config.getUserName(), indexName));
                    idx.setImplementedName(indexName);
                }

                i++;
            }

            stmt.executeBatch();
            stmt.close();
            //stmt1.executeBatch();
            stmt1.close();
        }
        catch (Exception E) {
            System.out.println("implement_configuration: " + E.getMessage());
            E.printStackTrace();
        }
    }

    public void disable_configuration(Configuration config, Connection conn) {
        // does nothing
    }

    public void drop_configuration(PhysicalConfiguration config, Connection conn) {
        try {
            Statement stmt = conn.createStatement();
            for (String tableName : config.getIndexedTableNames()) {
                for (Index idx: config.getIndexesForTable(tableName)) {
                    if(idx.isImplemented())
                        stmt.addBatch("drop index " + idx.getImplementedName());
                }
            }

            stmt.executeBatch();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }        
    }

    //rewrite a query to use the indexes in a config
    public String prepareQueryForEnumeration(QueryDesc QD, PhysicalConfiguration config, boolean iac, boolean nlj) {
        throw new UnsupportedOperationException();
    }

    public String prepareQueryForConfiguration(QueryDesc QD, PhysicalConfiguration config, boolean iac, boolean nlj) {
        throw new UnsupportedOperationException();
    }

    public void removeQueryPrepareation(QueryDesc qd) {
    }
}
