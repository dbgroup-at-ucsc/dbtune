package edu.ucsc.dbtune.deployAware.test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;

import java.io.File;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.utils.RTimerN;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

public class CreateIndexTest2 {
    static String costSQL;

    public static double getCreateIndexCost(Optimizer optimizer, Index a)
            throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("select ");
        boolean first = true;
        for (Column col : a) {
            if (first)
                first = false;
            else
                sql.append(",");
            sql.append(col.getName());
        }
        sql.append(" from " + a.getSchema().getName() + "."
                + a.getTable().getName() + " order by");
        first = true;
        for (Column col : a) {
            if (first)
                first = false;
            else
                sql.append(",");
            sql.append(" " + col.getName()
                    + (a.isAscending(col) ? " asc" : " desc"));
        }
        costSQL = sql.toString();
        SQLStatementPlan sqlPlan = optimizer.explain(sql.toString()).getPlan();
        return sqlPlan.getRootOperator().getAccumulatedCost();
    }

    public static String getCreateIndexCmd(String name, Index a)
            throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("create index  " + name);
        sql.append(" on " + a.getSchema().getName() + "."
                + a.getTable().getName() + " (");
        boolean first = true;
        for (Column col : a) {
            if (first)
                first = false;
            else
                sql.append(",");
            sql.append(" " + col.getName()
                    + (a.isAscending(col) ? " asc" : " desc"));
        }
        sql.append(")");
        return sql.toString();
    }

    public static void testIndex() throws Exception {
        Environment en = Environment.getInstance();
        DatabaseSystem db = newDatabaseSystem(en);
        WorkloadLoader loader=new WorkloadLoader("test","tpch-500-counts","workload.sql","recommend");

        Workload workload = loader.getWorkload(en);
        Set<Index> indexes = loader.getIndexes(workload, db);

        InumOptimizer inumoptimizer = (InumOptimizer) db.getOptimizer();
        DB2Optimizer db2optimizer = (DB2Optimizer) inumoptimizer.getDelegate();

        int id = 0;
        for (Index index : indexes) {
            String indexName = "tst_" + id;
            double cost = getCreateIndexCost(db2optimizer, index);
            String create = getCreateIndexCmd(indexName, index);
            id++;
            Statement st = db.getConnection().createStatement();
            RTimerN timer = new RTimerN();
            st.execute(create);
            double createTime = timer.getSecondElapse();
            st.close();

            String drop = "drop index " + indexName;
            st = db.getConnection().createStatement();
            timer = new RTimerN();
            st.execute(drop);
            double dropTime = timer.getSecondElapse();
            st.close();

            Rt.np(cost + "\t" + createTime + "\t" + dropTime + "\t"
                    + index.toString() + "\t" + costSQL + "\t" + create + "\t"
                    + drop);
            if (System.in.available() > 0)
                break;
        }
    }

    public static void main(String[] args) throws Exception {
        WorkloadLoader loader=new WorkloadLoader("test","OST","workload.sql","recommend");
        Environment en = Environment.getInstance();
        en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/" + loader.dbName);
        en.setProperty("username", "db2inst1");
        en.setProperty("password", "db2inst1admin");
        en.setProperty("workloads.dir", "resources/workloads/db2");
        DatabaseSystem db = newDatabaseSystem(en);

        Workload workload = loader.getWorkload(en);
        InumOptimizer inumoptimizer = (InumOptimizer) db.getOptimizer();
        DB2Optimizer db2optimizer = (DB2Optimizer) inumoptimizer.getDelegate();

        for (int i = 0; i < workload.size(); i++) {
            SQLStatement sql = workload.get(i);
            double cost = db2optimizer.explain(sql).getTotalCost();
            Statement st = db.getConnection().createStatement();
            RTimerN timer = new RTimerN();
            st.execute(sql.getSQL());
            double time = timer.getSecondElapse();
            st.close();
            Rt.np(cost + "\t" + time+"\t"+(cost/time));
            if (System.in.available() > 0)
                break;
        }
    }
}
