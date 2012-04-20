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
import edu.ucsc.dbtune.seq.utils.Rt;
import edu.ucsc.dbtune.seq.utils.Rx;
import edu.ucsc.dbtune.util.Environment;
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

    public static void main(String[] args) throws Exception {
        Environment en = Environment.getInstance();
        DatabaseSystem db = newDatabaseSystem(en);
        DATTest2.testSet = "tpch-500-counts";
        DATTest2.querySize = 100;
        DATTest2.indexSize = 200;

        Workload workload = DATTest2.getWorkload(en);
        Set<Index> indexes = DATTest2.getIndexes(workload, db);

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
            if (System.in.available()>0)
                break;
        }
    }
}
