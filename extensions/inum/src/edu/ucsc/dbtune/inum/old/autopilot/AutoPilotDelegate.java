package edu.ucsc.dbtune.inum.old.autopilot;


import com.google.common.base.Joiner;
import edu.ucsc.dbtune.inum.old.Config;
import edu.ucsc.dbtune.inum.old.model.Configuration;
import edu.ucsc.dbtune.inum.old.model.Index;
import edu.ucsc.dbtune.inum.old.model.PhysicalConfiguration;
import edu.ucsc.dbtune.inum.old.model.Plan;
import edu.ucsc.dbtune.inum.old.model.QueryDesc;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Dec 10, 2007
 * Time: 7:19:42 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class AutoPilotDelegate {
    public abstract Plan getExecutionPlan(Connection conn, String Q) throws SQLException;

    public abstract void implement_configuration(autopilot ap, PhysicalConfiguration configuration, Connection conn);

    public abstract void disable_configuration(Configuration config, Connection conn);

    public abstract void drop_configuration(PhysicalConfiguration configuration, Connection conn);

    //rewrite a query to use the indexes in a config
    public abstract String prepareQueryForEnumeration(QueryDesc QD, PhysicalConfiguration config, boolean iac, boolean nlj);

    public abstract String prepareQueryForConfiguration(QueryDesc QD, PhysicalConfiguration config, boolean iac, boolean nlj);

    public void init_database(Connection conn) {
    }

    public void prepareConnection(Connection conn) throws SQLException {
    }

    public int getIndexSize(Index index) {
        return 0;
    }

    public int getConfigSize(PhysicalConfiguration pc) {
        return 0;
    }

    public abstract void removeQueryPrepareation(QueryDesc qd);

    //public abstract QueryColumnInfo getQueryInfo(QueryDesc qd);
    public String getCreateStatements(PhysicalConfiguration config) {
        int idxCount = 0;
        StringBuffer buf = new StringBuffer();

        for (Iterator iterator = config.indexes(); iterator.hasNext();) {
            Index index = (Index) iterator.next();
            String idxname = "index_" + idxCount++;
            String sql = "create index " + idxname + " on " + index.getTableName() + "(" + Joiner
                .on(", ").join(index.getColumns()) + ");" + Config.NewLine;
            buf.append(sql);
            index.setImplementedName(idxname);
        }

        return buf.toString();
    }

    public String getDropStatements(PhysicalConfiguration config) {
        StringBuffer buf = new StringBuffer();

        for (Iterator iterator = config.indexes(); iterator.hasNext();) {
            Index index = (Index) iterator.next();
            String idxname = index.getImplementedName();
            String sql = "drop index " + idxname + Config.NewLine;
            buf.append(sql);
        }

        return buf.toString();
    }
}
