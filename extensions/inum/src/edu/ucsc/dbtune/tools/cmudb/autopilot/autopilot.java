package edu.ucsc.dbtune.tools.cmudb.autopilot;

import edu.ucsc.dbtune.tools.cmudb.Config;
import edu.ucsc.dbtune.tools.cmudb.model.Configuration;
import edu.ucsc.dbtune.tools.cmudb.model.Index;
import edu.ucsc.dbtune.tools.cmudb.model.PhysicalConfiguration;
import edu.ucsc.dbtune.tools.cmudb.model.Plan;
import edu.ucsc.dbtune.tools.cmudb.model.QueryDesc;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

import snaq.db.ConnectionPool;

public class autopilot {

    MSGlobalInfo G;
    static int count;
    String url, user, pass;
    ThreadLocalConnection tlc = new ThreadLocalConnection();
    AutoPilotDelegate delegate;
    private Set initedConnection = new HashSet();
    private ConnectionPool cp;
    private static final Logger _log = Logger.getLogger("autopilot");
    private Map indexSizes = new HashMap();
    
    public autopilot() {
        switch (Config.TYPE) {
            case DB2:
                this.delegate = new Db2AutoPilotDelegate(this);
                break;
            case MS:
                this.delegate = new MSAutoPilotDelegate();
                break;
            case ORCL:
                this.delegate = new OrclAutoPilotDelegate();
                break;
            case PGSQL:
                this.delegate = new PostgresAutoPilotDelegate(this);
                break;
        }
    }

    public void init_database() {
        try {

            Connection result_conn;

            Properties props = edu.ucsc.dbtune.tools.cmudb.Config.getDatabaseProperties();
            Class.forName(props.getProperty("driver"));

            this.url = props.getProperty("url");
            this.url = url.replace("${database}",props.getProperty("database")).trim();
            this.user = props.getProperty("user");
            this.pass = props.getProperty("password");

            cp = new ConnectionPool("default",
 	                                         5,
	                                         10,
	                                         600000,
	                                         this.url,
	                                         this.user,
	                                         pass);

            result_conn = getConnection();

            delegate.init_database(result_conn);

            result_conn.close();
        }
        catch (Exception e) {
            System.err.println("implement::implement(): Failed to connect " + e.getMessage());
        }
        
        // Thread.dumpStack();
    }

    public Connection getConnection() { 
        // get a connection from the connection pool.
        try {
            Connection conn = cp.getConnection(1000);
            if(!initedConnection.contains(conn)) {
                _log.info("getConnection: getting a new connection");
                delegate.prepareConnection(conn);
                initedConnection.add(conn);
            }

            return conn;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Cann't get a connection to the database");
        }
    }

    public synchronized void freeConnection(Connection conn) {
        try {
            conn.close();
        } catch (Exception ex) {
        }
    }

    public void dispose() {
        // Thread.dumpStack();
        cp.release();
    }

    public Plan optimizer_cost(String Q) {
        return optimizer_cost(Q, false, false);
    }

    public String prepareQueryForConfiguration(QueryDesc qd, PhysicalConfiguration config, boolean iac, boolean nlj) {
        return delegate.prepareQueryForConfiguration(qd, config, iac, nlj);
    }

    public String prepareQueryForEnumeration(QueryDesc qd, PhysicalConfiguration config, boolean iac, boolean nlj) {
        return delegate.prepareQueryForEnumeration(qd, config, iac, nlj);
    }

    public Plan optimizer_cost(String Q, boolean accessCost, boolean nlj) {
        String query = null;
        try {
            Connection conn = getConnection();
            Plan plan;
            try {
                plan = delegate.getExecutionPlan(conn, Q);
            } finally {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            return plan;
        }
        catch (Exception E) {
            System.err.println("optimizer_cost: " + query);
            E.printStackTrace();
            return null;
        }
    }

    int row_count(Index configuration) {
        try {
            String tableName = configuration.getTableName();
            table_info ti = (table_info) G.global_table_info.get(tableName.toLowerCase());
            return ti.row_count;
        }
        catch (Exception E) {
            E.printStackTrace();
            return -1;
        }
    }

    //returns the total size of the config in terms of 8K pages ...
    public synchronized void implement_configuration(PhysicalConfiguration configuration) {
        final Connection connection = getConnection();
        try {
            delegate.implement_configuration(this, configuration, connection);
        } finally {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized String getCreateSQL(PhysicalConfiguration config) {
        return delegate.getCreateStatements(config);
    }

    public synchronized String getDropSQL(PhysicalConfiguration config) {
        return delegate.getDropStatements(config);
    }

    public void disable_configuration(Configuration config) {
        if(config.implementedIndexes == null) {
            return;
        }
        
        Connection conn = getConnection();
        try {
            delegate.disable_configuration(config, conn);
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized void drop_configuration(PhysicalConfiguration configuration) {
        Connection conn = getConnection();
        try {
            delegate.drop_configuration(configuration, conn);
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public int getIndexSize(Index index) {
        Integer size = (Integer) indexSizes.get(index.getKey());
        if(size == null) {
            _log.info("getIndexSize: Uncached index: " + index.getKey());
            size = delegate.getIndexSize(index);
            indexSizes.put(index.getKey(), size);
        }

        return size;
    }

    public int getConfigSize(PhysicalConfiguration pc) {
        int size = 0;
        for (Iterator iterator = pc.indexes(); iterator.hasNext();) {
            Index index = (Index) iterator.next();
            size += getIndexSize(index);
        }
        return size;
    }

    public void removeQueryPreparation(QueryDesc desc) {
        delegate.removeQueryPrepareation(desc);
    }

    public void setIndexSizeMap(Map indexSizes) {
        this.indexSizes = indexSizes;
    }

    private class ThreadLocalConnection extends ThreadLocal {
        public Object initialValue() {
            try {
                Connection conn = DriverManager.getConnection(url, user, pass);
                delegate.prepareConnection(conn);
                return conn;
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}