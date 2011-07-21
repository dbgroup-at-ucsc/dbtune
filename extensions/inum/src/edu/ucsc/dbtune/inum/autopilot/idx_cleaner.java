package edu.ucsc.dbtune.inum.autopilot;

import edu.ucsc.dbtune.inum.Config;
import edu.ucsc.dbtune.inum.DbType;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: Debabrata Dash
 * Date: Mar 15, 2006
 * Time: 4:10:15 PM
 * The file is a copyright of CMU, all rights reserved.
 */
public class idx_cleaner {
    private static final Logger _log = Logger.getLogger("idx_cleaner");
    
    public static void main(String[] args) {
        String prefix = args.length >= 1 ? args[0] : getDefaultIndexName();

        autopilot ap = new autopilot();

        try {
            ap.init_database();
            Connection conn = ap.getConnection();
            if(Config.TYPE.equals(DbType.MS))
                cleanIndexesMS(prefix, conn);
            else if(Config.TYPE.equals(DbType.DB2))
                cleanIndexesDB2(prefix, conn);
            else if(Config.TYPE.equals(DbType.PGSQL)) {
                cleanIndexesPgSQL(prefix, conn);
            } else 
                throw new UnsupportedOperationException();
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        catch (Exception E) {
            E.printStackTrace();

        }
    }

    private static void cleanIndexesPgSQL(String prefix, Connection conn) throws SQLException {
        String query = "SELECT\n" +
                "c.relname as \"Name\",\n" +
                "CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' END as \"Type\",\n" +
                "c2.relname as \"Table\"\n" +
                "FROM pg_catalog.pg_class c\n" +
                "    JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid\n" +
                "    JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid\n" +
                "WHERE c.relkind IN ('i','') and c.relname like '" + prefix + "%'\n" +
                "ORDER BY 1,2;";

        cleanIndexes(conn, query);
    }

    public static String getDefaultIndexName() {
        if (Config.TYPE.equals(DbType.MS)) {
            return "idxdesigner";
        } else if(Config.TYPE.equals(DbType.DB2)){
            return "IDX";
        } else if(Config.TYPE.equals(DbType.PGSQL)){
            return "index_";
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public static void cleanIndexesDB2(String prefix, Connection conn) throws SQLException {
        String query = "delete from advise_index where name like '" + prefix +"%'";
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(query);
        stmt.close();
    }

    public static void cleanIndexesMS(String prefix, Connection conn) throws SQLException {
        String query = "select o.name+'.'+i.name from sysindexes i, sysobjects o where i.id = o.id and i.name like '" + prefix + "%'";

        cleanIndexes(conn, query);
    }

    private static void cleanIndexes(Connection conn, String query) throws SQLException {
        List idxes = new ArrayList();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
            String idxName = rs.getString(1);
            idxes.add(idxName);
        }
        rs.close();

        for (int i = 0; i < idxes.size(); i++) {
            String idxName = (String) idxes.get(i);
            String update = "drop index " + idxName;

            _log.info("cleanIndexes: " + update);
            Statement stmt1 = conn.createStatement();
            stmt1.execute(update);
            stmt1.close();
        }
    }
}
