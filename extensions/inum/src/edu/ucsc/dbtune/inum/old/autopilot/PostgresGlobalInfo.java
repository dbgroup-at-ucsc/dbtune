/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.ucsc.dbtune.inum.old.autopilot;

import edu.ucsc.dbtune.inum.old.Config;
import edu.ucsc.dbtune.inum.old.commons.Utils;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Vector;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class PostgresGlobalInfo {

    /*TableName / columns*/
    public Hashtable<String, Vector<ColumnInfo>> global_table_info = new Hashtable<String, Vector<ColumnInfo>>();
    ;
    private static final Logger log = Logger.getLogger(PostgresGlobalInfo.class);
    private Connection conn;
    private Hashtable<String, String> primaryKeys;

    public Hashtable<String, Double> sizes = new Hashtable();
    private Map<String, Vector<String>> global_column_to_table_map = new HashMap();

    public Connection getConn() {
        return conn;
    }

    public void setConn(Connection conn) {
        this.conn = conn;
    }

    public PostgresGlobalInfo() {
        conn = null;
    }

    public void getConnection() {
        try {
            Properties props = Config.getDatabaseProperties();
            String url = props.getProperty("url");
            Class.forName(props.getProperty("driver"));
            conn = DriverManager.getConnection(url.replace("${database}", props.getProperty("database")).trim(), props);
        } catch (Exception e) {
            log.error("No connection to the server", e);
        }
    }

    public void closeConnection() {
        try {
            conn.close();
        } catch (SQLException e) {
            log.error("Error closing connection to the server", e);
        }
        conn = null;
    }

    //read table info from db
    public void getTableInfo() {
        if (!global_table_info.isEmpty()) return;

        try {
            Statement st1 = getConn().createStatement();
            String sqlquery = "select relname,relpages, reltuples, relfilenode from pg_class where relnamespace = 2200 and relam =0";
            PreparedStatement pst = getConn().prepareStatement(sqlquery);
            ResultSet res = st1.executeQuery(sqlquery);
            String tbName = "";
            int relpages = 0;
            float rrows = 0;
            int reloid = 0;

            while (res.next()) {
                tbName = (String) res.getString(1);

                relpages = res.getInt(2);
                rrows = res.getFloat(3);
                reloid = res.getInt(4);

                int relrows = Float.floatToIntBits(rrows);
                Vector<ColumnInfo> theInfo = getTableColumnInfo(tbName, relpages, relrows, reloid);
                global_table_info.put(tbName, theInfo);
            }
        } catch (SQLException ex) {
            Logger.getLogger(PostgresGlobalInfo.class.getName()).log(Level.ERROR, null, ex);
        }
    }

    public Hashtable<String, Double> getDatabaseTableSize() {
        Iterator iterator = global_table_info.keySet().iterator();
        while (iterator.hasNext()) {
            String tableName = (String) iterator.next();
            if (!sizes.containsKey(tableName)) {
                Double size = this.getRelPages(tableName);
                if (size != null)
                    sizes.put(tableName, size);
            }
        }
        return sizes;
    }

    //read table info from db
    public Vector<ColumnInfo> getTableInfo(String tableName) {
        Vector<ColumnInfo> theInfo = null;
        try {
            Statement st1 = conn.createStatement();
            String sqlquery = "select relpages, reltuples, relfilenode from pg_class where " +
                    "relnamespace = 2200 and relam =0 and relname = '" + tableName + "'";
            PreparedStatement pst = conn.prepareStatement(sqlquery);
            ResultSet res = st1.executeQuery(sqlquery);

            int relpages = 0;
            float rrows = 0;
            int reloid = 0;

            while (res.next()) {
                relpages = res.getInt(1);
                rrows = res.getFloat(2);
                reloid = res.getInt(3);

                int relrows = Float.floatToIntBits(rrows);
                theInfo = getTableColumnInfo(tableName, relpages, relrows, reloid);
            }
        } catch (SQLException ex) {
            Logger.getLogger(PostgresGlobalInfo.class.getName()).log(Level.ERROR, null, ex);
        }
        return theInfo;
    }

    /* *
      * Get: attname, attnum, atttypid
      * select attname, attnum, atttypid from pg_attribute where attrelid = 'TableName' and attnum > 0
      * */

    public Vector<ColumnInfo> getTableColumnInfo(String tbName, int relpages, int relrows, int reloid) {
        Vector<ColumnInfo> columns = new Vector<ColumnInfo>();

        try {
            Statement st1 = getConn().createStatement();
            //			String sqlquery = "select attname, attnum, atttypid from pg_attribute where attnum > 0 and attrelid = " +reloid;
            String sqlquery = "select attname, attnum, atttypid, attnotnull from pg_attribute where attnum > 0 and attrelid = " + reloid;
            PreparedStatement pst = getConn().prepareStatement(sqlquery);
            ResultSet res = st1.executeQuery(sqlquery);

            int x = 0;
            while (res.next()) {
                //process each column
                ColumnInfo columnInfo = new ColumnInfo();

                columnInfo.setColName(res.getString(1));
                columnInfo.setAttnum(res.getInt(2));
                x = (Integer) res.getInt(3);
                String tip = getColumnType(x);
                String nullable = res.getString(4);
                if (nullable.compareTo("f") == 0)
                    columnInfo.setNullable(true);
                else
                    columnInfo.setNullable(false);
                columnInfo.setAtttypid(x);
                columnInfo.setColType(tip);

                columns.add(columnInfo);
            } //end process each column
        } catch (SQLException ex) {
            Logger.getLogger(PostgresGlobalInfo.class.getName()).log(Level.ERROR, null, ex);
        }
        return columns;
    }

    public Properties getProperties() {
        Properties props = new Properties();
        Iterator iterator = global_table_info.keySet().iterator();
        while (iterator.hasNext()) {
            String tableName = (String) iterator.next();
            Vector<ColumnInfo> temp = (Vector<ColumnInfo>) global_table_info.get(tableName);
            for (int i = 0; i < temp.size(); i++)
                props.setProperty(temp.get(i).getColName().toUpperCase(), tableName);
        }
        return props;
    }

    /*Get tables containing columnName*/
    public Vector<String> getTables(String columnName) {
        if (!global_column_to_table_map.containsKey(columnName)) {
            Vector<String> names = new Vector<String>();
            Iterator iterator = global_table_info.keySet().iterator();
            while (iterator.hasNext()) {
                String tableName = (String) iterator.next();
                Vector<ColumnInfo> temp = global_table_info.get(tableName);
                for (int i = 0; i < temp.size(); i++) {
                    if (temp.get(i).getColName().compareTo(columnName.toLowerCase()) == 0 || temp.get(i).getColName().compareTo(columnName.toUpperCase()) == 0)
                        names.add(tableName);
                }
            }

            global_column_to_table_map.put(columnName, names);
            return names;
        }

        return global_column_to_table_map.get(columnName);
    }

    /*Get list of tables*/
    public Vector<String> getTables() {
        Vector<String> names = new Vector<String>();
        Iterator iterator = global_table_info.keySet().iterator();
        while (iterator.hasNext())
            names.add((String) iterator.next());
        Collections.sort(names);
        return names;
    }

    /*Get primary Keys: return <TableName, Column>*/
    public Hashtable<String, String> getPrimaryKeys() {
        boolean open = true;
        if (conn == null) {
            getConnection();
            open = false;
        }
        Hashtable<String, String> primaryKeys = new Hashtable<String, String>();
        try {
            Statement st1 = getConn().createStatement();
            String sqlquery = "select table_name, column_name  from information_schema.constraint_column_usage " +
                    "where constraint_name LIKE '%_pkey'";
            ResultSet res = st1.executeQuery(sqlquery);

            String tableName;
            String column;
            while (res.next()) {
                tableName = res.getString(1);
                column = res.getString(2);
                primaryKeys.put(tableName, column.toLowerCase());
            }
        } catch (SQLException ex) {
            Logger.getLogger(PostgresGlobalInfo.class.getName()).log(Level.ERROR, null, ex);
            ex.printStackTrace();
        }
        if (open == false)
            closeConnection();
        return primaryKeys;
    }

    public String getPrimaryKey(String tableName) {
        if (this.primaryKeys == null || !this.primaryKeys.containsKey(tableName)) {
            this.primaryKeys = getPrimaryKeys();
        }
        return primaryKeys.get(tableName);
    }

    /*Get list of columns for a table*/

    public Vector<ColumnInfo> getColumns(String tableName) {
        return global_table_info.get(tableName);
    }

    public Hashtable<String, Vector<ColumnInfo>> getDatabase() {
        return global_table_info;
    }

    public ResultSet getMinMaxRows(String tableName, String attribute) {
        ResultSet res = null;
        boolean open = true;
        if (conn == null) {
            getConnection();
            open = false;
        }
        try {
            Statement st1 = getConn().createStatement();
            String sqlquery = "select max(" + attribute + "), min(" + attribute + "), count(*) from " + tableName;
            System.out.println("Query:" + sqlquery);
            PreparedStatement pst = getConn().prepareStatement(sqlquery);
            res = st1.executeQuery(sqlquery);
        } catch (SQLException ex) {
            Logger.getLogger(PostgresGlobalInfo.class.getName()).log(Level.ERROR, null, ex);
        }
        if (open == false)
            closeConnection();
        return res;
    }

    /*Get number of tuples*/

    public Integer getNumberOfTuples(String tableName) {
        Integer val = 0;
        ResultSet res = null;
        boolean open = true;
        if (conn == null) {
            getConnection();
            open = false;
        }
        try {
            Statement st1 = getConn().createStatement();
            String sqlquery = "select reltuples from pg_class where relname = '" + tableName + "'";
            //System.out.println("Query:" +sqlquery);
            PreparedStatement pst = getConn().prepareStatement(sqlquery);
            res = st1.executeQuery(sqlquery);
            if (res != null) {
                while (res.next())
                    val = new Integer(res.getString(1));
            }
        } catch (SQLException ex) {
            Logger.getLogger(PostgresGlobalInfo.class.getName()).log(Level.ERROR, null, ex);
        }
        if (open == false)
            closeConnection();
        return val;
    }

    /*
         null_frac //fraction of column entries that are null
         avg_width //average width per entry in column (bytes)
         most_common_vals; //most common values in the column
         most_common_freqs; //frequencies of the most common values
         histogram_bounds; // list of values that divide the column's values into groups of approximately equal population
     */
    public ResultSet getStatistics(String tableName, String attribute) {
        //SELECT null_frac, n_distinct, most_common_vals, most_common_freqs FROM pg_stats WHERE tablename='name' AND attname='name';
        ResultSet res = null;
        boolean open = true;
        if (conn == null) {
            getConnection();
            open = false;
        }
        try {
            Statement st1 = getConn().createStatement();
            String sqlquery = "select null_frac, avg_width, n_distinct, most_common_vals, most_common_freqs, histogram_bounds from pg_stats where tablename = '" + tableName + "' and attname = '" + attribute + "'";
            System.out.println("Query:" + sqlquery);
            PreparedStatement pst = getConn().prepareStatement(sqlquery);
            res = st1.executeQuery(sqlquery);
        } catch (SQLException ex) {
            Logger.getLogger(PostgresGlobalInfo.class.getName()).log(Level.ERROR, null, ex);
        }
        if (open == false)
            closeConnection();
        return res;
    }

    /* *
      * relfilenode: select relfilenode, reltuples from pg_class where relname ='TableName';
      * Get Name of the on-disk file of this relation; 0 if none
      * */
    public String getrelfilenodeANDreltuples(String tableName) {
        String relfilenode = "";
        String reltuples = "";
        String relpages = "";
        boolean open = true;
        if (conn == null) {
            getConnection();
            open = false;
        }
        try {
            Statement st1 = getConn().createStatement();
            String sqlquery = "select relfilenode, reltuples, relpages from pg_class where relname = '" + tableName + "'";
            System.out.println("sqlquery:"+sqlquery);
            PreparedStatement pst = getConn().prepareStatement(sqlquery);
            ResultSet res = st1.executeQuery(sqlquery);

            if (res != null) {
                while (res.next()) {
                    relfilenode = new String(res.getString(1));
                    reltuples = new String(res.getString(2));
                    relpages = new String(res.getString(3));
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(PostgresGlobalInfo.class.getName()).log(Level.ERROR, null, ex);
        }
        if (open == false)
            closeConnection();
        return relfilenode + "_" + reltuples + "_" + relpages;
    }

    /*
         insert into pg_statistic (
                     select newtableoid, ncols_char, stanullfrac, stawidth, stadistinct,
                     stakind1, stakind2, stakind3, stakind4, staop1, staop2, staop3, staop4,
                     stanumbers1, stanumbers2, stanumbers3, stanumbers4, stavalues1, stavalues2, stavalues3, stavalues4
                     from pg_statistic where starelid =  "OLDTABLEOID" and staattnum =  COLUMNOID)
      */

    public void updatepgStatisticsSlow(String tableName, String newTableOID, String oldTableOID)
    {
        int counter = 1;
        boolean open = true;
        if( conn == null){
            getConnection();
            open = false;
        }
        Vector<ColumnInfo> list = global_table_info.get(tableName);
        for (int i = 0; i < list.size(); i++)
        {
            String command = "insert into pg_statistic ( select "+newTableOID+", "+counter+", stanullfrac, stawidth, stadistinct," +
            " stakind1, stakind2, stakind3, stakind4, staop1, staop2, staop3, staop4," +
            "stanumbers1, stanumbers2, stanumbers3, stanumbers4, stavalues1, stavalues2, stavalues3, stavalues4" +
            " from pg_statistic where starelid = "+oldTableOID +" and staattnum = ";
            counter++;
            try {
                Statement st1 = getConn().createStatement();
                String sqlquery = command + list.get(i).getAttnum() + ")";
                PreparedStatement pst = getConn().prepareStatement(sqlquery);
                pst.executeUpdate();
                pst.close();
            } catch (SQLException ex) {
                Logger.getLogger(PostgresGlobalInfo.class.getName()).log(Level.ERROR, null, ex);}
        }
        if( open == false)
            closeConnection();
    }

    public void updatepgStatistics(String tableName, String newTableOID, String oldTableOID) {
        int counter = 1;
        boolean open = true;
        if (conn == null) {
            getConnection();
            open = false;
        }
        Vector<ColumnInfo> list = global_table_info.get(tableName);
        List attnums = new ArrayList();
        for (int i = 0; i < list.size(); i++) {
            attnums.add(list.get(i).attnum);
        }
        Collections.sort(attnums);
        Integer minattnum = (Integer) attnums.get(0);

        String command = "insert into pg_statistic ( select " + newTableOID + ", staattnum , stanullfrac, stawidth, stadistinct," +
                " stakind1, stakind2, stakind3, stakind4, staop1, staop2, staop3, staop4," +
                "stanumbers1, stanumbers2, stanumbers3, stanumbers4, stavalues1, stavalues2, stavalues3, stavalues4" +
                " from pg_statistic where starelid = " + oldTableOID + " and staattnum in (" + Utils
            .join(attnums, ",") + "))";
        try {
            PreparedStatement pst = getConn().prepareStatement(command);
            pst.executeUpdate();
            pst.close();
        } catch (SQLException ex) {
            Logger.getLogger(PostgresGlobalInfo.class.getName()).log(Level.ERROR, null, ex);
            ex.printStackTrace();
        }
        if (open == false)
            closeConnection();
    }

    /* *
      *update pg_class set reltuples = reltuples, relpages = relpages where relname = 'newTableName'
      * */
    public void updatepgClass(String newTableName, String reltuples, String relpages) {
        boolean open = true;
        if (conn == null) {
            getConnection();
            open = false;
        }
        try {
            Statement st1 = getConn().createStatement();
            String sqlquery = "update pg_class set reltuples = " + reltuples + ", relpages = " + relpages + " where relname = '" + newTableName + "'";
            PreparedStatement pst = getConn().prepareStatement(sqlquery);
            pst.executeUpdate();
            pst.close();
        } catch (SQLException ex) {
            Logger.getLogger(PostgresGlobalInfo.class.getName()).log(Level.ERROR, null, ex);
        }
    }

    /*Get partition selectivity*/
    public int getSelectivity(String tableName, String wherePart) {
        ResultSet res = null;
        int rows = 0, estRows = 0;
        boolean open = true;
        if (conn == null) {
            getConnection();
            open = false;
        }
        try {
            Statement st1 = getConn().createStatement();
            String explainQuery = "explain select * from " + tableName + " where " + wherePart;

            /*
                        // Dash: this used to take lots of time.
                        String sqlquery = "select count(*) from "+tableName+" where "+wherePart;
                        //System.out.println("Query:" +sqlquery);
                        res = st1.executeQuery(sqlquery);
                        if( res != null ){
                            while (res.next()) {
                                rows = new Integer(res.getString(1));
                            }
                        }
                        res.close();
            */

            res = st1.executeQuery(explainQuery);
            if (res.next()) {
                String topRow = res.getString(1);
                int idx = topRow.indexOf("rows=");
                if (idx >= 0) {
                    estRows = new Scanner(topRow.substring(idx + 5)).nextInt();
                }
            }
            res.close();
            st1.close();
        } catch (SQLException ex) {
            Logger.getLogger(PostgresGlobalInfo.class.getName()).log(Level.ERROR, null, ex);
        }


        if (open == false)
            closeConnection();
        return estRows;
    }

    public double getTableSize(String tableName) {
        ResultSet res = null;
        double size = 0;
        boolean open = true;
        if (conn == null) {
            getConnection();
            open = false;
        }
        try {
            Statement st1 = getConn().createStatement();
            String sqlquery = "select pg_size_pretty(pg_relation_size('" + tableName + "'))";
            PreparedStatement pst = getConn().prepareStatement(sqlquery);
            res = st1.executeQuery(sqlquery);
            if (res != null) {
                while (res.next()) {
                    size = new Double(res.getString(1).substring(0, res.getString(1).indexOf(' ')));
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(PostgresGlobalInfo.class.getName()).log(Level.ERROR, null, ex);
        }
        if (open == false)
            closeConnection();
        return size;
    }

    public double getRelPages(String virtualTableName) {
        ResultSet res = null;
        int size = 0;
        boolean open = true;
        if (conn == null) {
            getConnection();
            open = false;
        }
        try {
            Statement st1 = getConn().createStatement();
            String sqlquery = "select relpages from pg_class where relname = '" + virtualTableName + "'";
            PreparedStatement pst = getConn().prepareStatement(sqlquery);
            res = st1.executeQuery(sqlquery);
            if (res != null) {
                while (res.next()) {
                    size = new Integer(res.getString(1));
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(PostgresGlobalInfo.class.getName()).log(Level.ERROR, null, ex);
        }
        if (open == false)
            closeConnection();
        return size;
    }

    public void executeQuery(String query) {
        try {
            Statement st1 = getConn().createStatement();
            PreparedStatement pst = getConn().prepareStatement(query);
            pst.executeQuery();
            pst.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
            System.out.println("Exception: " + ex.getMessage());
            //Logger.getLogger(Partitioning.class.getName()).log(Level.ERROR, null, ex);
        }
    }

    public void printInfo() {
        Iterator iterator = global_table_info.keySet().iterator();
        while (iterator.hasNext()) {
            String tableName = (String) iterator.next();
            System.out.println("Table: " + tableName);
            System.out.println(global_table_info.get(tableName));
        }
    }

    public static String getColumnType(int x) {
        if (x == 23)
            return "integer";
        else if (x == 20)
            return "bigint";
        else if (x == 21)
            return "smallint";
        else if (x == 17)
            return "bytea";
        else if (x == 701)
            return "double precision";
        else if (x == 1043)
            return "varchar";
        else if (x == 1042)
            return "character";
        else if (x == 1700)
            return "numeric(8,2)";
        else if (x == 1082)
            return "date";
        else if (x == 1114)
            return "timestamp";
        else
            System.out.println("Unknown type: " + x);
        return "";
    }

}
