package edu.ucsc.dbtune.tools.cmudb.autopilot;
//Class to retrieve all needed information from a database at initialization time

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

public class MSGlobalInfo {

    public HashMap global_table_info;
    Connection conn;
    public HashMap views;


    public String view(String name) {
        return (String) views.get(name.toUpperCase());
    }


    void create_pk_statistics() {

        String Q = "select o.name as oname, i.name as iname from sysindexes i, sysobjects o where i.id = o.id and i.indid = 1 and o.xtype = 'U'";
        try {
            Statement stmt = conn.createStatement();
            stmt.execute(Q);
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                String oname = rs.getString("oname");
                String iname = rs.getString("iname");
                String Q1 = "update statistics " + oname + " " + iname + " with fullscan ";
                System.err.println("create_pk_statistics: " + Q1);
                Statement stmt2 = conn.createStatement();
                stmt2.execute(Q1);
                stmt2.close();
            }
        }

        catch (Exception e) {
            System.err.println("pk_statistics: " + e.getMessage());
        }
    }


    String mod_dash(String s) {
        int i = 0;
        String result = new String("");
        char[] c = new char[1];
        for (i = 0; i < s.length(); i++) {
            c[0] = s.charAt(i);
            if (c[0] == '_')
                result += "[_]";
            else {

                result += new String(c);
            }
        }
        return result;
    }


    public int create_base_statistics(String col_name, String base_name) {
        String Q = "create statistics _WA_Sys_" + col_name + " on " + base_name + " (" + col_name + ")";

        System.err.println("create_base_statistics: " + Q);
        try {
            Statement mystmt = conn.createStatement();
            mystmt.execute(Q);
            mystmt.close();
        }

        catch (Exception e) {
            System.err.println("create_base_statistics: " + e.getMessage());
        }
        return 0;
    }


    void get_base_statistics(ColumnInfo theInfo, String table_name) {
        String col_name_dash = mod_dash(theInfo.getColName());
        String Q = "select i.id as ID, i.indid as INDID from sysindexes i, sysobjects o where i.id = o.id and i.name like '[_]WA[_]Sys[_]" + col_name_dash + "%' and o.name = '" + table_name + "'";

        try {
            Statement mystmt = conn.createStatement();
            theInfo.sysindexes_id = 0;
            theInfo.sysindexes_indid = 0;


            mystmt.execute(Q);
            ResultSet rs = mystmt.getResultSet();

            while (rs.next()) {
                theInfo.sysindexes_id = rs.getInt("ID");
                theInfo.sysindexes_indid = rs.getInt("INDID");
            }

            if (theInfo.sysindexes_id == 0 && theInfo.sysindexes_indid == 0) {
                create_base_statistics(theInfo.getColName(), table_name);
            }

            mystmt.close();
        }
        catch (Exception e) {
            System.err.println("get_base_statistics error: " + e.getMessage());
        }
    }

    public MSGlobalInfo(Connection conn) {
        try {
            global_table_info = new HashMap();

            //Retrieve all the tables...
            String Q1 = "select o.name as NAME, i.rowcnt as ROWCNT, i.dpages as DPAGES from sysindexes i, sysobjects o where o.id = i.id and (indid = 1 or indid = 0)  and o.xtype = 'U'";
            Statement stmt = conn.createStatement();
            stmt.execute(Q1);
            ResultSet rs1 = stmt.getResultSet();

            while (rs1.next()) {
                //Process each table
                String name = rs1.getString("NAME").toLowerCase();
                int rowCount = rs1.getInt("ROWCNT");
                int dpages = rs1.getInt("DPAGES");

                table_info theInfo = get_table_info(conn, name, rowCount, dpages);

                global_table_info.put(theInfo.table_name, theInfo);
            }
        }


        catch (Exception e) {
            System.err.println("MSGlobalInfo: " + e.getMessage());
            e.printStackTrace();
        }

    }

    public table_info get_table_info(Connection conn, String name, int rowCount, int dpages) throws SQLException {
        table_info theInfo = new table_info();
        theInfo.table_name = new String(name);
        theInfo.row_count = rowCount;
        theInfo.col_info = new HashMap();
        theInfo.dpages = dpages;

        //System.err.println("*** TABLE NAME: " + theInfo.table_name);

        //Obtain primary key columns
        ArrayList theKeys = new ArrayList();
        String Q3 = "sp_pkeys " + theInfo.table_name;
        Statement stmt3 = conn.createStatement();
        stmt3.execute(Q3);
        ResultSet rs3 = stmt3.getResultSet();

        //System.err.println("\t*** PRIMARY KEYS");
        int rs_size = 0;
        while (rs3.next()) {
            rs_size++;
            String theKey = new String(rs3.getString("COLUMN_NAME"));
            theKeys.add(theKey.toUpperCase());
            //System.err.println("\t*** " + theKey);

        }
        stmt3.close();

        theInfo.pkeys = new ArrayList();
        for (int k = 0; k < rs_size; k++)
            theInfo.pkeys.add(new Integer(0));

        //Obtain column information
        String Q2 = "select c.name as col_name, t.name as col_type, case t.variable when 0 then c.length else -c.length end as col_size , c.xprec as prec, c.xscale as scale, c.isnullable as nullable from sysobjects o, syscolumns c, systypes t where o.id = c.id and c.xtype = t.xtype and o.name = '" + theInfo.table_name + "'";
        Statement stmt2 = conn.createStatement();
        stmt2.execute(Q2);
        ResultSet rs2 = stmt2.getResultSet();
        while (rs2.next()) {
            //Process each column
        	ColumnInfo theColInfo = new ColumnInfo();
            theColInfo.colName = new String(rs2.getString("col_name").toUpperCase());
            theColInfo.colType = new String(rs2.getString("col_type"));
            theColInfo.col_size = rs2.getInt("col_size");
            theColInfo.precision = rs2.getInt("prec");
            theColInfo.scale = rs2.getInt("scale");
            theColInfo.nullable = rs2.getInt("nullable") == 1;

            if (theColInfo.getColType().equals("char") || theColInfo.getColType().equals("varchar")) {
                int theSize = theColInfo.col_size;
                if (theSize < 0)
                    theSize = -theSize;

                theColInfo.colType += "(" + theSize + ")";
            }

            if (theColInfo.getColType().equals("decimal")) {
                theColInfo.colType += " (" + theColInfo.precision + " , " + theColInfo.scale + ")";
            }

            //Sort out the primary keys...
            int theIndex = theKeys.indexOf(theColInfo.getColName());
            if (theIndex != -1) {
                theColInfo.is_pk = true;
                theInfo.pkeys.set(theIndex, theColInfo);
            }

            //System.err.println("\t*** " + theColInfo.col_name + " " + theColInfo.col_type + " " + theColInfo.col_size + " " + theColInfo.sysindexes_id + " " + theColInfo.sysindexes_indid + " " + theColInfo.is_pk);

            theInfo.col_info.put(theColInfo.getColName(), theColInfo);
        }

        stmt2.close();
        return theInfo;
    }

}