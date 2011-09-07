package edu.ucsc.dbtune.inum.old.autopilot;
//Class to retrieve all needed information from a database at initialization time

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Arrays;

public class DB2GlobalInfo {

    public HashMap global_table_info;
    Connection conn;
    public HashMap views;


    public String view(String name) {
        return (String) views.get(name.toUpperCase());
    }


    public DB2GlobalInfo(Connection conn) {
        try {
            global_table_info = new HashMap();

            //Retrieve all the tables...
            String Q1 = "select tabname as NAME, card as ROWCNT, npages as DPAGES, KEYINDEXID as INDEXID from syscat.tables where tabschema = 'DB2ADMIN' and not(tabname like 'ADVISE_%' or tabname like 'EXPLAIN_%')";
            
            Statement stmt = conn.createStatement();
            stmt.execute(Q1);
            ResultSet rs1 = stmt.getResultSet();

            while (rs1.next()) {
                //Process each table
                String name = rs1.getString("NAME").toLowerCase();
                int rowCount = rs1.getInt("ROWCNT");
                int dpages = rs1.getInt("DPAGES");
                int indexId = rs1.getInt("INDEXID");

                table_info theInfo = get_table_info(conn, name, rowCount, dpages, indexId);

                global_table_info.put(theInfo.table_name, theInfo);
            }
        }


        catch (Exception e) {
            System.err.println("MSGlobalInfo: " + e.getMessage());
            e.printStackTrace();
        }

    }

    public table_info get_table_info(Connection conn, String name, int rowCount, int dpages, int indexId) throws SQLException {
        table_info theInfo = new table_info();
        theInfo.table_name = new String(name);
        theInfo.row_count = rowCount;
        theInfo.col_info = new HashMap();
        theInfo.dpages = dpages;

        //System.err.println("*** TABLE NAME: " + theInfo.table_name);

        //Obtain primary key columns
        ArrayList theKeys = new ArrayList();
        String Q3 = "select colnames from syscat.indexes where tabname = '"+name.toUpperCase()+"' and iid = " + indexId;
        Statement stmt3 = conn.createStatement();
        stmt3.execute(Q3);
        ResultSet rs3 = stmt3.getResultSet();

        //System.err.println("\t*** PRIMARY KEYS");
        String colNames = null;
        if (rs3.next()) {
            colNames = rs3.getString(1);

            String cols[] = colNames.split("\\+");
            theKeys.addAll(Arrays.asList(cols));
            String str = (String) theKeys.get(0);
            if(str.length() == 0) {
                theKeys.remove(0);
            }
        }
        rs3.close();
        stmt3.close();

        theInfo.pkeys = new ArrayList();
        for (int k = 0; k < theKeys.size(); k++)
            theInfo.pkeys.add(new Integer(0));

        //Obtain column information
        String Q2 = "select colname as col_name, typename as col_type, avgcollen as col_size , NULLS as nullable from syscat.columns where tabname = '" + theInfo.table_name.toUpperCase() + "'";
        Statement stmt2 = conn.createStatement();
        stmt2.execute(Q2);
        ResultSet rs2 = stmt2.getResultSet();
        while (rs2.next()) {
            //Process each column
        	ColumnInfo theColInfo = new ColumnInfo();
            theColInfo.colName = new String(rs2.getString("col_name").toUpperCase());
            theColInfo.colType = new String(rs2.getString("col_type"));
            theColInfo.col_size = rs2.getInt("col_size");
            theColInfo.nullable = rs2.getString("nullable").equals("Y");

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