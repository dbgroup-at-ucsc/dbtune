package edu.ucsc.dbtune.bip;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.util.Environment;
import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.metadata.SQLTypes.isDateTimeLiteral;
import static edu.ucsc.dbtune.metadata.SQLTypes.isString;


public class RefreshFunctionTPCHGeneration 
{
    protected static DatabaseSystem db;
    protected static Environment    en;
    
    /**
     * Setup for the test.
     */
    @BeforeClass
    public static void beforeClassSetUp() throws Exception
    {
        en = Environment.getInstance();
        db = newDatabaseSystem(en);
    }
    
    @Test
    public void testInsertUpdateGeneration() throws Exception
    {
        // delete statements
        constructDeleteStmts();
        
        // insert statements
        constructInsertStmts();
    }
    
    
    /**
     * Read in the file name and construct the corresponding delete statements
     */
    protected static void constructDeleteStmts() throws IOException
    {
        String fileName;
        StringBuilder sb;        
        List<String> lines;
        List<Integer> keys;
        String[] elements;
        
        // read in the orders delete statement
        fileName = en.getWorkloadsFoldername() + "/tpch-inserts-deletes/delete.u1.1";
        lines = readFile(fileName);
        
        keys = new ArrayList<Integer>();
        
        for (int i = 0; i < 50; i++) {
            elements = lines.get(i).split("\\|");
            keys.add(Integer.parseInt(elements[0]));    
        }
                    
        //for (String line : lines) {
          //  elements = line.split("\\|");
          //  keys.add(Integer.parseInt(elements[0]));        
        //}
        
        // construct delete statements
        sb = new StringBuilder();
        sb.append(constructDeleteStmtOrderTable(keys));
        sb.append(constructDeleteStmtLineItemTable(keys));
        
        // write to file
        fileName = en.getWorkloadsFoldername() + "/tpch-deletes/workload.sql";
        writeToFile(fileName, sb);
    }
    
    
    // The algorithm for RF2: refresh function 2
    // LOOP (SF * 1500) TIMES
    // DELETE FROM ORDERS WHERE O_ORDERKEY = [value]
    // DELETE FROM LINEITEM WHERE L_ORDERKEY = [value]
    // END LOOP
    protected static void constructInsertStmts() throws IOException, SQLException
    {
        String fileName;
        String tabName;
        StringBuilder sb;        
        List<String> lineLI;
        List<String> lineOrder;
        Table table;
        
        // LINE ITEM
        sb = new StringBuilder();
        fileName = en.getWorkloadsFoldername() + "/tpch-inserts-deletes/lineitem.tbl.u1";
        lineLI = readFile(fileName);
        
        tabName = "tpch.lineitem";
        table = db.getCatalog().<Table>findByName(tabName);
        
        for (int i = 0; i < 50; i++) 
            sb.append(insertRow(tabName, lineLI.get(i), table));
        
        //for (String line : lineLI) 
          //  sb.append(insertRow(tabName, line, table));
        
        // ORDERS
        fileName = en.getWorkloadsFoldername() + "/tpch-inserts-deletes/orders.tbl.u1";
        lineOrder = readFile(fileName);
        
        tabName = "tpch.orders";
        table = db.getCatalog().<Table>findByName(tabName);
        
        for (int i = 0; i < 50; i++) 
            sb.append(insertRow(tabName, lineOrder.get(i), table));
        
        //for (String line : lineOrder) 
          //  sb.append(insertRow(tabName, line, table));
        
        // write to file
        fileName = en.getWorkloadsFoldername() + "/tpch-inserts/workload.sql";
        writeToFile(fileName, sb);
    }
    
    
    /**
     * Extract the information from {@line} and formulate an INSERT statement
     * 
     * @param line
     *      The raw data           
     * @param table
     *      The table on which the statement is defined on
     *      
     * @return The INSERT statement 
     */
    protected static String insertRow(String tabName, String line, Table table)
    {
        String[] elements = line.split("\\|");
        String result = " INSERT INTO " + tabName + " VALUES(";
        
        int pos = 0;
        
        for (Column col : table.columns()){
            
            if (isString(col.getDataType()) || isDateTimeLiteral(col.getDataType())) {
                result += "\'";
                result += elements[pos];
                result += "\'";
            } else 
                result += elements[pos];
            
            result += ",";
            pos++;
        }
        
        // replace the last comma
        result = result.substring(0, result.length() - 1);        
        result += ");\n";
        
        return result;
    }
    
    
    /**
     * Construct the list of delete statement on TPCH.ORDERS table.
     * 
     * @param keys
     *      List of the primary key of rows that are being deleted.
     *      
     * @return
     *      A set of delete statements in {@code StringBuilder} form.
     *      
     */
    protected static StringBuilder constructDeleteStmtOrderTable(List<Integer> keys)
    {
        StringBuilder sb = new StringBuilder();
        
        for (int key : keys)
            sb.append(" DELETE FROM TPCH.ORDERS WHERE O_ORDERKEY = " + key + ";\n");
        
        return sb;
    }
    
    /**
     * Construct the list of delete statement on TPCH.LINEITEM table.
     * 
     * @param keys
     *      List of the primary key of rows that are being deleted.
     *      
     * @return
     *      A set of delete statements in {@code StringBuilder} form.
     *      
     */
    protected static StringBuilder constructDeleteStmtLineItemTable(List<Integer> keys)
    {
        StringBuilder sb = new StringBuilder();
        
        for (int key : keys)
            sb.append(" DELETE FROM TPCH.LINEITEM WHERE L_ORDERKEY = " + key + ";\n");
        
        return sb;
    }
    
    /**
     * Read from a file, and return a list of string, where each string represents
     * for a row in the data file. 
     */
    protected static List<String> readFile(String fileName) throws IOException
    {
        List<String> lines;
        BufferedReader reader;
        String line;
        
        reader = new BufferedReader(new FileReader(fileName));
        lines = new ArrayList<String>();
        
        while ((line = reader.readLine()) != null)
            lines.add(line);
        
        reader.close();
        
        return lines;
    }
   
    
    /**
     * Write the string in the given {@code sb} into the given file
     * 
     * @param fileName
     *      The file name that is written to
     *      
     * @param sb
     *      The content to write into file
     */
    protected static void writeToFile(String fileName, StringBuilder sb) throws IOException
    {
        PrintWriter out;
        out = new PrintWriter(new FileWriter(fileName, false));
        out.print(sb);
        out.close();
    }
}
