/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ucsc.dbtune.inum.old.autopilot;

import edu.ucsc.dbtune.inum.old.model.Configuration;
import edu.ucsc.dbtune.inum.old.model.Index;
import edu.ucsc.dbtune.inum.old.model.PhysicalConfiguration;
import edu.ucsc.dbtune.inum.old.model.Plan;
import edu.ucsc.dbtune.inum.old.model.QueryDesc;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.Strings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author cristina
 */
public class PostgresAutoPilotDelegate extends AutoPilotDelegate {

    private autopilot ap;
    //Connection conn;
    private int noIndexes;
    private HashMap indexNames;
    private HashMap indTbNames;
    private HashMap indColumnNames;
    private HashMap indInfoOID;
    private boolean disNestLoop = false;
    private HashMap auxParent;
    private HashMap indexNameIndNr;//will keep the key = index name; value= the index nr for that index
    private HashMap preparedIndInfoOID;//stores the info from indInfoOID for the indexes which are set from the preparedQueryForConfig.. such that at execution time this indexes are used
    private HashMap preparedIndexNames;//the same as preparedIndInfoOID, but this one is for indexNames

    public PostgresAutoPilotDelegate(autopilot ap) {
        this.ap = ap;
        indexNames = new HashMap();
        indTbNames = new HashMap();
        indColumnNames = new HashMap();
        indInfoOID = new HashMap();
        auxParent = new HashMap();
        indexNameIndNr = new HashMap();
        preparedIndInfoOID = new HashMap();
        preparedIndexNames = new HashMap();
        noIndexes = 0;
    }

    public PostgresAutoPilotDelegate() {
        // this.ap = ap;
        indexNames = new HashMap();
        indTbNames = new HashMap();
        indColumnNames = new HashMap();
        indInfoOID = new HashMap();
        auxParent = new HashMap();
        indexNameIndNr = new HashMap();
        preparedIndInfoOID = new HashMap();
        preparedIndexNames = new HashMap();
        noIndexes = 0;
    }

    //TODO: Here I will get the plan for a query. Obs: I will also have have somehow the param about the virtual index
    public Plan getExecutionPlan(Connection conn, String Q) {
        PostgresPlan plan = new PostgresPlan();
        String plaString = getPlanQuery(conn, Q, disNestLoop); // this is in a string format. Now process
        plan.setPlan(plaString);
        parsePlan(plaString, plan);
        plan.analyzePlan();

        return plan;
    }


    /* Available operators: ./src/backend/commands/explain.c
	Result, Append
	BitmapAnd, BitmapOr
	Nested Loop, Nested Loop Left Join, Nested Loop Full Join, Nested Loop Right Join, Nested Loop IN Join,
	Merge Join, Merge Left Join, Merge Full Join, Merge Right Join, Merge IN Join,
	Hash, 
	Hash Join, Hash Left Join, Hash Full Join, Hash Right Join, Hash IN Join,
	Seq Scan, Index Scan, Bitmap Index Scan, Bitmap Heap Scan, Tid Scan, Subquery Scan, Function Scan, Values Scan 
	Materialize, Sort, Group, 
	Aggregate, GroupAggregate, HashAggregate, 
	Unique
	SetOp Intersect, SetOp Intersect All, SetOp Except, SetOp Except All
	Limit
     */
    public String returnOperator(String name) {
        String ret = "";
        if (name.indexOf("Result") > -1) //Result
            return "RESULT";
        if (name.indexOf("Append") > -1) //Append
            return "APPEND";
        if (name.indexOf("BitmapAnd") > -1) //BitmapAnd
            return "BITMAP_AND";
        if (name.indexOf("BitmapOr") > -1) //BitmapOr
            return "BITMAP_OR";
        if (name.indexOf("Nested Loop") > -1) //Nested Loop, Nested Loop Left Join, Nested Loop Full Join, Nested Loop Right Join, Nested Loop IN Join
            return "NLJOIN";
        //Merge Join, Merge Left Join, Merge Full Join, Merge Right Join, Merge IN Join
        if ((name.indexOf("Merge Join") > -1) || (name.indexOf("Merge Left Join") > -1) || (name.indexOf("Merge Full Join") > -1) || (name.indexOf("Merge Right Join") > -1) || (name.indexOf("Merge IN Join") > -1))
            return "MSJOIN";
        //Hash Join, Hash Left Join, Hash Full Join, Hash Right Join, Hash IN Join
        if ((name.indexOf("Hash Join") > -1) || (name.indexOf("Hash Left Join") > -1) || (name.indexOf("Hash Full Join") > -1) || (name.indexOf("Hash Right Join") > -1) || (name.indexOf("Hash IN Join") > -1)) 
            return "HSJOIN";
        if (name.indexOf("Hash") > -1) //Hash
            return "HASH";
        if (name.indexOf("Seq Scan") > -1) //Seq Scan 
            return "TBSCAN";
        if (name.indexOf("Bitmap Index Scan") > -1) //Bitmap Index Scan 
            return "BP_IXSCAN";
        if (name.indexOf("Bitmap Heap Scan") > -1) //Bitmap Heap Scan 
            return "BP_HSCAN";
        if (name.indexOf("Index Scan") > -1) //Index Scan 
            return "IXSCAN";
        if (name.indexOf("Tid Scan") > -1) //Tid Scan 
            return "TIDSCAN";
        if (name.indexOf("Subquery Scan") > -1) //Subquery Scan 
            return "SUBQUERY_SCAN";
        if (name.indexOf("Function Scan") > -1) //Function Scan 
            return "FUNCTION_SCAN";
        if (name.indexOf("Values Scan") > -1) //Values Scan 
            return "VALUES_SCAN";
        if (name.indexOf("Materialize") > -1) //Materialize 
            return "MATERIALIZE";
        if (name.indexOf("Sort") > -1) //Sort 
            return "SORT";
        if (name.indexOf("Group") > -1) //Group 
            return "GROUP";
        if (name.indexOf("Aggregate") > -1) //Aggregate 
            return "AGGREGATE";
        if (name.indexOf("GroupAggregate") > -1) //GroupAggregate 
            return "GP_AGGREGATE";
        if (name.indexOf("HashAggregate") > -1) //HashAggregate 
            return "H_AGGREGATE";
        if (name.indexOf("Unique") > -1) //Unique 
            return "UNIQUE";
        //SetOp Intersect, SetOp Intersect All, SetOp Except, SetOp Except All
        if ((name.indexOf("SetOp Intersect") > -1) || (name.indexOf("SetOp Intersect All") > -1) || (name.indexOf(" SetOp Except") > -1) || (name.indexOf("SetOp Except All") > -1))  
            return "SETOP";
        if (name.indexOf("LIMIT") > -1) //Limit 
            return "LIMIT";
        
        //return unknown name...
        ret="UNKNOWN TYPE in " + name;
        
        return ret;
    }

    //Return target (For Indexes, Table Scan etc)
    public String returnTarget(String nodeName, String prim) 
    {
        String ret = "";
        if (nodeName.equals("IXSCAN")) {//IndexScan: " using %s"
            int x = prim.indexOf(" using ");
            int y = prim.indexOf(" ", x + 7);
            ret = prim.substring(x + 7, y);
        }
      //SeqScan, BitMapHeapScan, TidScan: " on %s"
        if (nodeName.equals("TBSCAN") || nodeName.equals("BP_HSCAN") || nodeName.equals("TIDSCAN")) {   
            int x = prim.indexOf(" on ");//FUNCTION_SCAN,VALUES_SCAN
            int y = prim.indexOf(" ", x + 4);
            ret = prim.substring(x + 4, y);
        }
        //BitMapIndexScan, Function Scan, Values Scan : " on %s"
        if (nodeName.equals("BP_IXSCAN") || nodeName.equals("FUNCTION_SCAN") || nodeName.equals("VALUES_SCAN")) {   
            int x = prim.indexOf(" on ");
            int y = prim.indexOf(" ", x + 4);
            ret = prim.substring(x + 4, y);
        }
        if (nodeName.equals("SUBQUERY_SCAN")) {//SubQuery Scan: " %s"
            int x = prim.indexOf("Subquery Scan ");
            int y = prim.indexOf(" ", x + 14);
            ret = prim.substring(x + 14, y);
        }

        return ret;
    }

    //used in order to parse the plan and put elem in the plan
    /**
     * @param plaString
     * @param plan
     */
    public void parsePlan(String plaString, Plan plan) 
    {
    	/*Parse Query*/
    	ArrayList<String> lines = new ArrayList<String>(); 
    	ArrayList<Integer> parentList = new ArrayList<Integer>(); 
    	String current = "->  " + plaString;

    	String regex ="\\->";
    	current = current.replaceAll("\\r|\\n", "");

    	Pattern p = Pattern.compile(regex);
    	Matcher m = p.matcher(current);

    	while (m.find()) 
    	{ 
    		int end = m.end();
    		int posNext = current.indexOf("->", end);
    		String str = "";
    		int count = 0;
    		if(posNext != -1)
    		{
    			str = current.substring(end+2, posNext);
        		for (int  i = (str.length() - 1) ; i > 0; i--)
        			if(Character.isWhitespace(str.charAt(i)))
        				count++;
        			else
        				break;
        		parentList.add(count);
    		}
    		else
    		{
    			str = current.substring(end+2);
    		}
        if(!Strings.isEmpty(str)){
          lines.add(str);
        }
    	}
		parentList.add(0, -1);

		//Plan parsed, parents parsed
    	//example: Hash Join  (cost=174080.39..9364262539.50 rows=1 width=193)
    	for (int i = 0; i < lines.size(); i++)
    	{
    		String temp = lines.get(i);
    		int posOpenParenthesis = temp.indexOf("(");
    		int posCost = temp.indexOf("cost=");
    		int posDoubleDot = temp.indexOf("..");
    		int posSpaceAfterDoubleDot = temp.indexOf(" ", posDoubleDot);
    		int posRows = temp.indexOf("rows=");
    		int posSpaceAfterRows = temp.indexOf(" ", posRows);
    		
    		String operator = returnOperator(temp.substring(0, posOpenParenthesis - 1));
    		String target = returnTarget(operator, temp);
    		Float costFirstRow = new Float(temp.substring(posCost+5, posDoubleDot));
    		Float costWholeOperation = new Float(temp.substring(posDoubleDot+2, posSpaceAfterDoubleDot));
    		Double rows = new Double(temp.substring(posRows+5, posSpaceAfterRows));
    		
    		//Find parent!!!
    		int parent = -1;
    		if( i == 0)//root node
    			 parent = -1;
    		else
    		{        		
        		int len = parentList.get(i);
       			for (int  j = i ; j >= 0; j--)
    			{
    				if(parentList.get(j) < len || parentList.get(j) < 0)
    				{
    					parent = j;
    					break;
    				}
    			}
    		}
            PostgresPlanDesc rot = new PostgresPlanDesc(i, parent, operator, target, rows, costWholeOperation, costFirstRow);
            plan.add(rot);
    	}
    	
    }

    public void implement_configuration(autopilot ap, PhysicalConfiguration configuration, Connection conn)
    {
        Set usedTables = configuration.getIndexedTableNames();// get the tables which have indexes

        for (Iterator iterator = usedTables.iterator(); iterator.hasNext();) {

            String tableName = (String) iterator.next();
            Set usedIndexes = configuration.getIndexesForTable(tableName);
            //for each index used by a table
            for (Iterator iterator2 = usedIndexes.iterator(); iterator2.hasNext();) 
            {
                Index index = (Index) iterator2.next();//an index element
                putInCacheIndex(index, conn);// put it into the cache

            }//end for usedIndexes
        }//end for usedTables
    }

    public void disable_configuration(Configuration config, Connection conn) {
    }

    //used to clear all my caches about indexes
    public void drop_configuration(PhysicalConfiguration configuration, Connection conn) {
        indexNames.clear();
        indTbNames.clear();
        indColumnNames.clear();
        indInfoOID.clear();
        disNestLoop = true;// this is not necessary
        auxParent.clear();
        indexNameIndNr.clear();
        preparedIndInfoOID.clear();//this is not really mandatory because i do it in removeQueryPreparation
        preparedIndexNames.clear();//this is not really mandatory because i do it in removeQueryPreparation
    }

    //rewrite a query to use the indexes in a config
    public String prepareQueryForEnumeration(QueryDesc QD, PhysicalConfiguration config, boolean iac, boolean nlj) {
        return prepareQueryForConfiguration(QD, config, iac, nlj);
    }

    //gets the plan in a String for , as it is got from the postgres
    public String getPlanQuery(Connection conn, String Q, boolean nestedLoopDis) {
        String result = "";

        String indInf = "";
        for (int i = 0; i < noIndexes; i++) {
            if (preparedIndInfoOID.containsKey(i)) {
                indInf = indInf + indInfoOID.get(i) + ",";
            }
        }
        
        //cut the last,
        if (indInf.length() > 1) {
            indInf = indInf.substring(0, indInf.length() - 1);
        }
        
        String indN = "";
        for (int i = 0; i < noIndexes; i++) {
            if (preparedIndexNames.containsKey(i)) {
                indN = indN + "\"" + indexNames.get(i) + "\"" + ",";
            }
        }
        
        //cut the last,
        if (indN.length() > 1) {
            indN = indN.substring(0, indN.length() - 1);
        }
        Q = Q.replace("'", "\\'");

        try {
            Statement st1 = conn.createStatement();
            String sqlquery;

            if (nestedLoopDis) {
                sqlquery = "select tfunction( '" + Q + "','{" + indInf + "}',1,'{" + indN + "}')";
            } else {
                sqlquery = "select tfunction( '" + Q + "','{" + indInf + "}',0,'{" + indN + "}')";
            }

            PreparedStatement pst = conn.prepareStatement(sqlquery);
            ResultSet res = st1.executeQuery(sqlquery);

            String s2 = "";
            s2=s2+"";
            if (res.next()) // s = (Double)res.getObject(1);
            {
                s2 = res.getString(1);
            }
            result = s2;
          Console.streaming().info(toStringResultSet(res));
        } catch (SQLException ex) {
          Console.streaming().error("Error while trying to get a query execution plan", ex);
        }
        return result;
    }

    private static String toStringResultSet(ResultSet resultSet) throws SQLException {
      if(resultSet == null) return "ResultSet is Null.";
      final ResultSetMetaData metaData    = resultSet.getMetaData();
      final int               columnCount = metaData.getColumnCount();

      final StringBuilder     content     = new StringBuilder();
      final String            recordTag   = "Record#";
      final StringBuilder     record      = new StringBuilder(recordTag);
      while(resultSet.next()){
        for (int i = 1; i <= columnCount; i++) {
          record.append(i).append("=").append(resultSet.getString(i));
          if(i < columnCount) {
            record.append(", ").append(recordTag);
          }
        }
        content.append(record);
      }

      return content.toString();
    }

    //return the col OID for a given tableOID tbOID and a give column name, aux
    int getColOID(int tbOID, String aux, Connection conn) {
        Integer tb;
        tb = tbOID;
        int coloid = 0;

        String tableoid = "";
        tableoid = tb.toString();

        try {
            Statement st1 = conn.createStatement();
            String sqlquery = "select attnum from pg_attribute where attrelid = " + tableoid + " and attname = " + aux;
            PreparedStatement pst = conn.prepareStatement(sqlquery);
            ResultSet res = st1.executeQuery(sqlquery);

            Integer s = 0;
            if (res.next()) {
                s = (Integer) res.getInt(1);
            }
            coloid = s;


        } catch (SQLException ex) {
            //Logger.getLogger(TableDialog.class.getName()).log(Level.SEVERE, null, ex);
        }

        return coloid;
    }

    //gets the string neewd for indexOID info
    //the parameter nrInd is the key in the HashMap for that index
    public String getInfoOID(int nrInd, Connection conn) {
        // get table oid

        String name = "";
        if (indTbNames.containsKey(nrInd)) //this is done only for null pointer access prevention
            name = (String) indTbNames.get(nrInd);
        name = "'" + name + "'";
        int tbOID = 0;

        try {
            Statement st1 = conn.createStatement();
            String sqlquery = "select relfilenode from pg_class where relname = " + name;
            PreparedStatement pst = conn.prepareStatement(sqlquery);
            ResultSet res = st1.executeQuery(sqlquery);

            Integer s = 0;
            if (res.next()) {
                s = (Integer) res.getInt(1);
            }
            tbOID = s;

        } catch (SQLException ex) {
            //Logger.getLogger(TableDialog.class.getName()).log(Level.SEVERE, null, ex);
        }

        int nrcol = 0;
        String cols = "";
        cols = (String) indColumnNames.get(nrInd);
        int indx1 = 0;
        int indx2 = 0;
        String aux = "";
        String res = "";
        int coid;

        indx2 = cols.indexOf(",", indx1);

        while (indx2 != -1) {
            if (indx1 > 0) {
                aux = cols.substring(indx1 + 1, indx2);
            } else {
                aux = cols.substring(indx1, indx2);
            }

            coid = getColOID(tbOID, aux, conn);
            if (coid == 0) {
                nrcol--;
            } else {
                res = res + "," + coid;
            }
            //res

            indx1 = indx2;
            indx2 = cols.indexOf(",", indx1 + 1);
            nrcol++;
        }

        if (indx1 > 0) {
            aux = cols.substring(indx1 + 1, cols.length());
        } else {
            aux = cols.substring(indx1, cols.length());
        }
        nrcol++;
        coid = getColOID(tbOID, aux, conn);
        if (coid == 0) {
            nrcol--;
        } else {
            res = res + "," + coid;
        }

        //add the nr and table oid;
        String res2 = "";
        res2 = res2 + nrcol + "," + tbOID + res;
        return res2;
    }

    private final Set<Index> preparedIndexes = new HashSet<Index>();
    public Set<Index> getPreparedIndexes(){
      return preparedIndexes;
    }

    //get info from the cache index and put it into the cache prepared for query execution plan
    public void prepareInCacheIndex(Index idx) {
        preparedIndexes.add(idx);
        String idxName = idx.getImplementedName();
        if (indexNameIndNr.containsKey(idxName)) {
            int idxNr = (Integer) indexNameIndNr.get(idxName);//get the indexNr for that indexName
            //now add the infor we neew into the prepared.. structures
            if (indInfoOID.containsKey(idxNr)) {
                String info = (String) indInfoOID.get(idxNr);
                preparedIndInfoOID.put(idxNr, info);
            } else System.out.println("PROBLEM: index not found in cache");
            if (indexNames.containsKey(idxNr)) {
                preparedIndexNames.put(idxNr, (String) indexNames.get(idxNr));
            } else System.out.println("PROBLEM: index not found in cache");


        } else System.out.println("PROBLEM: index not found in cache");
    }

    //put index in the cache
    public void putInCacheIndex(Index idx, Connection conn) {
        //add this index to the  cache
        String tbName = idx.getTableName();
        LinkedHashSet columns = (LinkedHashSet) idx.getColumns();
        String cols = "";
        if (columns.size() > 0) {
            for (Iterator iterator = columns.iterator(); iterator.hasNext();) {
                cols = cols + "'" + (String) iterator.next() + "',";
            }//end for
            if (cols.length() > 1) {
                cols = cols.substring(0, cols.length() - 1);
                cols = cols.toLowerCase();
            }
            // put info in cache
            String indN = Strings.isEmpty(idx.getImplementedName()) ? "idx_" + tbName + "_" + noIndexes : idx.getImplementedName();
            indexNames.put(noIndexes, indN);
            indTbNames.put(noIndexes, tbName);
            indColumnNames.put(noIndexes, cols);
            String inoO = getInfoOID(noIndexes, conn);
            indInfoOID.put(noIndexes, inoO);
            indexNameIndNr.put(indN, noIndexes);
            //set the index name
            idx.setImplementedName(indN);
            noIndexes++;
        }//end if
    }

    //here i will only implement the structures i need
    public String prepareQueryForConfiguration(QueryDesc QD, PhysicalConfiguration config, boolean iac, boolean nlj) {
        String querySql = QD.queryString;
        disNestLoop = !nlj; //disable nested loop

        Set usedTables = config.getIndexedTableNames();// get the tables which have indexes
        for (Iterator iterator = usedTables.iterator(); iterator.hasNext();) {

            String tableName = (String) iterator.next();
            Set usedIndexes = config.getIndexesForTable(tableName);
            for (Iterator iterator2 = usedIndexes.iterator(); iterator2.hasNext();) {
                Index index = (Index) iterator2.next();//an index element 
                prepareInCacheIndex(index);// put it into the cache
            }//end for usedIndexes
        }//end for usedTables

        return querySql;
    }

    public void removeQueryPrepareation(QueryDesc qd) {
        preparedIndInfoOID.clear();
        preparedIndexNames.clear();
        auxParent.clear();
    }

    //return the index size
    public int getIndexSize(Index index) 
    {
        int ret = 0;
        Connection conn = ap.getConnection();
        String table = index.getTableName();
        table = table.toLowerCase();
        table = "'" + table + "'";
        String cols = "";
        LinkedHashSet columns = (LinkedHashSet) index.getColumns();
        for (Iterator iterator = columns.iterator(); iterator.hasNext();) {
            String col = (String) iterator.next();
            cols = cols.concat("\"" + col.toLowerCase() + "\",");
        }
        if (cols.length() > 1) 
        	cols = cols.substring(0, cols.length() - 1);//cut the last ,
        //now call the function which returns the size
        try {
            Statement st1 = conn.createStatement();
            // todo(Huascar) turn this sqlQuery into RecommendIndexes function
            String sqlquery = "select indxsize(" + table + ",'{" + cols + "}')";
            ResultSet res = st1.executeQuery(sqlquery);
            String s = "";
            if (res.next()) {
                s = res.getString(1);//size in pages
            }

            ret = Integer.valueOf(s);
            res.close();
            st1.close();
        } catch (SQLException ex) {
            //Logger.getLogger(TableDialog.class.getName()).log(Level.SEVERE, null, ex);
            ex.printStackTrace();
        }

        try {
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        index.setTheSize(ret);
        return ret;
    }

    //main used for testing
    public static void main(String args[]) {
        PostgresAutoPilotDelegate posa = new PostgresAutoPilotDelegate();
    }
}


