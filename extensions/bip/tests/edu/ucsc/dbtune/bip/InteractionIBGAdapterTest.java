package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import edu.ucsc.dbtune.util.Rt;

public class InteractionIBGAdapterTest extends DivTestSetting 
{   
    public void postProcessKarlWorkload() throws Exception
    {
        getEnvironmentParameters();
        
        if (!isPostprocess)
            return;
        
        //removeLineWithoutSelectClause();
        //getSubsetUpdateStmts(10);
        /*
        List<String> sqls = new ArrayList<String>();
        String stmt;
        
        for (int i = 0; i < workload.size(); i++) { 
            stmt = workload.get(i).getSQL();
            
            if (stmt.contains("SELECT"))
                sqls.add(postProcessKarlWorkload(stmt));
            else 
                sqls.add(stmt + " ; ");
        }
        
        // write to file
        String fileName;
       
        fileName = en.getWorkloadsFoldername() + "/workload1.sql";
        writeListOfStmtsToFile(sqls, fileName);
       */
    }
    
    /**
     * todo
     * @param sql
     * @return
     */
    protected static String postProcessKarlWorkload(String sql)
    {   
        /*
        if (sql.contains("tpce.trade_type") 
                || sql.contains("tpce.charge")
                || sql.contains("tpce.commission_rate")
                || sql.contains("tpce.news_item")
                || sql.contains("tpce.news_xref"))
            return "";
        */
        
        // SELECT 15, COUNT(*) FROM nref.protein table1, nref.source table2, nref.neighboring_seq table0 
        // WHERE table1.seq_length BETWEEN 1351 AND 6626 AND table1.last_updated BETWEEN 'Fri Aug 09 22:59:05 PDT 2002' AND 'Tue Aug 13 22:59:05 PDT 2002' 
        // AND table1.nref_id=table2.nref_id;
        StringBuffer buffer = new StringBuffer(sql);
        
        int posFrom, posWhere;
        String fromClause, whereClause;
        Map<String, String> relationAlias;
        
        // get the position of from and where
        posFrom = buffer.lastIndexOf("FROM");
        posWhere = buffer.lastIndexOf("WHERE");
        
        fromClause = buffer.substring(posFrom + 4, posWhere);
        whereClause = buffer.substring(posWhere + 5, buffer.length());
        
        relationAlias = mapTableAlias(fromClause);
        
        fromClause = " FROM ";
        
        for (Map.Entry<String, String> entry : relationAlias.entrySet()) { 
            whereClause = replace(whereClause, entry.getKey(), entry.getValue());
            fromClause += entry.getValue();
            fromClause += ", ";
        }
        
        fromClause = fromClause.substring(0, fromClause.length() - 2);
        
        
        StringBuilder sb = new StringBuilder();
        sb.append(sql.substring(0, posFrom)).append(fromClause)
                .append(" WHERE ").append(whereClause);
        
        String result = sb.toString() + " ; ";
        
        if (!result.contains("tpcc.orderline") && result.contains("tpcc.order"))
            result = replace(result, "tpcc.order", "tpcc.orders");
        
        
        Rt.p(" result: " + result);
        return result;
    }
    
    /**
     * todo
     * @param fromClause
     * @return
     */
    protected static Map<String, String> mapTableAlias(String fromClause)
    {
        Map<String, String> map = new HashMap<String, String>();
        
        String[] lists = fromClause.split(",");
        String[] pairs;
        int length;
        
        for (String s : lists) {
            pairs = s.split("\\s");
            
            length = pairs.length;
            map.put(pairs[length -  1], pairs[length - 2]);
        }
        
        return map;
    }
    
    /**
     * todo
     * @param str
     * @param pattern
     * @param replace
     * @return
     */
    protected static String replace(String str, String pattern, String replace) {
        int s = 0;
        int e = 0;
        StringBuffer result = new StringBuffer();

        while ((e = str.indexOf(pattern, s)) >= 0) {
            result.append(str.substring(s, e));
            result.append(replace);
            s = e+pattern.length();
        }
        result.append(str.substring(s));
        return result.toString();
    }

    /**
     * todo
     * @throws Exception
     */
    protected static void removeLineWithoutSelectClause() throws Exception
    {
        BufferedReader reader;
        StringBuilder sb;
        String line;

        String file = en.getWorkloadsFoldername() + "/workload.sql";
        reader = new BufferedReader(new FileReader(file));
        sb = new StringBuilder();

        List<String> sqls = new ArrayList<String>();
        
        // keep only statement
        while ((line = reader.readLine()) != null) {

            line = line.trim();

            if (line.startsWith("--"))
                continue;

            if (line.endsWith(";")) {
                sb.append(line.substring(0, line.length() - 1));

                final String sql = sb.toString();

                if (!sql.isEmpty()) {
                    sb.append(";");
                    sqls.add(sb.toString());
                }
                sb = new StringBuilder();
            }
        }

        reader.close();
        
        // write to file
        writeListOfStmtsToFile(sqls, file);    
    }
    
    /**
     * todo
     * @param count
     */
    protected static void getSubsetUpdateStmts(int count) throws Exception
    {
        int numQueries;
        int numUpdates; 
        
        numQueries = 0;
        numUpdates = 0;
        
        List<String> updates = new ArrayList<String>();
        int size = 0;
        
        for (int i = 0; i < workload.size(); i++)
            if (workload.get(i).getSQLCategory().isSame(SELECT))
                numQueries++;
            else {
                numUpdates++;
                
                if (size < count) {
                    updates.add(workload.get(i).getSQL() + " ; ");
                    size++;
                }
            }
       
        Rt.p(" Number of queries = " + numQueries + "\n" 
                           + " Number of updates = " + numUpdates);
        
        String file = en.getWorkloadsFoldername() + "/workload.sql";
        // write to file
        writeListOfStmtsToFile(updates, file);
    }
    
    /**
     * todo
     * @param sqls
     */
    protected static void writeListOfStmtsToFile(List<String> sqls, String fileName) throws Exception
    {
        
        // write to file
        PrintWriter out;
        out = new PrintWriter(new FileWriter(fileName), false);
        
        for (String sql : sqls)
            out.println(sql);
               
        out.close();
    }
    
    
    
    /**
     * Write the experimental results to file
     * 
     * @param fileName
     *  todo
     * @param entries
     * @throws Exception
     */
    protected static void writeDivInfoToFile(String fileName, List<DivTestEntry> entries) 
                        throws Exception
    {
        PrintWriter   out;
        
        out = new PrintWriter(new FileWriter(fileName), false);

        for (DivTestEntry entry : entries)
            out.print(entry.toString());

        out.close();
    }
}
