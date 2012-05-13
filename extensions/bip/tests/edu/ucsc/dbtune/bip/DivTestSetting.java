package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.workload;

import static edu.ucsc.dbtune.workload.SQLCategory.INSERT;
import static edu.ucsc.dbtune.workload.SQLCategory.DELETE;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;



import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.bip.div.DivBIP;
import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;
 
import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;
import static edu.ucsc.dbtune.workload.SQLCategory.UPDATE;

/**
 * The common setting parameters for DIVBIP test
 * 
 * @author Quoc Trung Tran
 *
 */
public class DivTestSetting 
{
    protected static boolean isLoadEnvironmentParameter = false;
    protected static DatabaseSystem db;
    protected static Environment    en;
    protected static Optimizer io;
    protected static Workload workload;
    
    protected static int nReplicas;
    protected static int loadfactor;
    protected static double B;    
    
    protected static List<Double> listBudgets;
    protected static List<Integer> listNumberReplicas;
    
    protected static DivBIP div;
    protected static DivConfiguration divConf;
    
    protected static Set<Index> candidates;
    protected static double fQuery;    
    protected static double fUpdate;
    protected static double sf;
    protected static String folder;
    
    // for Debugging purpose only
    protected static boolean isExportToFile;
    protected static boolean isTestCost;
    protected static boolean isShowRecommendation;
    protected static boolean isDB2Cost;
    protected static long totalIndexSize;
    protected static boolean isGetAverage;
    protected static boolean isPostprocess; 
    protected static boolean isAllImbalanceConstraint;
    
    /**
     * Retrieve the environment parameters set in {@code dbtune.cfg} file
     * 
     * @throws Exception
     */
    protected static void getEnvironmentParameters() throws Exception
    {
        if (isLoadEnvironmentParameter)
            return;
        
        en = Environment.getInstance();
        db = newDatabaseSystem(en);        
        io = db.getOptimizer();
        
        if (!(io instanceof InumOptimizer))
            return;
        
        folder = en.getWorkloadsFoldername();
        
        // get workload and candidate
        workload = workload(folder);
        
        
        isLoadEnvironmentParameter = true;
        isPostprocess = false;
        
        System.out.println(" Number of statement in the workload: " + workload.size()
                        + " workload folder: " + folder);
    }
    
    
    /**
     * Set common parameter (e.g., workload, number of replicas, etc. )
     * 
     * @throws Exception
     */
    protected static void setParameters() throws Exception
    {  
        fUpdate = 1;
        fQuery = 1;
        sf = 1500;
        
        for (SQLStatement sql : workload)
            if (sql.getSQLCategory().isSame(INSERT)) {
                
                // for TPCH workload
                if (sql.getSQL().contains("orders")) 
                    sql.setStatementWeight(sf);
                else if (sql.getSQL().contains("lineitem"))
                    sql.setStatementWeight(sf * 3.5);
            }   
            else if (sql.getSQLCategory().isSame(DELETE))
                sql.setStatementWeight(sf);            
            else if (sql.getSQLCategory().isSame(SELECT))
                sql.setStatementWeight(fQuery);
            else if (sql.getSQLCategory().isSame(UPDATE))
                sql.setStatementWeight(fUpdate);
        
        // debugging purpose
        isExportToFile = false;
        isTestCost = false;
        isShowRecommendation = false;        
        isGetAverage = false;
        isDB2Cost = false;
        isAllImbalanceConstraint = false;
        
        div = new DivBIP();
        
        // space budget
        double oneMB = Math.pow(2, 20);
        listBudgets = new ArrayList<Double>();
        for (Integer b : en.getListSpaceBudgets()) 
            listBudgets.add(b * oneMB);
        
        // number of replicas
        listNumberReplicas = new ArrayList<Integer>(en.getListNumberOfReplicas());
        
        
        // default value of B, nreplica, and loadfactor
        B = listBudgets.get(0);
        nReplicas = listNumberReplicas.get(0);
        loadfactor = (int) Math.ceil( (double) nReplicas / 2);
    }
    
    
    /**
     * Compute the query execution cost for statements in the given workload
     * 
     * @param conf
     *      A configuration
     *      
     * @throws Exception
     */
    protected static double computeWorkloadCostDB2(Workload workload, Set<Index> conf) 
                            throws Exception
    {   
        double db2Cost;
        double cost;
        
        db2Cost = 0.0;

        for (SQLStatement sql : workload) {
            cost = io.getDelegate().explain(sql, conf).getTotalCost();
            db2Cost += cost * sql.getStatementWeight();
        }
        
        return db2Cost;
    }

    
    /**
     * Compute the query execution cost for statements in the given workload
     * 
     * @param conf
     *      A configuration
     *      
     * @throws Exception
     */
    protected static List<Double> computeQueryCostsDB2(SQLStatement sql,
                                                       DivConfiguration divConf) 
                                                       throws Exception
    {
        double cost;
        List<Double> costs = new ArrayList<Double>();
        
        for (int r = 0; r < nReplicas; r++) {   
            cost = io.getDelegate().explain(sql, divConf.indexesAtReplica(r)).getTotalCost();
            costs.add(cost);
        }
        
        return costs;
    }
    
    /**
     * Compute the query execution cost for statements in the given workload
     * 
     * @param conf
     *      A configuration
     *      
     * @throws Exception
     */
    protected static List<Double> computeQueryCostsInum(Set<Index> conf) throws Exception
    {
        InumPreparedSQLStatement inumPrepared;        
        double inumCost;
        List<Double> costs = new ArrayList<Double>();
        
        for (SQLStatement sql : workload)  {
            
            inumPrepared = (InumPreparedSQLStatement) io.prepareExplain(sql);
            inumCost = inumPrepared.explain(conf).getTotalCost();
            costs.add(inumCost);
            
        }
        
        return costs;
    }
    
    /**
     * Compute the query execution cost for statements in the given workload
     * 
     * @param conf
     *      A configuration
     *      
     * @throws Exception
     */
    protected static void computeQueryCosts(Set<Index> conf) throws Exception
    {
        InumPreparedSQLStatement inumPrepared;
        double db2cost;
        double inumcost;
        
        System.out.println("==============================================");
        System.out.println("Candidate: " + conf.size()
                            + " Workload size: " + workload.size());
        for (Index index : conf)
            System.out.print(index.getId() + " ");
        System.out.println("\n ID  TYPE DB2   INUM   DB2/ INUM");
        int id = 0;
        
        for (SQLStatement sql : workload) {
            
            db2cost = io.getDelegate().explain(sql, conf).getTotalCost();
            inumPrepared = (InumPreparedSQLStatement) io.prepareExplain(sql);
            inumcost = inumPrepared.explain(conf).getTotalCost();
            
            System.out.println(id + " " + sql.getSQLCategory() + " " + db2cost + " " + inumcost + " " 
                                + (double) db2cost / inumcost);
            
            id++;
        }
    }

    @Test
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
        
        
        System.out.println(" result: " + result);
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
       
        System.out.println(" Number of queries = " + numQueries + "\n" 
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
}
 