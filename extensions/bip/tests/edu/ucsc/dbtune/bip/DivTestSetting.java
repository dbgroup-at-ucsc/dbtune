package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.workload;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.db2.DB2Advisor;
import edu.ucsc.dbtune.bip.div.DivBIP;
import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.bip.div.UtilConstraintBuilder;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;
 
import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;
import static edu.ucsc.dbtune.workload.SQLCategory.UPDATE;
import static edu.ucsc.dbtune.bip.CandidateGeneratorFunctionalTest.readCandidateIndexes;
import edu.ucsc.dbtune.divgdesign.DivgDesign.QueryCostAtPartition;

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
    protected static List<Double> nodeImbalances;
    protected static List<Double> queryImbalances;
    protected static List<Double> failureImbalances;
    protected static List<Integer> listNumberReplicas;
    
    protected static DivBIP div;
    protected static DivConfiguration divConf;
    
    protected static Set<Index> candidates;
        
    // update weight = 10^{updateRatio} * size_of_relation
    // default value
    protected static double updateRatio = Math.pow(10, -3);
    protected static String folder;
    
    protected static DB2Advisor db2Advis;
    protected static String dbName;
    protected static String wlName;
    protected static boolean isShowOptimizerCost;
    
    protected static double timeBIP;
    
    // for Debugging purpose only
    protected static double totalIndexSize;
    protected static boolean isExportToFile = false;
    protected static boolean isTestCost = false;
    protected static boolean isShowRecommendation = false;
    protected static boolean isDB2Cost = false;
    protected static boolean isGetAverage = false;
    protected static boolean isPostprocess = false;
    protected static boolean isCoPhyDesign = false;
    
    
    /**
     * Retrieve the environment parameters set in {@code dbtune.cfg} file
     * 
     * @throws Exception
     */
    public static void getEnvironmentParameters() throws Exception
    {
        if (isLoadEnvironmentParameter)
            return;
        
        en = Environment.getInstance();        
        db = newDatabaseSystem(en);        
        io = db.getOptimizer();
        
        if (!(io instanceof InumOptimizer))
            return;
        
        folder = en.getWorkloadsFoldername();
        isShowOptimizerCost = en.getIsShowOptimizerCost();
        dbName = db.getCatalog().getName().toLowerCase();
        String[] tmp = en.getWorkloadsFoldername().split("/");
        wlName = tmp[tmp.length - 1].toLowerCase();
        Rt.p(" db name = " + dbName + " wl name = " + wlName
                + " is show optimzier cost = "
                + isShowOptimizerCost);
        
        // get workload and candidates
        workload = workload(folder);
        db2Advis = new DB2Advisor(db);
        candidates = new HashSet<Index>();
        
        if (!isCoPhyDesign){
            candidates = readCandidateIndexes(folder, db2Advis);
        
            long totalSize = 0;
            for (Index i : candidates)
                totalSize += i.getBytes();
            
            Rt.p(" Total size = " + (totalSize / Math.pow(2, 20)));
        } 
        
        /*
        for (SQLStatement sql : workload)
            if (sql.getSQLCategory().isSame(INSERT)
                    || sql.getSQLCategory().isSame(DELETE)) {
                // for TPCH workload
                if (sql.getSQL().contains("orders")) 
                    sql.setStatementWeight(fInsert);
                else if (sql.getSQL().contains("lineitem"))
                    sql.setStatementWeight((int) (fInsert * 4));
            }           
            else if (sql.getSQLCategory().isSame(SELECT))
                sql.setStatementWeight(fQuery);
            else if (sql.getSQLCategory().isSame(UPDATE))
                sql.setStatementWeight(fUpdate);
        */
        // TODO: add weight into the workload declaration
        for (SQLStatement sql : workload)                   
            if (sql.getSQLCategory().isSame(SELECT))
                sql.setStatementWeight(1);
            else {
                // Set Statement weight = |relation size| * updateRatio
                InumPreparedSQLStatement preparedStmt
                        = (InumPreparedSQLStatement) io.prepareExplain(sql);
                ExplainedSQLStatement inumExplain = preparedStmt.explain(new HashSet<Index>());
                Table tbl = inumExplain.getUpdatedTable();
                //int fUpdate = (int) (updateRatio * tbl.getCardinality());
                int fUpdate = 1; 
                sql.setStatementWeight(fUpdate);
                Rt.p(" fupdate = " + fUpdate);
            }
       
        
        div = new DivBIP();
        
        Rt.p("DIVpaper: # statements in the workload =  " + workload.size()
                + " # candidates in the workload = " + candidates.size()
                + " workload folder = " + folder);
        
        isLoadEnvironmentParameter = true;
    }
    
    
    /**
     * Set common parameter (e.g., workload, number of replicas, etc. )
     * 
     * @throws Exception
     */
    public static void setParameters() throws Exception
    {   
        // debugging purpose
        isExportToFile = false;
        isTestCost = false;
        isShowRecommendation = false;        
        isGetAverage = false;
        isDB2Cost = false;
        
        div = new DivBIP();
        
        // space budget
        double oneMB = Math.pow(2, 20);
        listBudgets = new ArrayList<Double>();
        for (Integer b : en.getListSpaceBudgets()) 
            listBudgets.add(b * oneMB);
        
        // number of replicas
        listNumberReplicas = new ArrayList<Integer>(en.getListNumberOfReplicas());
        
        // default values of B, nreplica, and loadfactor
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
        Rt.p(" # indexes: " + conf.size()
                        + " # workload: " + workload.size());
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
     * Compute the load-imbalance factor for the given divergent configuration
     * @param conf
     * @param workload
     * @return
     */
    public static double computeLoadImbalance(DivConfiguration conf, Workload workload)
                throws Exception
    {
        List<Double> replicas = new ArrayList<Double>();
        for (int r = 0; r < conf.getNumberReplicas(); r++)
            replicas.add(0.0);
        List<Double> costs;
        double newCost;
        // TODO: assume no update statement
        for (SQLStatement sql: workload){
            costs = computQueryCostReplica(sql, conf);
            for (int r = 0; r < conf.getNumberReplicas(); r++) {
                newCost = replicas.get(r) + costs.get(r);
                replicas.set(r, newCost);
            }   
        }
        
        return UtilConstraintBuilder.maxRatioInList(replicas);
    }
    
    /**
     * Compute the query execution cost for statements in the given workload
     * 
     * @param conf
     *      A configuration
     *      
     * @throws Exception
     */
    protected static List<Double> computQueryCostReplica(SQLStatement sql,
                                                         DivConfiguration divConf) 
                                                       throws Exception
    {
        double cost;
        List<QueryCostAtPartition> costPartitions = new ArrayList<QueryCostAtPartition>();
        int nReplicas = divConf.getNumberReplicas();
        List<Double> results = new ArrayList<Double>();
        
        for (int r = 0; r < nReplicas; r++) {   
            cost = io.getDelegate().explain(sql, divConf.indexesAtReplica(r)).getTotalCost();
            costPartitions.add(new QueryCostAtPartition(r, cost));
            results.add(0.0);
        }
        Collections.sort(costPartitions);
        int id;
        for (int k = 0; k < divConf.getLoadFactor(); k++){
            id = costPartitions.get(k).getPartitionID();
            cost = costPartitions.get(k).getCost() * sql.getStatementWeight() 
                        / divConf.getLoadFactor();
            results.set(id, cost);
        }
        
        return results;
    }
    
    /**
     * Compute the query execution cost for statements in the given workload
     * 
     * @param conf
     *      A configuration
     *      
     * @throws Exception
     */
    protected static List<Double> computeQueryCostsInum(Workload workload, Set<Index> conf) 
                throws Exception
    {
        InumPreparedSQLStatement inumPrepared;        
        double inumCost;
        List<Double> costs = new ArrayList<Double>();
        
        int q = 0;
        
        for (SQLStatement sql : workload)  {
            
            inumPrepared = (InumPreparedSQLStatement) io.prepareExplain(sql);
            inumCost = inumPrepared.explain(conf).getTotalCost();
            costs.add(inumCost);
            
            q++;
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
    protected static List<Double> computeCostsDB2(Workload workload, Set<Index> conf) throws Exception
    {           
        double db2Cost;
        List<Double> costs = new ArrayList<Double>();
        
        for (SQLStatement sql : workload)  {
            db2Cost = io.getDelegate().explain(sql, conf).getTotalCost();
            costs.add(db2Cost);
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
    protected static void compareDB2InumQueryCosts(Workload workload, Set<Index> conf) throws Exception
    {
        InumPreparedSQLStatement inumPrepared;
        double db2cost;
        double inumcost;
        
        Rt.p("==============================================");
        Rt.p("Candidate: " + conf.size()
              + " Workload size: " + workload.size());
        
        Rt.p("\n ID  TYPE DB2   INUM   DB2/ INUM");
        int id = 0;
        double totalDB2 = 0.0;
        double totalInum = 0.0;
        List<Integer> goodStmts = new ArrayList<Integer>();
        boolean isGood;
        double ratio;
        
        for (SQLStatement sql : workload) {
            isGood = true;
            db2cost = io.getDelegate().explain(sql, conf).getTotalCost();
            inumPrepared = (InumPreparedSQLStatement) io.prepareExplain(sql);

            if (inumPrepared.getTemplatePlans().size() > 9){
                isGood = false;
                Rt.p("Not good because too many plans ");
                id++;
                continue;
            }
                
            inumcost = inumPrepared.explain(conf).getTotalCost();
            Rt.p(" stmt: " + sql.getSQL());
            if (inumcost < 0.0 || inumcost > Math.pow(10, 9)) { 
                Rt.p("NOT GOOD COST = " + inumcost 
                        + " id = " + id);
                isGood = false;
            }
            ratio = (double) db2cost / inumcost;
            if (ratio > 2 || ratio < 0.4) {
                isGood = false;
                Rt.p(" NOT GOOD, id = " + id);
            }
            if (isGood)
                goodStmts.add(id);
            
            Rt.p(id + " " + sql.getSQLCategory() + " " + db2cost + " " + inumcost + " " 
                                + ratio);
            totalDB2 += db2cost;
            totalInum += inumcost;
            id++;
        }
        
        Rt.p(" total INUM = " + totalInum
                + " total DB2 = " + totalDB2
                + " DB2 / INUM = " + (totalDB2 / totalInum));
        
        Rt.p(" Number of good statements = " + goodStmts.size());
        
        // write to file
        id = 0;
        StringBuilder sb = new StringBuilder();
        for (int i : goodStmts){
            sb.append("-- query" + id + "\n");
            sb.append(workload.get(i).getSQL() + ";" + "\n");
            id++;
        }
        
        File folderDir = new File (en.getWorkloadsFoldername());
        PrintWriter out = new PrintWriter(new FileWriter(folderDir + "/workload_good.sql"), false);
        out.println(sb.toString());
        out.close();
    }

    /**
     * Compute the query execution cost for statements in the given workload
     * 
     * @param conf
     *      A configuration
     *      
     * @throws Exception
     */
    protected static void showComputeQueryCostsInum(int q, Set<Index> conf) throws Exception
    {
        InumPreparedSQLStatement inumPrepared;        
        SQLStatement sql=workload.get(q);
        inumPrepared = (InumPreparedSQLStatement) io.prepareExplain(sql);
        Rt.p(" INUM plan: " + inumPrepared.explain(conf)
                + " cost: " + inumPrepared.explain(conf).getTotalCost());
    }
    
    
}
 