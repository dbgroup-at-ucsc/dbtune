package edu.ucsc.dbtune.divgdesign;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.bip.indexadvisor.CoPhy;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import static edu.ucsc.dbtune.bip.core.InumQueryPlanDesc.preparedStmts;
import static edu.ucsc.dbtune.bip.util.LogListener.EVENT_POPULATING_INUM;

import static edu.ucsc.dbtune.workload.SQLCategory.DELETE;
import static edu.ucsc.dbtune.workload.SQLCategory.INSERT;
import static edu.ucsc.dbtune.workload.SQLCategory.NOT_SELECT;
import static edu.ucsc.dbtune.bip.div.UtilConstraintBuilder.maxRatioInList;

public class CoPhyDivgDesign extends DivgDesign 
{    
    private DatabaseSystem db;
    private InumOptimizer  io;
    private LogListener    logger;
    
    private CoPhy cophy;  
    private Map<SQLStatement, Set<Index>> recommendedIndexStmt;
    private double timeInum;
    private double timeAnalysis;
    
    protected Environment environment = Environment.getInstance();
    
    public CoPhyDivgDesign(DatabaseSystem db, InumOptimizer  io, LogListener logger,
                           Map<SQLStatement, Set<Index>> recommendedIndexStmt)
    {
        this.db = db;
        this.io = io;
        this.logger = logger;
        
        // initialize data structures
        cophy = new CoPhy();        
        this.recommendedIndexStmt = recommendedIndexStmt;
    }
    
    /**
     * Get the running time to populate INUM space
     * 
     * @return
     *      The running time (in milliseconds)
     */
    public double getInumTime()
    {
        return timeInum;
    }
    
    /**
     * Get the running time to analyze including formulating BIP and repartitioning 
     * 
     * @return
     *      The running time (in milliseconds)
     */
    public double getAnalysisTime()
    {
        return timeAnalysis;
    }
    
    @Override
    public void recommend(Workload workload, int nReplicas, int loadfactor, double B)
                          throws Exception
    {
        this.workload = workload;
        this.n = nReplicas;
        this.m = loadfactor;
        this.B = B;
        
        maxIters = 30;
        epsilon = 0.05;
        
        long start = System.currentTimeMillis();
        long runningTime = 0;
        // process 
        process();
        runningTime = System.currentTimeMillis() - start;
        
        // record the INUM time and analysis time
        timeInum = logger.getRunningTime(EVENT_POPULATING_INUM);
        timeAnalysis = runningTime - timeInum;
    }
    
       
    @Override
    protected Set<Index> getRecommendation(List<SQLStatement> sqls)
            throws Exception 
    {
        Set<Index> candidates = new HashSet<Index>();
        String fileQueryPlanDesc;
        File file;
        
        // check if we already generate candidate for this statement before
        for (SQLStatement sql : sqls)
            candidates.addAll(recommendedIndexStmt.get(sql));
        
        Optimizer io = db.getOptimizer();
        Workload wlPartition = new Workload(sqls);
        cophy.setCandidateIndexes(candidates);
        cophy.setWorkload(wlPartition); 
        cophy.setOptimizer(io);
        cophy.setSpaceBudget(B);
        cophy.setLogListenter(logger);
        
        // remove the file of QueryPlanDesc
        fileQueryPlanDesc = environment.getWorkloadsFoldername() + "/query-plan-desc.bin";
        file = new File(fileQueryPlanDesc);
        if (file.exists())
            file.delete();
        
        return cophy.solve().getRecommendation();
    }

    @Override
    protected List<QueryCostAtPartition> statementCosts(SQLStatement sql)
            throws SQLException 
    {
        InumPreparedSQLStatement inumStmt;
        ExplainedSQLStatement explain;
       
        // retrieve from the cache first
        inumStmt = preparedStmts.get(sql);
        if (inumStmt == null) {
            inumStmt  = (InumPreparedSQLStatement) io.prepareExplain(sql);
            preparedStmts.put(sql, inumStmt);
        }
        
       
        // iterate over every replica 
        // and compute the cost
        List<QueryCostAtPartition> costs = new ArrayList<QueryCostAtPartition>();
        double cost;
        //double baseTableCost;
        //double indexUpdateCost;
        
        for (int i = 0; i < n; i++) {
            
            explain = inumStmt.explain(indexesAtReplica.get(i));
            cost = explain.getTotalCost();
            
            // temporary take get select cost out of the cost
            if (sql.getSQLCategory().isSame(INSERT) || 
                    sql.getSQLCategory().isSame(DELETE)) {
                // cost of base table & update cost
                cost -= explain.getSelectCost();
                /*
                baseTableCost = explain.getBaseTableUpdateCost();
                indexUpdateCost = cost - baseTableCost;
                indexUpdateCost *= INDEX_UPDATE_COST_FACTOR;
                
                cost = baseTableCost + indexUpdateCost;
                */                
            }
            
            // not consider base table cost for now            
            if (sql.getSQLCategory().isSame(NOT_SELECT))
                cost -= explain.getBaseTableUpdateCost();
            
            cost *= sql.getStatementWeight();            
            costs.add(new QueryCostAtPartition(i, cost));
        }
            
        return costs;
    }
    
    /**
     * Compute the maximum imbalance query
     * 
     * @return
     */
    public double getImbalanceQuery() throws SQLException
    {
        List<QueryCostAtPartition> costs;
        List<Double> values;
        double maxRatio = -1;
        double ratio; 
        
        for (int i = 0; i < workload.size(); i++) {
            
            if (workload.get(i).getSQLCategory().isSame(NOT_SELECT))
                continue;
            
            costs = statementCosts(workload.get(i));
            
            // an query statement, get the top-m best cost
            Collections.sort(costs);
            
            values = new ArrayList<Double>();
            for (int j = 0; j < m; j++)
                values.add(costs.get(j).getCost());
            
            ratio = maxRatioInList(values);
            maxRatio = (maxRatio > ratio) ? maxRatio : ratio; 
            
        }
        
        return maxRatio;
    }
    
    public double getImbalanceReplica() throws SQLException
    {
        List<Double> replicas = new ArrayList<Double>();
        for (int i = 0; i < n; i++)
            replicas.add(0.0);
        
        List<QueryCostAtPartition> costs;
        SQLStatement sql;
        double newCost;
        int pID;
        
        for (int j = 0; j < workload.size(); j++) {
        
            sql   = workload.get(j);
            costs = statementCosts(sql);
            
            
            // update statement
            if (sql.getSQLCategory().isSame(NOT_SELECT))
                for (int i = 0; i < n; i++) { 
                    newCost = replicas.get(i) + costs.get(i).getCost();
                    replicas.set(i, newCost);
                }
            else {
                // an query statement, get the top-m best cost
                Collections.sort(costs);
                
                for (int k = 0; k < m; k++) {                    
                    pID = costs.get(k).getPartitionID();
                    newCost = replicas.get(pID) + costs.get(k).getCost() / m;
                    replicas.set(pID, newCost);
                }
            }
        }
        
        
        return maxRatioInList(replicas);
    }

}
