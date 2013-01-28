package edu.ucsc.dbtune.divgdesign;

import java.sql.SQLException;
import java.util.ArrayList;
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
import edu.ucsc.dbtune.workload.SQLStatement;

import static edu.ucsc.dbtune.bip.core.InumQueryPlanDesc.preparedStmts;
import static edu.ucsc.dbtune.bip.util.LogListener.EVENT_POPULATING_INUM;

import static edu.ucsc.dbtune.workload.SQLCategory.DELETE;
import static edu.ucsc.dbtune.workload.SQLCategory.INSERT;
import static edu.ucsc.dbtune.bip.core.InumQueryPlanDesc.INDEX_UPDATE_COST_FACTOR;

public class CoPhyDivgDesign extends DivgDesign 
{    
    private DatabaseSystem db;
    private InumOptimizer  io;
    private LogListener    logger;
    
    private CoPhy cophy;  
    private Map<SQLStatement, Set<Index>> recommendedIndexStmt;
    private double timeInum;
    private double timeAnalysis;
    
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
    public void recommend(List<SQLStatement> workload, int nReplicas, int loadfactor, double B)
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
        
        // check if we already generate candidate for this statement before
        for (SQLStatement sql : sqls)
            candidates.addAll(recommendedIndexStmt.get(sql));
        
        Optimizer io = db.getOptimizer();
        List<SQLStatement> wlPartition = new ArrayList<SQLStatement>(sqls);
        cophy.setCandidateIndexes(candidates);
        cophy.setWorkload(wlPartition); 
        cophy.setOptimizer(io);
        cophy.setSpaceBudget(B);
        cophy.setLogListenter(logger);
        
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
        double baseTableCost;
        double indexUpdateCost;
        
        for (int i = 0; i < n; i++) {
            
            explain = inumStmt.explain(indexesAtReplica.get(i));
            cost = explain.getTotalCost();
            
            // temporary take get select cost out of the cost
            if (sql.getSQLCategory().isSame(INSERT) || 
                    sql.getSQLCategory().isSame(DELETE)) {
                // cost of base table & update cost
                cost -= explain.getSelectCost();
                baseTableCost = explain.getBaseTableUpdateCost();
                indexUpdateCost = cost - baseTableCost;
                indexUpdateCost *= INDEX_UPDATE_COST_FACTOR;
                
                cost = baseTableCost + indexUpdateCost;                
            }
            
            cost *= sql.getStatementWeight();
            
            /*
            if (sql.getSQLCategory().isSame(DELETE) ||
                    sql.getSQLCategory().isSame(INSERT) ) {
                System.out.println(sql.getSQLCategory() + "\n"
                                    + " replica = " + i + " cost = " + cost + "\n"
                                    + " base table cost: " + explain.getBaseTableUpdateCost()
                                    + "\n"
                                    + " configuration: " + indexesAtReplica.get(i).size()
                                    + "\n"
                                    + explain.getPlan());
            }
            */
            
            costs.add(new QueryCostAtPartition(i, cost));
        }
            
        return costs;
    }

}
