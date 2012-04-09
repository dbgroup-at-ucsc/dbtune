package edu.ucsc.dbtune.divgdesign;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.caliper.internal.guava.collect.Lists;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.bip.indexadvisor.CoPhy;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import static edu.ucsc.dbtune.bip.core.InumQueryPlanDesc.preparedStmts;

public class CoPhyDivgDesign extends DivgDesign 
{    
    private DatabaseSystem db;
    private InumOptimizer  io;
    private LogListener    logger;
    
    private CoPhy cophy;  
    private Map<SQLStatement, Set<Index>> recommendedIndexStmt;
    
    
    public CoPhyDivgDesign(DatabaseSystem db, InumOptimizer  io, LogListener logger)
    {
        this.db = db;
        this.io = io;
        this.logger = logger;
        
        // initialize data structures
        cophy = new CoPhy();        
        recommendedIndexStmt = new HashMap<SQLStatement, Set<Index>>();
    }
    
    @Override
    protected Set<Index> getRecommendation(List<SQLStatement> sqls) throws Exception
    {
        CandidateGenerator candGen =
            new OptimizerCandidateGenerator(db.getOptimizer().getDelegate());
        
        Set<Index> candidates = new HashSet<Index>();
        Set<Index> candidate  = new HashSet<Index>();
        
        // check if we already generate candidate for this statement before
        for (SQLStatement sql : sqls) {
            
            candidate = recommendedIndexStmt.get(sql);
            
            if (candidate == null) {
                
                Workload wl = new Workload(Lists.newArrayList(sql));
                candidate = candGen.generate(wl);                
                recommendedIndexStmt.put(sql, candidate);
                
            }
            candidates.addAll(candidate);
        }
        
        Optimizer io = db.getOptimizer();
        Workload wlPartition = new Workload(sqls);
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
        
        for (int i = 0; i < n; i++) {            
            cost = inumStmt.explain(indexesAtReplica.get(i)).getTotalCost();
            costs.add(new QueryCostAtPartition(i, cost));
        }
            
        return costs;
    }

}
