package edu.ucsc.dbtune.bip.interactions;

import ilog.concert.IloException;
import ilog.cplex.IloCplex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.advisor.interactions.IndexInteraction;
import edu.ucsc.dbtune.bip.core.AbstractBIPSolver;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.util.LogListener;
 
/**
 * The class is responsible for solving the interaction problem: 
 * Given a query {@code q}, finds pairs of indexes {@code (c,d)} that interact with respect to 
 * {@code q}; i.e., {@code doi_q(c,d) >= delta}.
 *  
 * @author Quoc Trung Tran
 *
 */
public class InteractionBIP extends AbstractBIPSolver
{	
    protected Map<IndexInteraction, Integer> cacheInteractingPairs;
    protected InteractionOutput              interactionOutput;
    protected Optimizer                      optimizer;
    
    protected double delta;
    protected int    numCplexCall;
    protected boolean isApproximation;
    
    
    public InteractionBIP(double delta)
    {
        this.delta        = delta;
        this.numCplexCall = 0;
        this.isApproximation = false;
        this.cacheInteractingPairs = new HashMap<IndexInteraction, Integer>();
    }
	
    /**
     * Set whether to use the approximation strategy
     * 
     * @param isApprox
     *      {@code true} if we use the approximation strategy,
     *      {@code false} otherwise.
     *      
     */
    public void setApproximiationStrategy(boolean isApprox)
    {
        this.isApproximation = isApprox;
    }
    /**
     * Set the conventional optimizer that will be used to verify the correctness of BIP solution
     * @param optimizer
     *      A conventional optimizer (e.g., DB2Optimizer)
     */
    public void setConventionalOptimizer(Optimizer optimizer)
    {
        this.optimizer = optimizer;
    }
    
    
    @Override
    public IndexTuningOutput solve() throws Exception
    {   
        // 1. Communicate with INUM 
        // to derive the query plan description including internal cost, index access cost,
        // index at each slot, etc.  
        logger.setStartTimer();
        populatePlanDescriptionForStatements();
        logger.onLogEvent(LogListener.EVENT_POPULATING_INUM);
        
        // 2. Iterate over the list of query plan descs that have been derived
        interactionOutput = new InteractionOutput();
        int i = 0;
        SQLStatement sql;
        
        try {
            // start CPLEX
            cplex = new IloCplex();
                       
            // allow the solution differed 5% from the actual optimal value
            cplex.setParam(IloCplex.DoubleParam.EpGap, 0.05);
            // not output the log of CPLEX
            cplex.setOut(null);
            // not output the warning
            cplex.setWarning(null);
        }
        catch (IloException e) {
            System.err.println("Concert exception caught: " + e);
        }
        
        for (QueryPlanDesc desc : queryPlanDescs) {
            sql = workload.get(i);
            findInteractions(sql, desc);
            i++;
        }
        
        Rt.p("L95, number of CPLEX calls: " + numCplexCall);
        
        return getOutput();
    }
    
    
    @Override
    protected IndexTuningOutput getOutput() 
    {
        return interactionOutput;
    }
   
    
    /**
     * Find pair of interaction w.r.t to the given statement
     * @param sql  
     *      The giving SQL statement
     * @param desc
     *      The query plan description of the statement
     * @throws IloException 
     */
    protected void findInteractions(SQLStatement sql, QueryPlanDesc desc) throws IloException 
    {   
        // Derive list of indexes that might interact
        List<Index> candidates = new ArrayList<Index>();
        
        for (Index index : desc.getIndexes()) {
            if (!(index instanceof FullTableScanIndex))
            candidates.add(index);
        }
        
        Index indexc, indexd;        
        IndexInteraction pair;
        
        List<RestrictModel> listIIP = new ArrayList<RestrictModel>();
        
        for (int pos_c = 0; pos_c < candidates.size(); pos_c++) {            
            for (int pos_d = pos_c + 1; pos_d < candidates.size(); pos_d++) {
                
                indexc = candidates.get(pos_c);
                indexd = candidates.get(pos_d);
                
                if (cacheInteractingPairs.containsKey(new IndexInteraction(indexc, indexd))
                   || cacheInteractingPairs.containsKey(new IndexInteraction(indexd, indexc)) 
                   ) 
                   continue;                
                else 
                    listIIP.add(new RestrictModel(cplex, desc, logger, delta, indexc, indexd, 
                                              candidates, isApproximation));
            }
        }
        
        // Build the BIP to check the interaction
        for (RestrictModel restrict : listIIP) {
            
            if (restrict.solve(sql, optimizer)) {
                // cache pairs of interaction
                indexc = restrict.getFirstIndex();
                indexd = restrict.getSecondIndex();
                
                // store the first index has ID less than ID of the second index
                if (indexc.getId() < indexd.getId())                    
                    pair = new IndexInteraction(indexc, indexd);
                else 
                    pair = new IndexInteraction(indexd, indexc);
                
                cacheInteractingPairs.put(pair, 1);
                
                // store in the output
                pair.setDoiBIP(restrict.getDoiBIP());
                pair.setDoiOptimizer(restrict.getDoiOptimizer());
                interactionOutput.add(pair);
            } 
            
            restrict.clear();
            numCplexCall++;
            
            if (numCplexCall % 100 == 0)
                Rt.p(" PROCESSING statement: " + desc.getStatementID()
                                    + " number of CPLEX calls: " + numCplexCall
                                    + " number of interactions: " + interactionOutput.size());
        }
        
        numCplexCall += listIIP.size();
           
    }
    
    
    @Override
    protected void buildBIP() 
    {
        
    } 
}
 
