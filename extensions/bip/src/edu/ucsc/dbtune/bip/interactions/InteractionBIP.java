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
import edu.ucsc.dbtune.bip.core.InumQueryPlanDesc;
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
    protected Map<Index, Integer> mapIndexPartitionID;
    protected List<StablePartition> listPartitions;
    
    public InteractionBIP(double delta)
    {
        this.delta        = delta;
        this.numCplexCall = 0;
        this.isApproximation = false;
        this.cacheInteractingPairs = new HashMap<IndexInteraction, Integer>();
        this.mapIndexPartitionID = new HashMap<Index, Integer>(); 
        this.listPartitions = new ArrayList<StablePartition>(); 
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
        int counter = 0;
        SQLStatement sql;
        
        try {
            // start CPLEX
            cplex = new IloCplex();
                       
            // allow the solution differed 5% from the actual optimal value
            cplex.setParam(IloCplex.DoubleParam.EpGap, 0.05);
            // not output the log of CPLEX
            // not output the log of CPLEX
            if (!environment.getIsShowCPlexOutput())
                cplex.setOut(null);
            // not output the warning
            cplex.setWarning(null);
        }
        catch (IloException e) {
            System.err.println("Concert exception caught: " + e);
        }
        
        for (QueryPlanDesc desc : queryPlanDescs) {
            Rt.p("number of template plans: " + desc.getNumberOfTemplatePlans());
            sql = workload.get(counter);
            findInteractions(sql, desc);
            counter++;
            
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
    protected void findInteractions(SQLStatement sql, QueryPlanDesc desc) throws Exception 
    {   
        // Derive list of indexes that might interact
        List<Index> candidates = new ArrayList<Index>();
        
        for (Index index : desc.getIndexes()) {
            if (!(index instanceof FullTableScanIndex))
                candidates.add(index);
        }
        
        Index indexc, indexd;
        IndexInteraction pair;
        
        for (int pos_c = 0; pos_c < candidates.size(); pos_c++) {            
            for (int pos_d = pos_c + 1; pos_d < candidates.size(); pos_d++) {
                
                indexc = candidates.get(pos_c);
                indexd = candidates.get(pos_d);
                 
                if (indexc.getId() <= indexd.getId())
                    pair = new IndexInteraction(indexc, indexd);
                else 
                    pair = new IndexInteraction(indexd, indexc);
                
                if (cacheInteractingPairs.containsKey(pair)) 
                    continue;
                
                RestrictModel restrict = new RestrictModel(cplex, desc, logger, delta, indexc, indexd, 
                        desc.getIndexes(), isApproximation);
                    
                if (restrict.solve(sql, optimizer)) {
                    cacheInteractingPairs.put(pair, 1);
                    // store in the output
                    pair.setDoiBIP(restrict.getDoiBIP());
                    pair.setDoiOptimizer(restrict.getDoiOptimizer());
                    interactionOutput.add(pair);
                } 
                numCplexCall++;
            }
        }
        
        Rt.p(" PROCESSING statement: " + desc.getStatementID()
                + " number of CPLEX calls: " + numCplexCall
                + " number of interactions: " + interactionOutput.size());
    }
    
    
    /**
     * Find pair of interaction w.r.t to the given statement
     * @param sql  
     *      The giving SQL statement
     * @param desc
     *      The query plan description of the statement
     * @throws IloException 
     */
    protected void findInteractionsSP(SQLStatement sql, QueryPlanDesc desc) throws Exception 
    {   
        // Derive list of indexes that might interact
        List<Index> candidates = new ArrayList<Index>();
        
        for (Index index : desc.getIndexes()) {
            if (!(index instanceof FullTableScanIndex))
                candidates.add(index);
        }
        
        Index indexc, indexd;
        int partitionC, partitionD;
        
        
        for (int pos_c = 0; pos_c < candidates.size(); pos_c++) {            
            for (int pos_d = pos_c + 1; pos_d < candidates.size(); pos_d++) {
                
                indexc = candidates.get(pos_c);
                indexd = candidates.get(pos_d);
                 
                partitionC = getPartitionID(indexc);
                partitionD = getPartitionID(indexd);

                // two indexes have been detected as interacting
                if (partitionC == partitionD && partitionC >= 0)
                    continue;
                
                RestrictModel restrict = new RestrictModel(cplex, desc, logger, delta, indexc, indexd, 
                        desc.getIndexes(), isApproximation);
                    
                if (restrict.solve(sql, optimizer)) {
                    if (partitionC >= 0 && partitionD >= 0)
                        mergePartition(partitionC, partitionD, indexc, indexd);
                    else if (partitionC >= 0 && partitionD < 0)
                        addIndexToPartition(partitionC, indexd);
                    else if (partitionD >= 0 && partitionC < 0)
                        addIndexToPartition(partitionD, indexc);
                    else {
                        // create a new partition
                        int idPartition = listPartitions.size();
                        StablePartition sb = new StablePartition(idPartition);
                        listPartitions.add(sb);
                        addIndexToPartition(idPartition, indexc);
                        addIndexToPartition(idPartition, indexd);
                    } 
                        // store in the output
                    //pair.setDoiBIP(restrict.getDoiBIP());
                    //    pair.setDoiOptimizer(restrict.getDoiOptimizer());
                    //    interactionOutput.add(pair);
                } 
                numCplexCall++;
            }
        }
        
        Rt.p(" PROCESSING statement: " + desc.getStatementID()
                + " number of CPLEX calls: " + numCplexCall
                + " stable partitions: " + listPartitions);
    }
    
    
    @Override
    protected void buildBIP() 
    {
        
    } 
    
    private int getPartitionID(Index index)
    {
        if (mapIndexPartitionID.containsKey(index))
            return mapIndexPartitionID.get(index);
        else
            return -1;
    }
    
    /**
     * todo
     * @param partitionC
     * @param partitionD
     * @param indexc
     * @param indexd
     */
    private void mergePartition(int partitionC, int partitionD, Index indexc, Index indexd)
    {
        Rt.p("Merge " + partitionD + " into " + partitionC);
        listPartitions.get(partitionD).addIndex(indexd);
        Rt.p(" partition d : " + listPartitions.get(partitionD).getIndexes().size());
        Rt.p(" partition c : " + listPartitions.get(partitionC).getIndexes().size());
        // Merge indexes from partitionD into partitionC
        for (Index index : listPartitions.get(partitionD).getIndexes()) {
            listPartitions.get(partitionC).addIndex(index);
            mapIndexPartitionID.put(index, partitionC);
        }
        
        listPartitions.get(partitionD).markNotUsed();
        Rt.p("AFTER partition c : " + listPartitions.get(partitionC).getIndexes().size());
    }
    
    /**
     * Add index into an existing partition
     * 
     * @param id
     *      The identifier in listPartitions
     *  
     * @param index
     */
    private void addIndexToPartition(int id, Index index)
    {
        listPartitions.get(id).addIndex(index);
        mapIndexPartitionID.put(index, id);
    }
    
    static class StablePartition
    {   
        private int partitionID;
        private List<Index> indexes;
        private boolean hasUsed;
        
        public StablePartition(int id)
        {
            this.partitionID = id;
            this.indexes = new ArrayList<Index>();
            this.hasUsed = true;
        }
        
        public boolean isUsed()
        {
            return hasUsed;
        }
        
        public void markNotUsed()
        {
            hasUsed = false;
        }
        
        public int getId()
        {
            return partitionID;
        }
        
        public void addIndex(Index index)
        {
            indexes.add(index);
        }
        
        public List<Index> getIndexes()
        {
            return this.indexes;
        }
        
        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            
            sb.append("id " + partitionID)
            .append(" List index ");
            
            for (Index index : indexes)
                sb.append(index.getId() + " ");
            
            sb.append("\n");
            return sb.toString();
        }
    }
}
 
