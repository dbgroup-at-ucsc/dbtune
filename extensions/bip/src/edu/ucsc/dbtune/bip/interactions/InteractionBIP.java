package edu.ucsc.dbtune.bip.interactions;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.advisor.interactions.IndexInteraction;
import edu.ucsc.dbtune.bip.core.AbstractBIPSolver;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.util.LogListener;
 
/**
 * The class is responsible for solving the interaction problem: 
 * Given a query {@code q}, finds pairs of indexes {@code (c,d)} 
 * that interact with respect to {@code q}; i.e., {@code doi_q(c,d) >= delta}.
 *  
 * @author Quoc Trung Tran
 *
 */
public class InteractionBIP extends AbstractBIPSolver
{	
    protected Map<IndexInteraction, Integer> cacheInteractingPairs;
    protected InteractionOutput interactionOutput;
    protected Optimizer optimizer;
    
    protected double delta;
    
    public InteractionBIP(double delta)
    {
        this.delta = delta;
        this.cacheInteractingPairs = new HashMap<IndexInteraction, Integer>();
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
    public IndexTuningOutput solve() throws SQLException, IOException
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
        
        for (QueryPlanDesc desc : listQueryPlanDescs) {
            sql = workload.get(i);
            findInteractions(sql, desc);
            i++;
        }
        
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
     */
    protected void findInteractions(SQLStatement sql, QueryPlanDesc desc) 
    {   
        // Derive list of indexes that might interact
        List<Index> indexes = new ArrayList<Index>();
        Map<Index, Integer> mapIndexSlotID = new HashMap<Index, Integer>();
        
        // Note: NOT consider FTS
        for (int i = 0; i < desc.getNumberOfSlots(); i++) {
            for (Index index : desc.getListIndexesWithoutFTSAtSlot(i)) {
                indexes.add(index);
                mapIndexSlotID.put(index, i);
            }   
        }
        
        Index indexc, indexd;
        int ic, id;
        List<RestrictIIP> listIIP = new ArrayList<RestrictIIP>();
        
        for (int pos_c = 0; pos_c < indexes.size(); pos_c++) {
            indexc = indexes.get(pos_c);
            ic = mapIndexSlotID.get(indexc);
            
            for (int pos_d = pos_c + 1; pos_d < indexes.size(); pos_d++) {
                
                indexd = indexes.get(pos_d);                
                IndexInteraction pair = new IndexInteraction(indexc, indexd);
                
                if (cacheInteractingPairs.containsKey(pair)) 
                    continue;
                else {
                    
                    IndexInteraction symetricPair = new IndexInteraction(indexd, indexc);
                    if (cacheInteractingPairs.containsKey(symetricPair)) 
                        continue;
                    
                }
                
                id = mapIndexSlotID.get(indexc);
                
                // call the BIP solution
                RestrictIIP restrict = new RestrictIIP(desc, logger, delta, indexc, indexd, 
                                              candidateIndexes, ic, id);
                listIIP.add(restrict);
            }
        }
        
        // Build the BIP to check the interaction
        for (RestrictIIP restrict : listIIP) {
            
            if (restrict.solve(sql, optimizer)) {
                // cache pairs of interaction
                IndexInteraction pair = new IndexInteraction
                                            (restrict.getFirstIndex(), restrict.getSecondIndex());
                cacheInteractingPairs.put(pair, 1);
                
                // store in the output
                pair.setDoiBIP(restrict.getDoiBIP());
                pair.setDoiOptimizer(restrict.getDoiOptimizer());
                interactionOutput.add(pair);
            } 
        }
    }
    
    
    @Override
    protected void buildBIP() 
    {
        
    } 
}
 
