package edu.ucsc.dbtune.bip.core;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.InumOptimizer;

/**
 * This interface corresponds to a statement in the given workload.
 * It serves as a cache that stores constants relating to INUM's template plans 
 * such as: internal plan cost, index access costs. 
 *  
 * @author tqtrung@soe.ucsc.edu
 *
 */
public interface QueryPlanDesc 
{   
    /**
     * Communicate with INUM to generate query plan description: the number of template plans, internal costs, 
     * index access costs, etc. 
     * 
     * @param optimizer
     *      The INUM optimizer 
     * @param candidateIndexes
     *      The set of candidate indexes   
     * 
     * {\bf Note: }The index full table scan is placed at the last position in the list of indexes
     * at each slot. The {@code candidateIndexes} does not contain full table scan indexes.  
     *     
     * @throws SQLException
     *      when there is erros in connecting with {@code optimizer} 
     */
    void generateQueryPlanDesc(InumOptimizer optimizer, Set<Index> candidateIndexes) throws SQLException;
    
    /**
     * Retrieve the number of template plans
     *      
     * {\bf Note}: The result of this function is the value of the constant {\it K_q}
     * in the paper    
     */
    int getNumberOfTemplatePlans();
    
    /**
     * Retrieve the number of slots that this statement has
     *     
     */
    int getNumberOfSlots();
    
    /**
     * Retrieve the internal plan cost of the {@code k}^{th} template plan
     * 
     * @param k
     *      The plan ID that we need to retrieve the internal plan cost
     *      
     * {\bf Note}: The result of this function is the value of the constant {\it \beta_{qk}}
     * in the paper.      
     */
    double getInternalPlanCost(int k);

    /**
     * Retrieve the index access cost of the given index in a particular plan
     * 
     * @param k
     *      The ID of the plan
     * @param index    
     *      The index to retrieve the access cost
     *      
     * {\bf Note}: The result of this function is the value of the constant {\it \gamma_{qkia}}
     * in the paper. From the given {@code index}, we can infer the slot it is placed.     
     */
    double getAccessCost(int k, Index index);

    /**
     * Retrieve the statement ID
     * 
     *  
     */
    int getStatementID();
    
    
    /**
     * Retrieve the list of tables that are referenced by the statement
     * 
     * @return
     *      A list of tables 
     */
    List<Table> getTables();
    
    /**
     * Retrieve the list of indexes (including FTS indexes) that are stored in a particular slot
     * @param i
     *      The slot on which we retrieve the set of indexes
     */
    List<Index> getListIndexesAtSlot(int i);

    /**
     * Retrieve the list of indexes (excluding FTS indexes) that are stored in a particular slot
     * @param i
     *      The slot on which we retrieve the list of indexes
     */
    List<Index> getListIndexesWithoutFTSAtSlot(int i);
}
