package edu.ucsc.dbtune.bip.core;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.workload.SQLCategory;

/**
 * This interface corresponds to a statement in the given workload. It serves as a cache that 
 * stores constants relating to INUM's template plans such as: internal plan cost, index access 
 * costs. 
 *  
 * @author Quoc Trung Tran
 *
 */
public interface QueryPlanDesc 
{   
    /**
     * Communicate with INUM to generate query plan description: the number of template plans, 
     * internal costs, index access costs, etc. 
     * 
     * @param optimizer
     *      The INUM optimizer 
     * @param candidateIndexes
     *      The set of candidate indexes   
     * 
     * {\bf Note: }The index full table scan is placed at the last position in the list of indexes
     * at each slot.   
     *     
     * @throws SQLException
     *      when there is error in connecting with {@code optimizer} 
     */
    void generateQueryPlanDesc(InumOptimizer optimizer, Set<Index> candidateIndexes) 
                              throws SQLException;
    
    /**
     * Retrieve the number of template plans corresponding to the SQLStatement that this
     * object corresponds to. 
     *      
     * @return
     *      The number of template plans.
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
     * @return  
     *      The internal plan cost.
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
     * @return 
     *      The index access cost.
     *      
     * {\bf Note}: The result of this function is the value of the constant {\it \gamma_{qkia}}
     * in the paper. From the given {@code index}, we can infer the slot (i) that the index is 
     * placed.     
     */
    double getAccessCost(int k, Index index);
    
    /**
     * Retrieve the base table update cost (which is a constant)
     * 
     * @return
     *      The base table update cost if the statement is an UPDATE,
     *      0, otherwise
     */
    double getBaseTableUpdateCost();

    /**
     * Retrieve the statement ID
     * 
     * @return 
     *      The statement ID
     *  
     */
    int getStatementID();
    
    
    /**
     * Retrieve the list of tables that are referenced by the statement
     * 
     * @return
     *      The list of refenced tables. 
     */
    List<Table> getTables();
    
    /**
     * Retrieve the list of indexes (including FTS indexes) that are stored in 
     * the given slot {@code i}.
     * 
     * @param i
     *      The slot on which we retrieve the set of indexes
     * @return
     *      The list of indexes at slot i.       
     */
    List<Index> getIndexesAtSlot(int i);

    /**
     * Retrieve the list of indexes (excluding FTS indexes) that are stored in 
     * the given slot {@code i}.
     * 
     * @param i
     *      The slot on which we retrieve the list of indexes
     * @return 
     *      The list of indexes at slot i.    
     */
    List<Index> getIndexesWithoutFTSAtSlot(int i);
    
    /**
     * 
     * Retrieve the set of indexes that are compatible with at least one slot in one template plan
     * of the given statement. Note that this method does not take the Full Table Scan Index into 
     * account.
     * 
     * @param i
     *      The slot ID
     *      
     * @return  
     *      The set of indexes that are compatible with at least one slot in one template plan
     *      of the query.
     */
    Set<Index> getActiveIndexesAtSlot(int i);
    
    /**
     * Retrieve the SQL type of the statement (e.g., SELECT, UPDATE statement)
     * 
     * @return
     *      The category of the statement. 
     */
    SQLCategory getSQLCategory();
    
    /**
     * Returns the update cost for the given index. This cost doesn't include the {@code SELECT} 
     * shell cost, i.e. the cost of retrieving the tuples that will be affected by the update.
     *
     * @param index
     *      a {@link edu.ucsc.dbtune.metadata.Index} object.
     * @return
     *      maintenance cost for the given index; zero if the statement isn't an update or if the 
     *      {@link #getUpdatedConfiguration updated configuration} assigned to the statement doesn't 
     *      contain the given index.
     */
    public double getUpdateCost(Index index);
}
