package edu.ucsc.dbtune.bip.core;

import java.sql.SQLException;
import java.util.List;

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
     * The method communicates with the given optimizer, {@code optimizer},
     * and populates the INUM space for the statement that this object
     * corresponds to
     * 
     * @param optimizer
     *      The INUM optimizer
     */
    void populateInumSpace(InumOptimizer optimizer);
    
    /**
     * Generate query plan description: the number of template plans, internal costs, 
     * index access costs, etc. )
     * 
     * @param listWorkloadTables
     *      The list of all tables that are referenced by statements in the workload
     *      that the corresponding statement of this object belong to 
     * @param poolIndexes
     *      The pool of candidate indexes   
     * 
     * {\bf Note: }The index full table scan is placed at the last position in the list of indexes
     *  at each slot.  
     *     
     * @throws SQLException 
     */
    void generateQueryPlanDesc(List<Table> listWorkloadTables, IndexPool poolIndexes) throws SQLException;
    
    
    /**
     * Retrieve the number of template plans
     *      
     * {\bf Note}: The result of this function is the value of the constant {\it K_q}
     * in the paper    
     */
    int getNumberOfTemplatePlans();

    /**
     * Retrieve the number of ``global'' slots that each template plan has;
     * where this number is equal to the number of relations in the database schema
     * that the query is defined on.  The result of this function is the value of 
     * the constant {\it n} in the paper. 
     * 
     */
    int getNumberOfGlobalSlots();
    

    /**
     * Retrieve the number of indexes that are stored in a particular slot
     * @param i
     *      The slot on which we retrieve the number of indexes
     */
    int getNumberOfIndexesEachSlot(int i);

    /**
     * Retrieve the internal plan cost of the {@code k}-template plan
     * 
     * @param k
     *      The index of the plan that we need to retrieve its internal plan cost
     *      
     * {\bf Note}: The result of this function is the value of the constant {\it \beta_{qk}}
     * in the paper.      
     */
    double getInternalPlanCost(int k);

    /**
     * Retrieve the index access cost of the index stored at a particular slot
     * 
     * @param k
     *      The position of the template plan
     * @param i
     *      The slot on which the index is stored
     * @param a
     *      The position of the index in the slot
     *      
     * {\bf Note}: The result of this function is the value of the constant {\it \gamma_{qkia}}
     * in the paper.     
     */
    double getIndexAccessCost(int k, int i, int a);

    /**
     * Retrieve the statement ID
     * 
     * {\bf Note}: Whenever a class of {@code QueryPlanDesc} processes a statement,
     * it will automatically assign an ID for this statement. 
     */
    int getStatementID();
    
    
    /**
     * Retrieve the index in the corresponding slot
     * @param i 
     *      The position of the relation
     * @param a
     *      The position of this index in the list of indexes belonging to this relation
     * @return
     *      Index
     */
    Index getIndex(int i, int a);
    
    /**
     * Check if the relation at the position {@code idSlot} is referenced by the query
     * 
     * @param idSlot
     *     The ID of the slot
     * @return
     *     {@code boolean}: true if the relation at the position {@code idSlot} is referenced by the query
     */    
    boolean isSlotReferenced(int idSlot);
    
    /**
     * Retrieve the list of tables that are referenced by the statement
     * 
     * @return
     *      A list of tables 
     */
    List<Table> getTables();
    
    
    /**  
     * Map the position in each slot of every index in the pool of candidate to the pool ID 
     * of the corresponding index
     * 
     * @param poolIndexes
     *      The pool that stores candidate indexes
     */
    void mapIndexInSlotToPoolID(IndexPool poolIndexes);
}
