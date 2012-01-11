package edu.ucsc.dbtune.bip.util;

import java.sql.SQLException;
import java.util.List;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * This interfaces serves as a cache that stores constants relating to INUM's template plans 
 * such as: internal plan cost, index access costs a particular statement in the given
 * workload of the problem setting  
 *  
 * @author tqtrung@soe.ucsc.edu
 *
 */
public interface QueryPlanDesc 
{
    /**
     * Each index in the pool of candidate indexes are placed into a particular slot
     * of the query plan, represented by an {@code IndexInSlot} object,
     * which includes: statement ID, slot ID, and the position in slot.  
     *  
     * This function maps the position in each slot   
     * of every index in the pool of candidate to the pool ID of the corresponding index
     * @param poolIndexes
     *      The pool that stores candidate indexes
     *      
     * {\bf Note:} Our assumption is that every index is defined on exactly one relation.     
     */
    void mapIndexInSlotToPoolID(BIPIndexPool poolIndexes);
    
    /**
     * Retrieve the number of template plans
     *      
     * {\bf Note}: The result of this function is the value of the constant {\it K_q}
     * in the paper    
     */
    int getNumberOfTemplatePlans();

    /**
     * Retrieve the number of slots that each template plan has
     * 
     * {\bf Note:} The number of slots is equal to the number of relations in the database schema
     * that the query is defined on. The result of this function is the value of the constant {\it n}
     * in the paper. 
     * 
     */
    int getNumberOfSlots();
    

    /**
     * Retrieve the number of indexes that are stored in a particular slot
     * @param i
     *      The slot on which we retrieve the number of indexes
     */
    int getNumberOfIndexesEachSlot(int i);

    /**
     * Retrieve the internal plan cost of the {@code k}^th template plan
     * 
     * @param k
     *      The index of the plan that we need to retrieve its internal plan cost
     *      
     * {\bf Note}: The result of this function is the value of the constant {\it \beta_{qk}}
     * in the paper.      
     */
    double getInternalPlanCost(int k);

    /**
     * Retrieve the index acess cost of the index stored at a particular slot
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
     * Populate query plan description: the number of template plans, internal cost, 
     * index access cost, etc. )
     * 
     * @param communicator
     *      The class to communicate with INUM to get InumSpace
     * @param schema
     *      The schema on which this statement refers to     
     * @param stmt
     *      A SQL statement
     * @param poolIndexes
     *      The pool of candidate indexes   
     * 
     * {\bf Note}:
     * <p>
     * <ol> 
     *  <li> There does not contain the full table scan in {@code poolIndexes}</li>
     *  <li> The index full table scan is always assigned the last position in the list of indexes
     *     at each slot </li>
     *  <li> Optimization: {@code schema} can be modified to contain only
     *  relations that are referenced in the workload </li>
     * </ol>
     * </p>  
     *     
     * @throws SQLException 
     */
    void generateQueryPlanDesc (InumCommunicator communicator, Schema schema,
                                List<IndexFullTableScan> listFullTableScanIndexes,
                                SQLStatement stmt, BIPIndexPool poolIndexes) 
                                throws SQLException;
    
    
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
}
