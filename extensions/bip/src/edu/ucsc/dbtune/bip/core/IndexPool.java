package edu.ucsc.dbtune.bip.core;

import java.util.List;

import edu.ucsc.dbtune.bip.util.IndexInSlot;
import edu.ucsc.dbtune.metadata.Index;

/**
 * This class implements a pool that contains a set of candidate indexes
 * 
 * @author tqtrung@soe.ucsc.edu
 *
 */
public interface IndexPool 
{
    /**
     * Store the given {@code index} object into the pool
     * 
     * @param index
     *      An index object to be stored in the pool
     *      
     * {\b Note}: The index will be assigned a pool ID that is equivalent to the number of indexes
     * that are currently stored in the pool.      
     */
    void addIndex(Index index);

    /**
     * Get the pool ID of the given {@code index} object
     * 
     * @param index
     *      The index object to retrieve its pool ID           
     * @return
     *      The pool identifier the given {@code index} object
     *      or {@code IDX_NOT_EXIST} if the index is not stored in the pool
     */
    int getPoolID(Index index);
    
    /**
     * Retrieve the total number of indexes stored in the pool
     * 
     * @return
     *      The number of indexes
     */
    int getTotalIndex();

    /**
     * Retrieve an iterator of indexes.
     *
     * @return
     *     An iterator over the indexes stored in the pool
     */
    List<Index> indexes();

    /** 
     * Each index in the pool of candidate indexes are placed into a particular slot
     * of the query plan, represented by an {@code IndexInSlot} object,
     * which includes: statement ID, slot ID, and the position in slot.      
     * Our assumption is that every index is defined on exactly one relation.
     * 
     * This function maps the position in slot of {@code index} object to this object
     * 
     * @param iis
     *      The position of the given {@code index} object, including: 
     *      query statement ID, slot ID, and the position in the slot
     * @param index
     *      The index that is stored at {@code iis} position
     */
    void mapIndexInSlot(IndexInSlot iis, Index index);
    
    /**
     * Get the pool ID of the index stored at {@code iis} object
     * 
     * @param iis
     *      Index in slot object, corresponding to one index          
     * @return
     *      The pool ID
     */
    int getPoolID(IndexInSlot iis);
}
