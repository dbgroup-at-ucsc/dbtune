package edu.ucsc.dbtune.bip.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import edu.ucsc.dbtune.metadata.Index;

/**
 * This class stores all the candidate indexes that a BIP uses
 * 
 * @author tqtrung
 *
 */
public class BIPIndexPool 
{
    private List<Index> listIndexes;
    private Map<IndexInSlot, Integer> mapIndexInSlotToPoolID;
    private Map<Index, Integer> mapIndexToPoolID;
    public static int IDX_NOT_EXIST = -1;
    
    public BIPIndexPool()
    {
        mapIndexInSlotToPoolID = new HashMap<IndexInSlot, Integer>();
        mapIndexToPoolID = new HashMap<Index, Integer>();
        listIndexes = new ArrayList<Index>();
    }
    
    /**
     * Store the given {@code index} object into the pool
     * 
     * @param index
     *      An index object to be stored in the pool
     *      
     * {\b Note}: The index will be assigned a pool ID that is equivalent to the number of indexes
     * that are currently stored in the pool.      
     */
    public void addIndex(Index index) 
    {
        int id = listIndexes.size();
        listIndexes.add(index);
        mapIndexToPoolID.put(index, id);
    }
    
    /**
     * Get the pool ID of the given {@code index} object
     * 
     * @param index
     *      The index object to retrieve its pool ID           
     * @return
     *      The pool identifier the given {@code index} object
     *      or {@code IDX_NOT_EXIST} if the index is not stored in the pool
     */
    public int getPoolID(Index index)
    {
        Integer found = this.mapIndexToPoolID.get(index);
        if (found == null) {
            return BIPIndexPool.IDX_NOT_EXIST;
        }
        
        return found;
    }
    
    /**
     * Get the pool ID of the index stored at {@code iis} object
     * 
     * @param iis
     *      Index in slot object, corresponding to one index          
     * @return
     *      The pool ID
     */
    public int getPoolID(IndexInSlot iis)
    {
        Integer found = mapIndexInSlotToPoolID.get(iis);
        if (found == null) {
            return BIPIndexPool.IDX_NOT_EXIST;
        }
        return found;
    }
    
    /**
     * Map the position in slot of {@code index} object to this object
     * 
     * @param iis
     *      The position of the given {@code index} object, including: 
     *      query statement ID, slot ID, and the position in the slot
     * @param index
     *      The index that is stored at {@code iis} position
     */
    public void mapIndexInSlot(IndexInSlot iis, Index index)
    {
        int id = getPoolID(index);
        this.mapIndexInSlotToPoolID.put(iis, new Integer(id));
    }
    
    /**
     * Retrieve the total number of indexes stored in the pool
     * 
     * @return
     *      The number of indexes
     */
    public int getTotalIndex()
    {
        return this.listIndexes.size();
    }
    
    /**
     * Retrieve an iterator of indexes.
     *
     * @return
     *     An iterator over the indexes stored in the pool
     */
    public List<Index> indexes()
    {
       return this.listIndexes;
    }
}
