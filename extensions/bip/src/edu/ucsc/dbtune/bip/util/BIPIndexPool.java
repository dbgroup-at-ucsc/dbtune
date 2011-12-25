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
    
    public void addIndex(Index index) 
    {
        int id = listIndexes.size();
        listIndexes.add(index);
        mapIndexToPoolID.put(index, id);
    }
    
    public int getPoolID(Index index)
    {
        Integer found = this.mapIndexToPoolID.get(index);
        if (found == null) {
            return BIPIndexPool.IDX_NOT_EXIST;
        }
        
        return found;
    }
    
    public int getPoolID(IndexInSlot iis)
    {
        Integer found = mapIndexInSlotToPoolID.get(iis);
        if (found == null) {
            return BIPIndexPool.IDX_NOT_EXIST;
        }
        return found;
    }
    
    public void mapIndexInSlot(IndexInSlot iis, Index index)
    {
        int id = getPoolID(index);
        this.mapIndexInSlotToPoolID.put(iis, new Integer(id));
    }
    
    /**
     * Returns an iterator of indexes.
     *
     * @return
     *     an iterator over the indexes stored in this pool
     */
    public List<Index> indexes()
    {
       return this.listIndexes;
    }
}
