package edu.ucsc.dbtune.bip.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import edu.ucsc.dbtune.metadata.Index;


public class BIPIndexPool implements IndexPool 
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
    
    /* (non-Javadoc)
     * @see edu.ucsc.dbtune.bip.util.IndexPool#addIndex(edu.ucsc.dbtune.metadata.Index)
     */
    public void addIndex(Index index) 
    {
        int id = listIndexes.size();
        listIndexes.add(index);
        mapIndexToPoolID.put(index, id);        
    }
    
    /* (non-Javadoc)
     * @see edu.ucsc.dbtune.bip.util.IndexPool#getPoolID(edu.ucsc.dbtune.metadata.Index)
     */
    public int getPoolID(Index index)
    {
        Integer found = this.mapIndexToPoolID.get(index);
        if (found == null) {
            return BIPIndexPool.IDX_NOT_EXIST;
        }
        
        return found;
    }
    
    /* (non-Javadoc)
     * @see edu.ucsc.dbtune.bip.util.IndexPool#getPoolID(edu.ucsc.dbtune.bip.util.IndexInSlot)
     */
    public int getPoolID(IndexInSlot iis)
    {
        Integer found = mapIndexInSlotToPoolID.get(iis);
        if (found == null) {
            return BIPIndexPool.IDX_NOT_EXIST;
        }
        return found;
    }
    
    /* (non-Javadoc)
     * @see edu.ucsc.dbtune.bip.util.IndexPool#mapIndexInSlot(edu.ucsc.dbtune.bip.util.IndexInSlot, edu.ucsc.dbtune.metadata.Index)
     */
    public void mapIndexInSlot(IndexInSlot iis, Index index)
    {
        int id = getPoolID(index);
        this.mapIndexInSlotToPoolID.put(iis, new Integer(id));
    }
    
    /* (non-Javadoc)
     * @see edu.ucsc.dbtune.bip.util.IndexPool#getTotalIndex()
     */
    public int getTotalIndex()
    {
        return this.listIndexes.size();
    }
    
    /* (non-Javadoc)
     * @see edu.ucsc.dbtune.bip.util.IndexPool#indexes()
     */
    public List<Index> indexes()
    {
       return this.listIndexes;
    }
}
