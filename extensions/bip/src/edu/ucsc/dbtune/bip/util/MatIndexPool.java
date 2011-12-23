package edu.ucsc.dbtune.bip.util;

import edu.ucsc.dbtune.bip.util.MatIndex;
import edu.ucsc.dbtune.bip.util.IndexInSlot;
import edu.ucsc.dbtune.metadata.Index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The class stores all materialized indexes to be scheduled
 * @author tqtrung
 *
 */
public class MatIndexPool 
{
	private static List<MatIndex> listIndex = new ArrayList<MatIndex>();	
	private static Map<IndexInSlot, Integer> mapPosIndexGlobal = new HashMap<IndexInSlot, Integer>();
	private static Map<Index, Integer> mapIndexGlobal = new HashMap<Index, Integer>();
	public static int startCreate, startDrop, startRemain;	
	private static int numCandidateIndexes;
	private static final int IDX_NOT_EXIST = -1;
	private static AtomicInteger totalIndex = new AtomicInteger(0);
	
	/**
	 * Add a materialize index and returns the ID
	 * @param index
	 *     A materialize index
	 *     
	 * @return
	 *     A global ID managed by this pool
	 */
	public static int addMatIndex(Index index, int type)
	{	
	    int id = totalIndex.get();
	    MatIndex matIndex = new MatIndex(index, id, type);
		listIndex.add(matIndex);
		totalIndex.getAndIncrement();
		return id;
	}

	
	public static void mapIndexToGlobalId(Index index, int id)
	{
		mapIndexGlobal.put(index, new Integer(id));
	}
	
	public static int getGlobalIdofIndex(Index index)
	{
		Object found = mapIndexGlobal.get(index);
		if (found == null) {
			return IDX_NOT_EXIST;
		}
		
		return (Integer)found;
	}
	
	
	public static void mapIndexInSlotToGlobalId(IndexInSlot iis, int globalId)
	{
		mapPosIndexGlobal.put(iis, new Integer(globalId));
	}
	
	public static int getGlobalIdofIndexInSlot(IndexInSlot iis)
	{
		Object found = mapPosIndexGlobal.get(iis);
		if (found == null) {
			return IDX_NOT_EXIST;
		}
		
		return (Integer)found;
	}
	
	public static void setStartPosCreateType(int start)
	{
		startCreate = start;
	}
	
	public static int getStartPosCreateType()
	{
		return startCreate;
	}
	
	public static void setStartPosDropType(int start)
	{
		startDrop = start;
	}
	
	public static int getStartPosDropType()
	{
		return startDrop;
	}
	
	public static void setStartPosRemainType(int start)
	{
		startRemain = start;
	}
	
	public  static int getStartPosRemainType()
	{
		return startRemain;
	}
	
	
	public static int getTotalIndex()
	{
		return totalIndex.get();
	}
	
	public static MatIndex getMatIndex(int id)
	{
		return listIndex.get(id);
	}
	
	public static MatIndex getMatIndex(Index index)
	{
		int id = getGlobalIdofIndex(index);
		if (id == IDX_NOT_EXIST) {
			return null;
		}
		return listIndex.get(id);
	}
	
	public static void setNumCandidateIndexes(int num)
	{
	    numCandidateIndexes = num;
	}
	
	public static int getNumCandidateIndexes()
	{
	    return numCandidateIndexes;
	}
	
}
