package edu.ucsc.dbtune.bip.sim;

import edu.ucsc.dbtune.bip.sim.MatIndex;
import edu.ucsc.dbtune.bip.util.IndexInSlot;
import edu.ucsc.dbtune.metadata.Index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	public static int startCreate, startDrop, startRemain, totalIndex;	
	public static final int IDX_NOT_EXIST = -1;
	
	/**
	 * Get/Set methods
	 */
	public static void addMatIndex(MatIndex index)
	{	
		listIndex.add(index);
	}

	public static void mapIndexGlobalId(Index index, int id)
	{
		mapIndexGlobal.put(index, new Integer(id));
	}
	
	public static int getIndexGlobalId(Index index)
	{
		Object found = mapIndexGlobal.get(index);
		if (found == null)
		{
			return IDX_NOT_EXIST;
		}
		
		return (Integer)found;
	}
	
	
	public static void mapPosIndexGlobalId(IndexInSlot iis, int globalId)
	{
		mapPosIndexGlobal.put(iis, new Integer(globalId));
	}
	
	public static int getIndexGlobalId(IndexInSlot iis)
	{
		Object found = mapPosIndexGlobal.get(iis);
		if (found == null) {
			return IDX_NOT_EXIST;
		}
		
		return (Integer)found;
	}
	
	public static void setStartPosCreateIndexType(int start)
	{
		startCreate = start;
	}
	
	public static int getStartPosCreateIndexType()
	{
		return startCreate;
	}
	
	public static void setStartPosDropIndexType(int start)
	{
		startDrop = start;
	}
	
	public static int getStartPosDropIndexType()
	{
		return startDrop;
	}
	
	public static void setStartPosRemainIndexType(int start)
	{
		startRemain = start;
	}
	
	public  static int getStartPosRemainIndexType()
	{
		return startRemain;
	}
	
	public static void setTotalIndex(int total)
	{
		totalIndex = total;
	}
	
	public static int getTotalIndex()
	{
		return totalIndex;
	}
	
	public static MatIndex getMatIndex(int id)
	{
		return listIndex.get(id);
	}
	
	public static MatIndex getMatIndex(Index index)
	{
		int id = getIndexGlobalId(index);
		if (id == IDX_NOT_EXIST) {
			return null;
		}
		return listIndex.get(id);
	}
	
}
