package edu.ucsc.dbtune.bip.sim;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import edu.ucsc.dbtune.bip.util.QueryPlanDesc;
import edu.ucsc.dbtune.inum.InumSpace;
import edu.ucsc.dbtune.metadata.Index;

public class SimBIP {
	
	private SimCPlex cplex;
	
	public SimBIP() {}
	
	/**
	 * TODO: contact with INUM to get sufficient information before calling SimCPlex to formulate
	 * and run the BIP problem
	 * 
	 */
	public void run()
	{
		
	}
	/**	
	 * Scheduling materialization index
	 * 
	 * @param Sinit
	 * 		The initial configuration
	 * @param Smat
	 * 		The configuration to be materialized	
	 * @param listInumSpace
	 * 		Each Inum space corresponds to one query in the workload
	 * @param W
	 * 		The number of window time
	 * @param B
	 * 		The maximum space to be constrained all the materialized index at each the window time
	 * 
	 * 
	 * @return
	 * 		A sequence of index to be materialized (created/dropped) in the corresponding window time
	 */
	public List<MatIndex> schedule(List<Index> Sinit, List<Index> Smat, List<InumSpace> listInumSpace, int W, 
									List<Double> B)
	{
		List<Index> candidateIndexes = new ArrayList<Index>();
		// 1. Derive Sin, Sout, Sremain
		classifyTypeIndex(Sinit, Smat);
		
		// 2.Derive candidate indexes
		for (int idx = 0; idx < MatIndexPool.getTotalIndex(); idx++)
		{
			Index index = MatIndexPool.getMatIndex(idx).getIndex();
			candidateIndexes.add(index);
		}
		
		// 3. Derive the query plan description including internal cost, index access cost,
		// index at each slot ... 
		List<QueryPlanDesc> listQueryPlan = new ArrayList<QueryPlanDesc>();
		for (Iterator<InumSpace> iter = listInumSpace.iterator(); iter.hasNext();)
		{
			QueryPlanDesc desc = new QueryPlanDesc();
			desc.generateQueryPlanDesc(iter.next(), candidateIndexes);
			listQueryPlan.add(desc);
		}
		
		// 4. Create a mapping from Index in slot (of each query plan) into its global Id
		// (in @MapIndexPool array)
		mapIndexInSlotGlobal(listQueryPlan);
		
		// 5. Ask @SimCPlex to formulate BIP and run the BIP to derive the final result 
		// which is a list of materialized indexes, stored in @listMatIndex
		cplex = new SimCPlex();
		List<MatIndex> listMatIndex  = cplex.run(listQueryPlan, W, B);
		
		return listMatIndex;
	}
	
	/**
	 * Print the detail of the schedule
	 * 
	 * @param listIndex
	 * 		The list of materialized index
	 * @param W
	 * 		Number of window times
	 * 
	 * @return
	 * 		The string contains a sequence of command to implement the scheduling index materialization
	 * 
	 */
	public String printSchedule(List<MatIndex> listIndex, int W)
	{
		String strSchedule = "";
		for (int w = 0; w < W; w++)
		{
			strSchedule += "=== At window " + w + "-th:============\n"; 
			for (Iterator<MatIndex> iter = listIndex.iterator(); iter.hasNext(); )
			{
				MatIndex idx = iter.next();
				if (idx.getMatWindow() == w)
				{
					if (idx.getTypeMatIndex() == MatIndex.INDEX_TYPE_CREATE)
					{
						strSchedule += " CREATE: ";
					}
					else if (idx.getTypeMatIndex() == MatIndex.INDEX_TYPE_DROP)
					{
						strSchedule += " DROP ";
					}
					strSchedule += idx.getIndex().getName();
					strSchedule += "\n";
				}
			}
		}
		
		return strSchedule;
	}
	
	/**
	 * Classify the index into one of the three types: INDEX_TYPE_CREATE, INDEX_TYPE_DROP, INDEX_TYPE_REMAIN
	 * 
	 * @param Sinit
	 * 		The initial configuration
	 * @param Smat
	 * 		The materialized configuration
	 * 
	 */
	private void classifyTypeIndex(List<Index> Sinit, List<Index> Smat)
	{
		List<Index> Sin = new ArrayList<Index>();
		List<Index> Sout = new ArrayList<Index>();
		List<Index> Sremain = new ArrayList<Index>();
		Index idxInit, idxMat;
		
		// create a hash map to speed up the performance
		HashMap mapNameIndexInit = new HashMap<String, Integer>();
		HashMap mapNameIndexMat = new HashMap<String, Integer>();
		for (Iterator<Index> iterInit = Sinit.iterator(); iterInit.hasNext(); )
		{
			mapNameIndexInit.put(iterInit.next().getName(), 1);
		}
		
		for (Iterator<Index> iterMat = Sinit.iterator(); iterMat.hasNext(); )
		{
			mapNameIndexMat.put(iterMat.next().getName(), 1);
		}		
		
		// 1. Computer Sremain and Sout
		for (Iterator<Index> iterInit = Sinit.iterator(); iterInit.hasNext(); )
		{
			idxInit = iterInit.next();
			
			if (mapNameIndexMat.containsKey(idxInit.getName()) == true)
			{
				Sremain.add(idxInit);
			}
			else 		 
			{
				Sout.add(idxInit);
			}
		}
		
		
		for (Iterator<Index> iterMat = Smat.iterator(); iterMat.hasNext(); )
		{
			idxMat = iterMat.next();
			if (mapNameIndexInit.containsKey(idxMat.getName()) == false)
			{
				Sin.add(idxMat); 
			}
		}
		
		// 2. Store into@MatIndexPool
		// in the order of @Sin, @Sout, and @Sremain
		int globalId = 0;
		Index idx;
		String name;
		MatIndexPool.setStartPosCreateIndexType(globalId);
		for (Iterator<Index> iterIn = Sin.iterator(); iterIn.hasNext(); )
		{
			idx = iterIn.next();
			MatIndex matIndex = new MatIndex(idx, globalId, MatIndex.INDEX_TYPE_CREATE);
			MatIndexPool.addMatIndex(matIndex);			
			MatIndexPool.mapNameIndexGlobalId(idx.getName(), globalId);			
			globalId++; 
		}
		
		int test = MatIndexPool.getIndexGlobalId("table_0_index_0");
		
		MatIndexPool.setStartPosDropIndexType(globalId);
		for (Iterator<Index> iterOut = Sout.iterator(); iterOut.hasNext(); )
		{
			idx = iterOut.next();
			MatIndex matIndex = new MatIndex(idx, globalId, MatIndex.INDEX_TYPE_DROP);
			MatIndexPool.addMatIndex(matIndex);
			MatIndexPool.mapNameIndexGlobalId(idx.getName(), globalId);			
			globalId++;
		}
		
		MatIndexPool.setStartPosRemainIndexType(globalId);
		for (Iterator<Index> iterRemain = Sremain.iterator(); iterRemain.hasNext(); )
		{
			idx = iterRemain.next();
			MatIndex matIndex = new MatIndex(idx, globalId, MatIndex.INDEX_TYPE_REMAIN);
			MatIndexPool.addMatIndex(matIndex);
			MatIndexPool.mapNameIndexGlobalId(idx.getName(), globalId);			
			globalId++;
		}
		MatIndexPool.setTotalIndex(globalId);
	
	}
	
	/**
	 * Map each index in each slot per query into its global Id
	 * 
	 * @param listQueryPlan
	 * 		The list of query plans, created from the given workload
	 *  
	 */
	private void mapIndexInSlotGlobal(List<QueryPlanDesc> listQueryPlan)
	{
		
		int globalId;
		for (int q = 0; q < listQueryPlan.size(); q++)
		{
			QueryPlanDesc queryPlan = listQueryPlan.get(q);
			for (int i = 0; i < queryPlan.getNumRels(); i++)
			{
				for (int a = 0; a < queryPlan.getNumIndexEachSlot(i); a++)
				{
					Index idx = queryPlan.getIndex(i, a);
					globalId = MatIndexPool.getIndexGlobalId(idx.getName());
					IndexInSlot iis = new IndexInSlot(q,i,a);
					MatIndexPool.mapPosIndexGlobalId(iis, globalId);
				}
			}
		}
	}
}
