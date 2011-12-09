package edu.ucsc.dbtune.bip.sim;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.inum.InumSpace;
import edu.ucsc.dbtune.metadata.Index;

public class SimBIP 
{
	
	private SimCPlex cplex;
	
	public SimBIP() {}
	
	
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
	 * @throws SQLException 
	 */
	public List<MatIndex> schedule(List<Index> Sinit, List<Index> Smat, List<InumSpace> listInumSpace, int W, 
									List<Double> B) throws SQLException
	{
		List<Index> candidateIndexes = new ArrayList<Index>();
		// 1. Derive Sin, Sout, Sremain
		classifyTypeIndex(Sinit, Smat);
		
		// 2.Derive candidate indexes
		for (int idx = 0; idx < MatIndexPool.getTotalIndex(); idx++) {
			Index index = MatIndexPool.getMatIndex(idx).getIndex();
			candidateIndexes.add(index);
		}
		
		// 3. Derive the query plan description including internal cost, index access cost,
		// index at each slot ... 
		List<SimQueryPlanDesc> listQueryPlan = new ArrayList<SimQueryPlanDesc>();
		for (InumSpace inum : listInumSpace){
			SimQueryPlanDesc desc = new SimQueryPlanDesc();
			desc.generateQueryPlanDesc(inum, candidateIndexes);
			desc.simGenerateQueryPlanDesc(inum, candidateIndexes);
			listQueryPlan.add(desc);
		}
		
		// 5. Ask @SimCPlex to formulate BIP and run the BIP to derive the final result 
		// which is a list of materialized indexes, stored in @listMatIndex
		cplex = new SimCPlex();
		List<MatIndex> listMatIndex  = cplex.findIndexSchedule(listQueryPlan, W, B);
		
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
		for (int w = 0; w < W; w++) {
			strSchedule += "=== At window " + w + "-th:============\n";
			for (MatIndex idx : listIndex) {
				if (idx.getMatWindow() == w) {
					if (idx.getTypeMatIndex() == MatIndex.INDEX_TYPE_CREATE) {
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
		
		// create a hash map to speed up the performance
		Map<String, Integer> mapNameIndexInit = new HashMap<String, Integer>();
		Map<String, Integer> mapNameIndexMat = new HashMap<String, Integer>();
		for (Index index : Sinit){
			mapNameIndexInit.put(index.getName(), 1);
		}
		
		for (Index index : Smat){ 
			mapNameIndexMat.put(index.getName(), 1);
		}		
		
		// 1. Computer Sremain and Sout
		for (Index idxInit : Sinit) {	
			if (mapNameIndexMat.containsKey(idxInit.getName()) == true) {
				Sremain.add(idxInit);
			}
			else {
				Sout.add(idxInit);
			}
		}
		
		for (Index idxMat : Smat) {
			if (mapNameIndexInit.containsKey(idxMat.getName()) == false) {
				Sin.add(idxMat); 
			}
		}
		
		// 2. Store into@MatIndexPool
		// in the order of @Sin, @Sout, and @Sremain
		int globalId = 0;
		MatIndexPool.setStartPosCreateIndexType(globalId);
		for (Index idx : Sin) {
			MatIndex matIndex = new MatIndex(idx, globalId, MatIndex.INDEX_TYPE_CREATE);
			MatIndexPool.addMatIndex(matIndex);			
			MatIndexPool.mapNameIndexGlobalId(idx.getName(), globalId);			
			globalId++; 
		}
		
		MatIndexPool.setStartPosDropIndexType(globalId);
		for (Index idx : Sout) {
			MatIndex matIndex = new MatIndex(idx, globalId, MatIndex.INDEX_TYPE_DROP);
			MatIndexPool.addMatIndex(matIndex);
			MatIndexPool.mapNameIndexGlobalId(idx.getName(), globalId);			
			globalId++;
		}
		
		MatIndexPool.setStartPosRemainIndexType(globalId);
		for (Index idx: Sremain) {
			MatIndex matIndex = new MatIndex(idx, globalId, MatIndex.INDEX_TYPE_REMAIN);
			MatIndexPool.addMatIndex(matIndex);
			MatIndexPool.mapNameIndexGlobalId(idx.getName(), globalId);			
			globalId++;
		}
		MatIndexPool.setTotalIndex(globalId);
	}
}
