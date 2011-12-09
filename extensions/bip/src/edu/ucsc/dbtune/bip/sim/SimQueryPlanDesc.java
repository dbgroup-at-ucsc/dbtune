package edu.ucsc.dbtune.bip.sim;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.bip.util.IndexInSlot;
import edu.ucsc.dbtune.bip.util.QueryPlanDesc;
import edu.ucsc.dbtune.inum.InumSpace;
import edu.ucsc.dbtune.inum.InumStatementPlan;
import edu.ucsc.dbtune.metadata.Index;

public class SimQueryPlanDesc extends QueryPlanDesc 
{
	/**
	 * Supplement methods to the main method in super.generateQueryPlanDesc
	 * @param inum
	 * 		The Inum space	
	 * @param globalCandidateIndexes
	 * 		The global candidate indexes
	 * @throws SQLException
	 */
	public void simGenerateQueryPlanDesc(InumSpace inum, List<Index> globalCandidateIndexes) throws SQLException
	{
		double sizeMatIndex = 0.0;
		Set<InumStatementPlan> templatePlans = inum.getTemplatePlans();
		// Add the full-table-scan index into the pool of @MatIndexPool
		int globalId = MatIndexPool.getTotalIndex();
		for (int i = 0; i < n; i++){
			Index emptyIndex = getIndex(i, getNumIndexesEachSlot(i) - 1);
			MatIndex matIndex = new MatIndex(emptyIndex, globalId, MatIndex.INDEX_TYPE_REMAIN);
			MatIndexPool.addMatIndex(matIndex);
			MatIndexPool.mapNameIndexGlobalId(emptyIndex.getName(), globalId);			
			globalId++;
		}
		MatIndexPool.setTotalIndex(globalId);
		
		// Update the materialized index size and create the mapping table
		// from position in each slot into the global position
		int q = 0;
		for (InumStatementPlan plan : templatePlans) {
			for (int i = 0; i < n; i++) {
				for (int a = 0; a < getNumIndexesEachSlot(i); a++) {
					Index index = getIndex(i, a);
					globalId = MatIndexPool.getIndexGlobalId(index.getName());
					IndexInSlot iis = new IndexInSlot(q,i,a);
					MatIndexPool.mapPosIndexGlobalId(iis, globalId);
					
					if (a == getNumIndexesEachSlot(i) - 1){
						sizeMatIndex = 0;
					} else {
						sizeMatIndex = plan.getMaterializedIndexSize(index);
					}
					MatIndexPool.getMatIndex(index).setMatSize(sizeMatIndex);
				}							
			}
			q++;
		}
	}
}
