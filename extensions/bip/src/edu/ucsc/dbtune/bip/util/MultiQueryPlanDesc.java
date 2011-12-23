package edu.ucsc.dbtune.bip.util;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.bip.util.MatIndexPool;
import edu.ucsc.dbtune.bip.util.BIPAgentPerSchema;
import edu.ucsc.dbtune.bip.util.IndexInSlot;
import edu.ucsc.dbtune.bip.util.QueryPlanDesc;
import edu.ucsc.dbtune.inum.InumStatementPlan;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.SQLStatement;

public class MultiQueryPlanDesc extends QueryPlanDesc 
{
	/**
	 * This class is used when the BIP uses more than one statement at a time (e.g., SIM and DIV)
	 * Note that Index interaction only formulates BIP for one query plan desc at a time
	 * 
	 * Supplement the super class with the following additional operations:
	 *     - Update materialized index size
	 *     - Map index into their position in the ``global'' pool managed by {@code MatIndexPool}
	 *     
	 * @param inum
	 * 		The Inum space	
	 * @param globalCandidateIndexes
	 * 		The global candidate indexes
	 * @throws SQLException
	 */
    public void generateQueryPlanDesc(BIPAgentPerSchema agent, SQLStatement stmt, 
                List<Index> globalCandidateIndexes) throws SQLException
	{
        super.generateQueryPlanDesc(agent, stmt, globalCandidateIndexes);
        
		double sizeMatIndex = 0.0;
		Set<InumStatementPlan> templatePlans = inum.getTemplatePlans();
		
		// Update the materialized index size and create the mapping table
		// from position in each slot into the global position
		int q = getId();
		for (InumStatementPlan plan : templatePlans) {
			for (int i = 0; i < n; i++) {
				for (int a = 0; a < getNumIndexesEachSlot(i); a++) {
					Index index = getIndex(i, a);
					int globalId = MatIndexPool.getGlobalIdofIndex(index);
					IndexInSlot iis = new IndexInSlot(q,i,a);
					MatIndexPool.mapIndexInSlotToGlobalId(iis, globalId);
					
					if (a == getNumIndexesEachSlot(i) - 1){
						sizeMatIndex = 0;
					} else {
						sizeMatIndex = plan.getMaterializedIndexSize(index);
					}
					
					MatIndexPool.getMatIndex(index).setMatSize(sizeMatIndex);
				}							
			}
		}
	}
}
