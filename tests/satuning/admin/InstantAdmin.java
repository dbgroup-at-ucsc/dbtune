package satuning.admin;

import satuning.db.DB2Index;
import satuning.engine.ProfiledQuery;
import satuning.engine.selection.WorkFunctionAlgorithm;
import satuning.util.BitSet;
import satuning.util.Debug;

public class InstantAdmin implements WorkloadRunner {
	public BitSet[] getRecs(ProfiledQuery[] qinfos, WorkFunctionAlgorithm wfa) {
		int queryCount = qinfos.length;
		
		BitSet[] recs = new BitSet[queryCount];
		
		for (int q = 0; q < queryCount; q++) {
			ProfiledQuery query = qinfos[q];
			Debug.println("issuing query: " + query.sql);
				
			// analyze the query and get the recommendation
			wfa.newTask(query);
			recs[q] = new BitSet();
			for (DB2Index idx : wfa.getRecommendation()) {
				recs[q].set(idx.internalId());
			}
		}
		
		return recs;
	}
}
