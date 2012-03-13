package satuning.admin;

import satuning.db.DB2Index;
import satuning.engine.ProfiledQuery;
import satuning.engine.CandidatePool.Snapshot;
import satuning.engine.selection.WorkFunctionAlgorithm;
import satuning.util.BitSet;
import satuning.util.Debug;

public class SlowAdmin implements WorkloadRunner {
	Snapshot snapshot;
	final int lag;
	boolean doVoting;
	
	public SlowAdmin(Snapshot snapshot0, int lag0, boolean doVoting0) {
		snapshot = snapshot0;
		lag = lag0;
		doVoting = doVoting0;
	}
	
	public BitSet[] getRecs(ProfiledQuery[] qinfos, WorkFunctionAlgorithm wfa) {
		int queryCount = qinfos.length;
		
		BitSet[] recs = new BitSet[queryCount];
		
		for (int q = 0; q < queryCount; q++) {
			ProfiledQuery query = qinfos[q];
			Debug.println("issuing query: " + query.sql);
				
			// analyze the query and get the recommendation
			wfa.newTask(query);
			recs[q] = new BitSet();
			if (q % lag == lag - 1) {
				// accept the recommendation
				for (DB2Index idx : wfa.getRecommendation()) {
					recs[q].set(idx.internalId());
				}
				
				if (doVoting) {
					// cast the positive votes
					for (int i = recs[q].nextSetBit(0); i >= 0; i = recs[q].nextSetBit(i+1)) 
						if (!recs[q-1].get(i)) 
							wfa.vote(findIndex(i), true);
	
					// cast the negative votes
					for (int i = recs[q-1].nextSetBit(0); i >= 0; i = recs[q-1].nextSetBit(i+1))
						if (!recs[q].get(i))
							wfa.vote(findIndex(i), false);
				}
			}
			else if (q > 0) {
				recs[q].set(recs[q-1]);
			}
		}
		
		return recs;
	}
	
	DB2Index findIndex(int id) {
		DB2Index idx = snapshot.findIndexId(id);
		Debug.assertion(idx != null, "did not find index");
		return idx;
	}
}
