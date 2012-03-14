package satuning.admin;

import satuning.db.DB2Index;
import satuning.engine.ProfiledQuery;
import satuning.engine.CandidatePool.Snapshot;
import satuning.engine.selection.WorkFunctionAlgorithm;
import satuning.util.BitSet;
import satuning.util.Debug;

public class GoodAdmin implements WorkloadRunner {
	BitSet[] optRecs;
	Snapshot snapshot;

	public GoodAdmin(Snapshot snapshot0, BitSet[] recs) {
		snapshot = snapshot0;
		optRecs = recs;
	}

	public BitSet[] getRecs(ProfiledQuery[] qinfos, WorkFunctionAlgorithm wfa) {
		int queryCount = qinfos.length;
		
		BitSet[] recs = new BitSet[queryCount];
		
		for (int q = 0; q < queryCount; q++) {
			BitSet optRecPrev = q==0 ? new BitSet() : optRecs[q-1];
			// cast the positive votes
			for (int i = optRecs[q].nextSetBit(0); i >= 0; i = optRecs[q].nextSetBit(i+1)) 
				if (!optRecPrev.get(i)) 
					wfa.vote(findIndex(i), true);

			// cast the negative votes
			for (int i = optRecPrev.nextSetBit(0); i >= 0; i = optRecPrev.nextSetBit(i+1))
				if (!optRecs[q].get(i))
					wfa.vote(findIndex(i), false);
			
			ProfiledQuery query = qinfos[q];
			Debug.println("issuing query: " + query.sql);
				
			// analyze the query and get the recommendation
			wfa.newTask(query);
			recs[q] = new BitSet();
			for (DB2Index idx : wfa.getRecommendation()) {
				recs[q].set(idx.internalId());
			}
			
			if (recs[q].equals(optRecs[q])) Debug.println("OPT MATCHES");
		}
		
		return recs;
	}
	
	DB2Index findIndex(int id) {
		DB2Index idx = snapshot.findIndexId(id);
		Debug.assertion(idx != null, "did not find index");
		return idx;
	}
}
