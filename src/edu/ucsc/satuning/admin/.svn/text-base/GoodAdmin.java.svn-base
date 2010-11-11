package edu.ucsc.satuning.admin;

import edu.ucsc.satuning.db.DBIndex;
import edu.ucsc.satuning.engine.ProfiledQuery;
import edu.ucsc.satuning.engine.CandidatePool.Snapshot;
import edu.ucsc.satuning.engine.selection.WorkFunctionAlgorithm;
import edu.ucsc.satuning.util.BitSet;
import edu.ucsc.satuning.util.Debug;

import java.util.List;

import static edu.ucsc.satuning.admin.Overheads.OVERHEAD_FACTOR;

public class GoodAdmin<I extends DBIndex<I>> implements WorkloadRunner<I> {
	BitSet[] optRecs;
	Snapshot<I> snapshot;

	public GoodAdmin(Snapshot<I> snapshot0, BitSet[] recs) {
		snapshot = snapshot0;
		optRecs = recs;
	}

	public void getRecs(List<ProfiledQuery<I>> qinfos, WorkFunctionAlgorithm<I> wfa, BitSet[] recs, double[] overheads) {
		for (int q = 0; q < qinfos.size(); q++) {
			long uStart, uTotal = 0;
			BitSet optRecPrev = q==0 ? new BitSet() : optRecs[q-1];
			// cast the positive votes
			for (int i = optRecs[q].nextSetBit(0); i >= 0; i = optRecs[q].nextSetBit(i+1)) 
				if (!optRecPrev.get(i)) {
					uStart = System.nanoTime();
					wfa.vote(findIndex(i), true);
					uTotal += System.nanoTime() - uStart;
				}

			// cast the negative votes
			for (int i = optRecPrev.nextSetBit(0); i >= 0; i = optRecPrev.nextSetBit(i+1))
				if (!optRecs[q].get(i)) {
					uStart = System.nanoTime();
					wfa.vote(findIndex(i), false);
					uTotal += System.nanoTime() - uStart;
				}
			
			ProfiledQuery<I> query = qinfos.get(q);
			Debug.println("issuing query: " + query.sql);
				
			// analyze the query and get the recommendation
			uStart = System.nanoTime();
			wfa.newTask(query);
			Iterable<I> rec = wfa.getRecommendation();
			uTotal += System.nanoTime() - uStart;
			
			recs[q] = new BitSet();
			for (I idx : rec) {
				recs[q].set(idx.internalId());
			}
			
			if (recs[q].equals(optRecs[q])) Debug.println("OPT MATCHES");

			overheads[q] = (uTotal / OVERHEAD_FACTOR);
		}
	}
	
	I findIndex(int id) {
		I idx = snapshot.findIndexId(id);
		Debug.assertion(idx != null, "did not find index");
		return idx;
	}
}
