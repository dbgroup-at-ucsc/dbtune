package edu.ucsc.satuning.admin;

import edu.ucsc.satuning.db.DBIndex;
import edu.ucsc.satuning.engine.ProfiledQuery;
import edu.ucsc.satuning.engine.CandidatePool.Snapshot;
import edu.ucsc.satuning.engine.selection.WorkFunctionAlgorithm;
import edu.ucsc.satuning.util.BitSet;
import edu.ucsc.satuning.util.Debug;

import java.util.List;

import static edu.ucsc.satuning.admin.Overheads.OVERHEAD_FACTOR;

public class SlowAdmin<I extends DBIndex<I>> implements WorkloadRunner<I> {
	Snapshot<I> snapshot;
	final int lag;
	boolean doVoting;
	
	public SlowAdmin(Snapshot<I> snapshot0, int lag0, boolean doVoting0) {
		snapshot = snapshot0;
		lag = lag0;
		doVoting = doVoting0;
	}

	public void getRecs(List<ProfiledQuery<I>> qinfos, WorkFunctionAlgorithm<I> wfa, BitSet[] recs, double[] overheads) {		
		for (int q = 0; q < qinfos.size(); q++) {
			long uStart, uTotal = 0;
			ProfiledQuery<I> query = qinfos.get(q);
			Debug.println("issuing query: " + query.sql);
				
			// analyze the query and get the recommendation
			uStart = System.nanoTime();
			wfa.newTask(query);
			uTotal += System.nanoTime() - uStart;
			recs[q] = new BitSet();
			if (q % lag == lag - 1) {
				// accept the recommendation
				uStart = System.nanoTime();
				Iterable<I> rec = wfa.getRecommendation();
				uTotal += System.nanoTime() - uStart;
				for (I idx : rec) {
					recs[q].set(idx.internalId());
				}
				
				if (doVoting) {
					// cast the positive votes
					for (int i = recs[q].nextSetBit(0); i >= 0; i = recs[q].nextSetBit(i+1)) 
						if (!recs[q-1].get(i)) {
							uStart = System.nanoTime();
							wfa.vote(findIndex(i), true);
							uTotal += System.nanoTime() - uStart;
						}
	
					// cast the negative votes
					for (int i = recs[q-1].nextSetBit(0); i >= 0; i = recs[q-1].nextSetBit(i+1))
						if (!recs[q].get(i)) {
							uStart = System.nanoTime();
							wfa.vote(findIndex(i), false);
							uTotal += System.nanoTime() - uStart;
						}
				}
			}
			else if (q > 0) {
				recs[q].set(recs[q-1]);
			}
			
			overheads[q] = (uTotal / OVERHEAD_FACTOR);
		}
	}
	
	I findIndex(int id) {
		I idx = snapshot.findIndexId(id);
		Debug.assertion(idx != null, "did not find index");
		return idx;
	}
}
