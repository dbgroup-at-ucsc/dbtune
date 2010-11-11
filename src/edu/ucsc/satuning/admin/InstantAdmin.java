package edu.ucsc.satuning.admin;

import edu.ucsc.satuning.db.DBIndex;
import edu.ucsc.satuning.engine.ProfiledQuery;
import edu.ucsc.satuning.engine.selection.WorkFunctionAlgorithm;
import edu.ucsc.satuning.util.BitSet;
import edu.ucsc.satuning.util.Debug;

import java.util.List;

import static edu.ucsc.satuning.admin.Overheads.OVERHEAD_FACTOR;

public class InstantAdmin<I extends DBIndex<I>> implements WorkloadRunner<I> {

	public void getRecs(List<ProfiledQuery<I>> qinfos, WorkFunctionAlgorithm<I> wfa, BitSet[] recs, double[] overheads) {
		for (int q = 0; q < recs.length; q++) {
			ProfiledQuery<I> query = qinfos.get(q);
			Debug.println("issuing query: " + query.sql);
				
			// analyze the query and get the recommendation
			long uStart = System.nanoTime();
			wfa.newTask(query);
			Iterable<I> rec = wfa.getRecommendation();
			long uEnd = System.nanoTime();
			
			recs[q] = new BitSet();
			for (I idx : rec) {
				recs[q].set(idx.internalId());
			}
			overheads[q] = (uEnd - uStart) / OVERHEAD_FACTOR;
		}
	}
}
