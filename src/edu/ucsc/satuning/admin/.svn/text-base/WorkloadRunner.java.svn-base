package edu.ucsc.satuning.admin;

import edu.ucsc.satuning.db.DBIndex;
import edu.ucsc.satuning.engine.ProfiledQuery;
import edu.ucsc.satuning.engine.selection.WorkFunctionAlgorithm;
import edu.ucsc.satuning.util.BitSet;

import java.util.List;

public interface WorkloadRunner<I extends DBIndex<I>> {
	void getRecs(List<ProfiledQuery<I>> qinfos, WorkFunctionAlgorithm<I> wfa, BitSet[] recs, double[] overheads);
}
