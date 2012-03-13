package satuning.admin;

import satuning.engine.ProfiledQuery;
import satuning.engine.selection.WorkFunctionAlgorithm;
import satuning.util.BitSet;

public interface WorkloadRunner {
	public BitSet[] getRecs(ProfiledQuery[] qinfos, WorkFunctionAlgorithm wfa);
}
