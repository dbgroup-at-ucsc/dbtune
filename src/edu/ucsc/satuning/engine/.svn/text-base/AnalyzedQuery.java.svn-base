package edu.ucsc.satuning.engine;

import edu.ucsc.satuning.db.DBIndex;
import edu.ucsc.satuning.util.BitSet;

public class AnalyzedQuery<I extends DBIndex<I>> {
	public ProfiledQuery<I> profileInfo;
	public BitSet[] partition;


	public AnalyzedQuery(ProfiledQuery<I> orig, BitSet[] partition0) {
		profileInfo = orig;
		partition = partition0;
	}

}
