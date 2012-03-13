package satuning.engine;

import satuning.util.BitSet;

public class AnalyzedQuery {
	public ProfiledQuery profileInfo;
	public BitSet[] partition;


	public AnalyzedQuery(ProfiledQuery orig, BitSet[] partition0) {
		profileInfo = orig;
		partition = partition0;
	}

}
