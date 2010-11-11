package edu.ucsc.satuning.engine.selection;

import edu.ucsc.satuning.db.DBIndex;

public interface DoiFunction<I extends DBIndex<I>> {
	public double doi(I a, I b);
}
