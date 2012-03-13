package satuning.engine.selection;

import satuning.db.DB2Index;

public interface DoiFunction {
	public double doi(DB2Index a, DB2Index b);
}
