package satuning.engine.selection;

import satuning.db.DB2Index;

public interface BenefitFunction {
	public double benefit(DB2Index a);
}
