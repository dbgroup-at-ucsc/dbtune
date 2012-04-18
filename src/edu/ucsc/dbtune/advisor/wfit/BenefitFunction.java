package edu.ucsc.dbtune.advisor.wfit;

import edu.ucsc.dbtune.metadata.Index;

public interface BenefitFunction {
    public double benefit(Index a);
}
