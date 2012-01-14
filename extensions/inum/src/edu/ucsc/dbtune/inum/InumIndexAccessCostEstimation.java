package edu.ucsc.dbtune.inum;

import com.google.common.base.Preconditions;
import edu.ucsc.dbtune.metadata.Index;
import java.util.Set;

/**
 * Default implementation of {@link IndexAccessCostEstimation} interface.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumIndexAccessCostEstimation implements IndexAccessCostEstimation {
  @Override public double estimateIndexAccessCost(OptimalPlan optimalPlan, Set<Index> indexes) {
    final OptimalPlan nonNullOptimalPlan = Preconditions.checkNotNull(optimalPlan);
    double sumOfIndexAccessCosts = 0.0;
    for (Index each : indexes) {
      sumOfIndexAccessCosts += nonNullOptimalPlan.getAccessCost(each.getTable().getName());
    }
    return sumOfIndexAccessCosts;
  }
}
