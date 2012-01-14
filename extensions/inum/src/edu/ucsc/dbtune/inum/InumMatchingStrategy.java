package edu.ucsc.dbtune.inum;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import edu.ucsc.dbtune.metadata.Index;
import static java.lang.Double.compare;
import java.util.Set;

/**
 * Default implementation of Inum's {@link MatchingStrategy}
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumMatchingStrategy implements MatchingStrategy {
  private final IndexAccessCostEstimation accessCostEstimator;

  InumMatchingStrategy(IndexAccessCostEstimation accessCostEstimator){
    this.accessCostEstimator = accessCostEstimator;
  }

  public InumMatchingStrategy(){
    this(new InumIndexAccessCostEstimation());
  }

  @Override public double estimateCost(
        String query, Set<Index> inputConfiguration, InumSpace inumSpace) {
    // matched optimal plans for "query" ...
    final Set<OptimalPlan> matchedOptimalPlans = matches(
        query, inputConfiguration, inumSpace
    );

    double min = 0.0;
    for(OptimalPlan p : matchedOptimalPlans){
      final double actual =  internalCost(p) + indexAccessCost(p, inputConfiguration);
      if(compare(min, 0.0) == 0 && compare(actual, 0.0) > 0) {
        min = actual;
        continue;
      }

      if(compare(actual, min) < 0/*check if actual is less than min*/) {
        min = actual;
      }
    }

    Preconditions.checkArgument(compare(min, 0.0) > 0, "invalid execution cost. It must be greater than zero.");
    return min;
  }

  @Override public IndexAccessCostEstimation getIndexAccessCostEstimation() {
    return accessCostEstimator;
  }

  private double indexAccessCost(OptimalPlan plan, Set<Index> configuration){
    return getIndexAccessCostEstimation().estimateIndexAccessCost(plan, configuration);
  }

  private static double internalCost(OptimalPlan optimalPlan){
    optimalPlan.computeInternalPlanCost();  // sum all subplans' costs.
    return optimalPlan.getInternalCost();
  }

  @Override public Set<OptimalPlan> matches(
          String sql, Set<Index> inputConfiguration, InumSpace inumSpace) {
    final Set<OptimalPlan> found = Sets.newHashSet();
    // assuming there is a match, later methods will pick the optimal plan with the min cost.
    final QueryRecord targetKey = new QueryRecord(sql, inputConfiguration);
    found.addAll(inumSpace.getOptimalPlans(targetKey));
    return found;
  }
}
