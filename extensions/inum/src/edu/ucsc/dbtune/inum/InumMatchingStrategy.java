package edu.ucsc.dbtune.inum;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseConnection;
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

  public InumMatchingStrategy(DatabaseConnection connection){
    this(new InumIndexAccessCostEstimation(Preconditions.checkNotNull(connection)));
  }

  @Override public IndexAccessCostEstimation getIndexAccessCostEstimation() {
    return accessCostEstimator;
  }

  @Override
  public double derivesCost(String query, OptimalPlan optimalPlan,
      Iterable<DBIndex> inputConfiguration) {
    // adding the cached cost + index access costs
    final double indexAccessCosts = getIndexAccessCostEstimation().estimateIndexAccessCost(query, inputConfiguration);
    return sumCachedCosts(optimalPlan) + indexAccessCosts;
  }

  private static double sumCachedCosts(OptimalPlan optimalPlan){
    optimalPlan.computeInternalPlanCost();  // sum all subplans' costs.
    return optimalPlan.getInternalCost();
  }

  @Override
  public OptimalPlan matches(InumSpace inumSpace, Iterable<DBIndex> inputConfiguration) {
    final Set<OptimalPlan> found = Sets.newHashSet();
    // assuming there is a match, pick the one with the min cost.
    final Set<DBIndex> key = Sets.newHashSet(inputConfiguration);
    for(Set<DBIndex> match : inumSpace.getAllInterestingOrders()){
      if(!Sets.intersection(match, key).isEmpty()){
        final Set<OptimalPlan> optimalPlans = inumSpace.getOptimalPlans(key);
        found.addAll(optimalPlans);
        break;
      }
    }

    return findOneWithMinCost(found);
  }

  private static OptimalPlan findOneWithMinCost(Set<OptimalPlan> matches){
    OptimalPlan min = new SqlExecutionOptimalPlan();
    for(OptimalPlan each : matches){
      if(compare(min.getTotalCost(), each.getTotalCost()) < 0) {
        min = each;
      }
    }
    return min;
  }
}
