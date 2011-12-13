package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.sql.Connection;
import java.util.Set;

import static java.lang.Double.compare;

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

  public InumMatchingStrategy(Connection connection){
    this(new InumIndexAccessCostEstimation(Preconditions.checkNotNull(connection)));
  }

  @Override public double derivesCost(String query, Set<OptimalPlan> optimalPlans,
      Configuration inputConfiguration) {
    // adding the cached cost + index access costs
    final double indexAccessCosts = getIndexAccessCostEstimation().estimateIndexAccessCost(query, inputConfiguration);
    final double derivedCost      = findOneWithMinCost(optimalPlans, indexAccessCosts);
    Preconditions.checkArgument(compare(derivedCost, 0.0) > 0, "invalid execution cost. It cannot be negative or zero.");
    return derivedCost;
  }

  private static double findOneWithMinCost(Set<OptimalPlan> matches, double indexAccessCost){
    OptimalPlan min = null;
    double derivedCost = 0.0;
    for (OptimalPlan each : matches){
      if (null == min) { // base case
        min         = each;
        derivedCost = sumCachedCosts(each) + indexAccessCost;
        continue;
      }

      final double first  = sumCachedCosts(min)  + indexAccessCost;
      final double second = sumCachedCosts(each) + indexAccessCost;
      if (compare(second, first) < 0/*check if second is less than first*/) {
        min         = each;
        derivedCost = second;
      }
    }
    return derivedCost;
  }

  @Override public IndexAccessCostEstimation getIndexAccessCostEstimation() {
    return accessCostEstimator;
  }

  private static boolean intersects(Configuration first, Configuration second){
    final Configuration c = new Configuration(Lists.<Index>newArrayList());
    if (first.size() < second.size()) {
      for (Index x : first.toList()) {
        if (second.toList().contains(x)) {
          c.add(x);
        }
      }
    } else {
      for (Index x : second.toList()) {
        if (first.toList().contains(x)) {
          c.add(x);
        }
      }
    }

    return !c.toList().isEmpty();
  }

  @Override public Set<OptimalPlan> matches(InumSpace inumSpace, Configuration inputConfiguration) {
    final Set<OptimalPlan> found = Sets.newHashSet();
    // assuming there is a match, pick the one with the min cost.
    final Configuration key = new Configuration(inputConfiguration);
    for (Configuration match : inumSpace.getAllInterestingOrders()){
      if (intersects(match, key)){
        final Set<OptimalPlan> optimalPlans = inumSpace.getOptimalPlans(key);
        found.addAll(optimalPlans);
        break;
      }
    }

    return found;
  }

  private static double sumCachedCosts(OptimalPlan optimalPlan){
    optimalPlan.computeInternalPlanCost();  // sum all subplans' costs.
    return optimalPlan.getInternalCost();
  }
}
