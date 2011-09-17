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

  @Override
  public double derivesCost(String query, OptimalPlan optimalPlan,
      Configuration inputConfiguration) {
    // adding the cached cost + index access costs
    final double indexAccessCosts = getIndexAccessCostEstimation().estimateIndexAccessCost(query, inputConfiguration);
    return sumCachedCosts(optimalPlan) + indexAccessCosts;
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

  @Override public IndexAccessCostEstimation getIndexAccessCostEstimation() {
    return accessCostEstimator;
  }

  private static boolean intersects(Configuration first, Configuration second){
    final Configuration c = new Configuration(Lists.<Index>newArrayList());
    // todo(Ivo) how is the cardinality of the config tied to the # of indexes the config contains?
    // they don't seem connected. For example..Initially, I wanted to write first.getCardinality() <
    // second.getCardinality(), but this did not work since the cardinality was 0. Therefore, I ended
    // up using first.toList().size()....etc. If the intention was to update the cardinality of
    // the configuration based on the # of stored indexes, then there is a bug in the configuration
    // class.
    if (first.toList().size() < second.toList().size()) {
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

  @Override
  public OptimalPlan matches(InumSpace inumSpace, Configuration inputConfiguration) {
    final Set<OptimalPlan> found = Sets.newHashSet();
    // assuming there is a match, pick the one with the min cost.
    final Configuration key = new Configuration(inputConfiguration);
    for(Configuration match : inumSpace.getAllInterestingOrders()){
      if(intersects(match, key)){
        final Set<OptimalPlan> optimalPlans = inumSpace.getOptimalPlans(key);
        found.addAll(optimalPlans);
        break;
      }
    }

    return findOneWithMinCost(found);
  }

  private static double sumCachedCosts(OptimalPlan optimalPlan){
    optimalPlan.computeInternalPlanCost();  // sum all subplans' costs.
    return optimalPlan.getInternalCost();
  }
}
