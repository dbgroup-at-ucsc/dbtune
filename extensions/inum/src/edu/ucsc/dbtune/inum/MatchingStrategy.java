package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.core.DBIndex;
import java.util.Set;

/**
 * This represents the matching logic that determines the optimality
 * of the reused plans. Given a set of strict rules, this logic will
 * efficiently assign, for each configuration input to INUM, the
 * corresponding optimal plan. Then, it will derive the query cost
 * without going to the optimizer, simply by adding the cached cost
 * to the index access costs computed on-the-fly.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface MatchingStrategy {
  /**
   * derives the query cost without going to the optimizer, simply by adding the cached cost to
   * the index access costs computed on-the-fly.
   *
   * @param optimalPlan
   *    the corresponding {@link OptimalPlan optimal plan} for a given input configuration.
   * @param inputConfiguration
   *    the input configuration be evaluated.
   * @return
   *    the query cost obtained by adding the cached cost in the {@link OptimalPlan optimal plan}
   *    to the index access costs computed on-the-fly.
   */
  double derivesCost(OptimalPlan optimalPlan, Iterable<DBIndex> inputConfiguration);

  /**
   * it matches the input configuration to its corresponding optimal plan.
   * @param optimalPlans
   *    the optimal plans cached in the {@link InumSpace INUM Space}.
   * @param inputConfiguration
   *    an input configuration for which we will find its optimal plan.
   * @return
   *    the matched {@link OptimalPlan optimal plan} for the given input
   *    configuration.
   */
  OptimalPlan matches(Set<OptimalPlan> optimalPlans, Iterable<DBIndex> inputConfiguration);
}
