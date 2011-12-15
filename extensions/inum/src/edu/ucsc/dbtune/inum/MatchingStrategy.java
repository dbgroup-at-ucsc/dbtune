package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.metadata.Configuration;
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
   * @return the index cost estimation {@link IndexAccessCostEstimation algorithm}.
   */
  IndexAccessCostEstimation getIndexAccessCostEstimation();

  /**
   * derives the query cost without going to the optimizer, simply by adding the cached cost to
   * the index access costs computed on-the-fly.
   *
   * @param query
   *    single query.
   * @param inputConfiguration
   *    the input configuration be evaluated.
   * @param inumSpace
   *    the set of optimal plans maintained by INUM.
   * @return
   *    the query cost obtained by adding the cached cost in the {@link OptimalPlan optimal plan}
   *    to the index access costs computed on-the-fly.
   */
  double estimateCost(String query, Configuration inputConfiguration, InumSpace inumSpace);

  /**
   * it matches the input configuration to its corresponding optimal plan.  If the input matches
   * more than one optimal plan, then pick the optimal plan with min cost.
   *
   *
   * @param sql
   *    single SQL query.
   * @param inputConfiguration
   *    an input configuration for which we will find its optimal plan.
   * @param inumSpace
   *    the space containing all cached optimal plans.
   * @return
   *    the matched {@link OptimalPlan optimal plan} for the given input
   *    configuration.
   */
  Set<OptimalPlan> matches(String sql, Configuration inputConfiguration, InumSpace inumSpace);
}
