package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.core.DBIndex;
import java.util.Set;

/**
 * It represents the INUM Space or Cached Plans. This cache is
 * a {@code set} that contains, for each {@code query}, a number of
 * alternative execution plans. Each plan in the INUM Space is
 * optimal for one or more possible {@link Iterable<DBIndex> input}
 * configurations.
 *
 * The correctness of the INUM Space is guaranteed by these two
 * properties (Def 3.1 in the INUM VLDB paper):
 *
 * <ul>
 *   <li>Each subplan is derived from an optimal plan for some configuration.</li>
 *   <li>The INUM Space contains all the subplans with the above property.</li>
 * </ul>
 *
 * The above two properties simply writes that the INUM Space will contain the
 * optimal plan for any input configuration. This suggests that if a plan is
 * optimal for some configuration {@code C}, it might in fact remain optimal
 * for a set of configurations that are "similar" to {@code C}.
 *
 * By reusing these plans we will get accurate cost estimates without invoking
 * the optimizer.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface InumSpace {
  /**
   * clears the Inum space (cached plans). This will help the garbage collector to dispose the objects
   * found in this cache appropriately.
   */
  void clear();

  /**
   * get the set of optimal plans for the given key.
   *
   * @param key
   *    index or interesting orders.
   * @return
   *    the set of cached optimal plans for the given key.
   */
  Set<OptimalPlan> getOptimalPlans(Set<DBIndex> key);

  /**
   * Get all the {@link OptimalPlan optimal plan}s from the
   * {@link InumSpace INUM Space}.
   *
   * @return
   *    the set of cached optimal plans.
   */
  Set<OptimalPlan> getAllSavedOptimalPlans();

  /**
   * Save a set of optimal plans per query in the INUM Space.
   *
   * @param interestingOrders
   *      a single combination of interesting orders.
   * @param optimalPlans
   *      a set of optimal plans to be saved in the {@link InumSpace INUM Space}.
   *      {@link OptimalPlan Plans} that haven't been saved in the {@link InumSpace INUM Space}
   *      are considered "dirty" plans.
   * @return
   *      a reference to the set of saved optimal plans, useful if you wish to hold a reference to
   *      the set for checking post-conditions or other purposes (e.g., logging).
   */
  Set<OptimalPlan> save(Set<DBIndex> interestingOrders, Set<OptimalPlan> optimalPlans);
}
