package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.core.DBIndex;
import java.util.Set;

/**
 * Represents the set-up phase of the INUM. The precomputation step
 * populates the INUM Space at *initialization time*, by invoking the
 * optimizer in order to reveal the set of optimal plans that need to be cached per
 * query.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface Precomputation {
  /**
   * @return the populated or empty {@link InumSpace INUM Space}.
   */
  InumSpace getInumSpace();

  /**
   * For every workload there is a set-up phase, where the single
   * optimal plan is obtained through an optimizer call with a
   * representative configuration. The resulting {@link OptimalPlan}.
   *
   * The resulting optimal plan is saved in the {@link InumSpace INUM Space},
   * by calling the {@link InumSpace#save(DBIndex, Set) InumSpace#save(Set<OptimalPlan>)} method.
   * After successfully saving the plan, a reference to the optimal plans is returned to the
   * caller of this method.
   *
   * The {@link InumSpace INUM Space} should be computed (filled-in) in accordance to Theorem 4.1. For sake
   * of convenience, the process is described here:
   * &quot;Let query {@code Q} reference tables {@code T_1, ..., T_n} and let {@code O_i} be the set of
   * interesting orders for table {@code T_i}. We also include the &quot;empty&quot; interesting order
   * in {@code O_i}, to account for the indexes on {@code T_i} that do not cover an interesting order.
   *
   * The set {@code O = O_1 x O_2 x .... x O_n} contains all the combinations of interesting orders
   * that a configuration can cover For every member of {@code O} there exist a single optimal
   * {@code MHJ} plan. Thus, to compute the INUM space it is sufficient to invoke the optimizer once
   * for each member {@code o in O}.&quot;
   *
   * @param query
   *      single query. This query will be parsed so that its interesting orders are extracted.
   *      These interesting orders will be inputs of {@link MatchingStrategy matching logic}. The
   *      precomputation step should be skipped if the plans for a given query have been
   *      already precomputed. This is done by calling the {@link #skip(String)} functionality.
   * @param interestingOrders
   *      a representative configuration. The representative configuration could contain
   *      any set of indexes satisfying the non-join or non-interesting orders.
   * @return
   *      a reference to the set of optimal plans, useful if you wish to hold a reference to
   *      the set for checking post-conditions or other purposes (e.g., logging).
   */
  Set<OptimalPlan> setup(String query, Iterable<DBIndex> interestingOrders);

  /**
   * checks whether the precomputation step should be skipped for a
   * given query. This will prevent us from running the precomputation
   * step for the same query multiple times.
   *
   * @param query
   *    a single query.
   * @return
   *    {@code true} if the optimal plans for the workload have been already precomputed.
   *    {@code false} otherwise.
   */
  boolean skip(String query);
}
