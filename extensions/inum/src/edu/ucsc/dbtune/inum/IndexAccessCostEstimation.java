package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.core.DBIndex;

/**
 * Whenever the query cost for some input configuration needs to be evaluated,
 * this type could be used to estimate the cost of accessing the indexes in
 * the input configuration.
 *
 * The sum of the index access costs for the entire input configuration is
 * added to that of the internal subplan to obtain the final query cost.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface IndexAccessCostEstimation {
  /**
   * Estimate the cost of accessing the index in an input configuration.
   * Computing only the individual index access costs is much faster than
   * a full blown optimizer call.
   *
   * @param indexes indexes to be used in the index access cost calculation
   * @return
   *    the cost of accessing the given index.
   */
  double estimateIndexAccessCost(Iterable<DBIndex> indexes);
}
