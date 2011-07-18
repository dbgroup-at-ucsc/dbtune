package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.tools.cmudb.inum.PostgresIndexAccessGenerator;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface InumWhatIfOptimizer {

  /**
   * estimate the cost of executing a given workload without indexes involved.
   * @param workload
   *    a single workload
   * @return
   *    the estimated cost for executing a single workload without using
   *    indexes.
   * @see {@link #estimateCost(String, Iterable)}
   */
  double estimateCost(String workload);

  /**
   * estimate the cost involved in materializing a bag of hypothetical
   * indexes for a given workload. The bag of hypothetical indexes may
   * be empty.
   * @param workload
   *    a single workload
   * @param hypotheticalIndexes
   *    a bag of hypothetical indexes
   * @return
   *    the estimated cost for materializing a bag of hypothetical
   *    indexes.
   * @throws IndexAccessGenerationException if {@link PostgresIndexAccessGenerator}
   *    was unable to parse the workload.
   */
  double estimateCost(String workload, Iterable<DBIndex> hypotheticalIndexes);
}
