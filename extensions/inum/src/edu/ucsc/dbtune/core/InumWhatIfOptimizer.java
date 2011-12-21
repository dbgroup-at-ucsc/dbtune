package edu.ucsc.dbtune.core;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.inum.Inum;

/**
 * The (IN)dex (U)sage (M)odel (INUM) What-if optimizer.
 *
 * The InumWhatIfOptimizer takes as inputs a workload (single query)
 * and a configuration for evaluation. The output is the cost for
 * the query given a matched optimal plan.
 *
 * Internally, the implementation of this class will interact with
 * the {@link Inum INUM} main object. This class is responsible for
 * orchestrating the execution of INUM.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface InumWhatIfOptimizer
{
  /**
   * estimate the cost of executing a given query; an empty list of
   * hypothetical indexes (a.k.a., empty configuration) is provided.
   *
   * @param query
   *    a single SQL query.
   * @return
   *    the estimated cost for executing a single query without using
   *    indexes.
   * @see {@link #estimateCost(String, Configuration)}.
   * @throws SQLException
   *    if unable to estimate cost.
   */
  double estimateCost(String query)
      throws SQLException;

  /**
   * estimate the cost involved in materializing a bag of hypothetical
   * indexes (a.k.a., configuration) for a given query. The bag of hypothetical
   * indexes may be empty (a.k.a., empty configuration).
   *
   * When using this method for the very first time, there will be an initial performance cost
   * due to the set up phase of the Index Usage Model (INUM) functionality. Once INUM has been
   * set up, upcoming calls to this method won't experience this performance cost.
   *
   * @param query
   *    a single SQL query.
   * @param hypotheticalIndexes
   *    a hypothetical indexes configuration.
   * @return
   *    the estimated cost for materializing a bag of hypothetical
   *    indexes.
   * @throws SQLException
   *    if unable to estimate cost.
   */
  double estimateCost(String query, Set<Index> hypotheticalIndexes)
      throws SQLException;
}
