package edu.ucsc.dbtune.inum;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;

/**
 * It extracts interesting orders from a query.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface InterestingOrdersExtractor
{
  /**
   * extracts the interesting orders of a single query.
   *
   * @param singleQuery single SQL query.
   * @return a set of interesting orders.
   * @throws SQLException if unable to extract interesting orders.
   */
  Set<Index> extractInterestingOrders(String singleQuery)
      throws SQLException;
}
