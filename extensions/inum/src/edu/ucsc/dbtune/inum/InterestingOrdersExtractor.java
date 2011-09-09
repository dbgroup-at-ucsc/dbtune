package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.metadata.Configuration;
import java.util.Set;

/**
 * It extracts interesting orders from a query.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface InterestingOrdersExtractor {
  /**
   * extracts the interesting orders of a single query.
   * @param singleQuery
   *    single SQL query.
   * @return
   *    a set of interesting orders.
   */
  Configuration extractInterestingOrders(String singleQuery);
}
