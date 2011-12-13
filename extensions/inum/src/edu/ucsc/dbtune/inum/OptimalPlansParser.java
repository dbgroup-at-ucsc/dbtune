package edu.ucsc.dbtune.inum;

import java.util.Set;

/**
 * Parses the result obtained when calling the optimizer using a query and the
 * indexes of that query.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface OptimalPlansParser
{
  /**
   * parses the returned optimal plans in a String form. These plans are then turned into
   * a set of optimalplan objects.
   * @param returnedStatement
   *    optimal plans in String form.
   * @return
   *    a set of optimal plans.
   */
  Set<OptimalPlan> parse(String returnedStatement);
}
