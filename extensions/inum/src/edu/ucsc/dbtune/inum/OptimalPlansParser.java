package edu.ucsc.dbtune.inum;

/**
 * Parses the result obtained when calling the optimizer using a query and the indexes of that
 * query.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface OptimalPlansParser {

  /**
   * parses the returned optimal plan in a String form. This plan is then turned into an
   * optimal plan object.
   *
   * @param returnedStatement optimal plans in String form.
   * @return an optimal plan.
   */
  OptimalPlan parse(String returnedStatement);
}
