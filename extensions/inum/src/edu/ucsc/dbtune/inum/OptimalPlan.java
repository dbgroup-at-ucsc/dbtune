package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.core.optimizers.plan.SQLStatementPlan;

/**
 * An optimal query execution plan returned by the dbms's optimizer for
 * a given configuration.
 * 
 * todo(Huascar) once Ivo's refactoring of the DBIndex is tested
 * then we could see how to get rid of this interface and use 
 * SQLStatementPlan....
 * 
 * In the meantime, this class can extend this {@link SQLStatementPlan}
 * by using aggregation rather than normal inheritance.
 * 
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface OptimalPlan {
  /**
   * compute internal plan cost (subtract table access operations).
   */
  void computeInternalPlanCost();

  /**
   * @return the cost of the first node in
   *    the plan.
   */
  double getTotalCost();

  /**
   * Gets the cost for accessing a given table.
   * @param tableName
   *    table name as a look up key.
   * @return 
   *    the cost for accessing a given table. Or return 0.0f if 
   *    not found.
   */
  double getAccessCost(String tableName);

  /**
   * @return The internal plan cost.
   */
  double getInternalCost();

  /**
   * @return {@code true} if the plan has not been saved
   *    in the {@link InumSpace INUM Space}. {@code false}
   *    otherwise.
   */
  boolean isDirty();

  /**
   * sets the cost to access a given table. 
   * @param tableName
   *    the name of table that will be assigned
   *    an access cost.
   * @param cost
   *    the access cost value.
   */
  void setAccessCost(String tableName, double cost);
 
}
