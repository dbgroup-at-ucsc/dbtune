package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.core.optimizers.plan.SQLStatementPlan;
import java.util.List;

/**
 * An optimal query execution plan returned by the dbms's optimizer for
 * a given configuration.
 * 
 * todo(Huascar) once Ivo's refactoring of the DBIndex is tested
 * then we could see how to get rid of this interface and use 
 * SQLStatementPlan....
 *
 * todo(Ivo) explain how we can integrate the SQLStatementPlan into this? OptimalPlan interface is
 * pretty much different than SQLStatementPlan...both abstraction seems to server a different purpose.
 * 
 * In the meantime, this class can extend this {@link SQLStatementPlan}
 * by using aggregation rather than normal inheritance.
 * 
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface OptimalPlan {
  /**
   * Adds a subplan.
   * @param subplan
   *    subplan to be added.
   * @return
   *    {@code true} if the subplan was added. {@code false} otherwise.
   */
  boolean addSubplan(Subplan subplan);

  /**
   * compute internal plan cost (subtract table access operations).
   */
  void computeInternalPlanCost();

  /**
   * @return an immutable list of subplans part of this optimal plan.
   */
  List<Subplan> getInternalPlans();

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
   * @return The internal plan cost (sum all subplans' costs)
   */
  double getInternalCost();

  /**
   * @return {@code true} if the plan has not been saved
   *    in the {@link InumSpace INUM Space}. {@code false}
   *    otherwise.
   */
  boolean isDirty();

  /**
   * removes a subplan from the optimal plan.
   * @param subplan
   *    subplan to be removed.
   * @return
   *    {@code true} if the subplan was removed. {@code false} otherwise.
   */
  boolean removeSubplan(Subplan subplan);

  /**
   * sets the cost to access a given table. 
   * @param tableName
   *    the name of table that will be assigned
   *    an access cost.
   * @param cost
   *    the access cost value.
   */
  void setAccessCost(String tableName, double cost);

  /**
   * Internal subplans which will become part of an OptimalPlan.
   */
  interface Subplan {
    /**
     * @return the row id
     */
    int getRowId();

    /**
     * @return the parent id
     */
    int getParentId();

    /**
     * @return the operator.
     */
    String getOperator();

    /**
     * @return the target (For Indexes, Table Scan etc)
     */
    String getTarget();

    /**
     * @return the internal cost (cost of whole operation) of the subplan.
     */
    double getCost();

    /**
     * @return the init cost of subplan.
     */
    double getInitCost();

    /**
     * @return the number of tuples.
     */
    long getCardinality();
  }
 
}
