package edu.ucsc.dbtune.inum;

import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;

/**
 * An optimal query execution plan returned by the dbms's optimizer for a given configuration.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface OptimalPlan {

  /**
   * Adds a operator.
   *
   * @param operator operator to be added.
   * @return {@code true} if the operator was added. {@code false} otherwise.
   */
  boolean add(PhysicalOperator operator);

  /**
   * sets the access cost for used tables.
   *
   * @param configuration a physical configuration
   * @param queryRecord a descriptor of a SQL query
   * @see Index
   */
  void fixAccessCosts(Set<Index> configuration, QueryRecord queryRecord);

  /**
   * compute internal plan cost (subtract table access operations).
   */
  void computeInternalPlanCost();

  /**
   * @return an immutable list of subplans part of this optimal plan.
   */
  List<PhysicalOperator> getInternalPlans();

  /**
   * @return the cost of the first node in the plan.
   */
  double getTotalCost();

  /**
   * Gets the cost for accessing a given table.
   *
   * @param tableName table name as a look up key.
   * @return the cost for accessing a given table. Or return 0.0f if not found.
   */
  double getAccessCost(String tableName);

  /**
   * @return The internal plan cost (sum all subplans' costs)
   */
  double getInternalCost();

  /**
   * @return {@code true} if the plan has not been saved in the {@link InumSpace INUM Space}. {@code
   *         false} otherwise.
   */
  boolean isDirty();

  /**
   * removes a operator from the optimal plan.
   *
   * @param operator operator to be removed.
   * @return {@code true} if the operator was removed. {@code false} otherwise.
   */
  boolean remove(PhysicalOperator operator);

  /**
   * sets the cost to access a given table.
   *
   * @param tableName the name of table that will be assigned an access cost.
   * @param cost the access cost value.
   */
  void setAccessCost(String tableName, double cost);
}
