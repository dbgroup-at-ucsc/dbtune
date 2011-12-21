package edu.ucsc.dbtune.inum;

/**
 * Represents the {@link PhysicalOperator} type.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface PhysicalOperator {
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
   * @return the internal cost (cost of whole operation) of the physical operator.
   */
  double getCost();

  /**
   * @return the init cost of physical operator.
   */
  double getInitCost();

  /**
   * @return the number of tuples.
   */
  long getCardinality();
}
