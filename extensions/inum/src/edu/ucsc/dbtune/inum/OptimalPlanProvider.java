package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.metadata.Configuration;

/**
 * Helper interface for getting an execution plan that spans a range of internal plans.
 * The implementation of this interface will talk to the Optimizer class to get the plan
 * we are looking for.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface OptimalPlanProvider {
  String getSqlExecutionPlan(String sql, Configuration inputConfiguration);
}
