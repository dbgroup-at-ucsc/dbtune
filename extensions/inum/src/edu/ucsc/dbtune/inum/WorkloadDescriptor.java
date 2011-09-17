package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.SQLStatement;

import java.util.Set;

/**
 * It holds information that will be used in the {@link Precomputation set-up} and
 * {@link MatchingStrategy matching logic} steps.
 *
 * todo(Huascar) document methods...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface WorkloadDescriptor {
  double getBestInternalPlanCost();
  double[] getCandidateCosts();
  double getEmptyCost();
  Index getInterestingOrders();
  SQLStatement getSqlStatement();
  Set<String> getUsedTableNames();
}
