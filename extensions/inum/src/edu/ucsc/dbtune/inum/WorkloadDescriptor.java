package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.optimizers.SQLStatement;
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
  DBIndex getInterestingOrders();
  SQLStatement getSqlStatement();
  Set<String> getUsedTableNames();
}
