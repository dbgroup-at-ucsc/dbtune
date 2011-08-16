package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.core.DBIndex;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumIndexAccessCostEstimation implements IndexAccessCostEstimation {
  @Override public double estimateIndexAccessCost(Iterable<DBIndex> indexes) {
    return 0;  //todo(Huascar) implement this
  }
}
