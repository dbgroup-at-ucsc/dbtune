package edu.ucsc.dbtune.ml

import edu.ucsc.dbtune.core.optimizers.plan.StatementPlan
import weka.core.Instances

/**
 * Creates feature vectors that contain a pair of entries per distinct operator. For example, the 
 * following plan:
 *
 *
 */
class OperatorAggregationExtractor extends PlanInstancesExtractor {
  /**
   * Extracts {@code Instance} objects from a given set of plans.
   * <p>
   * There's one instance
   *
   * @param plans
   *   the set of plans that vectors are created from.
   * @return
   *   an {@code Instances} object containing one per plan.
   */
  def extract(plans: List[StatementPlan]): Instances = {
    new Instances()
  }
}
