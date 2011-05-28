// @license
package edu.ucsc.dbtune.ml

import edu.ucsc.dbtune.core.optimizers.plan.StatementPlan

import java.io.File

import weka.core.FastVector
import weka.core.Attribute
import weka.core.Instance
import weka.core.Instances
import weka.core.converters.ArffSaver

/**
 * Creates feature vectors ({@code Instances} in Weka's terminology) out of SQL plans, with the 
 * option of exporting them to an {@code .arff} file.
 */
trait PlanInstancesExtractor {

  /**
   * Extracts {@code Instance} objects from a given set of plans.
   *
   * @param plans
   *   the set of plans that vectors are created from.
   * @return
   *   an {@code Instances} object containing one per plan.
   */
  def extract(plans: List[StatementPlan]): Instances

  /**
   * Extracts {@code Instance} objects from a given set of plans and stores an {@code .arff} file 
   * that can be analyzed in the Weka UI.
   *
   * @param plans
   *   the set of plans that vectors are created from.
   * @param fileName
   *   name of the file the {@code Instances} object is exported to.
   */
  def extract(plans: List[StatementPlan], fileName: String): Unit = {
     var arffSaver = new ArffSaver
     var instances = extract(plans)

     arffSaver.setFile(new File(fileName))
     arffSaver.writeBatch()
  }
}
