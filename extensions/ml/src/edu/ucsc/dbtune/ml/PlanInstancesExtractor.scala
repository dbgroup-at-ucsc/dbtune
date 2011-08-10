// @license
package edu.ucsc.dbtune.ml

import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan

import java.io.File

import weka.core.Instances
import weka.core.converters.ArffSaver

/** Creates feature vectors (`Instances` in Weka's terminology) out of SQL plans.
  *
  * Creates feature vectors (`Instances` in Weka's terminology) out of SQL plans, with the option of 
  * exporting them to an `.arff` file. */
trait PlanInstancesExtractor {

  /** Extracts `Instance` objects from a given set of plans.
    *
    * @param plans
    *   the set of plans that vectors are created from.
    * @return
    *   an `Instances` object containing one `Instance` object per plan. */
  def extract(plans: List[SQLStatementPlan]): Instances

  /** Extracts `Instance` objects and creates a `.arff` file.
    * 
    * and stores an `.arff` file.
    *
    * that can be analyzed in the Weka UI.
    *
    * @param plans
    *   the set of plans that vectors are created from.
    * @param fileName
    *   name of the file the `Instances` object is exported to.
    */
  def extract(plans: List[SQLStatementPlan], fileName: String): Unit = {
     var arffSaver = new ArffSaver
     var instances = extract(plans)

     instances.setRelationName(fileName)
     arffSaver.setInstances(instances)
     arffSaver.setFile(new File(fileName))
     arffSaver.writeBatch()
  }
}
