package edu.ucsc.dbtune.ml

import edu.ucsc.dbtune.core.optimizers.plan.StatementPlan

import weka.core.FastVector
import weka.core.Attribute
import weka.core.Instance
import weka.core.Instances

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions.asScalaBuffer

import java.util.NoSuchElementException

/** Extracts feature vectors out of `StatementPlan` objects.
  *
  * Extracts feature vectors out of `StatementPlan` objects. For each distinct operator (specified 
  * in the `operatorNames` variable) there is a pair of elements in the extracted vector. The first 
  * entry corresponds to the count of the operator and the second to the cost of the operator. The 
  * order of the operators in the vectors corresponds to the order given in the `operatorNames` 
  * list.
  *
  * For example, the following represents a plan (in JSON format for ease of illustration):
  *
  * <code>
  * "Plan": {
  *   "Node Type": "Nested Loop",
  *   "Startup Cost": 5,
  *   "Total Cost": 926,
  *   "Plan Rows": 1,
  *   "Plan Width": 0,
  *   "Plans": [
  *     {
  *        "Node Type": "Seq Scan",
  *        "Parent Relationship": "Outer",
  *        "Relation Name": "tbl",
  *        "Alias": "t1",
  *        "Startup Cost": 0.00,
  *        "Total Cost": 155.00,
  *        "Plan Rows": 10000,
  *        "Plan Width": 16
  *     },
  *     {
  *        "Node Type": "Seq Scan",
  *        "Parent Relationship": "Inner",
  *        "Relation Name": "tbl",
  *        "Alias": "t2",
  *        "Startup Cost": 0.00,
  *        "Total Cost": 428.00,
  *        "Plan Rows": 10000,
  *        "Plan Width": 16
  *     }
  *   ]
  * </code>
  *
  * In order to extract features from plans like the above, the list of possible operator names has 
  * to be passed as an argument to this class. For example:
  *
  * <code>
  * val operatorNames = List("Seq Scan", "Limit", "Nested Loop", "Hash Join", "Append")
  * </code>
  *
  * Then, [[OperatorAggregationExtractor.extract]] generates the following feature vector:
  *
  * <code>
  * | 2 |583| 0 | 0 | 1 |343| 0 | 0 | 0 | 0 |
  * |---|---|---|---|---|---|---|---|---|---|
  * | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |
  * </code>
  *
  * index 0 corresponds to `"Seq Scan"`; 1 to the sum of cardinalities of all `"Seq Scan"` operators 
  * in the plan; 2 and 3 to the `"Limit"` operator; and so on and so forth. Conceptually, the vector 
  * corresponds to:
  *
  * <code>
  * OperatorName|SeqScan| Limit | NLoop | HashJ | Apend |
  * ------------|---|---|---|---|---|---|---|---|---|---|
  * count / sum | 2 |583| 0 | 0 | 1 |343| 0 | 0 | 0 | 0 |
  * ------------|-------|-------|-------|-------|-------|
  *     index   | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |
  * </code>
  *
  * Note that the cost of the operator corresponds to the actual cost and not the accumulated cost.
  *
  * @constructor
  *   creates a new extractor with the given operator names
  * @param operatorNames
  *   list of possible operator names that a plan can have */
class OperatorAggregationExtractor(operatorNames: List[String]) extends PlanInstancesExtractor {
  /** The list of attributes that are contained in an instance */
  val attributes = {
    val atts = new FastVector
    for(operatorName <- operatorNames) {
      atts.addElement(new Attribute(operatorName))
      atts.addElement(new Attribute(operatorName + " cardinality"))
    }
    atts
  }

  /** Extracts `Instance` objects from a given set of plans.
    *
    * @param plans
    *   the set of plans that vectors are created from.
    * @return
    *   an `Instances` object containing one per plan.
    * @throws NoSuchElementException
    *   if an operator contained in the plan isn't a member of 
    *   [[OperatorAggregationExtractor.operatorNames]] */
  def extract(plans: List[StatementPlan]): Instances = {
    val data = new Instances("Instances", attributes, plans.size)
      
    for(plan <- plans) {
      data.add(createInstance(plan))
    }

    return data
  }

  /** Creates an instance out of the given plan.
    *
    * @param plan
    *   object where the instance is extracted from
    * @return
    *   an instance representing the plan
    * @throws NoSuchElementException
    *   if one of the operators in the plan isn't a member of 
    *   [[OperatorAggregationExtractor.operatorNames]] */
  def createInstance(plan: StatementPlan): Instance = {
    val values = new Array[Double](attributes.size)

    for(operator <- asScalaBuffer(plan.toList)) {
      val idx = operatorNames.indexOf(operator.getName) * 2

      if(idx < 0) throw new NoSuchElementException("Operator " + operator.getName + " not in list")

      values(idx+0) = values(idx+0) + 1.0;
      values(idx+1) = values(idx+1) + operator.getCost;
    }
    new Instance(1.0,values)
  }
}
