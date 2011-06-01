package edu.ucsc.dbtune.ml

import edu.ucsc.dbtune.core.optimizers.plan.StatementPlan
import edu.ucsc.dbtune.core.optimizers.plan.Operator
import edu.ucsc.dbtune.ml.OperatorAggregationExtractorTest._

import org.junit.Test
import org.junit.Assert.assertEquals

class OperatorAggregationExtractorTest {
  /** Checks that instances are extracted correctly */
  @Test def testBasicUsage = {
    val nameList  = List("Seq Scan", "Limit", "Nested Loop", "Index Scan", "Append")
    val extractor = new OperatorAggregationExtractor(nameList)

    val instances = extractor.extract(List(plan1,plan2))

    assertEquals(2, instances.numInstances)

    // check instance for plan 1
    assertEquals(2.0,         instances.instance(0).value(0), 0.1)
    assertEquals(20000.0,     instances.instance(0).value(1), 0.1)
    assertEquals(0.0,         instances.instance(0).value(2), 0.1)
    assertEquals(0.0,         instances.instance(0).value(3), 0.1)
    assertEquals(1.0,         instances.instance(0).value(4), 0.1)
    assertEquals(1.0,         instances.instance(0).value(5), 0.1)

    // check instance for plan 2
    assertEquals(0.0,     instances.instance(1).value(0), 0.1)
    assertEquals(0.0,     instances.instance(1).value(1), 0.1)
    assertEquals(1.0,     instances.instance(1).value(2), 0.1)
    assertEquals(12345.0, instances.instance(1).value(3), 0.1)
    assertEquals(0.0,     instances.instance(1).value(4), 0.1)
    assertEquals(0.0,     instances.instance(1).value(5), 0.1)
    assertEquals(1.0,     instances.instance(1).value(6), 0.1)
    assertEquals(123456.0,instances.instance(1).value(7), 0.1)
  }
}

object OperatorAggregationExtractorTest {
    val plan1 = {
        val root = new Operator("Nested Loop", 926.37, 1)
        val plan = new StatementPlan(root)

        plan.setChild(root, new Operator("Seq Scan",155.0,10000))
        plan.setChild(root, new Operator("Seq Scan",428.0,10000))
        plan
    }
    val plan2 = {
        val root = new Operator("Limit", 115.11, 12345)
        val plan = new StatementPlan(root)

        plan.setChild(root, new Operator("Index Scan",12.2,123456))
        plan
    }
}
