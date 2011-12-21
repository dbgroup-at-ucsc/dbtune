package edu.ucsc.dbtune.inum;

import com.google.common.collect.*;
import edu.ucsc.dbtune.spi.Console;
import edu.ucsc.dbtune.util.Strings;
import java.util.Set;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 * Tests the {@link OptimalPlansParser} interface.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class OptimalPlansParserTest
{
  private static final String EXECUTION_PLAN = "Hash Join  (cost=174080.39..9364262539.50 rows=1 width=193)";
  @Test public void testParsingOptimalPlans() throws Exception
 {
    final Set<OptimalPlan> optimalPlans = new InumOptimalPlansParser().parse(EXECUTION_PLAN);
    assertThat(!optimalPlans.isEmpty(), is(true));
    final OptimalPlan plan = getSingleOptimalPlan(optimalPlans);
    for (PhysicalOperator each : plan.getInternalPlans()){
      Console.streaming().info("About to test " + Strings.str(each));
      assertThat(each.getRowId(), equalTo(0));
      assertThat(each.getParentId(), equalTo(-1));
      assertThat(Strings.isEmpty(each.getTarget()), is(true));
      assertThat(Strings.same(each.getOperator(), "HSJOIN"), is(true));
      assertThat(Double.compare(each.getCost(), 9.3642625395E9), equalTo(0));
      assertThat(Double.compare(each.getInitCost(), 174080.39), equalTo(0));
      assertThat(each.getCardinality(), equalTo(1L));
    }
  }

  private static OptimalPlan getSingleOptimalPlan(Set<OptimalPlan> plans)
  {
    return Lists.newArrayList(plans).get(0);
  }

}
