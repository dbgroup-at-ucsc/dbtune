package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.SharedFixtures;
import edu.ucsc.dbtune.metadata.Index;
import java.util.HashSet;
import java.util.Set;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * Tests Inum's {@link MatchingStrategy matching logic}
 *
 * @author Huascar A. Sanchez
 */
public class MatchingStrategyTest {
  @Test public void testMatchingLogic() throws Exception {
    final String sql = "SELECT * FROM test;";
    final MatchingStrategy matchingLogic = new InumMatchingStrategy();
    final Set<Index> input = SharedFixtures.configureConfiguration();
    final InumSpace inumSpace = SharedFixtures.configureInumSpace(sql, input);
    final Set<OptimalPlan> plan = matchingLogic.matches(sql, input, inumSpace);
    assertThat(plan, notNullValue());
    assertThat(plan.size(), is(1));
  }

  @Test public void testDeriveCost() throws Exception {
    final String sql = "SELECT * FROM test;";
    final IndexAccessCostEstimation estimation = SharedFixtures.configureEstimator();

    final MatchingStrategy matchingLogic = new InumMatchingStrategy() {
      @Override public IndexAccessCostEstimation getIndexAccessCostEstimation() {
        return estimation;
      }
    };
    final Set<Index> input = SharedFixtures.configureConfiguration();
    final InumSpace inumSpace = SharedFixtures.configureInumSpace(sql, input);
    final double cost = matchingLogic.estimateCost(sql, input, inumSpace);
    assertThat(Double.compare(11.0, cost), is(0));
  }

  @Test(expected = NullPointerException.class)
  public void testIndexAccessCostEstimationOfNullOptimalPlan() throws Exception {
    final IndexAccessCostEstimation estimation = new InumIndexAccessCostEstimation();
    estimation.estimateIndexAccessCost(null, new HashSet<Index>());
    fail("if we got here, then this test has failed");
  }
}
