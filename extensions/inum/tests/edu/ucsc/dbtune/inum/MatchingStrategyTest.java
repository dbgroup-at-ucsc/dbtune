package edu.ucsc.dbtune.inum;

import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.SharedFixtures;
import edu.ucsc.dbtune.metadata.Index;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Tests Inum's {@link MatchingStrategy matching logic}
 *
 * @author Huascar A. Sanchez
 */
public class MatchingStrategyTest {
  @Test public void testMatchingLogic() throws Exception {
    final String sql = "SELECT * FROM test;";
    final MatchingStrategy matchingLogic = new InumMatchingStrategy(SharedFixtures.configureConnection());
    final Set<Index> input = SharedFixtures.configureConfiguration();
    final InumSpace inumSpace = SharedFixtures.configureInumSpace(sql, input);
    final Set<OptimalPlan> plan = matchingLogic.matches(sql, input, inumSpace);
    assertThat(plan, notNullValue());
    assertThat(plan.size(), is(1));
  }

  @Test public void testDeriveCost() throws Exception {
    final String sql = "SELECT * FROM test;";
    final IndexAccessCostEstimation estimation    = SharedFixtures.configureEstimator();

    final MatchingStrategy matchingLogic = new InumMatchingStrategy(SharedFixtures.configureConnection()) {
      @Override public IndexAccessCostEstimation getIndexAccessCostEstimation() {
        return estimation;
      }
    };
    final Set<Index> input = SharedFixtures.configureConfiguration();
    final InumSpace inumSpace = SharedFixtures.configureInumSpace(sql, input);
    final double cost = matchingLogic.estimateCost(sql, input, inumSpace);
    assertThat(Double.compare(11.0, cost), is(0));
  }

  @Test public void testIndexAccessCostEstimation() throws Exception {
    final String sql = "SELECT * FROM test;";
    final IndexAccessCostEstimation estimation    = new InumIndexAccessCostEstimation(SharedFixtures.configureConnection());
    final double cost = estimation.estimateIndexAccessCost(sql, new HashSet<Index>());
    // the cost is zero because we are passing an empty set of indexes...
    assertThat(Double.compare(0.0, cost), is(0));
  }
}
