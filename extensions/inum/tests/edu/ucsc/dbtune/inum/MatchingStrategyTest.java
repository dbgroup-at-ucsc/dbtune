package edu.ucsc.dbtune.inum;

import com.google.common.collect.Lists;
import edu.ucsc.dbtune.core.SharedFixtures;
import edu.ucsc.dbtune.core.metadata.Configuration;
import edu.ucsc.dbtune.core.metadata.Index;
import org.hamcrest.CoreMatchers;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 * Tests Inum's {@link MatchingStrategy matching logic}
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class MatchingStrategyTest {
  @Test public void testMatchingLogic() throws Exception {
    final MatchingStrategy matchingLogic = new InumMatchingStrategy(SharedFixtures.configureConnection());
    final Configuration    input         = SharedFixtures.configureConfiguration();
    final InumSpace        inumSpace     = SharedFixtures.configureInumSpace(input);
    final OptimalPlan      plan          = matchingLogic.matches(inumSpace, input);
    assertThat(plan, CoreMatchers.<Object>notNullValue());
  }

  @Test public void testDeriveCost() throws Exception {
    final IndexAccessCostEstimation estimation    = SharedFixtures.configureEstimator();

    final MatchingStrategy matchingLogic = new InumMatchingStrategy(SharedFixtures.configureConnection()) {
      @Override public IndexAccessCostEstimation getIndexAccessCostEstimation() {
        return estimation;
      }
    };
    final Configuration    input         = SharedFixtures.configureConfiguration();
    final InumSpace        inumSpace     = SharedFixtures.configureInumSpace(input);
    final OptimalPlan      plan          = matchingLogic.matches(inumSpace, input);
    final double           cost          = matchingLogic.derivesCost("lalalalaSql", plan, input);
    assertThat(Double.compare(10.0, cost), is(0));
  }

  @Test public void testIndexAccessCostEstimation() throws Exception {
    final IndexAccessCostEstimation estimation    = new InumIndexAccessCostEstimation(SharedFixtures.configureConnection());
    final double cost = estimation.estimateIndexAccessCost("lalala", new Configuration(
        Lists.<Index>newArrayList()));
    // the cost is zero because we are passing an empty set of indexes...
    assertThat(Double.compare(0.0, cost), is(0));
  }
}
