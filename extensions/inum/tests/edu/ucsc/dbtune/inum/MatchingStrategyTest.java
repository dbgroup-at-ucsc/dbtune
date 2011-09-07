package edu.ucsc.dbtune.inum;

import com.google.common.collect.Sets;
import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.SharedFixtures;
import java.util.Set;
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
    final DBIndex          index         = SharedFixtures.configureIndex(null);
    final MatchingStrategy matchingLogic = new InumMatchingStrategy(SharedFixtures.configureConnection(
        index));
    final Set<DBIndex>     input         = Sets.newHashSet();
    input.add(index);
    final InumSpace        inumSpace     = SharedFixtures.configureInumSpace(input);
    final OptimalPlan      plan          = matchingLogic.matches(inumSpace, input);
    assertThat(plan, CoreMatchers.<Object>notNullValue());
  }

  @Test public void testDeriveCost() throws Exception {
    final DBIndex                   index         = SharedFixtures.configureIndex(null);
    final IndexAccessCostEstimation estimation    = SharedFixtures.configureEstimator();

    final MatchingStrategy matchingLogic = new InumMatchingStrategy(SharedFixtures.configureConnection(
        index)) {
      @Override public IndexAccessCostEstimation getIndexAccessCostEstimation() {
        return estimation;
      }
    };
    final Set<DBIndex>     input         = Sets.newHashSet();
    input.add(index);
    final InumSpace        inumSpace     = SharedFixtures.configureInumSpace(input);
    final OptimalPlan      plan          = matchingLogic.matches(inumSpace, input);
    final double           cost          = matchingLogic.derivesCost("lalalalaSql", plan, input);
    assertThat(Double.compare(10.0, cost), is(0));
  }

  @Test public void testIndexAccessCostEstimation() throws Exception {
    final DBIndex                   index         = SharedFixtures.configureIndex(null);
    final IndexAccessCostEstimation estimation    = new InumIndexAccessCostEstimation(SharedFixtures.configureConnection(
        index));
    final double cost = estimation.estimateIndexAccessCost("lalala", Sets.<DBIndex>newHashSet());
    // the cost is zero because we are passing an empty set of indexes...
    assertThat(Double.compare(0.0, cost), is(0));
  }
}
