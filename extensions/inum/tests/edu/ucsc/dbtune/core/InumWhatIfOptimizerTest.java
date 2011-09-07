package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.inum.Inum;
import edu.ucsc.dbtune.util.Objects;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 * Tests the {@link InumWhatIfOptimizer} implementation.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumWhatIfOptimizerTest {
  @Test public void testQueryCostEstimation() throws Exception {
    final InumWhatIfOptimizer optimizer = configureWhatIfOptimizer();
    double cost = optimizer.estimateCost("SELECT * FROM PERSONS;");
    assertThat(Double.compare(7.0, cost), equalTo(0));
  }

  @Test public void testQueryCostEstimation_NonEmpty_HypotheticalIndexes() throws Exception {

  }

  @Test public void testStoppingInumDirectlyFromOptimizer() throws Exception {
    final InumWhatIfOptimizer optimizer = configureWhatIfOptimizer();
    final InumWhatIfOptimizerImpl castOptimizer = Objects.cast(optimizer,
        InumWhatIfOptimizerImpl.class);
    castOptimizer.endInum();
    assertThat(castOptimizer.getInum().isEnded(), is(true));
  }

  private static InumWhatIfOptimizer configureWhatIfOptimizer() throws Exception {
    final Inum inum      = SharedFixtures.configureInum();
    return new InumWhatIfOptimizerImpl(inum);
  }
}
