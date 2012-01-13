package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.SharedFixtures;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Objects;
import java.util.Set;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 * Tests the {@link InumWhatIfOptimizer} implementation.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumWhatIfOptimizerTest {
  @Test public void testQueryCostEstimation() throws Exception {

    final InumWhatIfOptimizer optimizer = SharedFixtures.configureWhatIfOptimizer();
    double cost = optimizer.estimateCost("SELECT * FROM PERSONS;");
    assertThat(Double.compare(7.0, cost), equalTo(0));
  }

  @Test public void testQueryCostEstimation_NonEmpty_HypotheticalIndexes() throws Exception {
    final Set<Index> hypotheticalIndexes = SharedFixtures.configureConfiguration();
    final String sql = "SELECT * FROM PERSONS;";
    final InumWhatIfOptimizer optimizer =
        SharedFixtures.configureWhatIfOptimizer(sql, hypotheticalIndexes);
    double cost = optimizer.estimateCost(sql);
    assertThat(Double.compare(7.0, cost), equalTo(0));
  }

  @Test public void testStoppingInumDirectlyFromOptimizer() throws Exception {
    final InumWhatIfOptimizer optimizer = SharedFixtures.configureWhatIfOptimizer();
    final InumWhatIfOptimizerImpl castOptimizer = Objects.cast(optimizer,
        InumWhatIfOptimizerImpl.class);
    castOptimizer.endInum();
    assertThat(castOptimizer.getInum().isEnded(), is(true));
  }
}
