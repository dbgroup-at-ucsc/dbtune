package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.SharedFixtures;
import edu.ucsc.dbtune.metadata.Configuration;
import java.util.Set;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 * Tests the {@link InumSpace INUM Space} interface.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumSpaceTest {
  @Test public void testPopulateInumSpace() throws Exception {
    final InumSpace     space = new InMemoryInumSpace();
    final String        sql     = "Select * from Table1;";
    final Configuration config  = SharedFixtures.configureConfiguration();
    final Key           key     = new Key(sql, config);
    final Set<OptimalPlan> plans = space.save(key, SharedFixtures.configureOptimalPlans()).getOptimalPlans(key);
    assertThat(!plans.isEmpty(), is(true));
    assertThat(!space.getAllSavedOptimalPlans().isEmpty(), is(true));
  }

  @Test public void testClearingInumSpace() throws Exception {
    final InumSpace     space   = new InMemoryInumSpace();
    final String        sql     = "Select * from Table1;";
    final Configuration config  = SharedFixtures.configureConfiguration();
    final Key           key     = new Key(sql, config);
    final Set<OptimalPlan> plans = space.save(key, SharedFixtures.configureOptimalPlans()).getOptimalPlans(key);
    assertThat(!plans.isEmpty(), is(true));
    space.clear();
    assertThat(space.getAllSavedOptimalPlans().isEmpty(), is(true));
  }

  @Test public void testRetrievalOfOptimalPlansPerKey() throws Exception {
    final InumSpace     space     = new InMemoryInumSpace();
    final String        sql       = "Select * from Table1;";
    final Configuration config    = SharedFixtures.configureConfiguration();
    final Key           key       = new Key(sql, config);
    space.save(key, SharedFixtures.configureOptimalPlans());
    final Set<OptimalPlan> found = space.getOptimalPlans(key);
    assertThat(!found.isEmpty(), is(true));
  }
}
