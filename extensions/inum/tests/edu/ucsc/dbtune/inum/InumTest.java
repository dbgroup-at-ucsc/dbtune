package edu.ucsc.dbtune.inum;

import com.google.common.collect.Lists;
import edu.ucsc.dbtune.core.SharedFixtures;
import edu.ucsc.dbtune.core.metadata.Configuration;
import edu.ucsc.dbtune.core.metadata.Index;
import edu.ucsc.dbtune.core.metadata.Table;
import edu.ucsc.dbtune.util.Combinations;
import java.util.Set;
import org.hamcrest.CoreMatchers;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * Test the {@link Inum} class.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumTest {
  @Test public void testInumSpaceGeneration() throws Exception {
    final Inum inum = SharedFixtures.configureInum();
    inum.start();
    assertThat(inum.getInumSpace(), CoreMatchers.<Object>notNullValue());
    assertThat(inum.getInumSpace().getAllSavedOptimalPlans().isEmpty(), is(false));
  }

  @Test public void testCombinationGeneration() throws Exception {
    final Table        table   = new Table("persons");
    final Configuration interestingOrders = SharedFixtures.configureConfiguration(table, 3, 3);

    final Set<Configuration> combinations = Combinations.findCombinations(interestingOrders);
    assertThat(combinations.size(), equalTo(8));
  }


  @Test (expected = InumExecutionException.class) public void testInumShutdown() throws Exception {
    final Inum inum = SharedFixtures.configureInum();
    inum.end();
    final Configuration emptyConfiguration = new Configuration(
      Lists.<Index>newArrayList());
    inum.estimateCost("lalala", emptyConfiguration);
    fail("if we reached this line...then test has failed");
  }
}
