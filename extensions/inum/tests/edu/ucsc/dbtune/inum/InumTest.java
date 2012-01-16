package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.SharedFixtures;
import static edu.ucsc.dbtune.SharedFixtures.configureConfiguration;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.util.ConfigurationUtils;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
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
    assertThat(inum.getInumSpace(), notNullValue());
    assertThat(inum.getInumSpace().getAllSavedOptimalPlans().isEmpty(), is(false));
  }

  @Test public void testCartesianProductGeneration() throws Exception {
    final Table table = new Table(new Schema(new Catalog("persons_db"), "persons_sch"), "persons");
    final Set<Index> interestingOrders = configureConfiguration(table, 3, 3);

    final Set<List<Index>> combinations = ConfigurationUtils.cartesianProductOf(interestingOrders);
    assertThat(combinations.size(), equalTo(4)); // 3 elements from the same table and 1 empty list
    System.out.println(combinations);
  }

  @Test(expected = InumExecutionException.class) public void testInumShutdown() throws Exception {
    final Inum inum = SharedFixtures.configureInum();
    inum.end();
    final Set<Index> emptyConfiguration = new HashSet<Index>();
    inum.estimateCost("lalala", emptyConfiguration);
    fail("if we reached this line...then the test has failed");
  }
}
