package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.core.SharedFixtures;
import edu.ucsc.dbtune.core.metadata.Configuration;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import org.junit.After;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the default implementation of {@link Precomputation} type.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumPrecomputationTest {
  private DatabaseConnection mockConnection;

  @Before public void setUp() throws Exception {
    mockConnection = SharedFixtures.configureConnection();
  }

  @Test (expected = NullPointerException.class)
  public void testInumSpaceNotYetCreated() throws Exception {
    final Precomputation setup = new InumPrecomputation(mockConnection);
    setup.getInumSpace();
    fail("Test has failed if we got to this point.");
  }

  @Test public void testInumSpaceBuilding_SingleElement() throws Exception {
    final Precomputation setup                   = new InumPrecomputation(mockConnection);
    final Configuration  configurationOfOneIndex = SharedFixtures.configureConfiguration();
    setup.setup("Some query", configurationOfOneIndex);

    final InumSpace is = setup.getInumSpace();
    final Integer   size         = is.getAllSavedOptimalPlans().size();
    final Integer   expectedSize = 2; // why 2? We are including the empty interesting order plus the non-empty one (suggested by INUM's VLDB paper)/
    assertThat(size, equalTo(expectedSize));
  }

  @Test public void testSkippingQuery() throws Exception {
    final Precomputation setup = new InumPrecomputation(mockConnection);
    final Configuration configurationOfOneIndex = SharedFixtures.configureConfiguration();
    setup.setup("Some query", configurationOfOneIndex);
    assertThat(setup.skip("Some query"), is(true));
  }

  @After public void tearDown() throws Exception {
    mockConnection = null;
  }
}
