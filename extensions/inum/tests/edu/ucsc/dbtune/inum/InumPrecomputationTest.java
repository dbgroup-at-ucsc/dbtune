package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.SharedFixtures;
import edu.ucsc.dbtune.metadata.Configuration;
import java.sql.Connection;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import org.junit.After;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the default implementation of {@link Precomputation} type.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumPrecomputationTest
{
  private Connection mockConnection;

  @Before public void setUp() throws Exception
 {
    mockConnection = SharedFixtures.configureConnection();
  }

  @Test public void testInumSpaceBuilding_SingleElement() throws Exception
 {
    final Precomputation setup                   = new InumPrecomputation(mockConnection);
    final Configuration  configurationOfOneIndex = SharedFixtures.configureConfiguration();
    setup.setup("SELECT * FROM test;", configurationOfOneIndex);

    final InumSpace is    = setup.getInumSpace();
    final Integer   size  = is.getAllSavedOptimalPlans().size();
    final Integer   expectedSize = 2; // why 2? We are including the empty interesting order plus the non-empty one (suggested by INUM's VLDB paper)/
    assertThat(size, equalTo(expectedSize));
  }

  @Test public void testSkippingQuery() throws Exception
 {
    final Precomputation setup = new InumPrecomputation(mockConnection);
    final Configuration configurationOfOneIndex = SharedFixtures.configureConfiguration();
    setup.setup("SELECT * FROM test;", configurationOfOneIndex);
    assertThat(setup.skip("SELECT * FROM test;"), is(true));
  }

  @After public void tearDown() throws Exception
 {
    mockConnection = null;
  }
}
