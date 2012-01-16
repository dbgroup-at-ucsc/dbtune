package edu.ucsc.dbtune.inum;

import com.google.common.collect.Lists;
import edu.ucsc.dbtune.SharedFixtures;
import edu.ucsc.dbtune.metadata.Index;
import java.sql.Connection;
import java.util.List;
import java.util.Set;
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
public class InumPrecomputationTest {
  private Connection mockConnection;

  @Before public void setUp() throws Exception {
    mockConnection = SharedFixtures.configureConnection();
  }

  @Test public void testInumSpaceBuilding_SingleElement() throws Exception {
    final Precomputation setup = new InumPrecomputation(mockConnection);
    final List<Set<Index>> list = configuration();
    setup.setup("SELECT * FROM test;", list);

    final InumSpace is = setup.getInumSpace();
    final Integer size = is.getAllSavedOptimalPlans().size();
    final Integer expectedSize
        = 2; // why 2? We are including the empty interesting order plus the non-empty one (suggested by INUM's VLDB paper)/
    assertThat(size, equalTo(expectedSize));
  }

  @Test public void testSkippingQuery() throws Exception {
    final Precomputation setup = new InumPrecomputation(mockConnection);
    final List<Set<Index>> list = configuration();
    setup.setup("SELECT * FROM test;", list);
    assertThat(setup.skip("SELECT * FROM test;"), is(true));
  }

  private static List<Set<Index>> configuration() throws Exception {
    final Set<Index> configurationOfOneIndex = SharedFixtures.configureConfiguration();
    final List<Set<Index>> list = Lists.newArrayList();
    list.add(configurationOfOneIndex);
    return list;
  }

  @After public void tearDown() throws Exception {
    mockConnection = null;
  }
}
