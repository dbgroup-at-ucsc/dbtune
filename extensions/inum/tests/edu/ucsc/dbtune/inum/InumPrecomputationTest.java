package edu.ucsc.dbtune.inum;

import com.google.common.collect.Sets;
import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseConnection;
import java.sql.Connection;
import java.util.Set;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import org.junit.After;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests the default implementation of {@link Precomputation} type.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumPrecomputationTest {
  private DatabaseConnection mockConnection;

  @Before public void setUp() throws Exception {
    mockConnection = Mockito.mock(DatabaseConnection.class);
    Connection jdbcConnection = Mockito.mock(Connection.class);
    Mockito.when(mockConnection.getJdbcConnection()).thenReturn(jdbcConnection);
  }

  @Test (expected = NullPointerException.class)
  public void testInumSpaceNotYetCreated() throws Exception {
    final Precomputation setup = new InumPrecomputation(mockConnection);
    setup.getInumSpace();
    fail("Test has failed if we got to this point.");
  }

  @Test public void testInumSpaceBuilding_SingleElement() throws Exception {
    final Precomputation setup = new InumPrecomputation(mockConnection);
    final Set<DBIndex> configurationOfOneIndex = configureConfiguration();
    setup.setup("Some query", configurationOfOneIndex);
    final InumSpace is = setup.getInumSpace();
    final Integer size         = is.getAllSavedOptimalPlans().size();
    final Integer expectedSize = 1;
    assertThat(size, equalTo(expectedSize));
  }

  private static Set<DBIndex> configureConfiguration() {
    final DBIndex index = Mockito.mock(DBIndex.class);
    final Set<DBIndex> configurationOfOneIndex = Sets.newHashSet();
    configurationOfOneIndex.add(index);
    return configurationOfOneIndex;
  }

  @Test public void testSkippingQuery() throws Exception {
    final Precomputation setup = new InumPrecomputation(mockConnection);
    final Set<DBIndex> configurationOfOneIndex = configureConfiguration();
    setup.setup("Some query", configurationOfOneIndex);
    assertThat(setup.skip("Some query"), is(true));
  }

  @After public void tearDown() throws Exception {
    mockConnection = null;
  }
}
