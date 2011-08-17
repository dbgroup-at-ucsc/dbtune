package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.core.DBIndex;
import java.util.HashSet;
import java.util.Set;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests the {@link InumSpace INUM Space} interface.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumSpaceTest {
  @Test public void testPopulateInumSpace() throws Exception {
    final InumSpace space = new InmemoryInumSpace();
    final Set<OptimalPlan> plans = space.save(configureIndex(), configureOptimalPlans());
    assertThat(!plans.isEmpty(), is(true));
    assertThat(!space.getAllSavedOptimalPlans().isEmpty(), is(true));
  }

  @Test public void testClearingInumSpace() throws Exception {
    final InumSpace space = new InmemoryInumSpace();
    final Set<OptimalPlan> plans = space.save(configureIndex(), configureOptimalPlans());
    assertThat(!plans.isEmpty(), is(true));
    space.clear();
    assertThat(space.getAllSavedOptimalPlans().isEmpty(), is(true));
  }

  @Test public void testRetrievalOfOptimalPlansPerKey() throws Exception {
    final InumSpace space = new InmemoryInumSpace();
    final DBIndex   key   = configureIndex();
    space.save(key, configureOptimalPlans());
    final Set<OptimalPlan> found = space.getOptimalPlans(key);
    assertThat(!found.isEmpty(), is(true));
  }

  private static DBIndex configureIndex() throws Exception {
    DBIndex soleOne = Mockito.mock(DBIndex.class);
    Mockito.when(soleOne.internalId()).thenReturn(1);
    Mockito.when(soleOne.creationCost()).thenReturn(2.0);
    Mockito.when(soleOne.columnCount()).thenReturn(1);
    return soleOne;
  }

  private static Set<OptimalPlan> configureOptimalPlans() throws Exception {
    final OptimalPlan optimalPlan = Mockito.mock(OptimalPlan.class);
    return new HashSet<OptimalPlan>(){{add(optimalPlan);}};
  }
}
