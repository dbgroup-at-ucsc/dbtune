package edu.ucsc.dbtune.inum;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.core.IndexExtractor;
import java.util.Set;
import org.hamcrest.CoreMatchers;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test the {@link Inum} class.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumTest {
  @Test public void testInumSpaceGeneration() throws Exception {
    final Inum inum = configureInum();
    inum.start();
    assertThat(inum.getInumSpace(), CoreMatchers.<Object>notNullValue());
    assertThat(inum.getInumSpace().getAllSavedOptimalPlans().isEmpty(), is(false));
  }

  private static DatabaseConnection configureConnection() throws Exception {
    DBIndex soleOne = Mockito.mock(DBIndex.class);
    Mockito.when(soleOne.internalId()).thenReturn(1);
    Mockito.when(soleOne.creationCost()).thenReturn(2.0);
    Mockito.when(soleOne.columnCount()).thenReturn(1);
    final Set<DBIndex> configuration = Sets.newHashSet();
    configuration.add(soleOne);
    IndexExtractor extractor = Mockito.mock(IndexExtractor.class);
    Mockito.when(extractor.recommendIndexes(Mockito.anyString())).thenReturn(configuration);
    DatabaseConnection connection = Mockito.mock(DatabaseConnection.class);
    Mockito.when(connection.getIndexExtractor()).thenReturn(extractor);
    return connection;
  }

  private static Inum configureInum() throws Exception {
    final DatabaseConnection connection     = configureConnection();
    final InumSpace          inumSpace      = configureInumSpace();
    final Precomputation     precomputation = configurePrecomputation(inumSpace);
    final MatchingStrategy   matchingLogic  = configureMatchingLogic(inumSpace);

    return Inum.newInumInstance(connection, precomputation, matchingLogic);
  }

  private static InumSpace configureInumSpace() throws Exception {
    final InumSpace inumSpace = Mockito.mock(InumSpace.class);
    final Set<OptimalPlan> plans = configureSingleOptimalPlan();
    Mockito.when(inumSpace.getAllSavedOptimalPlans()).thenReturn(plans);
    Mockito.when(inumSpace.save(plans)).thenReturn(plans);
    return inumSpace;
  }

  private static MatchingStrategy configureMatchingLogic(InumSpace inumSpace) throws Exception {
    final MatchingStrategy matchingLogic = Mockito.mock(MatchingStrategy.class);
    final OptimalPlan plan = Lists.newArrayList(inumSpace.getAllSavedOptimalPlans()).get(0);
    Mockito.when(matchingLogic.matches(Mockito.anySet(), Mockito.anySet())).thenReturn(plan);
    final double cost = plan.getTotalCost();
    Mockito.when(matchingLogic.derivesCost(Mockito.eq(plan), Mockito.anySet())).thenReturn(cost);
    return matchingLogic;
  }

  private static Precomputation configurePrecomputation(InumSpace inumSpace) throws Exception {
    final Precomputation    setup     = Mockito.mock(Precomputation.class);
    Mockito.when(setup.getInumSpace()).thenReturn(inumSpace);
    final Set<OptimalPlan> plans = inumSpace.getAllSavedOptimalPlans();
    Mockito.when(setup.setup(Mockito.anyString(), Mockito.anySetOf(DBIndex.class))).thenReturn(plans);

    return setup;
  }

  private static Set<OptimalPlan> configureSingleOptimalPlan() throws Exception {
    final Set<OptimalPlan> singleOne = Sets.newHashSet();
    final OptimalPlan opp = Mockito.mock(OptimalPlan.class);
    Mockito.when(opp.getAccessCost(Mockito.anyString())).thenReturn(2.0);
    Mockito.when(opp.getInternalCost()).thenReturn(1.0);
    Mockito.when(opp.getTotalCost()).thenReturn(7.0);
    Mockito.when(opp.isDirty()).thenReturn(false);
    singleOne.add(opp);
    return singleOne;
  }
}
