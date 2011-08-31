package edu.ucsc.dbtune.inum;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.core.IndexExtractor;
import edu.ucsc.dbtune.util.Combinations;
import edu.ucsc.dbtune.util.Strings;
import java.util.Set;
import org.hamcrest.CoreMatchers;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
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

  @Test public void testCombinationGeneration() throws Exception {
    final Set<DummyIndex> indexes = Sets.newHashSet();
    for(int idx = 0; idx < 3; idx++){
      indexes.add(new DummyIndex(configureIndex("create index " + idx)));
    }

    final Set<Set<DummyIndex>> combinations = Combinations.findCombinations(indexes);
    assertThat(combinations.size(), equalTo(8));
  }

  @Test (expected = InumExecutionException.class) public void testInumShutdown() throws Exception {
    final Inum inum = configureInum();
    inum.end();
    inum.estimateCost("lalala", Sets.<DBIndex>newHashSet());
    fail("if we reached this line...then test has failed");
  }

  private static DatabaseConnection configureConnection(DBIndex index) throws Exception {
    final Set<DBIndex> configuration = Sets.newHashSet();
    configuration.add(index);
    IndexExtractor extractor = Mockito.mock(IndexExtractor.class);
    Mockito.when(extractor.recommendIndexes(Mockito.anyString())).thenReturn(configuration);
    DatabaseConnection connection = Mockito.mock(DatabaseConnection.class);
    Mockito.when(connection.getIndexExtractor()).thenReturn(extractor);
    return connection;
  }

  private static DBIndex configureIndex(String text) {
    DBIndex soleOne = Mockito.mock(DBIndex.class);
    Mockito.when(soleOne.internalId()).thenReturn(1);
    Mockito.when(soleOne.creationCost()).thenReturn(2.0);
    if(!Strings.isEmpty(text)) Mockito.when(soleOne.creationText()).thenReturn(text);
    Mockito.when(soleOne.columnCount()).thenReturn(1);
    return soleOne;
  }

  public static Inum configureInum() throws Exception {
    final DBIndex                     index          = configureIndex(null);
    final DatabaseConnection          connection     = configureConnection(index);
    final InumSpace                   inumSpace      = configureInumSpace(index);
    final Precomputation              precomputation = configurePrecomputation(inumSpace);
    final MatchingStrategy            matchingLogic  = configureMatchingLogic(inumSpace);
    final InterestingOrdersExtractor  ioExtractor    = configureIOExtractor();

    return Inum.newInumInstance(connection, precomputation, matchingLogic, ioExtractor);
  }

  private static InterestingOrdersExtractor configureIOExtractor(){
    final InterestingOrdersExtractor extractor = Mockito.mock(InterestingOrdersExtractor.class);
    final DBIndex idx1 = configureIndex("1");
    final DBIndex idx2 = configureIndex("2");
    final DBIndex idx3 = configureIndex("3");

    final Set<DBIndex> indexes = Sets.newHashSet();
    indexes.add(idx1);
    indexes.add(idx2);
    indexes.add(idx3);

    Mockito.when(extractor.extractInterestingOrders(Mockito.anyString())).thenReturn(indexes);
    return extractor;
  }

  private static InumSpace configureInumSpace(DBIndex index) throws Exception {
    final InumSpace inumSpace = Mockito.mock(InumSpace.class);
    final Set<OptimalPlan> plans = configureSingleOptimalPlan();
    final Set<DBIndex> key = Sets.newHashSet();
    key.add(index);
    Mockito.when(inumSpace.getAllSavedOptimalPlans()).thenReturn(plans);
    Mockito.when(inumSpace.save(key, plans)).thenReturn(plans);
    return inumSpace;
  }

  private static MatchingStrategy configureMatchingLogic(InumSpace inumSpace) throws Exception {
    final MatchingStrategy matchingLogic = Mockito.mock(MatchingStrategy.class);
    final OptimalPlan plan = Lists.newArrayList(inumSpace.getAllSavedOptimalPlans()).get(0);
    Mockito.when(matchingLogic.matches(Mockito.eq(inumSpace), Mockito.anySetOf(DBIndex.class))).thenReturn(plan);
    final double cost = plan.getTotalCost();
    Mockito.when(matchingLogic.derivesCost(Mockito.anyString(), Mockito.eq(plan), Mockito.anySetOf(DBIndex.class))).thenReturn(cost);
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

  private static class DummyIndex implements
      Comparable<DummyIndex> /*comparable? why? just because we want to be able to sort them....*/ {
    private final DBIndex index;

    DummyIndex(DBIndex index){
      this.index = index;
    }

    @Override public int compareTo(DummyIndex comparableIndex) {
      return index.creationText().compareTo(comparableIndex.index.creationText());
    }
  }
}
