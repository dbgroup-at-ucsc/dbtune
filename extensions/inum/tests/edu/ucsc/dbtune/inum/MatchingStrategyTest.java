package edu.ucsc.dbtune.inum;

import com.google.common.collect.Sets;
import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.core.IndexExtractor;
import edu.ucsc.dbtune.util.Strings;
import java.sql.Connection;
import java.util.Set;
import org.hamcrest.CoreMatchers;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests Inum's {@link MatchingStrategy matching logic}
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class MatchingStrategyTest {
  @Test public void testMatchingLogic() throws Exception {
    final DBIndex          index         = configureIndex(null);
    final MatchingStrategy matchingLogic = new InumMatchingStrategy(configureConnection(index));
    final Set<DBIndex>     input         = Sets.newHashSet();
    input.add(index);
    final InumSpace        inumSpace     = configureInumSpace(input);
    final OptimalPlan      plan          = matchingLogic.matches(inumSpace, input);
    assertThat(plan, CoreMatchers.<Object>notNullValue());
  }

  @Test public void testDeriveCost() throws Exception {
    final DBIndex                   index         = configureIndex(null);
    final IndexAccessCostEstimation estimation    = configureEstimator();

    final MatchingStrategy matchingLogic = new InumMatchingStrategy(configureConnection(index)) {
      @Override public IndexAccessCostEstimation getIndexAccessCostEstimation() {
        return estimation;
      }
    };
    final Set<DBIndex>     input         = Sets.newHashSet();
    input.add(index);
    final InumSpace        inumSpace     = configureInumSpace(input);
    final OptimalPlan      plan          = matchingLogic.matches(inumSpace, input);
    final double           cost          = matchingLogic.derivesCost("lalalalaSql", plan, input);
    assertThat(Double.compare(10.0, cost), is(0));
  }

  @Test public void testIndexAccessCostEstimation() throws Exception {
    final IndexAccessCostEstimation estimation    = configureEstimator();

  }

  private static IndexAccessCostEstimation configureEstimator() {
    final IndexAccessCostEstimation estimation = Mockito.mock(IndexAccessCostEstimation.class);
    Mockito.when(estimation.estimateIndexAccessCost(Mockito.anyString(), Mockito.anySetOf(DBIndex.class))).thenReturn(10.0);
    return estimation;  //tocode
  }

  private static DatabaseConnection configureConnection(DBIndex index) throws Exception {
    final Set<DBIndex> configuration = Sets.newHashSet();
    configuration.add(index);
    IndexExtractor extractor = Mockito.mock(IndexExtractor.class);
    Mockito.when(extractor.recommendIndexes(Mockito.anyString())).thenReturn(configuration);
    DatabaseConnection connection = Mockito.mock(DatabaseConnection.class);
    Mockito.when(connection.getIndexExtractor()).thenReturn(extractor);
    final Connection jdbcConnection = Mockito.mock(Connection.class);
    Mockito.when(connection.getJdbcConnection()).thenReturn(jdbcConnection);
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

  private static InumSpace configureInumSpace(Set<DBIndex> key) throws Exception {
    final InumSpace inumSpace = Mockito.mock(InumSpace.class);
    final Set<OptimalPlan> plans = configureSingleOptimalPlan();
    Mockito.when(inumSpace.getAllSavedOptimalPlans()).thenReturn(plans);
    Mockito.when(inumSpace.save(key, plans)).thenReturn(plans);
    final Set<Set<DBIndex>> ios = Sets.newHashSet();
    ios.add(key);
    Mockito.when(inumSpace.getAllInterestingOrders()).thenReturn(ios);
    return inumSpace;
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
