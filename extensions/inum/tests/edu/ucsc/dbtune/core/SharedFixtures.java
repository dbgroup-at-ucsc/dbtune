package edu.ucsc.dbtune.core;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import edu.ucsc.dbtune.inum.IndexAccessCostEstimation;
import edu.ucsc.dbtune.inum.InterestingOrdersExtractor;
import edu.ucsc.dbtune.inum.Inum;
import edu.ucsc.dbtune.inum.InumSpace;
import edu.ucsc.dbtune.inum.MatchingStrategy;
import edu.ucsc.dbtune.inum.OptimalPlan;
import edu.ucsc.dbtune.inum.Precomputation;
import edu.ucsc.dbtune.util.Strings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import org.mockito.Mockito;

/**
 * Contains a set of tests fixtures that can be shared among all inum tests.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public final class SharedFixtures {
  private SharedFixtures(){}

  public static DatabaseConnection configureConnection(DBIndex index) throws Exception {
    final Set<DBIndex> configuration = Sets.newHashSet();
    configuration.add(index);
    IndexExtractor extractor = Mockito.mock(IndexExtractor.class);
    Mockito.when(extractor.recommendIndexes(Mockito.anyString())).thenReturn(configuration);
    DatabaseConnection connection = Mockito.mock(DatabaseConnection.class);
    Connection jdbcConnection = configureJdbcConnection();
    Mockito.when(connection.getIndexExtractor()).thenReturn(extractor);
    Mockito.when(connection.getJdbcConnection()).thenReturn(jdbcConnection);
    Mockito.when(connection.isOpened()).thenReturn(true);
    Mockito.when(connection.isClosed()).thenReturn(false);
    return connection;
  }

  public static DatabaseConnection configureConnection() throws Exception {
    return configureConnection(configureIndex());
  }

  private static Connection configureJdbcConnection() throws Exception {
    final Connection jdbcConnection = Mockito.mock(Connection.class);
    final Statement  statement      = configureStatement();
    final PreparedStatement preparedStatement = configurePrepareStatement();
    Mockito.when(jdbcConnection.createStatement()).thenReturn(statement);
    Mockito.when(jdbcConnection.prepareStatement(Mockito.anyString())).thenReturn(preparedStatement);
    return jdbcConnection;
  }

  private static Statement configureStatement() throws Exception {
    final Statement statement = Mockito.mock(Statement.class);
    Mockito.when(statement.execute(Mockito.anyString())).thenReturn(true);
    final ResultSet mockResultSet = configureResultSet();
    Mockito.when(statement.executeQuery(Mockito.anyString())).thenReturn(mockResultSet);
    return statement;
  }

  private static PreparedStatement configurePrepareStatement() throws Exception {
    final PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
    Mockito.when(preparedStatement.execute(Mockito.anyString())).thenReturn(true);
    final ResultSet mockResultSet = configureResultSet();
    Mockito.when(preparedStatement.executeQuery()).thenReturn(mockResultSet);
    return preparedStatement;
  }

  private static ResultSet configureResultSet() throws Exception {
    return Mockito.mock(ResultSet.class);
  }

  public static DBIndex configureIndex(String text) {
    DBIndex soleOne = Mockito.mock(DBIndex.class);
    Mockito.when(soleOne.internalId()).thenReturn(1);
    Mockito.when(soleOne.creationCost()).thenReturn(2.0);
    if(!Strings.isEmpty(text)) Mockito.when(soleOne.creationText()).thenReturn(text);
    Mockito.when(soleOne.columnCount()).thenReturn(1);
    return soleOne;
  }

  public static DBIndex configureIndex() throws Exception {
    return configureIndex(null);
  }

  public static Inum configureInum() throws Exception {
    final DBIndex                     index          = configureIndex(null);
    final DatabaseConnection          connection     = configureConnection(index);
    final InumSpace inumSpace      = configureInumSpace(index);
    final Precomputation precomputation = configurePrecomputation(inumSpace);
    final MatchingStrategy matchingLogic  = configureMatchingLogic(inumSpace);
    final InterestingOrdersExtractor ioExtractor    = configureIOExtractor();

    return Inum.newInumInstance(connection, precomputation, matchingLogic, ioExtractor);
  }

  public static InterestingOrdersExtractor configureIOExtractor(){
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

  public static InumSpace configureInumSpace(DBIndex index) throws Exception {
    final InumSpace inumSpace = Mockito.mock(InumSpace.class);
    final Set<OptimalPlan> plans = configureSingleOptimalPlan();
    final Set<DBIndex> key = Sets.newHashSet();
    key.add(index);
    Mockito.when(inumSpace.getAllSavedOptimalPlans()).thenReturn(plans);
    Mockito.when(inumSpace.save(key, plans)).thenReturn(plans);
    return inumSpace;
  }

  public static MatchingStrategy configureMatchingLogic(InumSpace inumSpace) throws Exception {
    final MatchingStrategy matchingLogic = Mockito.mock(MatchingStrategy.class);
    final OptimalPlan plan = Lists.newArrayList(inumSpace.getAllSavedOptimalPlans()).get(0);
    Mockito.when(matchingLogic.matches(Mockito.eq(inumSpace), Mockito.anySetOf(DBIndex.class))).thenReturn(plan);
    final double cost = plan.getTotalCost();
    Mockito.when(matchingLogic.derivesCost(Mockito.anyString(), Mockito.eq(plan), Mockito.anySetOf(DBIndex.class))).thenReturn(cost);
    return matchingLogic;
  }

  public static Precomputation configurePrecomputation(InumSpace inumSpace) throws Exception {
    final Precomputation    setup     = Mockito.mock(Precomputation.class);
    Mockito.when(setup.getInumSpace()).thenReturn(inumSpace);
    final Set<OptimalPlan> plans = inumSpace.getAllSavedOptimalPlans();
    Mockito.when(setup.setup(Mockito.anyString(), Mockito.anySetOf(DBIndex.class))).thenReturn(plans);

    return setup;
  }

  public static Set<OptimalPlan> configureSingleOptimalPlan() throws Exception {
    final Set<OptimalPlan> singleOne = Sets.newHashSet();
    final OptimalPlan opp = Mockito.mock(OptimalPlan.class);
    Mockito.when(opp.getAccessCost(Mockito.anyString())).thenReturn(2.0);
    Mockito.when(opp.getInternalCost()).thenReturn(1.0);
    Mockito.when(opp.getTotalCost()).thenReturn(7.0);
    Mockito.when(opp.isDirty()).thenReturn(false);
    singleOne.add(opp);
    return singleOne;
  }

  public static InumSpace configureInumSpace(Set<DBIndex> key) throws Exception {
    final InumSpace inumSpace = Mockito.mock(InumSpace.class);
    final Set<OptimalPlan> plans = configureSingleOptimalPlan();
    Mockito.when(inumSpace.getAllSavedOptimalPlans()).thenReturn(plans);
    Mockito.when(inumSpace.save(key, plans)).thenReturn(plans);
    final Set<Set<DBIndex>> ios = Sets.newHashSet();
    ios.add(key);
    Mockito.when(inumSpace.getAllInterestingOrders()).thenReturn(ios);
    return inumSpace;
  }

  public static IndexAccessCostEstimation configureEstimator() {
    final IndexAccessCostEstimation estimation = Mockito.mock(IndexAccessCostEstimation.class);
    Mockito.when(estimation.estimateIndexAccessCost(Mockito.anyString(), Mockito.anySetOf(DBIndex.class))).thenReturn(10.0);
    return estimation;
  }

  public static Set<OptimalPlan> configureOptimalPlans() throws Exception {
    final OptimalPlan optimalPlan = Mockito.mock(OptimalPlan.class);
    return new HashSet<OptimalPlan>(){{add(optimalPlan);}};
  }

  public static Set<DBIndex> configureConfiguration() throws Exception {
    final DBIndex index = configureIndex();
    final Set<DBIndex> configurationOfOneIndex = Sets.newHashSet();
    configurationOfOneIndex.add(index);
    return configurationOfOneIndex;
  }
}
