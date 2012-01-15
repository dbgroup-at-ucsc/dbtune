package edu.ucsc.dbtune;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import static edu.ucsc.dbtune.SharedFixtures.NameGenerator.generateRandomName;
import edu.ucsc.dbtune.inum.IndexAccessCostEstimation;
import edu.ucsc.dbtune.inum.InterestingOrdersExtractor;
import edu.ucsc.dbtune.inum.Inum;
import edu.ucsc.dbtune.inum.InumSpace;
import edu.ucsc.dbtune.inum.InumWhatIfOptimizer;
import edu.ucsc.dbtune.inum.InumWhatIfOptimizerImpl;
import edu.ucsc.dbtune.inum.MatchingStrategy;
import edu.ucsc.dbtune.inum.OptimalPlan;
import edu.ucsc.dbtune.inum.Precomputation;
import edu.ucsc.dbtune.inum.QueryRecord;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import static edu.ucsc.dbtune.metadata.Index.CLUSTERED;
import static edu.ucsc.dbtune.metadata.Index.UNIQUE;
import static edu.ucsc.dbtune.metadata.SQLTypes.INTEGER;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.mockito.Mockito;

/**
 * Contains a set of tests fixtures that can be shared among all inum tests.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public final class SharedFixtures {
  private SharedFixtures() {
  }

  public static Connection configureConnection() throws Exception {
    return configureJdbcConnection();
  }

  private static Connection configureJdbcConnection() throws Exception {
    final Connection jdbcConnection = Mockito.mock(Connection.class);
    final Statement statement = configureStatement();
    final PreparedStatement preparedStatement = configurePrepareStatement();
    Mockito.when(jdbcConnection.createStatement()).thenReturn(statement);
    Mockito.when(jdbcConnection.prepareStatement(Mockito.anyString()))
        .thenReturn(preparedStatement);
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

  public static Inum configureInum() throws Exception {
    final Set<Index> configuration = configureConfiguration();
    return configureInum("SELECT * FROM TABLE1;", configuration);
  }

  public static Inum configureInum(String sql, Set<Index> configuration) throws Exception {
    final Connection connection = configureConnection();
    final InumSpace inumSpace = configureInumSpace(sql, configuration);
    final Precomputation precomputation = configurePrecomputation(inumSpace);
    final MatchingStrategy matchingLogic = configureMatchingLogic(inumSpace);
    final InterestingOrdersExtractor ioExtractor = configureIOExtractor(configuration);

    return Inum.newInumInstance(
        connection, precomputation, matchingLogic, ioExtractor);
  }

  public static InterestingOrdersExtractor configureIOExtractor(Set<Index> configuration) throws
      Exception {
    final InterestingOrdersExtractor extractor = Mockito.mock(InterestingOrdersExtractor.class);
    final Set<Index> indexes = new HashSet<Index>(configuration);
    Mockito.when(extractor.extractInterestingOrders(Mockito.anyString())).thenReturn(indexes);
    return extractor;
  }

  public static InterestingOrdersExtractor configureIOExtractor() throws Exception {
    final Set<Index> indexes = configureConfiguration(new Table(new Schema(new Catalog("testc"),
        "tests"), "test"), 3, 3);
    return configureIOExtractor(indexes);
  }

  public static MatchingStrategy configureMatchingLogic(InumSpace inumSpace) throws Exception {
    final MatchingStrategy matchingLogic = Mockito.mock(MatchingStrategy.class);
    final Set<OptimalPlan> plans = inumSpace.getAllSavedOptimalPlans();
    Mockito.when(matchingLogic.matches(Mockito.anyString(), Mockito.<Set<Index>>any(),
        Mockito.eq(inumSpace))).thenReturn(plans);
    final double cost = Lists.newArrayList(plans).get(0).getTotalCost();
    Mockito.when(matchingLogic.estimateCost(Mockito.anyString(), Mockito.<Set<Index>>any(),
        Mockito.eq(inumSpace))).thenReturn(cost);
    return matchingLogic;
  }

  public static Precomputation configurePrecomputation(InumSpace inumSpace) throws Exception {
    final Precomputation setup = Mockito.mock(Precomputation.class);
    Mockito.when(setup.getInumSpace()).thenReturn(inumSpace);
    Mockito.when(setup.setup(Mockito.anyString(), Mockito.<Set<Index>>any())).thenReturn(inumSpace);

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

  public static InumSpace configureInumSpace(String sql, Set<Index> config) throws Exception {
    final InumSpace inumSpace = Mockito.mock(InumSpace.class);
    final Set<OptimalPlan> plans = configureSingleOptimalPlan();
    Mockito.when(inumSpace.getAllSavedOptimalPlans()).thenReturn(plans);
    final QueryRecord key = new QueryRecord(sql, config);
    Mockito.when(inumSpace.save(key, plans)).thenReturn(inumSpace);
    Mockito.when(inumSpace.getOptimalPlans(Mockito.<QueryRecord>any())).thenReturn(plans);
    final Set<QueryRecord> ios = Sets.newHashSet();
    ios.add(key);
    Mockito.when(inumSpace.keySet()).thenReturn(ios);
    return inumSpace;
  }

  public static IndexAccessCostEstimation configureEstimator() {
    final IndexAccessCostEstimation estimation = Mockito.mock(IndexAccessCostEstimation.class);
    Mockito.when(estimation.estimateIndexAccessCost(Mockito.any(OptimalPlan.class),
        Mockito.<Set<Index>>any())).thenReturn(10.0);
    return estimation;
  }

  @SuppressWarnings("serial")
  public static Set<OptimalPlan> configureOptimalPlans() throws Exception {
    final OptimalPlan optimalPlan = Mockito.mock(OptimalPlan.class);
    return new HashSet<OptimalPlan>() {{add(optimalPlan);}};
  }

  public static Set<Index> configureConfiguration() throws Exception {
    return configureConfiguration(new Table(new Schema(new Catalog("testc"), "tests"), "test"), 1,
        2);
  }

  public static Set<Index> configureConfiguration(Table table, int noIndexes, int noColsPerIndex)
      throws Exception {
    final List<Column> cols = Lists.newArrayList();
    final List<Index> idxs = Lists.newArrayList();
    int i = 0;
    for (int idx = 0; idx < noIndexes; idx++) {
      for (int idx2 = 0; idx2 < noColsPerIndex; idx2++) {
        final Column col = new Column(table, generateRandomName(), INTEGER);
        cols.add(col);
      }
      idxs.add(new Index("idx_" + i, cols, idx == 0, CLUSTERED, UNIQUE));
      i++;
      cols.clear();
    }

    return new HashSet<Index>(idxs);
  }

  public static InumWhatIfOptimizer configureWhatIfOptimizer() throws Exception {
    final Inum inum = configureInum();
    return new InumWhatIfOptimizerImpl(inum);
  }

  public static InumWhatIfOptimizer configureWhatIfOptimizer(String sql, Set<Index> configuration)
      throws
      Exception {
    final Inum inum = configureInum(sql, configuration);
    return new InumWhatIfOptimizerImpl(inum);
  }

  static class CharacterFrequency {
    char character;
    float frequency;

    CharacterFrequency(char character, float frequency) {
      this.character = character;
      this.frequency = frequency;
    }
  }

  public static class NameGenerator {
    static CharacterFrequency[] frequencies = new CharacterFrequency[] {
        new CharacterFrequency('a', 0.8f),
        new CharacterFrequency('c', 0.4f),
        new CharacterFrequency('g', 0.2f),
        new CharacterFrequency('t', 0.6f)
    };

    public static String generateRandomName() {
      return generateName(frequencies.length);
    }

    public static String generateName(int noCharacters) {
      final StringBuilder name = new StringBuilder();
      for (int idx = 0; idx < noCharacters; idx++) {
        name.append(getRandomCharacter());
      }
      return name.toString();
    }

    private static char getRandomCharacter() {
      final float v = (float) Math.random();

      char c = frequencies[0].character;
      float f = frequencies[0].frequency;

      for (CharacterFrequency eachCharFreq : frequencies) {
        if (v < eachCharFreq.frequency && eachCharFreq.frequency < f) {
          c = eachCharFreq.character;
          f = eachCharFreq.frequency;
        }
      }

      return c;
    }
  }
}

