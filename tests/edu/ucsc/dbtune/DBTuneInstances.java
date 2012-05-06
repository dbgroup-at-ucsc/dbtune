package edu.ucsc.dbtune;

import java.io.StringReader;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.PGOptimizer;

import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.Identifiable;

import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.Environment.extractDriver;
import static edu.ucsc.dbtune.util.EnvironmentProperties.DBMS;
import static edu.ucsc.dbtune.util.EnvironmentProperties.EXHAUSTIVE;
import static edu.ucsc.dbtune.util.EnvironmentProperties.GREEDY;
import static edu.ucsc.dbtune.util.EnvironmentProperties.IBG;
import static edu.ucsc.dbtune.util.EnvironmentProperties.INUM;
import static edu.ucsc.dbtune.util.EnvironmentProperties.INUM_MATCHING_STRATEGY;
import static edu.ucsc.dbtune.util.EnvironmentProperties.INUM_SLOT_CACHE;
import static edu.ucsc.dbtune.util.EnvironmentProperties.INUM_SPACE_COMPUTATION;
import static edu.ucsc.dbtune.util.EnvironmentProperties.JDBC_URL;
import static edu.ucsc.dbtune.util.EnvironmentProperties.OPTIMIZER;
import static edu.ucsc.dbtune.util.EnvironmentProperties.PASSWORD;
import static edu.ucsc.dbtune.util.EnvironmentProperties.SUPPORTED_OPTIMIZERS;
import static edu.ucsc.dbtune.util.EnvironmentProperties.USERNAME;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 */
public final class DBTuneInstances
{
    protected static final String DB_URL     = "nothing.com";
    protected static final String DB_USR     = "neo";
    protected static final String DB_SCH     = "superSchema";
    protected static final String DB_PWD     = "password";
    protected static final String DB_TBL     = "R";
    protected static final String DB_CREATOR = "123";
    protected static final Random R          = new Random();

    /**
     * Utility class.
     */
    private DBTuneInstances()
    {
    }

    /**
     * Returns a configuration for DB2.
     *
     * @return
     *      a configured environment
     */
    public static Environment configureDB2()
    {
        Environment cfg = new Environment(configureUser());

        cfg.setProperty(JDBC_URL, "jdbc:db2://" + DB_URL);

        try {
            extractDriver(cfg);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        return cfg;
    }

    /**
     * Returns a configuration for an inexistent DBMS.
     *
     * @return
     *      a configured environment
     */
    public static Environment configureInexistentDBMS()
    {
        Environment cfg = new Environment(configureUser());

        cfg.setProperty(JDBC_URL, "jdbc:superdbms//" + DB_URL);

        try {
            extractDriver(cfg);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        return cfg;
    }

    /**
     * Returns a configuration for postgres.
     *
     * @return
     *      a configured environment
     */
    public static Environment configureMySQL()
    {
        Environment cfg = new Environment(configureUser());

        cfg.setProperty(JDBC_URL, "jdbc:mysql://" + DB_URL);

        try {
            extractDriver(cfg);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        return cfg;
    }

    /**
     * Returns a configuration for postgres.
     *
     * @return
     *      a configured environment
     */
    public static Environment configurePG()
    {
        Environment cfg = new Environment(configureUser());

        cfg.setProperty(JDBC_URL, "jdbc:postgresql://" + DB_URL);

        try {
            extractDriver(cfg);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        return cfg;
    }

    /**
     * Returns a configuration with generic connectivity settings.
     *
     * @return
     *      a configured environment
     */
    public static Properties configureUser()
    {
        Properties cfg = new Properties();

        cfg.setProperty(USERNAME, DB_USR);
        cfg.setProperty(PASSWORD, DB_PWD);

        return cfg;
    }

    /**
     * Returns a configuration with random dbms properties.
     *
     * @return
     *      a configured environment
     */
    public static Environment configureAny()
    {
        switch(R.nextInt(3)) {
            case 0:
                return configureDB2();
            case 1:
                return configureMySQL();
            case 2:
                return configurePG();
            default:
                return configurePG();
        }
    }

    /**
     * Returns a configuration with generic connectivity settings.
     *
     * @param cfg
     *      the configuration of the environment
     * @return
     *      a configured environment
     */
    public static Environment configureDBMSOptimizer(Environment cfg)
    {
        cfg.setProperty(OPTIMIZER, DBMS);
        return cfg;
    }

    /**
     * Returns a configuration with generic connectivity settings.
     *
     * @param cfg
     *      the configuration of the environment
     * @return
     *      a configured environment
     */
    public static Environment configureIBGOptimizer(Environment cfg)
    {
        if (cfg.getProperty(OPTIMIZER) == null || cfg.getProperty(OPTIMIZER).equals(""))
            cfg.setProperty(OPTIMIZER, IBG);
        else
            cfg.setProperty(OPTIMIZER, cfg.getProperty(OPTIMIZER) + "," + IBG);

        return cfg;
    }

    /**
     * Returns a configuration with generic connectivity settings.
     *
     * @param cfg
     *      the configuration of the environment
     * @return
     *      a configured environment
     */
    public static Environment configureINUMOptimizer(Environment cfg)
    {
        if (cfg.getProperty(OPTIMIZER) == null || cfg.getProperty(OPTIMIZER).equals(""))
            cfg.setProperty(OPTIMIZER, INUM);
        else
            cfg.setProperty(OPTIMIZER, cfg.getProperty(OPTIMIZER) + "," + INUM);

        cfg.setProperty(INUM_SLOT_CACHE, R.nextInt(2) == 0 ? "OFF" : "ON");
        cfg.setProperty(INUM_SPACE_COMPUTATION, R.nextInt(2) == 0 ? EXHAUSTIVE : IBG);
        cfg.setProperty(INUM_MATCHING_STRATEGY, R.nextInt(2) == 0 ? GREEDY : EXHAUSTIVE);
        return cfg;
    }

    /**
     * Returns an iterator containing all the supported {@link Optimizer} implementations.
     *
     * <b>Important:</b> This method assumes that {@link java.sql.DriverManager} and the 
     * corresponding {@link java.sql.Connection} objects that {@link 
     * DatabaseSystem#newDatabaseSystem} uses in the creation of new {@link DatabaseSystem} objects 
     * have been mocked appropriately. Failure to comply with this will cause unexpected results 
     * like having resources being hanged (when connections aren't properly closed).
     *
     * @see EnvironmentProperties#SUPPORTED_OPTIMIZERS
     * @param env
     *      the environment
     * @return
     *      the list of available optimizers
     * @throws SQLException
     *      if a new database system can't be constructed
     */
    public static Iterable<Optimizer> getSupportedOptimizersIterator(Environment env)
        throws SQLException
    {
        List<Optimizer> opts;
        Environment     conf;

        opts = new ArrayList<Optimizer>();

        for (String optId : SUPPORTED_OPTIMIZERS) {

            conf = new Environment(env);

            conf.setProperty(OPTIMIZER, optId);

            opts.add(newDatabaseSystem(new Environment(conf)).getOptimizer());
        }

        return opts;
    }

    /**
     * Creates a catalog with 2 schemas, 3 tables per schema and 4 columns and 4 indexes per table.
     *
     * @return
     *      a configured catalog
     */    
    public static Catalog configureCatalog()
    {
        return configureCatalog(2, 3, 4, true);
    }

    /**
     * Creates a catalog with 2 schemas, 3 tables per schema and 4 columns and 4 indexes per table.
     *
     * @return
     *      a configured catalog
     */    
    public static Catalog configureCatalogWithoutIndexes()
    {
        return configureCatalog(2, 3, 4, false);
    }

    /**
     * Create a catalog with the specified characteristics of Schema information.
     * 
     * @param numSchema
     *      Number of schema
     * @param numTables
     *      Number of relations in each schema
     * @param numColumns
     *      Number of columns in each table
     * @param buildIndexes
     *      whether to build indexes
     * @return
     *      Catalog instance
     * 
     * @throws SQLException
     */
    public static Catalog configureCatalog(
            int numSchema, int numTables, int numColumns, boolean buildIndexes)
    {
        Catalog catalog = new Catalog("catalog_0");

        try {
            for (int j = 0; j < numSchema; j++) {
                Schema schema = new Schema(catalog, "schema_" + j);
                int counter = 0;
                for (int k = 0; k < numTables; k++) {
                    Table table = new Table(schema, "table_" + k);

                    for (int l = 0; l < numColumns; l++)
                        new Column(table, "column_" + l, l + 1);

                    if (!buildIndexes)
                        continue;
                    for (Set<Column> cols : Sets.powerSet(new HashSet<Column>(table.columns()))) {
                        if (cols.size() == 0)
                            continue;
                        new Index(
                            table.getName() + "_index_" + counter++, new ArrayList<Column>(cols));
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


        return catalog;
    }

    /**
     * Creates the IBG that is used as sample in {@link IndexBenefitGraph}.
     *
     * @param indexes
     *      map of indexes
     * @return
     *      the configured graph
     */
    public static IndexBenefitGraph configureIndexBenefitGraph(Map<String, Set<Index>> indexes)
    {
        Set<Index> abcd = indexes.get("abcd");
        Set<Index> abc = indexes.get("abc");
        Set<Index> bcd = indexes.get("bcd");
        Set<Index> ac = indexes.get("ac");
        Set<Index> bc = indexes.get("bc");
        Set<Index> cd = indexes.get("cd");
        Set<Index> a = indexes.get("a");
        Set<Index> b = indexes.get("b");
        Set<Index> c = indexes.get("c");
        Set<Index> d = indexes.get("d");

        IndexBenefitGraph.Node abcdNode  = new IndexBenefitGraph.Node(abcd, 0);
        IndexBenefitGraph.Node abcNode = new IndexBenefitGraph.Node(abc, 1);
        IndexBenefitGraph.Node bcdNode = new IndexBenefitGraph.Node(bcd, 2);
        IndexBenefitGraph.Node acNode = new IndexBenefitGraph.Node(ac, 3);
        IndexBenefitGraph.Node bcNode = new IndexBenefitGraph.Node(bc, 4);
        IndexBenefitGraph.Node cdNode = new IndexBenefitGraph.Node(cd, 5);
        IndexBenefitGraph.Node cNode = new IndexBenefitGraph.Node(c, 6);
        IndexBenefitGraph.Node dNode = new IndexBenefitGraph.Node(d, 7);

        abcdNode.addChild(abcNode, Iterables.get(d, 0));
        abcdNode.addChild(bcdNode, Iterables.get(a, 0));
        abcNode.addChild(acNode, Iterables.get(b, 0));
        abcNode.addChild(bcNode, Iterables.get(a, 0));
        bcdNode.addChild(cdNode, Iterables.get(b, 0));
        bcNode.addChild(cNode, Iterables.get(c, 0));
        cdNode.addChild(dNode, Iterables.get(c, 0));
        cdNode.addChild(cNode, Iterables.get(d, 0));

        abcdNode.setCost(20);
        abcNode.setCost(45);
        bcdNode.setCost(50);
        acNode.setCost(80);
        bcNode.setCost(50);
        cdNode.setCost(65);
        cNode.setCost(80);
        dNode.setCost(80);

        return new IndexBenefitGraph(abcdNode, 80);
    }

    /**
     * @param cat
     *      the catalog from which the graph is constructed
     * @return
     *     a list with all the subsets of the set (a,b,c,d), where the positions of the IndexBitSet 
     *     objects are a=0, b=1, c=2, d=3
     */
    public static Map<String, Set<Index>> configurePowerSet(Catalog cat)
    {
        Map<String, Set<Index>> map = new HashMap<String, Set<Index>>();
        List<Index> indexes = cat.schemas().get(0).indexes();

        Set<Index> e = new HashSet<Index>();
        Set<Index> a = new HashSet<Index>();
        Set<Index> b = new HashSet<Index>();
        Set<Index> c = new HashSet<Index>();
        Set<Index> d = new HashSet<Index>();
        Set<Index> ab = new HashSet<Index>();
        Set<Index> ac = new HashSet<Index>();
        Set<Index> ad = new HashSet<Index>();
        Set<Index> bc = new HashSet<Index>();
        Set<Index> bd = new HashSet<Index>();
        Set<Index> cd = new HashSet<Index>();
        Set<Index> abc = new HashSet<Index>();
        Set<Index> acd = new HashSet<Index>();
        Set<Index> bcd = new HashSet<Index>();
        Set<Index> abcd = new HashSet<Index>();

        a.add(indexes.get(0));

        b.add(indexes.get(1));

        c.add(indexes.get(2));

        d.add(indexes.get(3));
    
        ab.add(indexes.get(0));
        ab.add(indexes.get(1));

        ac.add(indexes.get(0));
        ac.add(indexes.get(2));

        ad.add(indexes.get(0));
        ad.add(indexes.get(3));

        bc.add(indexes.get(1));
        bc.add(indexes.get(2));

        bd.add(indexes.get(1));
        bd.add(indexes.get(3));

        cd.add(indexes.get(2));
        cd.add(indexes.get(3));

        abc.add(indexes.get(0));
        abc.add(indexes.get(1));
        abc.add(indexes.get(2));

        acd.add(indexes.get(0));
        acd.add(indexes.get(2));
        acd.add(indexes.get(3));

        bcd.add(indexes.get(1));
        bcd.add(indexes.get(2));
        bcd.add(indexes.get(3));

        abcd.add(indexes.get(0));
        abcd.add(indexes.get(1));
        abcd.add(indexes.get(2));
        abcd.add(indexes.get(3));

        map.put("empty", e);
        map.put("a", a);
        map.put("b", b);
        map.put("c", c);
        map.put("d", d);
        map.put("ab", ab);
        map.put("ac", ac);
        map.put("ad", ad);
        map.put("bc", bc);
        map.put("bd", bd);
        map.put("cd", cd);
        map.put("abc", abc);
        map.put("acd", acd);
        map.put("bcd", bcd);
        map.put("abcd", abcd);

        return map;
    }

    /**
     * @param numOfElements
     *      number of elements in the list
     * @return
     *      a list of identifiable integers. The list contains no duplicates
     */
    public static List<Identifiable> configureListOfIdentifiables(int numOfElements)
    {
        List<Identifiable> list = new ArrayList<Identifiable>();

        for (int i = 0; i < numOfElements; i++) {
            list.add(new DBTuneInstances.TrivialIdentifiable(i));
        }

        return list;
    }

    /**
     * @param numOfElements
     *      number of elements in the list
     * @return
     *      a list of identifiable integers
     */
    public static Set<Identifiable> configureHashSetOfIdentifiables(int numOfElements)
    {
        Set<Identifiable> bas = new HashSet<Identifiable>();

        for (int i = 0; i < numOfElements; i++) {
            bas.add(new DBTuneInstances.TrivialIdentifiable(i));
        }

        return bas;
    }

    /**
     * Basic identifiable class.
     *
     * @author Ivo Jimenez
     */
    private static class TrivialIdentifiable implements Identifiable, 
            Comparable<TrivialIdentifiable>
    {
        private Integer i;

        /**
         * constructor.
         *
         * @param i
         *      the integer value.
         */
        public TrivialIdentifiable(int i)
        {
            this.i = new Integer(i);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getId()
        {
            return i;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int compareTo(TrivialIdentifiable o)
        {
            if (!(o instanceof TrivialIdentifiable))
                return 0;

            return ((TrivialIdentifiable) o).i.compareTo(i);
        }
    }

    /**
     * Returns 10 plans that query the TPCDS database.
     *
     * @param sch
     *      schema the is referenced in the queries
     * @return
     *      list with 3 plans
     * @throws Exception
     *      if fails
     * @see #PLANS_JSON
     */
    public static List<SQLStatementPlan> configurePlans(Schema sch) throws Exception
    {
        List<SQLStatementPlan> plans = new ArrayList<SQLStatementPlan>();

        for (String jsonPlan : PLANS_JSON.split("QUERYPLAN"))
            plans.add(PGOptimizer.parseJSON(new StringReader(jsonPlan), null));

        return plans;
    }

    //CHECKSTYLE:OFF
    /**
     * Taken from: http://bit.ly/vR8ujB
     */
    @SuppressWarnings("rawtypes")
    public static ResultSet makeResultSet(
            final List<String> aColumns, final List... rows)
        throws Exception
    {
        ResultSet result = mock(ResultSet.class);
        final AtomicInteger currentIndex = new AtomicInteger(-1);

        when(result.next()).thenAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock aInvocation) throws Throwable {
                return currentIndex.incrementAndGet() < rows.length;
            }
        });

        final ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
        Answer rowLookupAnswer = new Answer() {
            @Override
            public Object answer(InvocationOnMock aInvocation) throws Throwable {
                currentIndex.get();
                int columnIndex = aColumns.indexOf(argument.getValue());
                return rows[currentIndex.get()].get(columnIndex);
            }
        };
        when(result.getString(argument.capture())).thenAnswer(rowLookupAnswer);
        when(result.getShort(argument.capture())).thenAnswer(rowLookupAnswer);
        when(result.getDate(argument.capture())).thenAnswer(rowLookupAnswer);
        when(result.getInt(argument.capture())).thenAnswer(rowLookupAnswer);
        when(result.getLong(argument.capture())).thenAnswer(rowLookupAnswer);
        when(result.getDouble(argument.capture())).thenAnswer(rowLookupAnswer);
        when(result.getTimestamp(argument.capture())).thenAnswer(rowLookupAnswer);
        when(result.getRow()).thenAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock aInvocation) throws Throwable {
                 return currentIndex.get() - 1;
            }
        });
        when(result.last()).thenAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock aInvocation) throws Throwable {
                 currentIndex.set(rows.length);
                 return true;
            }
        });
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock aInvocation) throws Throwable {
                 currentIndex.set(-1);
                 return null;
            }
        }).when(result).beforeFirst();
        when(result.first()).thenAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock aInvocation) throws Throwable {
                 currentIndex.set(0);
                 return true;
            }
        });

        return result;
    }

    private static final String PLANS_JSON =
        " [                                                                                         " +
        "   {                                                                                       " +
        "     \"Plan\": {                                                                             " +
        "       \"Node Type\": \"Sort\",                                                                " +
        "       \"Startup Cost\": 15.00,                                                              " +
        "       \"Total Cost\": 15.01,                                                                " +
        "       \"Plan Rows\": 2,                                                                     " +
        "       \"Plan Width\": 8,                                                                    " +
        "       \"Output\": [\"(count(*))\", \"column_1\", \"column_2\"],                                   " +
        "       \"Sort Key\": [\"table_0.column_1\", \"table_0.column_2\"],                               " +
        "       \"Plans\": [                                                                          " +
        "         {                                                                                 " +
        "           \"Node Type\": \"Aggregate\",                                                       " +
        "           \"Strategy\": \"Hashed\",                                                           " +
        "           \"Parent Relationship\": \"Outer\",                                                 " +
        "           \"Startup Cost\": 14.97,                                                          " +
        "           \"Total Cost\": 14.99,                                                            " +
        "           \"Plan Rows\": 2,                                                                 " +
        "           \"Plan Width\": 8,                                                                " +
        "           \"Output\": [\"count(*)\", \"column_1\", \"column_2\"],                                 " +
        "           \"Plans\": [                                                                      " +
        "             {                                                                             " +
        "               \"Node Type\": \"Bitmap Heap Scan\",                                            " +
        "               \"Parent Relationship\": \"Outer\",                                             " +
        "               \"Relation Name\": \"table_0\",                                                 " +
        "               \"Schema\": \"schema_0\",                                                       " +
        "               \"Alias\": \"table_0\",                                                         " +
        "               \"Startup Cost\": 4.34,                                                       " +
        "               \"Total Cost\": 14.91,                                                        " +
        "               \"Plan Rows\": 9,                                                             " +
        "               \"Plan Width\": 8,                                                            " +
        "               \"Output\": [\"column_0\", \"column_1\", \"column_2\", \"column_3\"],                 " +
        "               \"Recheck Cond\": \"((table_0.column_0 >= 10) AND (table_0.column_0 < 2000))\", " +
        "               \"Plans\": [                                                                  " +
        "                 {                                                                         " +
        "                   \"Node Type\": \"Bitmap Index Scan\",                                       " +
        "                   \"Parent Relationship\": \"Outer\",                                         " +
        "                   \"Index Name\": \"table_0_index_7\",                                        " +
        "                   \"Startup Cost\": 0.00,                                                   " +
        "                   \"Total Cost\": 4.34,                                                     " +
        "                   \"Plan Rows\": 9,                                                         " +
        "                   \"Plan Width\": 0,                                                        " +
        "                   \"Index Cond\": \"((table_0.column_0 >= 10) AND (table_0.column_0 < 2000))\"" +
        "                 }                                                                         " +
        "               ]                                                                           " +
        "             }                                                                             " +
        "           ]                                                                               " +
        "         }                                                                                 " +
        "       ]                                                                                   " +
        "     }                                                                                     " +
        "   }                                                                                       " +
        " ] " +
        "QUERYPLAN " +
        " [                                                                                                                            " +
        "   {                                                                                                                          " +
        "     \"Plan\": {                                                                                                                " +
        "       \"Node Type\": \"Group\",                                                                                                  " +
        "       \"Startup Cost\": 61.03,                                                                                                 " +
        "       \"Total Cost\": 61.61,                                                                                                   " +
        "       \"Plan Rows\": 78,                                                                                                       " +
        "       \"Plan Width\": 8,                                                                                                       " +
        "       \"Output\": [\"table_0.column_2\", \"table_0.column_3\"],                                                                    " +
        "       \"Plans\": [                                                                                                             " +
        "         {                                                                                                                    " +
        "           \"Node Type\": \"Sort\",                                                                                               " +
        "           \"Parent Relationship\": \"Outer\",                                                                                    " +
        "           \"Startup Cost\": 61.03,                                                                                             " +
        "           \"Total Cost\": 61.22,                                                                                               " +
        "           \"Plan Rows\": 78,                                                                                                   " +
        "           \"Plan Width\": 8,                                                                                                   " +
        "           \"Output\": [\"table_0.column_2\", \"table_0.column_3\"],                                                                " +
        "           \"Sort Key\": [\"table_0.column_2\", \"table_0.column_3\"],                                                              " +
        "           \"Plans\": [                                                                                                         " +
        "             {                                                                                                                " +
        "               \"Node Type\": \"Hash Join\",                                                                                      " +
        "               \"Parent Relationship\": \"Outer\",                                                                                " +
        "               \"Join Type\": \"Inner\",                                                                                          " +
        "               \"Startup Cost\": 23.44,                                                                                         " +
        "               \"Total Cost\": 58.57,                                                                                           " +
        "               \"Plan Rows\": 78,                                                                                               " +
        "               \"Plan Width\": 8,                                                                                               " +
        "               \"Output\": [\"table_0.column_2\", \"table_0.column_3\"],                                                            " +
        "               \"Hash Cond\": \"(table_1.column_0 = table_0.column_0)\",                                                          " +
        "               \"Plans\": [                                                                                                     " +
        "                 {                                                                                                            " +
        "                   \"Node Type\": \"Seq Scan\",                                                                                   " +
        "                   \"Parent Relationship\": \"Outer\",                                                                            " +
        "                   \"Relation Name\": \"table_1\",                                                                                " +
        "                   \"Schema\": \"schema_0\",                                                                                      " +
        "                   \"Alias\": \"table_1\",                                                                                        " +
        "                   \"Startup Cost\": 0.00,                                                                                      " +
        "                   \"Total Cost\": 27.70,                                                                                       " +
        "                   \"Plan Rows\": 1770,                                                                                         " +
        "                   \"Plan Width\": 4,                                                                                           " +
        "                   \"Output\": [\"table_1.column_0\", \"table_1.column_1\", \"table_1.column_2\", \"table_1.column_3\"]                 " +
        "                 },                                                                                                           " +
        "                 {                                                                                                            " +
        "                   \"Node Type\": \"Hash\",                                                                                       " +
        "                   \"Parent Relationship\": \"Inner\",                                                                            " +
        "                   \"Startup Cost\": 23.32,                                                                                     " +
        "                   \"Total Cost\": 23.32,                                                                                       " +
        "                   \"Plan Rows\": 9,                                                                                            " +
        "                   \"Plan Width\": 16,                                                                                          " +
        "                   \"Output\": [\"table_0.column_2\", \"table_0.column_3\", \"table_0.column_0\", \"table_2.column_0\"],                " +
        "                   \"Plans\": [                                                                                                 " +
        "                     {                                                                                                        " +
        "                       \"Node Type\": \"Nested Loop\",                                                                            " +
        "                       \"Parent Relationship\": \"Outer\",                                                                        " +
        "                       \"Join Type\": \"Inner\",                                                                                  " +
        "                       \"Startup Cost\": 4.32,                                                                                  " +
        "                       \"Total Cost\": 23.32,                                                                                   " +
        "                       \"Plan Rows\": 9,                                                                                        " +
        "                       \"Plan Width\": 16,                                                                                      " +
        "                       \"Output\": [\"table_0.column_2\", \"table_0.column_3\", \"table_0.column_0\", \"table_2.column_0\"],            " +
        "                       \"Plans\": [                                                                                             " +
        "                         {                                                                                                    " +
        "                           \"Node Type\": \"Index Scan\",                                                                         " +
        "                           \"Parent Relationship\": \"Outer\",                                                                    " +
        "                           \"Scan Direction\": \"Forward\",                                                                       " +
        "                           \"Index Name\": \"table_2_index_43\",                                                                  " +
        "                           \"Relation Name\": \"table_2\",                                                                        " +
        "                           \"Schema\": \"schema_0\",                                                                              " +
        "                           \"Alias\": \"table_2\",                                                                                " +
        "                           \"Startup Cost\": 0.00,                                                                              " +
        "                           \"Total Cost\": 8.37,                                                                                " +
        "                           \"Plan Rows\": 1,                                                                                    " +
        "                           \"Plan Width\": 4,                                                                                   " +
        "                           \"Output\": [\"table_2.column_0\", \"table_2.column_1\", \"table_2.column_2\", \"table_2.column_3\"],        " +
        "                           \"Index Cond\": \"((table_2.column_0 >= 10) AND (table_2.column_0 < 2000) AND (table_2.column_2 = 3))\"" +
        "                         },                                                                                                   " +
        "                         {                                                                                                    " +
        "                           \"Node Type\": \"Bitmap Heap Scan\",                                                                   " +
        "                           \"Parent Relationship\": \"Inner\",                                                                    " +
        "                           \"Relation Name\": \"table_0\",                                                                        " +
        "                           \"Schema\": \"schema_0\",                                                                              " +
        "                           \"Alias\": \"table_0\",                                                                                " +
        "                           \"Startup Cost\": 4.32,                                                                              " +
        "                           \"Total Cost\": 14.84,                                                                               " +
        "                           \"Plan Rows\": 9,                                                                                    " +
        "                           \"Plan Width\": 12,                                                                                  " +
        "                           \"Output\": [\"table_0.column_0\", \"table_0.column_1\", \"table_0.column_2\", \"table_0.column_3\"],        " +
        "                           \"Recheck Cond\": \"(table_0.column_0 = table_2.column_0)\",                                           " +
        "                           \"Plans\": [                                                                                         " +
        "                             {                                                                                                " +
        "                               \"Node Type\": \"Bitmap Index Scan\",                                                              " +
        "                               \"Parent Relationship\": \"Outer\",                                                                " +
        "                               \"Index Name\": \"table_0_index_7\",                                                               " +
        "                               \"Startup Cost\": 0.00,                                                                          " +
        "                               \"Total Cost\": 4.32,                                                                            " +
        "                               \"Plan Rows\": 9,                                                                                " +
        "                               \"Plan Width\": 0,                                                                               " +
        "                               \"Index Cond\": \"(table_0.column_0 = table_2.column_0)\"                                          " +
        "                             }                                                                                                " +
        "                           ]                                                                                                  " +
        "                         }                                                                                                    " +
        "                       ]                                                                                                      " +
        "                     }                                                                                                        " +
        "                   ]                                                                                                          " +
        "                 }                                                                                                            " +
        "               ]                                                                                                              " +
        "             }                                                                                                                " +
        "           ]                                                                                                                  " +
        "         }                                                                                                                    " +
        "       ]                                                                                                                      " +
        "     }                                                                                                                        " +
        "   }                                                                                                                          " +
        " ] " +
        "QUERYPLAN " +
        " [                                                                                                            " +
        "   {                                                                                                          " +
        "     \"Plan\": {                                                                                                " +
        "       \"Node Type\": \"Sort\",                                                                                   " +
        "       \"Startup Cost\": 18.55,                                                                                 " +
        "       \"Total Cost\": 18.56,                                                                                   " +
        "       \"Plan Rows\": 2,                                                                                        " +
        "       \"Plan Width\": 4,                                                                                       " +
        "       \"Output\": [\"(count(*))\", \"table_1.column_2\"],                                                          " +
        "       \"Sort Key\": [\"table_1.column_2\"],                                                                      " +
        "       \"Plans\": [                                                                                             " +
        "         {                                                                                                    " +
        "           \"Node Type\": \"Seq Scan\",                                                                           " +
        "           \"Parent Relationship\": \"InitPlan\",                                                                 " +
        "           \"Subplan Name\": \"InitPlan 1 (returns $0)\",                                                         " +
        "           \"Relation Name\": \"table_2\",                                                                        " +
        "           \"Schema\": \"schema_0\",                                                                              " +
        "           \"Alias\": \"table_2\",                                                                                " +
        "           \"Startup Cost\": 0.00,                                                                              " +
        "           \"Total Cost\": 32.12,                                                                               " +
        "           \"Plan Rows\": 9,                                                                                    " +
        "           \"Plan Width\": 8,                                                                                   " +
        "           \"Filter\": \"(table_2.column_0 = table_2.column_2)\"                                                  " +
        "         },                                                                                                   " +
        "         {                                                                                                    " +
        "           \"Node Type\": \"Aggregate\",                                                                          " +
        "           \"Strategy\": \"Hashed\",                                                                              " +
        "           \"Parent Relationship\": \"Outer\",                                                                    " +
        "           \"Startup Cost\": 14.95,                                                                             " +
        "           \"Total Cost\": 14.97,                                                                               " +
        "           \"Plan Rows\": 2,                                                                                    " +
        "           \"Plan Width\": 4,                                                                                   " +
        "           \"Output\": [\"count(*)\", \"table_1.column_2\"],                                                        " +
        "           \"Plans\": [                                                                                         " +
        "             {                                                                                                " +
        "               \"Node Type\": \"Result\",                                                                         " +
        "               \"Parent Relationship\": \"Outer\",                                                                " +
        "               \"Startup Cost\": 4.34,                                                                          " +
        "               \"Total Cost\": 14.91,                                                                           " +
        "               \"Plan Rows\": 9,                                                                                " +
        "               \"Plan Width\": 4,                                                                               " +
        "               \"Output\": [\"table_1.column_0\", \"table_1.column_1\", \"table_1.column_2\", \"table_1.column_3\"],    " +
        "               \"One-Time Filter\": \"$0\",                                                                       " +
        "               \"Plans\": [                                                                                     " +
        "                 {                                                                                            " +
        "                   \"Node Type\": \"Bitmap Heap Scan\",                                                           " +
        "                   \"Parent Relationship\": \"Outer\",                                                            " +
        "                   \"Relation Name\": \"table_1\",                                                                " +
        "                   \"Schema\": \"schema_0\",                                                                      " +
        "                   \"Alias\": \"table_1\",                                                                        " +
        "                   \"Startup Cost\": 4.34,                                                                      " +
        "                   \"Total Cost\": 14.91,                                                                       " +
        "                   \"Plan Rows\": 9,                                                                            " +
        "                   \"Plan Width\": 4,                                                                           " +
        "                   \"Output\": [\"table_1.column_0\", \"table_1.column_1\", \"table_1.column_2\", \"table_1.column_3\"]," +
        "                   \"Recheck Cond\": \"((table_1.column_0 >= 50000) AND (table_1.column_0 < 100000))\",           " +
        "                   \"Plans\": [                                                                                 " +
        "                     {                                                                                        " +
        "                       \"Node Type\": \"Bitmap Index Scan\",                                                      " +
        "                       \"Parent Relationship\": \"Outer\",                                                        " +
        "                       \"Index Name\": \"table_1_index_26\",                                                      " +
        "                       \"Startup Cost\": 0.00,                                                                  " +
        "                       \"Total Cost\": 4.34,                                                                    " +
        "                       \"Plan Rows\": 9,                                                                        " +
        "                       \"Plan Width\": 0,                                                                       " +
        "                       \"Index Cond\": \"((table_1.column_0 >= 50000) AND (table_1.column_0 < 100000))\"          " +
        "                     }                                                                                        " +
        "                   ]                                                                                          " +
        "                 }                                                                                            " +
        "               ]                                                                                              " +
        "             }                                                                                                " +
        "           ]                                                                                                  " +
        "         }                                                                                                    " +
        "       ]                                                                                                      " +
        "     }                                                                                                        " +
        "   }                                                                                                          " +
        " ]";

    //CHECKSTYLE:ON
}
