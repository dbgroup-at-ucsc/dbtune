package edu.ucsc.dbtune;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.BitArraySet;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.Identifiable;

import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.metadata.Index.ASCENDING;
import static edu.ucsc.dbtune.metadata.Index.NON_UNIQUE;
import static edu.ucsc.dbtune.metadata.Index.SECONDARY;
import static edu.ucsc.dbtune.metadata.Index.UNCLUSTERED;
import static edu.ucsc.dbtune.util.Environment.extractDriver;
import static edu.ucsc.dbtune.util.EnvironmentProperties.DBMS;
import static edu.ucsc.dbtune.util.EnvironmentProperties.IBG;
import static edu.ucsc.dbtune.util.EnvironmentProperties.INUM;
import static edu.ucsc.dbtune.util.EnvironmentProperties.JDBC_URL;
import static edu.ucsc.dbtune.util.EnvironmentProperties.OPTIMIZER;
import static edu.ucsc.dbtune.util.EnvironmentProperties.PASSWORD;
import static edu.ucsc.dbtune.util.EnvironmentProperties.SUPPORTED_OPTIMIZERS;
import static edu.ucsc.dbtune.util.EnvironmentProperties.USERNAME;

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
        switch(new Random().nextInt(3)) {
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
        cfg.setProperty(OPTIMIZER, IBG);
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
        cfg.setProperty(OPTIMIZER, INUM);
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
        return configureCatalog(2, 3, 4);
    }
    

    /**
     * Create a catalog with the specified characteristics of Schema information.
     * 
     * @param numSchema
     *      Number of schema
     * @param numTables
     *      Number of relations in each schema
     * @param numIndexes
     *      Number of indexes (equivalent to number of columns) in each relation per schema
     * @return
     *      Catalog instance
     * 
     * @throws SQLException
     */
    public static Catalog configureCatalog(int numSchema, int numTables, int numIndexes)
    {
        Catalog catalog = new Catalog("catalog_0");

        try {
            for (int j = 0; j < numSchema; j++) {
                Schema schema = new Schema(catalog, "schema_" + j);
                int counter = 0;
                for (int k = 0; k < numTables; k++) {
                    Table table = new Table(schema, "table_" + k);
                    for (int l = 0; l < numIndexes; l++) {
                        Column column = new Column(table, "column_" + l, l + 1);
                        new Index(
                                table.getName() + "_index_" + counter++,
                                column, ASCENDING, SECONDARY, NON_UNIQUE, UNCLUSTERED);
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
     * @param cat
     *      the catalog from which the graph is constructed
     * @return
     *      the configured graph
     */
    public static IndexBenefitGraph configureIndexBenefitGraph(Catalog cat)
    {
        List<Index> indexes = cat.schemas().get(0).indexes();

        IndexBenefitGraph.Node root;
        IndexBenefitGraph.Node node1;
        IndexBenefitGraph.Node node2;
        IndexBenefitGraph.Node node3;
        IndexBenefitGraph.Node node4;
        IndexBenefitGraph.Node node5;
        IndexBenefitGraph.Node node6;
        IndexBenefitGraph.Node node7;
        IndexBenefitGraph.Node.Child child1;
        IndexBenefitGraph.Node.Child child2;
        IndexBenefitGraph.Node.Child child3;
        IndexBenefitGraph.Node.Child child4;
        IndexBenefitGraph.Node.Child child5;
        IndexBenefitGraph.Node.Child child6;
        IndexBenefitGraph.Node.Child child7;
        IndexBenefitGraph.Node.Child child8;
        Set<Index> rootibs;
        Set<Index> ibs;
        Set<Index> ibs1;
        Set<Index> ibs2;
        Set<Index> ibs3;
        Set<Index> ibs4;
        Set<Index> ibs5;
        Set<Index> ibs6;
        Set<Index> ibs7;

        rootibs = new BitArraySet<Index>();
        ibs     = new BitArraySet<Index>();
        ibs1    = new BitArraySet<Index>();
        ibs2    = new BitArraySet<Index>();
        ibs3    = new BitArraySet<Index>();
        ibs4    = new BitArraySet<Index>();
        ibs5    = new BitArraySet<Index>();
        ibs6    = new BitArraySet<Index>();
        ibs7    = new BitArraySet<Index>();

        rootibs.add(indexes.get(0));
        rootibs.add(indexes.get(1));
        rootibs.add(indexes.get(2));
        rootibs.add(indexes.get(3));

        ibs1.add(indexes.get(0));
        ibs1.add(indexes.get(1));
        ibs1.add(indexes.get(2));

        ibs2.add(indexes.get(1));
        ibs2.add(indexes.get(2));
        ibs2.add(indexes.get(3));

        ibs3.add(indexes.get(0));
        ibs3.add(indexes.get(2));

        ibs4.add(indexes.get(1));
        ibs4.add(indexes.get(2));

        ibs5.add(indexes.get(2));
        ibs5.add(indexes.get(3));

        ibs6.add(indexes.get(2));

        ibs7.add(indexes.get(3));

        root  = new IndexBenefitGraph.Node(rootibs, 0);
        node1 = new IndexBenefitGraph.Node(ibs1, 1);
        node2 = new IndexBenefitGraph.Node(ibs2, 2);
        node3 = new IndexBenefitGraph.Node(ibs3, 3);
        node4 = new IndexBenefitGraph.Node(ibs4, 4);
        node5 = new IndexBenefitGraph.Node(ibs5, 5);
        node6 = new IndexBenefitGraph.Node(ibs6, 6);
        node7 = new IndexBenefitGraph.Node(ibs7, 7);

        child1 = new IndexBenefitGraph.Node.Child(node1, indexes.get(3));
        child2 = new IndexBenefitGraph.Node.Child(node2, indexes.get(0));
        child3 = new IndexBenefitGraph.Node.Child(node3, indexes.get(1));
        child4 = new IndexBenefitGraph.Node.Child(node4, indexes.get(0));
        child5 = new IndexBenefitGraph.Node.Child(node5, indexes.get(1));
        child6 = new IndexBenefitGraph.Node.Child(node6, indexes.get(1));
        child7 = new IndexBenefitGraph.Node.Child(node6, indexes.get(3));
        child8 = new IndexBenefitGraph.Node.Child(node7, indexes.get(2));

        root.expand(0, child1);
        child1.getNode().expand(0, child3);
        child2.getNode().expand(0, child5);
        child4.getNode().expand(0, child6);
        child5.getNode().expand(0, child7);

        child1.setNext(child2);
        child3.setNext(child4);
        child7.setNext(child8);

        root.setCost(20);
        node1.setCost(45);
        node2.setCost(50);
        node3.setCost(80);
        node4.setCost(50);
        node5.setCost(65);
        node6.setCost(80);
        node7.setCost(80);

        // all indexes in ibg

        ibs.add(indexes.get(0));
        ibs.add(indexes.get(1));
        ibs.add(indexes.get(2));
        ibs.add(indexes.get(3));

        return new IndexBenefitGraph(root, 80);
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

        BitArraySet<Index> e = new BitArraySet<Index>();
        BitArraySet<Index> a = new BitArraySet<Index>();
        BitArraySet<Index> b = new BitArraySet<Index>();
        BitArraySet<Index> c = new BitArraySet<Index>();
        BitArraySet<Index> d = new BitArraySet<Index>();
        BitArraySet<Index> ab = new BitArraySet<Index>();
        BitArraySet<Index> ac = new BitArraySet<Index>();
        BitArraySet<Index> ad = new BitArraySet<Index>();
        BitArraySet<Index> bc = new BitArraySet<Index>();
        BitArraySet<Index> bd = new BitArraySet<Index>();
        BitArraySet<Index> cd = new BitArraySet<Index>();
        BitArraySet<Index> abc = new BitArraySet<Index>();
        BitArraySet<Index> acd = new BitArraySet<Index>();
        BitArraySet<Index> bcd = new BitArraySet<Index>();
        BitArraySet<Index> abcd = new BitArraySet<Index>();

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
    public static Set<Identifiable> configureBitArraySetOfIdentifiables(int numOfElements)
    {
        Set<Identifiable> bas = new BitArraySet<Identifiable>();

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
            public Object answer(InvocationOnMock aInvocation) throws Throwable {
                return currentIndex.incrementAndGet() < rows.length;
            }
        });

        final ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
        Answer rowLookupAnswer = new Answer() {
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

        return result;
    }
    //CHECKSTYLE:ON
}
