package edu.ucsc.dbtune;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph.Node;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.IndexBitSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.Environment.extractDriver;
import static edu.ucsc.dbtune.util.EnvironmentProperties.DBMS;
import static edu.ucsc.dbtune.util.EnvironmentProperties.IBG;
import static edu.ucsc.dbtune.util.EnvironmentProperties.INUM;
import static edu.ucsc.dbtune.util.EnvironmentProperties.JDBC_URL;
import static edu.ucsc.dbtune.util.EnvironmentProperties.OPTIMIZER;
import static edu.ucsc.dbtune.util.EnvironmentProperties.PASSWORD;
import static edu.ucsc.dbtune.util.EnvironmentProperties.SUPPORTED_OPTIMIZERS;
import static edu.ucsc.dbtune.util.EnvironmentProperties.USERNAME;

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

        cfg.setProperty(JDBC_URL, "jdbc:db2://" + DB_URL );

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
     */
    public static Iterable<Optimizer> getSupportedOptimizersIterator(Environment env)
        throws SQLException
    {
        List<Optimizer> opts;
        Environment     conf;

        opts = new ArrayList<Optimizer>();

        for (String optId : SUPPORTED_OPTIMIZERS) {

            conf = new Environment(env);

            conf.setProperty(OPTIMIZER,optId);

            opts.add(newDatabaseSystem(new Environment(conf)).getOptimizer());
        }

        return opts;
    }

    /**
     * Creates a catalog with 2 schemas, 3 tables per schema and 4 columns and 4 indexes per table
     */
    public static Catalog configureCatalog() throws SQLException
    {
        Catalog catalog = new Catalog("catalog_0");

        for (int j = 0; j < 2; j++) {
            Schema schema = new Schema(catalog,"schema_" + j);
            int counter = 0;
            for (int k = 0; k < 3; k++) {
                Table table = new Table(schema,"table_" + k);
                for (int l = 0; l < 4; l++) {
                    Column column = new Column(table,"column_" + l, l+1);

                    new Index(table.getName() + "_index_" + counter++, column);
                }
            }
        }

        return catalog;
    }

    /**
     * Creates the IBG that is used as sample in {@link IndexBenefitGraph}.
     */
    public static IndexBenefitGraph configureIndexBenefitGraph()
    {
        Node root;
        Node node1;
        Node node2;
        Node node3;
        Node node4;
        Node node5;
        Node node6;
        Node node7;
        Node.Child child1;
        Node.Child child2;
        Node.Child child3;
        Node.Child child4;
        Node.Child child5;
        Node.Child child6;
        Node.Child child7;
        Node.Child child8;
        IndexBitSet rootibs;
        IndexBitSet ibs1;
        IndexBitSet ibs2;
        IndexBitSet ibs3;
        IndexBitSet ibs4;
        IndexBitSet ibs5;
        IndexBitSet ibs6;
        IndexBitSet ibs7;

        rootibs = new IndexBitSet();
        ibs1    = new IndexBitSet();
        ibs2    = new IndexBitSet();
        ibs3    = new IndexBitSet();
        ibs4    = new IndexBitSet();
        ibs5    = new IndexBitSet();
        ibs6    = new IndexBitSet();
        ibs7    = new IndexBitSet();

        rootibs.add(0);
        rootibs.add(1);
        rootibs.add(2);
        rootibs.add(3);

        ibs1.add(0);
        ibs1.add(1);
        ibs1.add(2);

        ibs2.add(1);
        ibs2.add(2);
        ibs2.add(3);

        ibs3.add(0);
        ibs3.add(2);

        ibs4.add(1);
        ibs4.add(2);

        ibs5.add(2);
        ibs5.add(3);

        ibs6.add(2);

        ibs7.add(3);

        root  = new Node(rootibs, 0);
        node1 = new Node(ibs1, 1);
        node2 = new Node(ibs2, 2);
        node3 = new Node(ibs3, 3);
        node4 = new Node(ibs4, 4);
        node5 = new Node(ibs5, 5);
        node6 = new Node(ibs6, 6);
        node7 = new Node(ibs7, 7);

        child1 = new Node.Child(node1, 3);
        child2 = new Node.Child(node2, 0);
        child3 = new Node.Child(node3, 1);
        child4 = new Node.Child(node4, 0);
        child5 = new Node.Child(node5, 1);
        child6 = new Node.Child(node6, 1);
        child7 = new Node.Child(node6, 3);
        child8 = new Node.Child(node7, 2);

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

        IndexBitSet ibs = new IndexBitSet();
        ibs.add(0);
        ibs.add(1);
        ibs.add(2);
        ibs.add(3);

        return new IndexBenefitGraph(root, 80, ibs);
    }

    /**
     * @return
     *     a list with all the subsets of the set (a,b,c,d), where the positions of the IndexBitSet 
     *     objects are a=0, b=1, c=2, d=3
     */
    public static List<IndexBitSet> configurePowerSet()
    {
        List<IndexBitSet> list = new ArrayList<IndexBitSet>();
        IndexBitSet e = new IndexBitSet();
        IndexBitSet a = new IndexBitSet();
        IndexBitSet b = new IndexBitSet();
        IndexBitSet c = new IndexBitSet();
        IndexBitSet d = new IndexBitSet();
        IndexBitSet ab = new IndexBitSet();
        IndexBitSet ac = new IndexBitSet();
        IndexBitSet ad = new IndexBitSet();
        IndexBitSet bc = new IndexBitSet();
        IndexBitSet bd = new IndexBitSet();
        IndexBitSet cd = new IndexBitSet();
        IndexBitSet abc = new IndexBitSet();
        IndexBitSet acd = new IndexBitSet();
        IndexBitSet bcd = new IndexBitSet();
        IndexBitSet abcd = new IndexBitSet();

        a.add(0);

        b.add(1);

        c.add(2);

        d.add(3);
    
        ab.add(0);
        ab.add(1);

        ac.add(0);
        ac.add(2);

        ad.add(0);
        ad.add(3);

        bc.add(1);
        bc.add(2);

        bd.add(1);
        bd.add(3);

        cd.add(2);
        cd.add(3);

        abc.add(0);
        abc.add(1);
        abc.add(2);

        acd.add(0);
        acd.add(2);
        acd.add(3);

        bcd.add(1);
        bcd.add(2);
        bcd.add(3);

        abcd.add(0);
        abcd.add(1);
        abcd.add(2);
        abcd.add(3);

        list.add(e);
        list.add(a);
        list.add(b);
        list.add(c);
        list.add(d);
        list.add(ab);
        list.add(ac);
        list.add(ad);
        list.add(bc);
        list.add(bd);
        list.add(cd);
        list.add(abc);
        list.add(acd);
        list.add(bcd);
        list.add(abcd);

        return list;
    }
}
