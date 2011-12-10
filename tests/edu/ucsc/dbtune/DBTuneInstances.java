package edu.ucsc.dbtune;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;

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
public class DBTuneInstances
{
    protected static final String DB_URL     = "nothing.com";
    protected static final String DB_USR     = "neo";
    protected static final String DB_SCH     = "superSchema";
    protected static final String DB_PWD     = "password";
    protected static final String DB_TBL     = "R";
    protected static final String DB_CREATOR = "123";

    /**
     * Utility class
     */
    private DBTuneInstances(){}

    /**
     * Returns a configuration for DB2
     */
    public static Environment configureDB2()
    {
        Environment cfg = new Environment(configureUser());

        cfg.setProperty(JDBC_URL, "jdbc:db2://" + DB_URL );
        try{
            extractDriver(cfg);
        } catch(SQLException ex) {
            throw new RuntimeException(ex);
        }

        return cfg;
    }

    /**
     * Returns a configuration for an inexistent DBMS
     */
    public static Environment configureInexistentDBMS()
    {
        Environment cfg = new Environment(configureUser());

        cfg.setProperty(JDBC_URL, "jdbc:superdbms//" + DB_URL);
        try{
            extractDriver(cfg);
        } catch(SQLException ex) {
            throw new RuntimeException(ex);
        }

        return cfg;
    }

    /**
     * Returns a configuration for postgres
     */
    public static Environment configureMySQL()
    {
        Environment cfg = new Environment(configureUser());

        cfg.setProperty(JDBC_URL, "jdbc:mysql://" + DB_URL );
        try{
            extractDriver(cfg);
        } catch(SQLException ex) {
            throw new RuntimeException(ex);
        }

        return cfg;
    }

    /**
     * Returns a configuration for postgres
     */
    public static Environment configurePG()
    {
        Environment cfg = new Environment(configureUser());

        cfg.setProperty(JDBC_URL, "jdbc:postgresql://" + DB_URL );
        try{
            extractDriver(cfg);
        } catch(SQLException ex) {
            throw new RuntimeException(ex);
        }

        return cfg;
    }

    /**
     * Returns a configuration with generic connectivity settings
     */
    public static Properties configureUser()
    {
        Properties cfg = new Properties();

        cfg.setProperty(USERNAME, DB_USR);
        cfg.setProperty(PASSWORD, DB_PWD);

        return cfg;
    }

    /**
     * Returns a configuration with random dbms properties
     */
    public static Environment configureAny()
    {
        switch(new Random().nextInt(3))
        {
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
     * Returns a configuration with generic connectivity settings
     */
    public static Environment configureDBMSOptimizer(Environment cfg)
    {
        cfg.setProperty(OPTIMIZER, DBMS);
        return cfg;
    }

    /**
     * Returns a configuration with generic connectivity settings
     */
    public static Environment configureIBGOptimizer(Environment cfg)
    {
        cfg.setProperty(OPTIMIZER, IBG);
        return cfg;
    }

    /**
     * Returns a configuration with generic connectivity settings
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

        for(String optId : SUPPORTED_OPTIMIZERS) {

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

        for(int j = 0; j < 2; j++) {
            Schema schema = new Schema(catalog,"schema_" + j);
            int counter = 0;
            for(int k = 0; k < 3; k++) {
                Table table = new Table(schema,"table_" + k);
                for(int l = 0; l < 4; l++) {
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
        IBGNode root;
        IBGNode node1;
        IBGNode node2;
        IBGNode node3;
        IBGNode node4;
        IBGNode node5;
        IBGNode node6;
        IBGNode node7;
        IBGNode.IBGChild child1;
        IBGNode.IBGChild child2;
        IBGNode.IBGChild child3;
        IBGNode.IBGChild child4;
        IBGNode.IBGChild child5;
        IBGNode.IBGChild child6;
        IBGNode.IBGChild child7;
        IBGNode.IBGChild child8;
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

        rootibs.set(0);
        rootibs.set(1);
        rootibs.set(2);
        rootibs.set(3);

        ibs1.set(0);
        ibs1.set(1);
        ibs1.set(2);

        ibs2.set(1);
        ibs2.set(2);
        ibs2.set(3);

        ibs3.set(0);
        ibs3.set(2);

        ibs4.set(1);
        ibs4.set(2);

        ibs5.set(2);
        ibs5.set(3);

        ibs6.set(2);

        ibs7.set(3);

        root  = new IBGNode(rootibs, 0);
        node1 = new IBGNode(ibs1, 1);
        node2 = new IBGNode(ibs2, 2);
        node3 = new IBGNode(ibs3, 3);
        node4 = new IBGNode(ibs4, 4);
        node5 = new IBGNode(ibs5, 5);
        node6 = new IBGNode(ibs6, 6);
        node7 = new IBGNode(ibs7, 7);

        child1 = new IBGNode.IBGChild(node1, 3);
        child2 = new IBGNode.IBGChild(node2, 0);
        child3 = new IBGNode.IBGChild(node3, 1);
        child4 = new IBGNode.IBGChild(node4, 0);
        child5 = new IBGNode.IBGChild(node5, 1);
        child6 = new IBGNode.IBGChild(node6, 1);
        child7 = new IBGNode.IBGChild(node6, 3);
        child8 = new IBGNode.IBGChild(node7, 2);

        root.expand(0, child1);
        child1.node.expand(0, child3);
        child2.node.expand(0, child5);
        child4.node.expand(0, child6);
        child5.node.expand(0, child7);

        child1.next = child2;
        child3.next = child4;
        child7.next = child8;

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
        ibs.set(0);
        ibs.set(1);
        ibs.set(2);
        ibs.set(3);

        return new IndexBenefitGraph(root, 80, ibs);
    }
}
