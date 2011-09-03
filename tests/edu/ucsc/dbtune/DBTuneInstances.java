package edu.ucsc.dbtune;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.metadata.PGIndex;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.spi.Environment;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.Instances;

import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.DatabaseSystem.getSupportedOptimizers;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.*;

/**
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 */
public class DBTuneInstances {
    protected static final String DB_DB      = "jdbc:unknown://nothing.com";
    protected static final String DB_URL     = "superDB";
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
    public static Properties configureDB2()
    {
        Properties cfg = new Properties(configureGeneric());

        cfg.setProperty(JDBC_DRIVER, DB2);

        return cfg;
    }

    /**
     * Returns a configuration for an inexistent DBMS
     */
    public static Properties configureInexistentDBMS()
    {
        Properties cfg = new Properties(configureGeneric());

        cfg.setProperty(JDBC_DRIVER, "edu.ucsc.dbtune.superduperdbms");

        return cfg;
    }

    /**
     * Returns a configuration for postgres
     */
    public static Properties configureMySQL()
    {
        Properties cfg = new Properties(configureGeneric());

        cfg.setProperty(JDBC_DRIVER, MYSQL);

        return cfg;
    }

    /**
     * Returns a configuration for postgres
     */
    public static Properties configurePG()
    {
        Properties cfg = new Properties(configureGeneric());

        cfg.setProperty(JDBC_DRIVER, PG);

        return cfg;
    }

    /**
     * Returns a configuration with generic connectivity settings
     */
    public static Properties configureGeneric()
    {
        Properties cfg = new Properties();

        cfg.setProperty(URL,      DB_URL);
        cfg.setProperty(USERNAME, DB_USR);
        cfg.setProperty(PASSWORD, DB_PWD);

        return cfg;
    }

    /**
     * Returns a configuration with random dbms properties
     */
    public static Properties configureAny()
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
            return configureGeneric();
        }
    }

    /**
     * Returns a configuration with generic connectivity settings
     */
    public static Properties configureDBMSOptimizer(Properties cfg)
    {
        cfg.setProperty(OPTIMIZER, DBMS);
        return cfg;
    }

    /**
     * Returns a configuration with generic connectivity settings
     */
    public static Properties configureIBGOptimizer(Properties cfg)
    {
        cfg.setProperty(OPTIMIZER, IBG);
        return cfg;
    }

    /**
     * Returns a configuration with generic connectivity settings
     */
    public static Properties configureINUMOptimizer(Properties cfg)
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
     * @see DatabaseSystem#getSupportedOptimizers
     */
    public static Iterable<Optimizer> getSupportedOptimizersIterator(Environment env)
        throws SQLException
    {
        List<Optimizer> opts;
        Properties      conf;

        opts = new ArrayList<Optimizer>();

        for(String optId : getSupportedOptimizers()) {

            conf = env.getAll();

            conf.setProperty(OPTIMIZER,optId);

            opts.add(newDatabaseSystem(new Environment(conf)).getOptimizer());
        }

        return opts;
    }

    public static IndexBenefitGraph.IBGNode makeRandomIBGNode()
    {
        return makeIBGNode(new Random().nextInt());
    }

    public static IndexBenefitGraph.IBGNode makeIBGNode(int id){
        try {
            final Constructor<IndexBenefitGraph.IBGNode> c = IndexBenefitGraph.IBGNode.class.getDeclaredConstructor(IndexBitSet.class, int.class);
            c.setAccessible(true);
            return c.newInstance(new IndexBitSet(), id);
        } catch (Exception e) {
            throw new IllegalStateException("ERROR: unable to construct an IBGNode");
        }
    }

    public static Index newPGIndex(int indexId, int schemaId, List<Column> cols, List<Boolean> desc) throws Exception {
        return new PGIndex(new Table(new Schema(new Catalog("catalog"),"schema"),"table"),"index_"+new Random().nextInt(),schemaId, true, cols, desc, indexId, 3.0, 4.5, "Create");
    }

    public static List<Boolean> generateDescVals(int howmany){
        final List<Boolean> cols = Instances.newList();
        for(int idx = 0; idx < howmany; idx++){
            cols.add(true);
        }
        return cols;
    }
}
