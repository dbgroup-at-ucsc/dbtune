package edu.ucsc.dbtune;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.extraction.MetadataExtractor;
import edu.ucsc.dbtune.metadata.extraction.DB2Extractor;
import edu.ucsc.dbtune.metadata.extraction.MySQLExtractor;
import edu.ucsc.dbtune.metadata.extraction.PGExtractor;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.IBGOptimizer;
import edu.ucsc.dbtune.optimizer.MySQLOptimizer;
import edu.ucsc.dbtune.optimizer.PGOptimizer;
import edu.ucsc.dbtune.util.Environment;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.util.Properties;

import static edu.ucsc.dbtune.util.Environment.extractDriver;
import static edu.ucsc.dbtune.util.EnvironmentProperties.DB2;
import static edu.ucsc.dbtune.util.EnvironmentProperties.DBMS;
import static edu.ucsc.dbtune.util.EnvironmentProperties.IBG;
import static edu.ucsc.dbtune.util.EnvironmentProperties.INUM;
import static edu.ucsc.dbtune.util.EnvironmentProperties.MYSQL;
import static edu.ucsc.dbtune.util.EnvironmentProperties.PG;

/**
 * Represents a DBMS system. The class `DatabaseSystem` represents a DBMS and it's the main entry 
 * point to the API. The class is in charge of creating/providing with DBMS-specific objects. In 
 * general, its responsibilities are:
 * <p>
 * <ol>
 *   <li>Create DBMS-specific {@link Connection} objects.</li>
 *   <li>Provide with the proper {@link Optimizer} and {@link Catalog} objects.</li>
 *   <li>Create DBMS-specific {@link Index} objects.</li>
 * </ol>
 * <p>
 * <b>Note:</b>There's a single connection used to communicate with the system. The user is 
 * responsible for closing it.
 *
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 */
public class DatabaseSystem
{
    private Connection connection;
    private Optimizer  optimizer;
    private Catalog    catalog;

    /**
     * Creates a database system instance with the given elements. This effectively acts as a 
     * sort-of database system "trait".
     *
     * @param connection
     *     a JDBC connection
     * @param extractor
     *     a metadata extractor
     * @param optimizer
     *     an optimizer
     * @see Connection
     * @see Catalog
     * @see Optimizer
     */
    protected DatabaseSystem(
            Connection connection,
            MetadataExtractor extractor,
            Optimizer optimizer)
        throws SQLException
    {
        this.connection = connection;
        this.catalog    = extractor.extract(connection);
        this.optimizer  = optimizer;
    }

    /**
     * Returns the corresponding optimizer.
     *
     * @return
     *      an optimizer.
     * @see Optimizer
     */
    public Optimizer getOptimizer()
    {
        return optimizer;
    }

    /**
     * Returns the corresponding catalog
     *
     * @return
     *      a metadata extractor
     */
    public Catalog getCatalog()
    {
        return catalog;
    }

    /**
     * Returns the connection to the DBMS. The user is responsible for closing it.
     *
     * @return
     *      a connection
     * @see Connection
     */
    public Connection getConnection() throws SQLException {
        return connection;
    }

    /**
     * Creates and opens a connection to the DBMS. The caller is responsible for closing it.
     *
     * @return
     *      a connection
     * @see Connection
     */
    public static Connection newConnection(Environment env) throws SQLException
    {
        String url = env.getJdbcURL();
        String usr = env.getUsername();
        String pwd = env.getPassword();
        
        return DriverManager.getConnection(url,usr,pwd);
    }

    /**
     * Returns the corresponding metadata extractor.
     *
     * @return
     *      a metadata extractor
     */
    public static MetadataExtractor newExtractor(Environment env) throws SQLException
    {
        MetadataExtractor extractor = null;

        validate(env);

        if(env.getVendor().equals(MYSQL))
            extractor = new MySQLExtractor();
        else if(env.getVendor().equals(DB2))
            extractor = new DB2Extractor();
        else if(env.getVendor().equals(PG))
            extractor = new PGExtractor();
        else
            throw new SQLException("Unable to create an extractor for " + env.getVendor());

        return extractor;
    }

    /**
     * Returns the corresponding optimizer.
     *
     * @return
     *      an optimizer.
     * @see Optimizer
     */
    public static Optimizer newOptimizer(Environment env, Connection con) throws SQLException
    {
        Optimizer optimizer;

        validate(env);

        if(env.getVendor().equals(MYSQL))
            optimizer = new MySQLOptimizer(con);
        else if(env.getVendor().equals(DB2))
            optimizer = new DB2Optimizer(con);
        else if(env.getVendor().equals(PG))
            optimizer = new PGOptimizer(con);
        else
            throw new SQLException("Unable to create an optimizer interface for " + env.getVendor());

        if(env.getOptimizer().equals(DBMS))
            return optimizer;
        else if(env.getOptimizer().equals(IBG))
            return new IBGOptimizer(optimizer);
        else if(env.getOptimizer().equals(INUM))
            throw new SQLException("INUMOptimizer doesn't exist yet");
        else
            throw new SQLException("Unknown optimizer option: " + env.getOptimizer());
    }

    /**
     * Validates an {@link Environment} to be used by this class.
     *
     * @throws SQLException
     *     if {@link Environment#getVendor()}, {@link Environment#getJdbcURL}, {@link 
     *     Environment#getOptimizer} are null.
     */
    private static void validate(Environment env) throws SQLException
    {
        if(env.getJdbcURL() != null)
            extractDriver(env);

        if(env.getVendor() == null || env.getJdbcURL() == null ||
           env.getJdbcDriver() == null || env.getOptimizer() == null ||
           env.getUsername() == null || env.getPassword() == null )
            throw new SQLException("Missing a property");
    }

    /**
     * Creates a database system instance with the given properties. This effectively acts as a 
     * factory method that takes the description of a system along with connectivity information and 
     * creates a {@link Connection}, {@link Catalog} and {@link Optimizer} objects of the 
     * corresponding type, with the appropriate members. 
     *
     * @param properties
     *     used to access the properties of the system
     */
    public static DatabaseSystem newDatabaseSystem(Properties properties) throws SQLException
    {
        return Factory.newDatabaseSystem(new Environment(properties));
    }

    /**
     * Creates a database system instance with the given properties. This effectively acts as a 
     * factory method that takes the description of a system along with connectivity information and 
     * creates a {@link Connection}, {@link Catalog} and {@link Optimizer} objects of the 
     * corresponding type, with the appropriate members. 
     *
     * @param env
     *     an environment object used to access the properties of the system
     */
    public static DatabaseSystem newDatabaseSystem(Environment env) throws SQLException
    {
        return Factory.newDatabaseSystem(env);
    }

    /**
     * Creates a database system instance with the default properties from {@link Environment}.
     */
    public static DatabaseSystem newDatabaseSystem() throws SQLException
    {
        return Factory.newDatabaseSystem(Environment.getInstance());
    }

    /**
     * Class defined just to aid in testing. This class shouldn't be used by any client, only by {@code 
     * edu.ucsc.dbtune.DatabaseSystemTest}
     */
    protected static class Factory
    {
        /**
         * Creates a database system instance with the given properties. This effectively acts as a 
         * factory method that takes the description of a system along with connectivity information 
         * and creates a {@link Connection}, {@link Catalog} and {@link Optimizer} objects of the 
         * corresponding type, with the appropriate members. 
         *
         * @param env
         *     an environment object used to access the properties of the system
         */
        public static DatabaseSystem newDatabaseSystem(Environment env) throws SQLException
        {
            Connection        connection;
            Optimizer         optimizer;
            MetadataExtractor extractor;
            DatabaseSystem    db;

            validate(env);

            try {
                Class.forName(env.getJdbcDriver());
            } catch(Exception e) {
                throw new SQLException(e);
            }

            connection = newConnection(env);
            extractor  = newExtractor(env);
            optimizer  = newOptimizer(env, connection);

            db = new DatabaseSystem(connection, extractor, optimizer);

            optimizer.setCatalog(db.getCatalog());

            return db;
        }
    }
}
