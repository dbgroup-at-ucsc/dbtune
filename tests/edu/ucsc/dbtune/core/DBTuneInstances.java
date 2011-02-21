package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.core.metadata.*;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.Instances;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static edu.ucsc.dbtune.core.JdbcMocks.*;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class DBTuneInstances {
    public static final String DB_NAME         = "superDB";
    public static final String SCHEMA_NAME     = "superSchema";
    public static final String TABLE_NAME      = "R";
    public static final String TABLE_CREATOR   = "123";

    private DBTuneInstances(){}

    public static IndexBenefitGraph.IBGNode makeRandomIBGNode(){
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

    public static JdbcConnectionFactory newJdbcConnectionFactory(){
        return new JdbcConnectionFactory(){
            @Override
            public Connection makeConnection(String url,
                                             String driverClass, String username,
                                             String password, boolean autoCommit
            ) throws SQLException {
                final JdbcMocks.MockConnection conn = new JdbcMocks.MockConnection();
                conn.register(
                        makeMockStatement(true, true, conn),
                        makeMockPreparedStatement(true, true, conn)
                );




                return conn;
            }
        };
    }

    @SuppressWarnings({"RedundantTypeArguments"})
    public static ConnectionManager newDB2DatabaseConnectionManager(){
        try {
            return DBTuneInstances.newDatabaseConnectionManager(newDB2Properties()
            );
        } catch (Exception e) {
            throw new AssertionError("unable to create ConnectionManager");
        }
    }

    public static Properties newDB2Properties() {
        return new Properties(){
            {
                setProperty(JdbcConnectionManager.URL, "");
                setProperty(JdbcConnectionManager.USERNAME, "newo");
                setProperty(JdbcConnectionManager.PASSWORD, "hahaha");
                setProperty(JdbcConnectionManager.DATABASE, "matrix");
                setProperty(JdbcConnectionManager.DRIVER, "com.ibm.db2.jcc.DB2Driver");
            }
        };
    }

    public static ConnectionManager newPGDatabaseConnectionManager(){
        try {
            return DBTuneInstances.newDatabaseConnectionManager(newPGSQLProperties()
            );
        } catch (Exception e) {
            throw new AssertionError("unable to create ConnectionManager");
        }
    }

    public static Properties newPGSQLProperties() {
        return new Properties(){{
            setProperty(JdbcConnectionManager.URL, "");
            setProperty(JdbcConnectionManager.USERNAME, "newo");
            setProperty(JdbcConnectionManager.PASSWORD, "hahaha");
            setProperty(JdbcConnectionManager.DATABASE, "matrix");
            setProperty(JdbcConnectionManager.DRIVER, "org.postgresql.Driver");
        }};
    }

    public static PGIndex newPGIndex(int indexId, int schemaId, List<DatabaseColumn> cols, List<Boolean> desc){
        return new PGIndex(new PGIndexSchema(schemaId, true, cols, desc), indexId, 3.0, 4.5, "Create");
    }

    public static List<DatabaseColumn> generateColumns(int howmany){
        final List<DatabaseColumn> cols = Instances.newList();
        for(int idx = 0; idx < howmany; idx++){
            cols.add(new PGColumn(idx));
        }
        return cols;
    }

    public static List<Boolean> generateDescVals(int howmany){
        final List<Boolean> cols = Instances.newList();
        for(int idx = 0; idx < howmany; idx++){
            cols.add(true);
        }
        return cols;
    }

    @SuppressWarnings({"RedundantTypeArguments"})
    public static ConnectionManager newDatabaseConnectionManager(
            Properties props,
            JdbcConnectionFactory factory
    ) throws Exception {
        return JdbcConnectionManager.makeDatabaseConnectionManager(props, factory);
    }

    @SuppressWarnings({"RedundantTypeArguments"})
    public static ConnectionManager newDatabaseConnectionManager(
            Properties props
    ) throws Exception {
         return DBTuneInstances.newDatabaseConnectionManager(props, newJdbcConnectionFactory());
    }

    public static ConnectionManager newDatabaseConnectionManagerWithSwitchOffOnce(
            Properties props
    ) throws Exception {
        return DBTuneInstances.newDatabaseConnectionManager(props, makeJdbcConnectionFactoryWithSwitchOffOnce());
    }

    public static JdbcConnectionFactory makeJdbcConnectionFactoryWithSwitchOffOnce(){
        return new JdbcConnectionFactory(){
            @Override
            public Connection makeConnection(String url, String driverClass, String username, String password, boolean autoCommit) throws SQLException {
                final JdbcMocks.MockConnection conn = new JdbcMocks.MockConnection();
                conn.register(
                        makeMockStatementSwitchOffOne(true, true, conn),
                        makeMockPreparedStatementSwitchOffOne(true, true, conn)
                );
                return conn;
            }
        };
    }

    public static DB2Index newDB2Index(){
        try {
            return new DB2Index(newDb2IndexMetadata(newDB2IndexSchema()), 1.0);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static PGIndex newPGIndex(final int id){
        class PI extends PGIndex {
            PI() {
                super(newPGIndexSchema(), id, 0.0, 0.0, "");
            }
        }
        return new PI();
    }

    public static PGIndex newPGIndex(final int schemaId, final int id){
        class PI extends PGIndex {
            PI() {
                super(newPGIndexSchema(schemaId), id, 0.0, 0.0, "");
            }
        }
        return new PI();
    }

    public static PGIndex newPGIndex(final boolean flag, final int schemaId, final int id){
        class PI extends PGIndex {
            PI() {
                super(newPGIndexSchema(flag, schemaId), id, 0.0, 0.0, "");
            }
        }
        return new PI();
    }

    public static PGIndex newPGIndex(){
        return newPGIndex(1);
    }

    public static PGIndexSchema newPGIndexSchema(int id){
        return newPGIndexSchema(new Random().nextBoolean(), id);
    }

    public static PGIndexSchema newPGIndexSchema(boolean flag, int id){
        try {
            final Constructor<PGIndexSchema> c = PGIndexSchema.class.getDeclaredConstructor(int.class, boolean.class, List.class, List.class);
            c.setAccessible(true);
            final List<DatabaseColumn> columns      = Instances.newList();
            final List<Boolean>        isDescending = Instances.newList();
            return c.newInstance(id, flag, columns, isDescending);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public static PGIndexSchema newPGIndexSchema(){
        return newPGIndexSchema(123456);
    }

    public static DB2IndexMetadata newDb2IndexMetadata(DB2IndexSchema schema){
        try {
            final Constructor<DB2IndexMetadata> c = DB2IndexMetadata.class.getDeclaredConstructor(
                    DB2IndexSchema.class, int.class, String.class,
                    String.class, String.class, int.class, double.class);
            c.setAccessible(true);
            return c.newInstance(schema, 1, "no idea", "no idea", "N", 2, 5.0);
        } catch (Exception e) {
           throw new RuntimeException(e);
        }

    }

    public static DB2IndexSchema newDB2IndexSchema(){
        try {
            final Constructor<DB2IndexSchema> c = DB2IndexSchema.class.getDeclaredConstructor(
                    String.class, String.class, String.class,
                    List.class, List.class, String.class, String.class,
                    String.class
            );
            c.setAccessible(true);
            return c.newInstance(DB_NAME, TABLE_NAME, TABLE_CREATOR, new ArrayList<String>(), new ArrayList<Boolean>(), "U", "N", "REG");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
