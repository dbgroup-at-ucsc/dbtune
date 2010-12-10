/****************************************************************************
 * Copyright 2010 Huascar A. Sanchez                                        *
 *                                                                          *
 * Licensed under the Apache License, Version 2.0 (the "License");          *
 * you may not use this file except in compliance with the License.         *
 * You may obtain a copy of the License at                                  *
 *                                                                          *
 *     http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                          *
 * Unless required by applicable law or agreed to in writing, software      *
 * distributed under the License is distributed on an "AS IS" BASIS,        *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 * See the License for the specific language governing permissions and      *
 * limitations under the License.                                           *
 ****************************************************************************/
package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.core.metadata.DB2Index;
import edu.ucsc.dbtune.core.metadata.DB2IndexMetadata;
import edu.ucsc.dbtune.core.metadata.DB2IndexSchema;
import edu.ucsc.dbtune.core.metadata.PGIndex;
import edu.ucsc.dbtune.core.metadata.PGIndexSchema;
import edu.ucsc.dbtune.util.Instances;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static edu.ucsc.dbtune.core.JdbcMocks.makeMockPreparedStatement;
import static edu.ucsc.dbtune.core.JdbcMocks.makeMockPreparedStatementSwitchOffOne;
import static edu.ucsc.dbtune.core.JdbcMocks.makeMockStatement;
import static edu.ucsc.dbtune.core.JdbcMocks.makeMockStatementSwitchOffOne;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class DBTuneInstances {
    public static final String DB_NAME         = "superDB";
    public static final String SCHEMA_NAME     = "superSchema";
    public static final String TABLE_NAME      = "R";
    public static final String TABLE_CREATOR   = "123";

    private DBTuneInstances(){}

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
    public static DatabaseConnectionManager<DB2Index> newDB2DatabaseConnectionManager(){
        try {
            return DBTuneInstances.<DB2Index>newDatabaseConnectionManager(newDB2Properties()
            );
        } catch (Exception e) {
            throw new AssertionError("unable to create ConnectionManager");
        }
    }

    public static Properties newDB2Properties() {
        return new Properties(){
            {
                setProperty(JdbcDatabaseConnectionManager.URL, "");
                setProperty(JdbcDatabaseConnectionManager.USERNAME, "newo");
                setProperty(JdbcDatabaseConnectionManager.PASSWORD, "hahaha");
                setProperty(JdbcDatabaseConnectionManager.DATABASE, "matrix");
                setProperty(JdbcDatabaseConnectionManager.DRIVER, "com.ibm.db2.jcc.DB2Driver");
            }
        };
    }

    public static DatabaseConnectionManager<PGIndex> newPGDatabaseConnectionManager(){
        try {
            return DBTuneInstances.newDatabaseConnectionManager(newPGSQLProperties()
            );
        } catch (Exception e) {
            throw new AssertionError("unable to create ConnectionManager");
        }
    }

    public static Properties newPGSQLProperties() {
        return new Properties(){{
            setProperty(JdbcDatabaseConnectionManager.URL, "");
            setProperty(JdbcDatabaseConnectionManager.USERNAME, "newo");
            setProperty(JdbcDatabaseConnectionManager.PASSWORD, "hahaha");
            setProperty(JdbcDatabaseConnectionManager.DATABASE, "matrix");
            setProperty(JdbcDatabaseConnectionManager.DRIVER, "org.postgresql.Driver");
        }};
    }

    @SuppressWarnings({"RedundantTypeArguments"})
    public static <I extends DBIndex<I>> DatabaseConnectionManager<I> newDatabaseConnectionManager(
            Properties props,
            JdbcConnectionFactory factory
    ) throws Exception {
        return JdbcDatabaseConnectionManager.<I>makeDatabaseConnectionManager(props, factory);
    }

    @SuppressWarnings({"RedundantTypeArguments"})
    public static <I extends DBIndex<I>> DatabaseConnectionManager<I> newDatabaseConnectionManager(
            Properties props
    ) throws Exception {
         return DBTuneInstances.<I>newDatabaseConnectionManager(props, newJdbcConnectionFactory());
    }

    public static <I extends DBIndex<I>> DatabaseConnectionManager<I> newDatabaseConnectionManagerWithSwitchOffOnce(
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
            final List<Boolean>             isDescending = Instances.newList();
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
