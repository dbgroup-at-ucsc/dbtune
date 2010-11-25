/*
 * ****************************************************************************
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 *  ****************************************************************************
 */

package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.core.DatabaseTable;
import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.core.metadata.PGIndex;
import edu.ucsc.dbtune.core.metadata.DB2Index;
import edu.ucsc.dbtune.core.GenericDatabaseTable;

import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import static edu.ucsc.dbtune.core.JdbcDatabaseConnectionManager.makeDatabaseConnectionManager;

/**
 * This class represents a Schema, a container of tables.
 *
 * @author ivo@cs.ucsc.edu (Ivo Jimenez)
 */
public class Schema
{
    public List<DatabaseTable> tables;
    public String name;

    DatabaseConnection<?> connection;

    /**
     * Construct a schema database connection object.
     *
     * @param connection
     *     connection used to represent
     * @throws SQLException
     *     if something wrong goes during the establishment of a connection with the underlaying 
     *     DBMS
     */
    public Schema( DatabaseConnection<?> connection )
        throws SQLException
    {
        this.connection = connection;
        extractMetadata( connection );
    }

    /**
     * Given a DatabaseConnection object, it extracts the metadata(tables and base configuration) 
     * contained in the corresponding schema.
     *
     * @throws SQLException
     *     if a JDBC error is thrown during the extraction of metadata
     */
    private void extractMetadata( DatabaseConnection<?> con )
        throws SQLException
    {
        // DatabaseMetadataExtractor extractor;
        //
        // tables = extractor.getTables( connection );
        // config = extractor.getIndexes( connnection );
        
        DatabaseMetaData jdbcMetaData;
        ResultSet        rs;

        tables = new ArrayList<DatabaseTable>();

        jdbcMetaData = con.getJdbcConnection().getMetaData();

        if (jdbcMetaData == null)
        {
            throw new SQLException("Connection " + con + " doesn't handle JDBC metadata" );
        }

        String[] tableTypes = {"TABLE"};

        rs = jdbcMetaData.getTables(null,null,"%",tableTypes);

        while (rs.next())
        {
            tables.add(new GenericDatabaseTable(rs.getString(3)));
        }

        rs.close();
    }

    /**
     * Creates a Schema object corresponding to the given parameters.
     *
     * @param  url
     *     url of the system,database that the schema will correspond to. The url                   
     *     should be in a JDBC format (jdbc)
     * @param  usr
     *     username used to authenticate the connection
     * @param  pwd
     *     passsword used to authenticate the connection
     * @return
     *     the Schema object corresponding to the given parameters. A connection is actually 
     *     established with the DBMS system that the url
     * @throws Exception
     *     if something wrong goes during the creation of the Schema object
     */
    public static Schema connect( String url, String usr, String pwd )
        throws Exception
    {
        // DatabaseConnectionManager connectionManager;
        // Properties props;
        //
        // props.setProperty(JdbcDatabaseConnectionManager.URL, url);
        // props.setProperty(JdbcDatabaseConnectionManager.USERNAME, usr);
        // props.setProperty(JdbcDatabaseConnectionManager.PASSWORD, pwd);
        //
        // return makeDatabaseConnectionManager(props).connect();
        
        DatabaseConnectionManager<DB2Index> connectionManagerDB2;
        DatabaseConnectionManager<PGIndex>  connectionManagerPG;
        DatabaseConnection<?>               connection;

        Properties props;
        String[]   url_split;

        url_split = url.split(":");

        props = new Properties();

        props.setProperty(JdbcDatabaseConnectionManager.URL, url);
        props.setProperty(JdbcDatabaseConnectionManager.USERNAME, usr);
        props.setProperty(JdbcDatabaseConnectionManager.PASSWORD, pwd);

        if ( url_split[1].equals("db2") )
        {
            props.setProperty(JdbcDatabaseConnectionManager.DRIVER, "com.ibm.db2.jcc.DB2Driver");
            connectionManagerDB2 = makeDatabaseConnectionManager(props);
            connection = connectionManagerDB2.connect();
        }
        else if ( url_split[1].equals("postgresql") )
        {
            props.setProperty(JdbcDatabaseConnectionManager.DRIVER, "org.postgresql.Driver");
            connectionManagerPG = makeDatabaseConnectionManager( props );
            connection = connectionManagerPG.connect();
        }
        else
        {
            throw new Exception("Unknown driver class");
        }

        return new Schema(connection);
    }
}
