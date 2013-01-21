package edu.ucsc.dbtune.workload;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * A reader of a DBMS' query log. This reader monitors the query log of a DBMS in order to read any 
 * statement that gets executed in a given target database.
 *
 * @author Ivo Jimenez
 */
public abstract class QueryLogReader extends ObservableWorkloadReader
{
    /**
     * Creates a query log reader based on the connection's associated driver. The method reads the 
     * driver name and instantiates the appropriate query log reader.
     *
     * @param connection
     *      an environment object used to access the properties of the system
     * @return
     *      a QueryLogReader of the appropriate type
     * @throws SQLException
     *      if the connection is closed, there's no known reader for the connection's associated 
     *      driver or if the corresponding constructor raises an exception.
     */
    public static QueryLogReader newQueryLogReader(Connection connection)
        throws SQLException
    {
        if (connection.isClosed())
            throw new SQLException("Connection is closed");

        String driver = connection.getMetaData().getURL().toLowerCase();

        if (driver.contains("db2")) {
            return new DB2WorkloadReader(connection);
        } else {
            throw new SQLException("No known reader for driver " + driver);
        }
    }
}
