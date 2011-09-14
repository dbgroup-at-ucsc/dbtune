/* ************************************************************************** *
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
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied  *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
package edu.ucsc.dbtune.optimizer;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.workload.SQLCategory;
import edu.ucsc.dbtune.workload.SQLStatement;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The interface to the MySQL optimizer.
 *
 * @author Ivo Jimenez
 */
public class MySQLOptimizer extends Optimizer
{
    private Connection connection;
    private boolean    obtainPlan;

    /**
     * Creates a new optimizer for MySQL.
     *
     * @param connection
     *     JDBC connection used to communicate to a PostgreSQL system.
     * @throws SQLException
     *     if an error occurs while communicating to the server.
     */
    public MySQLOptimizer(Connection connection) throws SQLException
    {
        this.connection = connection;
        this.obtainPlan = false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedSQLStatement explain(SQLStatement sql, Configuration indexes) throws SQLException
    {
        ResultSet        rs;
        SQLStatementPlan sqlPlan;
        Configuration    usedConf = null;
        Statement        stmt;
        double[]         updateCost = null;
        double           selectCost = 0.0;

        create(indexes, connection);

        stmt = connection.createStatement();
        rs   = stmt.executeQuery("EXPLAIN EXTENDED " + sql.getSQL());

        if(!rs.next())
            throw new SQLException("No result from EXPLAIN statement");

        rs.close();
        stmt.close();

        sqlPlan = null;

        if(obtainPlan)
            sqlPlan = getPlan(connection,sql);

        drop(indexes, connection);

        return new PreparedSQLStatement(sql, sqlPlan, selectCost, updateCost, indexes, usedConf, 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration recommendIndexes(SQLStatement sql) throws SQLException
    {
        throw new SQLException("Not implemented yet");
    }

    /**
     * Returns the plan for the given statement
     *
     * @param connection
     *     connection to the DBMS
     * @param sql
     *     statement whose plan is retrieved
     * @return
     *     an execution plan for the given statement
     * @throws SQLException
     *     if something goes wrong while talking to the DBMS
     */
    protected SQLStatementPlan getPlan(Connection connection, SQLStatement sql)
        throws SQLException
    {
        throw new SQLException("Not implemented yet");
    }

    /**
     * Creates the given configuration as a set of hypothetical indexes in the database.
     *
     * @param indexes
     * @throws SQLException
     *      if an error occurs while communicating with the DBMS
     */
    private static void create(Configuration configuration, Connection connection) throws SQLException
    {
        for(Index index : configuration) {
            Statement stmt = connection.createStatement();
            stmt.execute("CREATE HYPOTHETICAL INDEX " + toString(index));
            stmt.close();
        }
    }
    
    /**
     * Drops the given configuration as a set of hypothetical indexes in the database.
     *
     * @param indexes
     * @throws SQLException
     *      if an error occurs while communicating with the DBMS
     */
    private static void drop(Configuration configuration, Connection connection) throws SQLException
    {
        for(Index index : configuration) {
            Statement stmt = connection.createStatement();
            stmt = connection.createStatement();
            stmt.execute("DROP INDEX " + index + " on " + index.getTable());
            stmt.close();
        }
    }

    /**
     * Returns a string representation of the given index.
     *
     * @param index
     *     an index
     * @return
     *     a string containing the MySQL-specific string representation of the given index
     */
    private static String toString(Index index) throws SQLException
    {
        StringBuilder sb    = new StringBuilder();
        boolean       first = true;

        sb.append(index.getName());
        sb.append(" on ");
        sb.append(index.getTable().getName());
        sb.append(" (");

        for(Column col : index.getColumns()) {
            if(first)
                first = false;
            else
                sb.append(",");

            sb.append(col.getName());
            sb.append(index.isDescending(col) ? " DESC" : " ASC");
        }

        sb.append(" )");

        return sb.toString();
    }

}
