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

import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.workload.SQLCategory;
import edu.ucsc.dbtune.workload.SQLStatement;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

/**
 * The interface to the MySQL optimizer.
 *
 * @author Ivo Jimenez
 */
public class MySQLOptimizer extends Optimizer
{
    private Connection connection;

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
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedSQLStatement explain(SQLStatement sql, Configuration configuration) throws SQLException
    {
        // XXX: issue #9 (mysqlpp project)
        if (sql.getSQLCategory().isSame(SQLCategory.NOT_SELECT))
            throw new SQLException("Can't explain " + sql.getSQLCategory() + " statements");

        SQLStatementPlan plan;
        Configuration    used;
        double           cost;

        create(configuration, connection);

        plan = getPlan(sql);
        used = new Configuration("used_configuration", plan.getIndexes());
        cost = getCost();

        drop(configuration, connection);

        return new PreparedSQLStatement(
                sql, plan, this, cost,
                Arrays.copyOf(new double[0], configuration.size()),
                configuration, used, 1);
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
     * Returns the plan for the given statement.
     *
     * @param sql
     *     statement which the plan is obtained for
     * @return
     *     an execution plan
     * @throws SQLException
     *     if something goes wrong while talking to the DBMS
     */
    protected SQLStatementPlan getPlan(SQLStatement sql)
        throws SQLException
    {
        // XXX: issue #105 - populate plan with correctly; height=2 plan is temporary
        SQLStatementPlan plan;
        Statement        stmt;
        ResultSet        rs;
        Operator         operator;
        Index            index;
        String           name;

        stmt = connection.createStatement();
        rs   = stmt.executeQuery("EXPLAIN " + sql.getSQL());
        plan = new SQLStatementPlan(sql, new Operator("root", 0.0, 0));

        while(rs.next()) {
            name = rs.getString("key");

            if(name == null)
                continue;

            operator = new Operator(rs.getString("table"), rs.getLong("rows"), 0);
            index    = catalog.findIndex(name);
            
            if(index == null)
                throw new SQLException("Can't find index " + name);

            operator.add(index);
            plan.setChild(plan.getRootOperator(), operator);
        }

        rs.close();
        stmt.close();

        return plan;
    }

    /**
     * Returns the cost of the statement that has been just explained.
     *
     * @return
     *     the cost of the plan
     * @throws SQLException
     *     if something goes wrong while talking to the DBMS
     */
    protected double getCost()
        throws SQLException
    {
        Statement stmt = connection.createStatement();
        ResultSet rs   = stmt.executeQuery("SHOW STATUS LIKE 'last_query_cost'");

        if(!rs.next())
            throw new SQLException("No result from SHOW STATUS statement");

        double cost = rs.getDouble("value");

        rs.close();
        stmt.close();
        
        return cost;
    }

    /**
     * Creates the given configuration as a set of hypothetical indexes in the database.
     *
     * @param configuration
     *     configuration being created
     * @param connection
     *     connection used to communicate with the DBMS
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
     * @param configuration
     *     configuration being dropped
     * @param connection
     *     connection used to communicate with the DBMS
     * @throws SQLException
     *     if an error occurs while communicating with the DBMS
     */
    private static void drop(Configuration configuration, Connection connection) throws SQLException
    {
        for(Index index : configuration) {
            Statement stmt = connection.createStatement();
            stmt = connection.createStatement();
            stmt.execute("DROP INDEX " + index.getName() +
                         " on " + index.getTable().getSchema().getName() +
                         "." + index.getTable().getName());
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
        sb.append(index.getTable().getSchema().getName());
        sb.append(".");
        sb.append(index.getTable().getName());
        sb.append(" (");

        for(Column col : index.getColumns()) {
            if(first)
                first = false;
            else
                sb.append(",");

            sb.append(col.getName());
            // not supported. Can be specified but it'll be ignored
            // sb.append(index.isDescending(col) ? " DESC" : " ASC");
        }

        sb.append(")");

        return sb.toString();
    }
}
