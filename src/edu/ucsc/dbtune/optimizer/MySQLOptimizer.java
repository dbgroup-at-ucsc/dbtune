package edu.ucsc.dbtune.optimizer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.util.BitArraySet;
import edu.ucsc.dbtune.workload.SQLCategory;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * The interface to the MySQL optimizer.
 *
 * @author Ivo Jimenez
 */
public class MySQLOptimizer extends AbstractOptimizer
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
    public ExplainedSQLStatement explain(SQLStatement sql, Set<Index> configuration)
        throws SQLException
    {
        // XXX: issue #9 (mysqlpp project)
        if (sql.getSQLCategory().isSame(SQLCategory.NOT_SELECT))
            throw new SQLException("Can't explain " + sql.getSQLCategory() + " statements");

        SQLStatementPlan plan;
        Set<Index>       used;
        double           cost;

        create(configuration, connection);

        plan = getPlan(sql);
        used = new BitArraySet<Index>(plan.getIndexes());
        cost = getCost();

        drop(configuration, connection);

        return new ExplainedSQLStatement(sql, plan, this, cost, 0.0, null, configuration, used, 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Index> recommendIndexes(SQLStatement sql) throws SQLException
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

        while (rs.next()) {
            name = rs.getString("key");

            if (name == null)
                continue;

            operator = new Operator(rs.getString("table"), rs.getLong("rows"), 0);
            index    = catalog.findIndex(name);
            
            if (index == null)
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

        if (!rs.next())
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
    private static void create(Set<Index> configuration, Connection connection) throws SQLException
    {
        for (Index index : configuration) {
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
    private static void drop(Set<Index> configuration, Connection connection) throws SQLException
    {
        for (Index index : configuration) {
            Statement stmt = connection.createStatement();
            stmt = connection.createStatement();
            stmt.execute("DROP INDEX " + index.getName() +
                         " on " + index.getTable().getFullyQualifiedName());
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
     * @throws SQLException
     *     if the name of a column contained in the index can't be obtained
     */
    private static String toString(Index index) throws SQLException
    {
        StringBuilder sb    = new StringBuilder();
        boolean       first = true;

        sb.append(index.getName());
        sb.append(" on ");
        sb.append(index.getTable().getFullyQualifiedName());
        sb.append(" (");

        for (Column col : index.columns()) {
            if (first)
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
