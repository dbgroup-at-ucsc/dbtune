/* **************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                         *
 *                                                                              *
 *   Licensed under the Apache License, Version 2.0 (the "License");            *
 *   you may not use this file except in compliance with the License.           *
 *   You may obtain a copy of the License at                                    *
 *                                                                              *
 *       http://www.apache.org/licenses/LICENSE-2.0                             *
 *                                                                              *
 *   Unless required by applicable law or agreed to in writing, software        *
 *   distributed under the License is distributed on an "AS IS" BASIS,          *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   *
 *   See the License for the specific language governing permissions and        *
 *   limitations under the License.                                             *
 * **************************************************************************** */
package edu.ucsc.dbtune.optimizer;

import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.workload.SQLCategory;
import edu.ucsc.dbtune.workload.SQLStatement;

import java.util.List;
import java.sql.SQLException;

import static edu.ucsc.dbtune.workload.SQLCategory.UPDATE;

/**
 * Represents a SQL statement that has been optimized. Each {@code SQLStatement} object is tied to a 
 * {@link Configuration} corresponding to the physical design considered by the optimizer at the 
 * time that the execution cost was estimated.
 * <p>
 * A prepared statement may have the knowledge required to optimize the query it corresponds to, given a physical 
 * configuration.
 * <p>
 * For example:
 * XXX: add an example of how this can be done through the getCost(Configuration) method
 *
 * @see    Optimizer
 * @author Ivo Jimenez
 */
public class PreparedSQLStatement
{
    /** statement corresponding to this prepared statement, i.e. SQL statement that was prepared */
    protected SQLStatement statement;

    /** configuration that was used to optimize the statement */
    protected Configuration configuration;

    /** cost assigned by an {@link Optimizer} */
    protected double cost;

    /** the optimized plan */
    protected SQLStatementPlan plan;

    /** a list of indexes that are used by the optimized plan */
    protected Configuration usedConfiguration;

    /**
     * For update statements, the cost that implies on each of the indexes containe in {@link 
     * #getConfiguration}
     */
    protected double[] updateCosts;

    /** the optimizer that constructed the statement */
    protected Optimizer optimizer;

    /** number of optimization calls done to produce this statement */
    protected int optimizationCount;

    /** the time it took an optimizer to produce this statement */
    protected double analysisTime;

    /** cached update cost */
    protected double updateCost;

    /**
     * construct a new {@code PreparedSQLStatement} object.
     *
     * @param sql
     *      the statement
     * @param cost
     *      execution cost
     * @param configuration
     *      configuration used to optimize the statement.
     */
    public PreparedSQLStatement(
            SQLStatement sql,
            double cost,
            Optimizer optimizer,
            Configuration configuration,
            Configuration usedConfiguration,
            int optimizationCount)
        throws SQLException
    {
        this(sql, null, optimizer, cost, null, configuration, usedConfiguration, optimizationCount);
    }

    /**
     * construct a new {@code PreparedSQLStatement} for an update statement.
     *
     * @param statement
     *     corresponding the statement
     * @param plan
     *     the statement plan. Might be null.
     * @param cost
     *     the execution cost. For update statements, this cost corresponds only to the SELECT 
     *     shell, i.e. no update costs are considered
     * @param updateCosts
     *     for update statements, an array of incurred update costs, where each element corresponds 
     *     to an index contained in in {@link #getConfiguration}. The array should be indexed using 
     *     each index's ordinal position with respect to the configuration({@link 
     *     Configuration#getOrdinalPosition}).
     * @param configuration
     *     configuration used when the statement was optimized
     * @param optimizationCount
     *     number of optimization calls done on to produce the statement
     * @throws SQLException
     *     if statement is of {@link SQLCategory#NOT_SELECT} category and the update array is null 
     *     or its length doesn't correspond to the configuration size.
     */
    public PreparedSQLStatement(
            SQLStatement statement,
            SQLStatementPlan plan,
            Optimizer optimizer,
            double cost,
            double[] updateCosts,
            Configuration configuration,
            Configuration usedConfiguration,
            int optimizationCount)
        throws SQLException
    {
        this.statement         = statement;
        this.plan              = plan;
        this.cost              = cost;
        this.updateCosts       = updateCosts;
        this.configuration     = configuration;
        this.optimizer         = optimizer;
        this.usedConfiguration = usedConfiguration;
        this.optimizationCount = optimizationCount;
        this.analysisTime      = 0.0;

        if(updateCosts == null)
            throw new SQLException("Update cost array is null");

        if(updateCosts.length != configuration.size())
            throw new SQLException(
                    "Incorrect update costs " + updateCosts.length +
                    " for configuration of size" + configuration.size());

        this.updateCost = getUpdateCost(getConfiguration().toList());
    }

    /**
     * copy constructor
     *
     * @param other
     *      object being copied
     */
    public PreparedSQLStatement(PreparedSQLStatement other)
    {
        this.configuration     = other.configuration;
        this.cost              = other.cost;
        this.optimizer         = other.optimizer;
        this.plan              = other.plan;
        this.statement         = other.statement;
        this.updateCosts       = other.updateCosts;
        this.updateCost        = other.updateCost;
        this.usedConfiguration = other.usedConfiguration;
    }

    /**
     * Assigns the cost of executing the statement.
     *
     * @param cost
     *      the execution cost of the statement.
     */
    void setCost(double cost)
    {
        this.cost = cost;
    }

    /**
     * Sets the configuration that the optimizer considered when it optimized the statement
     *
     * @param configuration
     *     the list of indexes considered at optimization time.
     */
    void setConfiguration(Configuration configuration)
    {
        this.configuration = configuration;
    }

    /**
     * Re-optimizes this statement, using the current context and the given the set of indexes. 
     * Given a configuration this method tries to determine what would the cost be, as if the 
     * optimizer would have used the information contained in the given statement. Different 
     * implementations will determine how to create a new PreparedSQLStatement in a custom way, 
     * depending on which information is available to them.
     * <p>
     * In other words, the purpose of this method is to have it execute (a sort-of 'mini') what-if 
     * optimization using the original context that the optimizer used when it optimized the 
     * statement, the information contained in {@link #getConfiguration}) among them.
     * <p>
     * This base implementation does a 'naive' what-if optimization by comparing the given 
     * configuration against the one contained in {@code statement}, proceeding in the following 
     * way:
     * <ul>
     * <li> If {@link #getUsedConfiguration} is exactly equal to the given {@code configuration}, it 
     * produces a new statement identical to {@code this}.
     * <li> If not, then the statement is explained again, with {@code configuration} as the new 
     * configuration.
     * </ul>
     *
     * @param configuration
     *      the configuration considered to estimate the cost of the new statement. This can (or 
     *      not) be the same as {@link #getConfiguration}.
     * @return
     *      a new statement
     * @throws SQLException
     *      if it's not possible to do what-if optimization on the given configuration
     */
    public PreparedSQLStatement explain(Configuration configuration)
        throws SQLException
    {
        if (configuration == getUsedConfiguration())
            return new PreparedSQLStatement(this);
        else
            return optimizer.explain(statement, configuration);
    }

    /**
     * Returns the cost of executing the statement. The cost returned is the cost that an optimizer 
     * estimated given the set of physical structures contained in {@link #getConfiguration}. For 
     * update statements, this cost corresponds to the SELECT shell only, i.e. no update costs are 
     * considered.
     *
     * @return
     *      the execution cost of the statement.
     */
    public double getCost()
    {
        return cost;
    }

    /**
     * Returns the UPDATE cost that executing the statement implies on the given index. The cost 
     * returned is the cost that an optimizer estimated given the set of physical structures 
     * contained in {@link #getConfiguration}. This cost doesn't include the SELECT shell cost, i.e. 
     * the cost of retrieving the tuples that will be affected by the update
     *
     * @param index
     *      a {@link edu.ucsc.dbtune.metadata.Index} object.
     * @return
     *      maintenance cost for that this statement implies for the given index. 0 if the statement 
     *      isn't an update, there are no update costs defined at all or the configuration assigned 
     *      to the statement doesn't contain the given index.
     */
    public double getUpdateCost(Index index)
    {
        if(!configuration.contains(index) || !statement.getSQLCategory().isSame(UPDATE))
            return 0.0;

        int position = configuration.getOrdinalPosition(index);

        if(position == -1)
            throw new RuntimeException("Wrong position in configuration " + position);

        return updateCosts[position];
    }

    /**
     * Returns the UPDATE cost that executing the statement implies on the given set of indexes. The 
     * cost returned is the cost that an optimizer estimated given the set of physical structures 
     * contained in {@link #getConfiguration}. This cost doesn't include the SELECT shell cost, i.e. 
     * the cost of retrieving the tuples that will be affected by the update
     *
     * @param indexes
     *      list of {@link edu.ucsc.dbtune.metadata.Index} objects.
     * @return
     *      aggregation of update costs of the given configuration.
     */
    public double getUpdateCost(List<Index> indexes)
    {
        double updateCost = 0.0;

        for(Index idx : indexes)
            updateCost += getUpdateCost(idx);

        return updateCost;
    }

    /**
     * Returns the update costs for the statement. If an index in {@code configuration} is contained 
     * in {@code getUpdatedConfiguration()}, then it's cost is aggregated into the returned value.
     *
     * @return
     *     the update costs for the given configuration.
     */
    public double getUpdateCost()
    {
        return updateCost;
    }

    /**
     * Returns the total cost of this statement. The total is equal to the sum of the statement's 
     * plan cost and the maintenance of each of the updated indexes. For non-update statements this 
     * is equal to {@link #getCost}
     *
     * @return
     *      the total cost of this query.
     * @see #getUpdatedConfiguration
     */
    public double getTotalCost()
    {
        return getCost() + getUpdateCost();
    }

    /**
     * Returns the statement that was used to generate this prepared statement.
     *
     * @return
     *     the SQL statement from which this prepared statement was obtained from.
     */
    public SQLStatement getStatement()
    {
        return statement;
    }

    /**
     * Determines whether a given index is used by the corresponding execution plan.
     *
     * @param index
     *     an index
     * @return
     *     {@code true} if used; {@code false} otherwise.
     */
    public boolean isUsed(Index index)
    {
        return usedConfiguration.contains(index);
    }

    /**
     * Returns the configuration that the optimizer considered when it optimized the statement. Note 
     * that this is different from {@link #getUsedConfiguration} and {@link 
     * #getUpdatedConfiguration}.
     *
     * @return
     *     the list of indexes considered at optimization time.
     */
    public Configuration getConfiguration()
    {
        return configuration;
    }

    /**
     * Returns the set of indexes that are used by the plan. Note that this is different from {@link 
     * #getConfiguration} and {@link #getUpdatedConfiguration}.
     *
     * @return
     *     the configuration that is used by the execution plan, i.e. the set of physical structures 
     *     that are read when calculating the answer of a statement.
     */
    public Configuration getUsedConfiguration()
    {
        Configuration usedConfiguration = new Configuration("used_conf");

        for(Index idx : getConfiguration())
            if( isUsed(idx) )
                usedConfiguration.add(idx);

        return usedConfiguration;
    }

    /**
     * For UPDATE statements, this method returns the set of indexes that are updated. Note that 
     * this is different from {@link #getConfiguration} and {@link #getUsedConfiguration}.
     *
     * @return
     *     the configuration that is used by the execution plan, i.e. the set of physical structures 
     *     that are read when calculating the answer of a statement.
     */
    public Configuration getUpdatedConfiguration()
    {
        Configuration updatedConfiguration = new Configuration("updated_conf");

        for(Index idx : getConfiguration()) {
            if(updateCosts[getConfiguration().getOrdinalPosition(idx)] != 0) {
                updatedConfiguration.add(idx);
            }
        }

        return updatedConfiguration;
    }

    /**
     * Returns the time that an optimizer took to produce this statement
     *
     * @return
     *     the analysis time.
     */
    public double getAnalysisTime()
    {
        return analysisTime;
    }

    /**
     * Gets the total count of optimizations that were handled/performed by the optimizer. An 
     * "optimization count" corresponds to an optimization call, i.e. the number of times that an 
     * optimizer was asked to estimate the cost of a statement.
     * <p>
     * The value is implementation-dependent and also depends on where the {@link 
     * PreparedSQLStatement} instance was produced. In some, this may refer to the number of times 
     * that the {@link Optimizer#explain(SQLStatement, Configuration)} was invoked, whereas in 
     * others it could refer to the number of times that internal structures of an {@link Optimizer} 
     * or {@link PreparedSQLStatement} where queried in order to simulate an optimizer call.
     *
     * @return
     *     the total count of performed what-if optimizations.
     */
    public int getOptimizationCount()
    {
        return optimizationCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append("Cost: " + getCost() + "\n");

        sb.append("All:\n");

        if(getConfiguration().size() > 0)
            sb.append(getConfiguration());

        sb.append("Used:\n");

        if(getUsedConfiguration().size() > 0)
            sb.append(getUsedConfiguration());

        return sb.toString();
    }
}
