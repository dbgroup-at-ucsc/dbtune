package edu.ucsc.dbtune.optimizer;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.util.BitArraySet;
import edu.ucsc.dbtune.workload.SQLStatement;

import static edu.ucsc.dbtune.workload.SQLCategory.UPDATE;

/**
 * Represents a SQL statement that has been optimized. Each {@code SQLStatement} object is tied to a 
 * {@link Configuration} corresponding to the physical design considered by the optimizer at the 
 * time that the execution cost was estimated.
 *
 * @see    Optimizer
 * @author Ivo Jimenez
 */
public class ExplainedSQLStatement
{
    /** statement corresponding to this explained statement, i.e. SQL statement that was explained */
    protected SQLStatement statement;

    /** configuration that was used to optimize the statement. */
    protected Set<Index> configuration;

    /** cost assigned by an {@link Optimizer}. */
    protected double cost;

    /** the optimized plan. */
    protected SQLStatementPlan plan;

    /** a list of indexes that are used by the optimized plan. */
    protected Set<Index> usedConfiguration;

    /**
     * For update statements, the cost that implies on each of the indexes containe in {@link 
     * #getConfiguration}.
     */
    protected Map<Index, Double> updateCosts;

    /** the optimizer that constructed the statement. */
    protected Optimizer optimizer;

    /** number of optimization calls done to produce this statement. */
    protected int optimizationCount;

    /** cached update cost. */
    protected double updateCost;

    /**
     * construct a new {@code ExplainedSQLStatement} for an update statement.
     *
     * @param statement
     *     corresponding the statement
     * @param plan
     *     the statement plan. Might be null.
     * @param cost
     *     the execution cost. For update statements, this cost corresponds only to the SELECT 
     *     shell, i.e. no update costs are considered
     * @param optimizer
     *      optimizer that explained the statement
     * @param updateCosts
     *     for update statements, a Map of incurred update costs, where each element corresponds to 
     *     an index contained in {@link #getConfiguration}
     * @param configuration
     *     configuration used when the statement was optimized
     * @param usedConfiguration
     *      configuration that the generated execution plan uses
     * @param optimizationCount
     *     number of optimization calls done on to produce the statement
     * @throws SQLException
     *     if statement is of {@link SQLCategory#NOT_SELECT} category and the update array is null 
     *     or its length doesn't correspond to the configuration size.
     */
    public ExplainedSQLStatement(
            SQLStatement statement,
            SQLStatementPlan plan,
            Optimizer optimizer,
            double cost,
            Map<Index, Double> updateCosts,
            Set<Index> configuration,
            Set<Index> usedConfiguration,
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

        if (updateCosts.size() != configuration.size())
            throw new SQLException(
                    "Incorrect update costs " + updateCosts.size() +
                    " for configuration of size" + configuration.size());

        if (!configuration.containsAll(updateCosts.keySet()))
            throw new SQLException(
                    "Not all indexes from configuration contained in update cost map");

        this.updateCost = getUpdateCost(getConfiguration());
    }

    /**
     * copy constructor.
     *
     * @param other
     *      object being copied
     */
    public ExplainedSQLStatement(ExplainedSQLStatement other)
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
     * Sets the configuration that the optimizer considered when it optimized the statement.
     *
     * @param configuration
     *     the list of indexes considered at optimization time.
     */
    void setConfiguration(Set<Index> configuration)
    {
        this.configuration = configuration;
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
        if (!configuration.contains(index) || !statement.getSQLCategory().isSame(UPDATE))
            return 0.0;

        return updateCosts.get(index);
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
    public double getUpdateCost(Set<Index> indexes)
    {
        double updateCost = 0.0;

        for (Index idx : indexes)
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
    public Set<Index> getConfiguration()
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
    public Set<Index> getUsedConfiguration()
    {
        Set<Index> usedConfiguration = new BitArraySet<Index>();

        for (Index idx : getConfiguration())
            if (isUsed(idx))
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
    public Set<Index> getUpdatedConfiguration()
    {
        Set<Index> updatedConfiguration = new BitArraySet<Index>();

        for (Map.Entry<Index, Double> e : updateCosts.entrySet())
            if (e.getValue() != 0)
                updatedConfiguration.add(e.getKey());

        return updatedConfiguration;
    }

    /**
     * Gets the total count of optimizations that were handled/performed by the optimizer. An 
     * "optimization count" corresponds to an optimization call, i.e. the number of times that an 
     * optimizer was asked to estimate the cost of a statement.
     * <p>
     * The value is implementation-dependent and also depends on where the {@link 
     * ExplainedSQLStatement} instance was produced. In some, this may refer to the number of times 
     * that the {@link Optimizer#explain(SQLStatement, Configuration)} was invoked, whereas in 
     * others it could refer to the number of times that internal structures of an {@link Optimizer} 
     * or {@link ExplainedSQLStatement} where queried in order to simulate an optimizer call.
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

        if (getConfiguration().size() > 0)
            sb.append(getConfiguration());

        sb.append("Used:\n");

        if (getUsedConfiguration().size() > 0)
            sb.append(getUsedConfiguration());

        return sb.toString();
    }
}
