package edu.ucsc.dbtune.optimizer;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.util.BitArraySet;
import edu.ucsc.dbtune.workload.SQLStatement;

import static edu.ucsc.dbtune.workload.SQLCategory.NOT_SELECT;

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

    /** the optimizer that constructed the statement. */
    protected Optimizer optimizer;

    /** the optimized plan. */
    protected SQLStatementPlan plan;

    /** execution cost (for selects) or the select shell cost for updates. */
    protected double selectCost;

    /** updated table. */
    protected Table updatedTable;

    /** cost of updating the base table. */
    protected double baseTableUpdateCost;

    /** For updates, the incurred cost of each index in {@link #getUpdatedConfiguration}.*/
    protected Map<Index, Double> indexUpdateCosts;

    /** configuration that was used to optimize the statement. */
    protected Set<Index> configuration;

    /** a list of indexes that are used by the optimized plan. */
    protected Set<Index> usedConfiguration;

    /** number of optimization calls done to produce this statement. */
    protected int optimizationCount;

    /**
     * construct a new {@code ExplainedSQLStatement} for an update statement.
     *
     * @param statement
     *      the corresponding statement
     * @param plan
     *      execution plan of the statement. <strong>Can be null</strong>
     * @param selectCost
     *      for {@link SQLCategory#SELECT} statements, the estimated execution cost. For {@link 
     *      SQLCategory#NOT_SELECT} statements, this cost corresponds only to the SELECT shell
     * @param optimizer
     *      optimizer that explained the statement
     * @param baseTableUpdateCost
     *      since {@link #getSelectCost} returns only the {@code SELECT} shell cost, update 
     *      statements have to get assigned with the actual update cost separately. Thus, for update 
     *      statements, the cost of updating the base table
     * @param updatedTable
     *      for update statements, the base table being updated
     * @param indexUpdateCosts
     *      for update statements, a Map of incurred update costs, where each element corresponds to 
     *      an index contained in {@link #getUpdatedConfiguration}. Implicitly, this also determines 
     *      the set of indexes that are updated by an {@code UPDATE} statement, that is, {@code 
     *      getUpdatedConfiguration.equals(updateCosts.keySet())} is {@code true}
     * @param configuration
     *      configuration used when the statement was optimized
     * @param usedConfiguration
     *      configuration that the generated execution plan uses
     * @param optimizationCount
     *      number of optimization calls done on the underlaying DBMS optimizer
     * @throws SQLException
     *      if the statement is a {@link SQLCategory#NOT_SELECT} one and the update costs ({@code 
     *      baseTableUpdateCost}; if not all the updated indexes in {@code indexUpdateCosts} refer 
     *      to {@code updatedTable}
     * @throws NullPointerException
     *      if any of {@code indexUpdateCosts}, {@code configuration} or {@code usedConfiguration} 
     *      is null
     */
    public ExplainedSQLStatement(
            SQLStatement statement,
            SQLStatementPlan plan,
            Optimizer optimizer,
            double selectCost,
            Table updatedTable,
            double baseTableUpdateCost,
            Map<Index, Double> indexUpdateCosts,
            Set<Index> configuration,
            Set<Index> usedConfiguration,
            int optimizationCount)
        throws SQLException
    {
        this.statement = statement;
        this.plan = plan;
        this.optimizer = optimizer;
        this.selectCost = selectCost;
        this.updatedTable = updatedTable;
        this.baseTableUpdateCost = baseTableUpdateCost;
        this.indexUpdateCosts = indexUpdateCosts;
        this.configuration = configuration;
        this.usedConfiguration = usedConfiguration;
        this.optimizationCount = optimizationCount;

        if (indexUpdateCosts == null || configuration == null || usedConfiguration == null)
            throw new NullPointerException("Null arguments");

        for (Index i : indexUpdateCosts.keySet())
            if (!updatedTable.equals(i.getTable()))
                throw new SQLException("Not all updated indexes are over the same table");
    }

    /**
     * copy constructor.
     *
     * @param other
     *      object being copied
     */
    public ExplainedSQLStatement(ExplainedSQLStatement other)
    {
        this.statement = other.statement;
        this.plan = other.plan;
        this.optimizer = other.optimizer;
        this.selectCost = other.selectCost;
        this.updatedTable = other.updatedTable;
        this.baseTableUpdateCost = other.baseTableUpdateCost;
        this.indexUpdateCosts = other.indexUpdateCosts;
        this.configuration = other.configuration;
        this.usedConfiguration = other.usedConfiguration;
        this.optimizationCount = other.optimizationCount;
    }

    /**
     * Returns the cost of executing the statement. The cost returned is the cost that an optimizer 
     * estimated given the set of physical structures contained in {@link #getConfiguration}. For 
     * update statements, this cost corresponds to the {@code SELECT} shell only, i.e. no update 
     * costs are considered.
     *
     * @return
     *      the execution cost of the statement; the {@code SELECT} shell cost for update statements
     */
    public double getSelectCost()
    {
        return selectCost;
    }

    /**
     * Returns the update cost of the statement. This cost doesn't include the {@code SELECT} shell 
     * cost, i.e. the cost of retrieving the tuples that will be affected by the update.
     *
     * @return
     *     the update cost. If the statement is a {@link SQLCategory#SELECT} statement, the value 
     *     returned is zero.
     */
    public double getUpdateCost()
    {
        return getBaseTableUpdateCost() + getUpdateCost(getUpdatedConfiguration());
    }

    /**
     * Returns the table that the update operates on.
     *
     * @return
     *     the updated base table. If the statement is a {@link SQLCategory#SELECT} statement, the 
     *     value returned is {@code null}.
     */
    public Table getUpdatedTable()
    {
        return updatedTable;
    }

    /**
     * Returns the update cost associated to the cost of updating the base table.
     *
     * @return
     *     the update cost of the base table. If the statement is a {@link SQLCategory#SELECT} 
     *     statement, the value returned is zero.
     */
    public double getBaseTableUpdateCost()
    {
        return baseTableUpdateCost;
    }

    /**
     * Returns the update cost for the given index. This cost doesn't include the {@code SELECT} 
     * shell cost, i.e. the cost of retrieving the tuples that will be affected by the update.
     *
     * @param index
     *      a {@link edu.ucsc.dbtune.metadata.Index} object.
     * @return
     *      maintenance cost for the given index; zero if the statement isn't an update or if the 
     *      {@link #getUpdatedConfiguration updated configuration} assigned to the statement doesn't 
     *      contain the given index.
     */
    public double getUpdateCost(Index index)
    {
        if (!statement.getSQLCategory().isSame(NOT_SELECT) || indexUpdateCosts.get(index) == null)
            return 0.0;

        return indexUpdateCosts.get(index);
    }

    /**
     * Returns the update cost associated to the given set of indexes.
     *
     * @param indexes
     *      list of {@link edu.ucsc.dbtune.metadata.Index} objects.
     * @return
     *      aggregation of update costs of the given configuration. Zero if {@link 
     *      SQLCategory#SELECT}.
     * @see #getUpdateCost(Index)
     */
    public double getUpdateCost(Set<Index> indexes)
    {
        double upCost = 0.0;

        if (!statement.getSQLCategory().isSame(NOT_SELECT))
            return upCost;

        for (Index idx : indexes)
            upCost += getUpdateCost(idx);

        return upCost;
    }

    /**
     * Returns the total cost of this statement. The total is equal to the sum of the statement's 
     * plan cost and the maintenance of each of the updated indexes. For {@link 
     * SQLCategory#NOT_SELECT} statements this is equal to {@link #getSelectCost}
     *
     * @return
     *      the total cost of this query.
     */
    public double getTotalCost()
    {
        return getSelectCost() + getUpdateCost();
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
     * Returns the plan corresponding to the statement that was used to generate this prepared 
     * statement.
     *
     * @return
     *     the execution plan. {@code null} if the plan wasn't passed to the constructor
     */
    public SQLStatementPlan getPlan()
    {
        return plan;
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
        return getUsedConfiguration().contains(index);
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
        return new BitArraySet<Index>(configuration);
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
        return new BitArraySet<Index>(usedConfiguration);
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
        return new BitArraySet<Index>(indexUpdateCosts.keySet());
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
    public int hashCode()
    {
        int code = 1;

        code = 37 * code + statement.hashCode();
        code = 37 * code + (int) Double.doubleToLongBits(selectCost);
        code = 37 * code + (int) Double.doubleToLongBits(baseTableUpdateCost);
        code = 37 * code + configuration.hashCode();
        code = 37 * code + usedConfiguration.hashCode();
        code = 37 * code + indexUpdateCosts.hashCode();

        if (updatedTable != null)
            code = 37 * code + updatedTable.hashCode();

        if (plan != null)
            code = 37 * code + plan.hashCode();

        return code;
    }

    /**
     * Same as {@link #equals} but ignores the execution plan.
     *
     * @param obj
     *      other object being compared
     * @return
     *      {@code true} if equals; {@code false} otherwise.
     */
    public boolean equalsIgnorePlan(Object obj)
    {
        if (this == obj)
            return true;
    
        if (!(obj instanceof ExplainedSQLStatement))
            return false;
    
        ExplainedSQLStatement o = (ExplainedSQLStatement) obj;
    
        if (updatedTable != null && !updatedTable.equals(o.updatedTable))
            return false;

        if (statement.equals(o.statement) &&
                selectCost == o.selectCost &&
                baseTableUpdateCost == o.baseTableUpdateCost &&
                indexUpdateCosts.equals(o.indexUpdateCosts) &&
                configuration.equals(o.configuration) &&
                usedConfiguration.equals(o.usedConfiguration))
            return true;
        
        return false;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
    
        if (!(obj instanceof ExplainedSQLStatement))
            return false;
    
        ExplainedSQLStatement o = (ExplainedSQLStatement) obj;
    
        if (plan != null && !plan.equals(o.plan))
            return false;

        return equalsIgnorePlan(obj);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append("\nSelect cost: " + getSelectCost() + "\n");

        if (statement.getSQLCategory().isSame(NOT_SELECT)) {
            sb.append("Base update cost: " + getBaseTableUpdateCost() + "\n");

            sb.append("Index update costs " + "\n");
            for (Index i : getUpdatedConfiguration())
                sb.append("   " + i + ": " + getUpdateCost(i) + "\n");
        }

        sb.append("\nAll:\n");

        if (getConfiguration().size() > 0)
            sb.append(getConfiguration());

        sb.append("\nUsed:\n");

        if (getUsedConfiguration().size() > 0)
            sb.append(getUsedConfiguration());

        sb.append("\nPlan:\n" + getPlan());

        return sb.toString();
    }
}
