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
import edu.ucsc.dbtune.metadata.SQLCategory;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.workload.SQLStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a SQL statement that has been optimized. Each {@code SQLStatement} object is tied to a 
 * {@link Configuration} corresponding to the physical design considered by the optimizer at the 
 * time that the execution cost was estimated.
 *
 * @see    Optimizer
 * @author Ivo Jimenez
 */
public class PreparedSQLStatement
{
    /** statement corresponding to this prepared statement, i.e. SQL statement that was prepared */
    protected SQLStatement statement;

    /** configuration that was used to optimize the statement */
    protected Iterable<? extends Index> configuration;
    // protected Configuration configuration;

    /** cost assigned by an {@link Optimizer} */
    protected double cost;

    /** the optimized plan */
    protected SQLStatementPlan plan;

    /** a list of indexes that are used by the optimized plan */
    protected List<Index> usedIndexes;

    private double[] updateCost; // deprecate when

    /**
     * Constructs a {@code PreparedSQLStatement} given its corresponding statement and the cost 
     * assigned to it.
     *
     * @param statement
     *      the corresponding {@link edu.ucsc.dbtune.workload.SQLStatement} representing the actual SQL statement.
     * @param cost
     *      cost sql statement.
     * @param configuration
     *      configuration used to optimize the statement.
     */
    public PreparedSQLStatement(SQLStatementPlan plan, double cost, Iterable<? extends Index> configuration) {
        this.statement     = plan.getStatement();
        this.plan          = plan;
        this.cost          = cost;
        this.configuration = configuration;
        this.usedIndexes   = new ArrayList<Index>();
    }

    /**
     * construct a new {@code PreparedSQLStatement} object.
     *
     * @param sql
     *      the contents of the statement
     * @param category
     *      sql category.
     * @param cost
     *      execution cost
     */
    public PreparedSQLStatement(String sql, SQLCategory category, double cost, Iterable<? extends Index> configuration) {
        this(sql,category,cost,null,configuration);
    }

    /**
     * construct a new {@code PreparedSQLStatement} object.
     *
     * @param category
     *      sql category.
     * @param overhead
     *      an array of incurred overheads.
     * @param totalCost
     *      total creation cost.
     */
    @Deprecated
    public PreparedSQLStatement(String sql, SQLCategory category, double cost, double[] updateCost, Iterable<? extends Index> configuration) {
        this.statement     = new SQLStatement(category,sql);
        this.plan          = null;
        this.updateCost    = updateCost;
        this.cost          = cost;
        this.configuration = configuration;
        this.usedIndexes   = new ArrayList<Index>();
    }

    /**
     * copy constructor
     *
     * @param other
     *      object being copied
     */
    public PreparedSQLStatement(PreparedSQLStatement other)
    {
        this.statement     = other.getStatement();
        this.plan          = other.plan;
        this.cost          = other.cost;
        this.configuration = other.configuration;
        this.updateCost    = other.updateCost;
        this.usedIndexes   = other.usedIndexes;
    }

    /**
     * Returns the cost of executing the statement.
     *
     * @return
     *      the execution cost of the statement.
     */
    public double getCost()
    {
        return cost;
    }

    /**
     * gets the maintenance cost of an index.
     *
     * @param index
     *      a {@link edu.ucsc.dbtune.metadata.Index} object.
     * @return
     *      maintenance cost.
     */
    @Deprecated
    public double getIndexMaintenanceCost(Index index) {
        if(!SQLCategory.DML.isSame(statement.getSQLCategory())){
            return 0.0;
        }

        if(updateCost == null) {
            return 0.0;
        }

        return updateCost[index.getId()];
    }

    /**
     * Returns the statement that corresponds to this.
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
     * @param
     *     an index
     * @return
     *     {@code true} if used; {@code false} otherwise.
     */
    public boolean isUsed(Index index)
    {
        return usedIndexes.contains(index);
    }

    /**
     * Assigns the execution plan.
     *
     * @param plan
     *     the plan
     */
    protected void setPlan(SQLStatementPlan plan)
    {
        this.plan = plan;
    }

    /**
     * Assigns the execution plan.
     *
     * @param plan
     *     the plan
     */
    public Iterable<? extends Index> getConfiguration()
    {
        return configuration;
    }
}
