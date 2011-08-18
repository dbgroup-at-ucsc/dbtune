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

import edu.ucsc.dbtune.ibg.IBGCoveringNodeFinder;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.ibg.InteractionBank;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.ConfigurationBitSet;
import edu.ucsc.dbtune.workload.SQLStatement;

import java.sql.SQLException;

/**
 * Prepared statements that are produced by the {@link IBGOptimizer}.
 *
 * @see IBGOptimizer
 * @author Karl Schnaitter
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 */
public class IBGPreparedSQLStatement extends PreparedSQLStatement
{
    private IndexBenefitGraph ibg;
    private InteractionBank   bank;

    private static final IBGCoveringNodeFinder NODE_FINDER = new IBGCoveringNodeFinder();

    /**
     * Constructs a prepared statement for the given statement.
     *
     * @param sql
     *     corresponding statement
     * @param configuration
     *     configuration that the this new explained statement will be assigned to
     * @param ibg
     *     IBG that this new statement will use to execute what-if optimization calls
     * @param bank
     *     interaction bank used
     * @param optimizationCount
     *     number of optimization calls that were done to produce {@code other}
     * @param analysisTime
     *     time it took the optimizer to prepare the statement
     */
    public IBGPreparedSQLStatement(
            SQLStatement      sql,
            Configuration     configuration,
            IndexBenefitGraph ibg,
            InteractionBank   bank,
            int               optimizationCount,
            double            analysisTime )
    {
        super(sql, 0.0, configuration);

        this.ibg               = ibg;
        this.bank              = bank;
        this.optimizationCount = optimizationCount;
        this.analysisTime   = analysisTime;
    }

    /**
     * Constructs a prepared statement out of another one and assigns the IBG-related information.
     *
     * @param other
     *     an existing prepared statement
     * @param configuration
     *     configuration that the this new explained statement will be assigned to
     * @param ibg
     *     IBG that this new statement will use to execute what-if optimization calls
     * @param optimizationCount
     *     number of optimization calls that were done to produce {@code other}
     */
    public IBGPreparedSQLStatement(
            PreparedSQLStatement other,
            Configuration        configuration,
            IndexBenefitGraph    ibg,
            int                  optimizationCount)
    {
        super(other);

        this.ibg               = ibg;
        this.configuration     = configuration;
        this.bank              = ibg.getInteractionBank();
        this.optimizationCount = optimizationCount;
        this.analysisTime      = ibg.getOverhead();
    }

    /**
     * copy constructor
     */
    public IBGPreparedSQLStatement(IBGPreparedSQLStatement other)
    {
        super(other);

        this.ibg               = other.ibg;
        this.bank              = other.ibg.getInteractionBank();
        this.optimizationCount = other.optimizationCount;
        this.analysisTime      = other.ibg.getOverhead();
    }

    /**
     * @return an {@link IndexBenefitGraph} of this query.
     */
    public IndexBenefitGraph getIndexBenefitGraph(){
        return ibg;
    }

    /**
     * @return an {@link InteractionBank} instance
     *      which will be used in the cost calculation of this query.
     */
    public InteractionBank getInteractionBank(){
        return bank;
    }

    /**
     * Uses the IBG to calculate obtain a new PreparedSQLStatement.
     *
     * @param configuration
     *      the configuration considered to estimate the cost of the new statement. This can (or 
     *      not) be the same as {@link #getConfiguration}.
     * @return
     *      a new statement
     * @throws SQLException
     *      if it's not possible to do what-if optimization on the given configuration
     */
    public PreparedSQLStatement explain(Configuration configuration) throws SQLException
    {
        optimizationCount++;

        if(!getConfiguration().getIndexes().containsAll(configuration.getIndexes())) {
            throw new SQLException("Configuration " + configuration +
                    " not contained in statement's" + getConfiguration());
        }

        IBGPreparedSQLStatement newStatement = new IBGPreparedSQLStatement(this);

        newStatement.setConfiguration(configuration);

        if(!configuration.isEmpty()) {
            newStatement.setCost(getIndexBenefitGraph().emptyCost());
            return newStatement;
        }

        if(configuration instanceof ConfigurationBitSet) {
            ConfigurationBitSet conf = (ConfigurationBitSet) configuration;
            newStatement.setCost(NODE_FINDER.findCost(getIndexBenefitGraph(),conf.getBitSet()));
        } else {
            throw new SQLException("can't recommend");
        }

        return newStatement;
    }
}
