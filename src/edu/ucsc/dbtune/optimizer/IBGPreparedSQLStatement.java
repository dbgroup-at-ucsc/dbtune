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
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.SQLCategory;
import edu.ucsc.dbtune.util.ToStringBuilder;
import edu.ucsc.dbtune.util.Instances;

import java.sql.SQLException;

/**
 * @author Karl Schnaitter
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 */
public class IBGPreparedSQLStatement extends PreparedSQLStatement {
    private IndexBenefitGraph ibg;
    private InteractionBank   bank;

    private static final IBGCoveringNodeFinder NODE_FINDER = new IBGCoveringNodeFinder();

    public IBGPreparedSQLStatement(
            PreparedSQLStatement preparedSQLStatement,
            IndexBenefitGraph    ibg,
            int                  whatIfCount)
    {
        super(preparedSQLStatement);

        this.ibg               = ibg;
        this.bank              = ibg.getInteractionBank();
        this.optimizationCount = whatIfCount;
        this.analysisTime      = ibg.getOverhead();
    }

    public IBGPreparedSQLStatement(
            String            sql,
            SQLCategory       sqlCategory,
            Iterable<? extends Index> configuration,
            IndexBenefitGraph ibg,
            InteractionBank   bank,
            int               whatIfCount,
            double            analysisTime )
    {
        super(sql, sqlCategory, 0.0, configuration);

        this.ibg               = ibg;
        this.bank              = bank;
        this.optimizationCount = whatIfCount;
        this.analysisTime   = analysisTime;
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
     *      not) be the same as {@link getConfiguration}.
     * @return
     *      a new statement
     * @throws SQLException
     *      if it's not possible to do what-if optimization on the given configuration
     */
    public PreparedSQLStatement explain(Iterable<? extends Index> configuration) throws SQLException
    {
        optimizationCount++;
        // XXX: compare configuration with this' to see if it's contained. Will be added as part of 
        // issue #82
        IBGPreparedSQLStatement newStatement = new IBGPreparedSQLStatement(this);

        if(!configuration.iterator().hasNext()) {
            newStatement.setCost(getIndexBenefitGraph().emptyCost());
        } else {
            newStatement.setCost(NODE_FINDER.findCost(getIndexBenefitGraph(),Instances.newBitSet(configuration)));
        }

        newStatement.setConfiguration(configuration);

        return newStatement;
    }

    @Override
    public String toString() {
        return new ToStringBuilder<IBGPreparedSQLStatement>(this)
               .add("index benefit graph", getIndexBenefitGraph())
               .add("interaction bank", getInteractionBank())
               .add("whatIfCount", getOptimizationCount())
               .add("ibg analysis time", getAnalysisTime())
               .toString();
    }
}
