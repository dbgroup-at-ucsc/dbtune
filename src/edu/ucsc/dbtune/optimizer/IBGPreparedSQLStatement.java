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

import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.ibg.IBGCoveringNodeFinder;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.ibg.InteractionBank;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.SQLCategory;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.ToStringBuilder;

/**
 * @author Karl Schnaitter
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 */
public class IBGPreparedSQLStatement extends PreparedSQLStatement {
    private static final IBGCoveringNodeFinder NODE_FINDER = new IBGCoveringNodeFinder();

    private final Snapshot          candidateSet;
    private final IndexBenefitGraph ibg;
    private final InteractionBank   bank;
    private final int               whatIfCount;
    private final double            ibgAnalysisTime;

    public IBGPreparedSQLStatement(
            PreparedSQLStatement preparedSQLStatement,
            Snapshot             candidateSet,
            IndexBenefitGraph    ibg,
            InteractionBank      bank,
            int                  whatIfCount,
            double               ibgAnalysisTime )
    {
        super(preparedSQLStatement);

        this.candidateSet    = candidateSet;
        this.ibg             = ibg;
        this.bank            = bank;
        this.whatIfCount     = whatIfCount;
        this.ibgAnalysisTime = ibgAnalysisTime;
    }

    public IBGPreparedSQLStatement(
            String            sql,
            SQLCategory       sqlCategory,
            Snapshot          candidateSet,
            IndexBenefitGraph ibg,
            InteractionBank   bank,
            int               whatIfCount,
            double            ibgAnalysisTime )
    {
        super(sql, sqlCategory, 0.0);

        this.candidateSet    = candidateSet;
        this.ibg             = ibg;
        this.bank            = bank;
        this.whatIfCount     = whatIfCount;
        this.ibgAnalysisTime = ibgAnalysisTime;
    }

    /**
     * Returns the interaction bank.
     *
     * @return
     *     the interaction bank
     */
    public InteractionBank getBank() {
        return bank;
    }

    /**
     * Returns the plan cost of this {@code query} given its index benefit graph and
     * an indexes configuration.
     *
     * @param graph
     *      an {@link IndexBenefitGraph} instance.
     * @param configuration
     *      indexes configuration.
     * @return
     *      the plan cost for a given indexes configuration based on
     *      an index benefit graph.
     */
    static double findIGBCost(IndexBenefitGraph graph, IndexBitSet configuration){
        return NODE_FINDER.findCost(graph, configuration);
    }

    /**
     * @return a {@link Snapshot snapshot} of
     *      the candidate indexes. 
     */
    public Snapshot getCandidateSnapshot(){
        return candidateSet;
    }

    /**
     * @return the whatIfCount for this query.
     */
    public int getWhatIfCount(){
        return whatIfCount;
    }

    /**
     * @return the ibg analysis time.
     */
    public double getIBGAnalysisTime(){
        return ibgAnalysisTime;
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
     * Returns the maintenance cost of this {@code query}.
     * @param configuration
     *      indexes configuration.
     * @return
     *      the maintenance cost of this {@code query}.
     */
    public double maintenanceCost(IndexBitSet configuration){
        if(!getStatement().getSQLCategory().isSame(SQLCategory.DML)){
            return 0;
        }

        double maintenanceCost = 0;
        for (Index eachIndex : candidateSet) {
            if (configuration.get(eachIndex.getId())) {
                maintenanceCost += getIndexMaintenanceCost(eachIndex);
            }
        }

        return maintenanceCost;
    }

    /**
     * Returns the plan cost of this {@code profiled query}.
     * @param configuration
     *      indexes configuration.
     * @return
     *      the plan cost of this {@code query}.
     */
    public double planCost(IndexBitSet configuration){
        return findIGBCost(ibg, configuration);
    }

    /**
     * Returns the total cost of this {@code query}. The total
     * is equal to the sum of the {@code query}'s plan cost and
     * the {@code query}'s maintenance cost.
     *
     * @param configuration
     *      indexes configuration.
     * @return
     *      the total cost of this query.
     */
    public double totalCost(IndexBitSet configuration){
        double plan  = planCost(configuration);
        double maint = maintenanceCost(configuration);
        double total = plan + maint;
        return total;
    }

    @Override
    public String toString() {
        return new ToStringBuilder<IBGPreparedSQLStatement>(this)
               .add("candidateSet", getCandidateSnapshot())
               .add("index benefit graph", getIndexBenefitGraph())
               .add("interaction bank", getInteractionBank())
               .add("whatIfCount", getWhatIfCount())
               .add("ibg analysis time", getIBGAnalysisTime())
               .toString();
    }
}
