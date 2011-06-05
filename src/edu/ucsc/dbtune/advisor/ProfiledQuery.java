/*
 * ****************************************************************************
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
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 *  ****************************************************************************
 */
package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.ExplainInfo;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.ibg.IBGCoveringNodeFinder;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.ibg.InteractionBank;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.ToStringBuilder;
import edu.ucsc.satuning.spi.Supplier;

import java.io.Serializable;

/**
 * @author Karl Schnaitter
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class ProfiledQuery <I extends DBIndex> implements Serializable {
    private static final IBGCoveringNodeFinder NODE_FINDER = new IBGCoveringNodeFinder();

    private final String            sql;
    private final ExplainInfo       explainInfo;
    private final Snapshot<I>       candidateSet;
    private final IndexBenefitGraph ibg;
    private final InteractionBank   bank;
    private final int               whatIfCount;
    private final double            ibgAnalysisTime;

    private ProfiledQuery(Builder<I> builder){
        sql             = builder.sql;
        explainInfo     = builder.explainInfo;
        candidateSet    = builder.candidateSet;
        ibg             = builder.ibg;
        bank            = builder.bank;
        whatIfCount     = builder.whatifCount;
        ibgAnalysisTime = builder.ibgAnalysisTime;
    }

    public InteractionBank getBank() {
        return bank;
    }

    /**
     * Returns the plan cost of this {@code query} given its index benefit graph and
     * an indexes configuration.
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
     * @return the actual {@code SQL query} wrapped into this object..
     */
    public String getSQL(){
        return sql;
    }

    /**
     * @return an {@link ExplainInfo} instance.
     */
    public ExplainInfo getExplainInfo(){
        return explainInfo;
    }

    /**
     * @return a {@link Snapshot snapshot} of
     *      the candidate indexes. 
     */
    public Snapshot<I> getCandidateSnapshot(){
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
        if(!explainInfo.isDML()){
            return 0;
        }

		double maintenanceCost = 0;
		for (I eachIndex : candidateSet) {
			if (configuration.get(eachIndex.internalId())) {
				maintenanceCost += explainInfo.getIndexMaintenanceCost(eachIndex);
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
     * @param configuration
     *      indexes configuration.
     * @return
     *      the total cost of this query.
     */
    public double totalCost(IndexBitSet configuration){
        Console.streaming().info("ProfiledQuery#totalCost(IndexBitSet) will use " + configuration + " for total cost (plan + maintenance)");
        double plan  = planCost(configuration);
        double maint = maintenanceCost(configuration);
        double total = plan + maint;
        Console.streaming().info("ProfiledQuery#totalCost(IndexBitSet) has calculated a total cost = " + total);
        return total;
    }

    @Override
    public String toString() {
        return new ToStringBuilder<ProfiledQuery<I>>(this)
               .add("sql", getSQL())
               .add("explainInfo", getExplainInfo())
               .add("candidateSet", getCandidateSnapshot())
               .add("index benefit graph", getIndexBenefitGraph())
               .add("interaction bank", getInteractionBank())
               .add("whatIfCount", getWhatIfCount())
               .add("ibg analysis time", getIBGAnalysisTime())
               .toString();
    }

    /**
     * A {@link ProfiledQuery}'s {@link Builder builder}.
     * @param <I>
     *      index type.
     */
    public static class Builder<I extends DBIndex> implements Supplier<ProfiledQuery<I>> {
        private final String                sql;
        private ExplainInfo explainInfo;
        private Snapshot<I> candidateSet;
        private IndexBenefitGraph           ibg;
        private InteractionBank             bank;
        private int                         whatifCount;     // value from DatabaseConnection after profiling
        private double                      ibgAnalysisTime; // in milliseconds

        public Builder(String sql){
            this.sql = sql;
        }

        public Builder<I> explainInfo(ExplainInfo value){
            explainInfo = value;
            return this;
        }

        public Builder<I> snapshotOfCandidateSet(Snapshot<I> value){
            candidateSet = value;
            return this;
        }

        public Builder<I> indexBenefitGraph(IndexBenefitGraph value){
            ibg = value;
            return this;
        }

        public Builder<I> interactionBank(InteractionBank value){
            bank = value;
            return this;
        }

        public Builder<I> whatIfCount(int value){
            // value from DatabaseConnection after profiling
            whatifCount = value;
            return this;
        }

        public Builder<I> indexBenefitGraphAnalysisTime(double value){
            // in milliseconds
            ibgAnalysisTime = value;
            return this;
        }


        @Override
        public ProfiledQuery<I> get() {
            return new ProfiledQuery<I>(this);
        }
    }
}