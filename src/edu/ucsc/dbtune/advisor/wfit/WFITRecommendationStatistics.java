package edu.ucsc.dbtune.advisor.wfit;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import edu.ucsc.dbtune.advisor.RecommendationStatistics;

import edu.ucsc.dbtune.metadata.Index;

import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * Holds information about the WFIT advising process. Essentially, extends the set of recommendation 
 * statistics by including information about the stable candidate partitioning.
 *
 * @author Ivo Jimenez
 */
public class WFITRecommendationStatistics extends RecommendationStatistics
{
    /**
     * @param algorithmName
     *      name of the algorithm that produced the recommendation
     */
    public WFITRecommendationStatistics(String algorithmName)
    {
        super(algorithmName);
    }

    /**
     * Adds a wfit-specific entry.
     *
     * @param sql
     *      statement for which this entry corresponds to
     * @param totalCost
     *      the cost of exeucting the statement
     * @param candidateSet
     *      indexes that were in the context of the recommender
     * @param partitioning
     *      partitioning of the candidate set
     * @param usefulness
     *      the usefulness of each index in the candidate set, with respect to the next query in the 
     *      workload stream
     * @param recommendation
     *      indexes that were recommended
     * @param benefits
     *      the benefits for each index in the recommendation
     * @param workFunctionScores
     *      scores the correspond to the work function values
     * @return
     *      the entry that has been just created
     */
    public Entry addNewEntry(
            SQLStatement sql,
            double totalCost,
            Set<Index> candidateSet,
            Set<Set<Index>> partitioning,
            Map<Index, Boolean> usefulness,
            Set<Index> recommendation,
            Map<Index, Double> benefits,
            Map<Set<Index>, Double> workFunctionScores)
    {
        Entry e = addNewEntry(sql, totalCost, candidateSet, partitioning, recommendation, benefits);

        e.workFunctionScores = workFunctionScores;
        e.usefulness = usefulness;

        return e;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Entry addNewEntry(
            SQLStatement sql,
            double totalCost,
            Set<Index> candidateSet,
            Set<Set<Index>> partitioning,
            Set<Index> recommendation,
            Map<Index, Double> benefits)
    {
        super.addNewEntry(sql, totalCost, candidateSet, partitioning, recommendation, benefits);

        RecommendationStatistics.Entry e = super.entries.remove(entries.size() - 1);

        Entry wfitEntry = new Entry(e);

        super.entries.add(wfitEntry);

        return wfitEntry;
    }

    /**
     * Extends an entry by adding information about the stable candidate partitioning.
     */
    public static class Entry extends RecommendationStatistics.Entry
    {
        private Map<Set<Index>, Double> workFunctionScores;
        private Map<Index, Boolean> usefulness;
        
        /**
         * copy constructor.
         *
         * @param e
         *      entry being copied
         */
        public Entry(RecommendationStatistics.Entry e)
        {
            this.sql = e.getSql();
            this.benefit = e.getBenefit();
            this.candidateSet = e.getCandidateSet();
            this.previousRecommendation = e.getPreviousRecommendation();
            this.recommendation = e.getRecommendation();
            this.benefits = e.getBenefits();
            this.transitionCost = e.getTransitionCost();
            this.totalCost = e.getTotalCost();
            this.totalWork = e.getTotalWork();
            this.candidatePartitioning = e.getCandidatePartitioning();
        }

        /**
         * Gets the workFunctionScores for this instance.
         *
         * @return The workFunctionScores.
         */
        public Map<Set<Index>, Double> getWorkFunctionScores()
        {
            return this.workFunctionScores;
        }

        /**
         * Gets the usefulness for this instance.
         *
         * @return The usefulness.
         */
        public Map<Index, Boolean> getUsefulness()
        {
            return this.usefulness;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();

            sb.append(super.toString());
            sb.append("   ").append("scores:\n");

            for (Set<Index> partition : candidatePartitioning) {
                int subsetNum = 1;
                for (Set<Index> subset : Sets.powerSet(partition))
                    sb.append("      [").append(subsetNum++).append("]")
                        .append(" = ").append(workFunctionScores.get(subset));
            }

            return sb.toString();
        }
    }
}
