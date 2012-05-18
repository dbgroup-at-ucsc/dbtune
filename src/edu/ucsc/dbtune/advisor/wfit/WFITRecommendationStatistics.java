package edu.ucsc.dbtune.advisor.wfit;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import edu.ucsc.dbtune.advisor.RecommendationStatistics;

import edu.ucsc.dbtune.metadata.Index;

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
     * @param totalCost
     *      the cost of exeucting the statement
     * @param candidateSet
     *      indexes that were in the context of the recommender
     * @param recommendation
     *      indexes that were recommended
     * @param candidateSetPartitioning
     *      partitioning of the candidate set
     * @param workFunctionScores
     *      scores the correspond to the work function values
     * @return
     *      the entry that has been just created
     */
    public Entry addNewEntry(
            double totalCost,
            Set<Index> candidateSet,
            Set<Index> recommendation,
            Set<Set<Index>> candidateSetPartitioning,
            Map<Set<Index>, Double> workFunctionScores)
    {
        Entry e = addNewEntry(totalCost, candidateSet, recommendation, candidateSetPartitioning);

        e.workFunctionScores = workFunctionScores;

        return e;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Entry addNewEntry(
            double totalCost,
            Set<Index> candidateSet,
            Set<Index> recommendation,
            Set<Set<Index>> candidateSetPartitioning)
    {
        super.addNewEntry(totalCost, candidateSet, recommendation, candidateSetPartitioning);

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
        
        /**
         * copy constructor.
         *
         * @param e
         *      entry being copied
         */
        public Entry(RecommendationStatistics.Entry e)
        {
            this.benefit = e.getBenefit();
            this.candidateSet = e.getCandidateSet();
            this.recommendation = e.getRecommendation();
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
