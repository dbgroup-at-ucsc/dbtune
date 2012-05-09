package edu.ucsc.dbtune.advisor.wfit;

import java.util.HashSet;
import java.util.Set;

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
     * {@inheritDoc}
     */
    @Override
    public Entry addNewEntry(
            double totalCost,
            Set<Index> candidateSet,
            Set<Index> recommendation)
    {
        super.addNewEntry(totalCost, candidateSet, recommendation);

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
        private Set<Set<Index>> candidatePartitioning;

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
            this.candidatePartitioning = new HashSet<Set<Index>>();
        }

        /**
         * Gets the candidatePartitioning for this instance.
         *
         * @return The candidatePartitioning.
         */
        public Set<Set<Index>> getCandidatePartitioning()
        {
            return this.candidatePartitioning;
        }

        /**
         * Sets the candidatePartitioning for this instance.
         *
         * @param candidatePartitioning The candidatePartitioning.
         */
        public void setCandidatePartitioning(Set<Set<Index>> candidatePartitioning)
        {
            this.candidatePartitioning = candidatePartitioning;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();

            sb.append(super.toString());
            sb.append("   ").append("partitions:\n");

            int i = 1;

            for (Set<Index> partition : candidatePartitioning)
                sb.append("      ").append(i++).append(": ").append(partition).append("\n");

            return sb.toString();
        }
    }
}
