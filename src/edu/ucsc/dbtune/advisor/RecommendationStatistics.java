package edu.ucsc.dbtune.advisor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.advisor.RecommendationStatistics.Entry;
import edu.ucsc.dbtune.metadata.Index;

import static edu.ucsc.dbtune.util.MetadataUtils.transitionCost;

/**
 * Holds information about the advising process.
 *
 * @author Ivo Jimenez
 */
public class RecommendationStatistics implements Iterable<Entry>
{
    protected List<Entry> entries = new ArrayList<Entry>();
    private double totalWorkSum;
    private String algorithmName;
    private Set<Index> previousRecommendation;

    /**
     * @param algorithmName
     *      name of the algorithm that produced the recommendation
     */
    public RecommendationStatistics(String algorithmName)
    {
        this.algorithmName = algorithmName;
        this.previousRecommendation = new HashSet<Index>();
    }

    /**
     * Adds a new entry to the statistics.
     *
     * @param totalCost
     *      the cost of exeucting the statement
     * @param candidateSet
     *      indexes that were in the context of the recommender
     * @param recommendation
     *      indexes that were recommended
     * @param candidateSetPartitioning
     *      partitioning of the candidate set
     * @return
     *      the entry that has been just created
     */
    public Entry addNewEntry(
            double totalCost,
            Set<Index> candidateSet,
            Set<Index> recommendation,
            Set<Set<Index>> candidateSetPartitioning)
    {
        Entry e = new Entry();

        double transitionCost = transitionCost(previousRecommendation, recommendation);

        totalWorkSum += totalCost + transitionCost;

        e.totalCost = totalCost;
        e.candidateSet = candidateSet;
        e.recommendation = recommendation;
        e.transitionCost = transitionCost;
        e.totalWork = totalWorkSum;
        e.candidatePartitioning = candidateSetPartitioning;

        entries.add(e);

        previousRecommendation = recommendation;
        
        return e;
    }

    /**
     * Removes all the entries.
     */
    public void clear()
    {
        entries.clear();
        previousRecommendation = new HashSet<Index>();
        totalWorkSum = 0.0;
    }

    /**
     * Returns the number of entries.
     *
     * @return
     *      number of elements in the stats
     */
    public int size()
    {
        return entries.size();
    }

    /**
     * Returns the entry with the given index.
     *
     * @param i
     *      index of the entry
     * @return
     *      an entry
     * @throws IndexOutOfBoundsException
     *      if is less than zero or greater than the size (minus one).
     */
    public Entry get(int i)
    {
        return entries.get(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Entry> iterator()
    {
        return entries.iterator();
    }

    /**
     * Gets the totalWorkSum for this instance.
     *
     * @return The totalWorkSum.
     */
    public double getTotalWorkSum()
    {
        return this.totalWorkSum;
    }

    /**
     * Sets the algorithmName for this instance.
     *
     * @param algorithmName The algorithmName.
     */
    public void setAlgorithmName(String algorithmName)
    {
        this.algorithmName = algorithmName;
    }

    /**
     * Gets the algorithmName for this instance.
     *
     * @return The algorithmName.
     */
    public String getAlgorithmName()
    {
        return this.algorithmName;
    }

    /**
     * Returns the last entry.
     *
     * @return
     *      the last entry; {@code null} if the list is empty
     */
    public RecommendationStatistics.Entry getLastEntry()
    {
        if (entries.isEmpty())
            return null;

        return entries.get(entries.size() - 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        int i = 1;

        for (Entry e : entries)
            sb.append("Statement ").append(i++).append("\n").append(e.toString());

        return sb.toString();
    }

    /**
     * Holds the data of an entry.
     */
    public static class Entry
    {
        protected Collection<Index> candidateSet;
        protected Collection<Index> recommendation;
        protected Set<Set<Index>> candidatePartitioning;
        protected double benefit;
        protected double transitionCost;
        protected double totalWork;
        protected double totalCost;

        /**
         * Gets the candidateSet for this instance.
         *
         * @return The candidateSet.
         */
        public Collection<Index> getCandidateSet()
        {
            return this.candidateSet;
        }

        /**
         * Gets the indexes for this instance.
         *
         * @return The indexes.
         */
        public Collection<Index> getRecommendation()
        {
            return this.recommendation;
        }

        /**
         * Gets the statement for this instance.
         *
         * @return The statement.
         */
        public double getCost()
        {
            return this.totalCost;
        }

        /**
         * Gets the benefit for this instance.
         *
         * @return The benefit.
         */
        public double getBenefit()
        {
            return this.benefit;
        }

        /**
         * Gets the transitionCost for this instance.
         *
         * @return The transitionCost.
         */
        public double getTransitionCost()
        {
            return this.transitionCost;
        }

        /**
         * Gets the totalWork for this instance.
         *
         * @return The totalWork.
         */
        public double getTotalWork()
        {
            return this.totalWork;
        }

        /**
         * Gets the totalCost for this instance.
         *
         * @return The totalCost.
         */
        public double getTotalCost()
        {
            return this.totalCost;
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

            sb.append("   ").append("cost: ").append(totalCost).append("\n");
            sb.append("   ").append("transitionCost: ").append(transitionCost).append("\n");
            sb.append("   ").append("totalWork: ").append(totalWork).append("\n");
            sb.append("   ").append("candidates:\n      ").append(candidateSet).append("\n");
            sb.append("   ").append("recommendation:\n      ").append(recommendation).append("\n");
            sb.append("   ").append("partitions:\n");

            int i = 1;

            for (Set<Index> partition : candidatePartitioning)
                sb.append("      ").append(i++).append(": ").append(partition).append("\n");

            return sb.toString();
        }
    }
}
