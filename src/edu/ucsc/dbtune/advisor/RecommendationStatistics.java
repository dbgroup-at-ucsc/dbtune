package edu.ucsc.dbtune.advisor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import edu.ucsc.dbtune.advisor.RecommendationStatistics.Entry;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;

/**
 * Holds information about the advising process.
 *
 * @author Ivo Jimenez
 */
public class RecommendationStatistics implements Iterable<Entry>
{
    private List<Entry> entries = new ArrayList<Entry>();
    private double totalWorkSum;
    private String algorithmName;

    /**
     * @param algorithmName
     *      name of the algorithm that produced the recommendation
     */
    public RecommendationStatistics(String algorithmName)
    {
        this.algorithmName = algorithmName;
    }

    /**
     * Adds a new entry to the statistics.
     * @param statement
     *      statement that the entry corresponds to
     * @param indexes
     *      indexes that the entry corresponds to
     * @param transitionCost
     *      transitionCost that the entry corresponds to
     */
    public void addNewEntry(
            ExplainedSQLStatement statement,
            Collection<Index> indexes,
            double transitionCost)
    {
        Entry e = new Entry();

        totalWorkSum += statement.getTotalCost() + transitionCost;

        e.statement = statement;
        e.indexes = indexes;
        e.transitionCost = transitionCost;
        e.totalWork = totalWorkSum;

        entries.add(e);
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
     * Gets the algorithmName for this instance.
     *
     * @return The algorithmName.
     */
    public String getAlgorithmName()
    {
        return this.algorithmName;
    }

    /**
     * Holds the data of an entry.
     */
    public static class Entry
    {
        private Collection<Index> indexes;
        private ExplainedSQLStatement statement;
        private double benefit;
        private double transitionCost;
        private double totalWork;

        /**
         * Gets the indexes for this instance.
         *
         * @return The indexes.
         */
        public Collection<Index> getIndexes()
        {
            return this.indexes;
        }

        /**
         * Gets the statement for this instance.
         *
         * @return The statement.
         */
        public ExplainedSQLStatement getStatement()
        {
            return this.statement;
        }

        /**
         * Gets the statement for this instance.
         *
         * @return The statement.
         */
        public double getCost()
        {
            return this.statement.getTotalCost();
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
    }
}
