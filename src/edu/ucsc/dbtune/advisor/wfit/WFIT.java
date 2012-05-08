package edu.ucsc.dbtune.advisor.wfit;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import edu.ucsc.dbtune.advisor.Advisor;
import edu.ucsc.dbtune.advisor.RecommendationStatistics;
import edu.ucsc.dbtune.advisor.interactions.DegreeOfInteractionFinder;
import edu.ucsc.dbtune.advisor.interactions.IBGDoiFinder;
import edu.ucsc.dbtune.advisor.interactions.InteractionBank;

import edu.ucsc.dbtune.metadata.Index;

import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.PreparedSQLStatement;

import edu.ucsc.dbtune.workload.SQLStatement;

import static edu.ucsc.dbtune.util.MetadataUtils.transitionCost;

/**
 * @author Ivo Jimenez
 */
public class WFIT extends Advisor
{
    private Optimizer optimizer;
    private Selector selector;
    private Set<Index> pool;
    private DegreeOfInteractionFinder doiFinder;
    private RecommendationStatistics stats;
    private Set<Index> previousState;
    private boolean isCandidateSetFixed;

    /**
     * Creates a WFIT advisor.
     *
     * @param optimizer
     *      used to execute what-if calls
     */
    public WFIT(Optimizer optimizer)
    {
        this(optimizer, new HashSet<Index>());

        this.isCandidateSetFixed = false;
    }

    /**
     * Creates a WFIT advisor.
     *
     * @param optimizer
     *      used to execute what-if calls
     * @param initialSet
     *      initial candidate set
     */
    public WFIT(Optimizer optimizer, Set<Index> initialSet)
    {
        this(optimizer, initialSet, new IBGDoiFinder());
    }

    /**
     * Creates a WFIT advisor.
     *
     * @param optimizer
     *      used to execute what-if calls
     * @param initialSet
     *      initial candidate set
     * @param doiFinder
     *      interaction finder
     */
    public WFIT(
            Optimizer optimizer, Set<Index> initialSet, DegreeOfInteractionFinder doiFinder)
    {
        this.optimizer = optimizer;
        this.doiFinder = doiFinder;

        int minId;

        if (initialSet.isEmpty())
            minId = Index.IN_MEMORY_ID.get();
        else
            minId = getMinimumId(initialSet);

        this.selector = new Selector(initialSet, minId);
        this.pool = new TreeSet<Index>(initialSet);
        this.stats = new WFITRecommendationStatistics("WFIT");
        this.previousState = new HashSet<Index>();
        this.isCandidateSetFixed = true;
    }

    /**
     * Adds a query to the set of queries that are considered for
     * recommendation.
     * 
     * @param sql
     *            sql statement
     * @throws SQLException
     *             if the given statement can't be processed
     */
    @Override
    public void process(SQLStatement sql) throws SQLException
    {
        if (!isCandidateSetFixed)
            pool.addAll(optimizer.recommendIndexes(sql));

        PreparedSQLStatement  pStmt = optimizer.prepareExplain(sql);
        ExplainedSQLStatement eStmt = pStmt.explain(pool);
        InteractionBank       bank  = doiFinder.degreeOfInteraction(pStmt, pool);

        selector.analyzeQuery(
                new ProfiledQuery(
                    sql.getSQL(),
                    pStmt,
                    eStmt,
                    pool,
                    bank,
                    0));

        addRecommendationStatisticsEntry(eStmt);
    }

    /**
     * Adds a new entry to the running {@link RecommendationStatistics} objects.
     *
     * @param eStatement
     *      statement that has just been added to WFIT
     */
    private void addRecommendationStatisticsEntry(ExplainedSQLStatement eStatement)
    {
        if (this.previousState == null)
            throw new RuntimeException("Previous state hasn't been initialized");

        Set<Index> newState = new HashSet<Index>();

        newState.addAll(selector.getRecommendation());
        
        WFITRecommendationStatistics.Entry e =
            (WFITRecommendationStatistics.Entry)
            stats.addNewEntry(
                eStatement.getTotalCost(),
                eStatement.getConfiguration(),
                selector.getRecommendation(),
                transitionCost(this.previousState, newState));

        e.setCandidatePartitioning(selector.getStablePartitioning(pool));

        this.previousState = newState;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Index> getRecommendation() throws SQLException
    {
        return selector.getRecommendation();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecommendationStatistics getRecommendationStatistics()
    {
        return stats;
    }

    /**
     * The statistics corresponding to the idealized {@code OPT} algorithm.
     *
     * @return
     *      recommendation statistics for {@code OPT}
     * @throws SQLException
     *      if the candidate set wasn't specified from the beginning
     */
    public RecommendationStatistics getOptimalRecommendationStatistics()
        throws SQLException
    {
        if (!isCandidateSetFixed)
            throw new SQLException("Can't produce OPT without specifying an initial candidate set");
            
        RecommendationStatistics optStats = new RecommendationStatistics("OPT");

        Set<Index> prevState = new HashSet<Index>();
        Set<Index> newState = new HashSet<Index>();

        int i = 0;

        for (Set<Index> optRecommendation : selector.getOptimalScheduleRecommendation(pool)) {

            newState = optRecommendation;

            optStats.addNewEntry(
                    selector.getCost(i, newState),
                    pool,
                    newState,
                    transitionCost(prevState, newState));

            prevState = newState;
        }

        return optStats;
    }

    /**
     * Gets the pool for this instance.
     *
     * @return The pool.
     */
    public Set<Index> getPool()
    {
        return this.pool;
    }

    /**
     * Returns the minimum id.
     *
     * @param indexes
     *      a collection of indexes
     * @return
     *      the minimum id
     */
    public static int getMinimumId(Collection<Index> indexes)
    {
        int minId = Integer.MAX_VALUE;

        for (Index i : indexes)
            if (i.getId() < minId)
                minId = i.getId();

        return minId;
    }
}
