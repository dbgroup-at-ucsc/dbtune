package edu.ucsc.dbtune.advisor.wfit;

import java.sql.SQLException;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import edu.ucsc.dbtune.DatabaseSystem;

import edu.ucsc.dbtune.advisor.RecommendationStatistics;
import edu.ucsc.dbtune.advisor.VoteableAdvisor;
import edu.ucsc.dbtune.advisor.WindowingAdvisor;
import edu.ucsc.dbtune.advisor.WorkloadObserverAdvisor;

import edu.ucsc.dbtune.advisor.interactions.DegreeOfInteractionFinder;
import edu.ucsc.dbtune.advisor.interactions.IBGDoiFinder;
import edu.ucsc.dbtune.advisor.interactions.InteractionBank;

import edu.ucsc.dbtune.metadata.Index;

import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.PreparedSQLStatement;

import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import static edu.ucsc.dbtune.util.MetadataUtils.findOrThrow;
import static edu.ucsc.dbtune.util.OptimizerUtils.getBenefits;

/**
 * @author Ivo Jimenez
 */
public class WFIT extends WorkloadObserverAdvisor implements VoteableAdvisor, WindowingAdvisor
{
    /**
     * in order to make the advisor behave as a non-observer, we just assign a workload that doesn't
     * correspond to any workload that a statement will have. See {@link WorkloadObserverAdvisor}
     * for more.
     */
    private static final Workload NO_WORKLOAD = new Workload("no_workload_given");

    private DatabaseSystem db;
    private SATuningDBTuneTranslator wfitDriver;
    private Set<Index> pool;
    private DegreeOfInteractionFinder doiFinder;
    private WFITRecommendationStatistics stats;
    private WFITRecommendationStatistics optStats;
    private boolean isCandidateSetFixed;

    /**
     * Creates a WFIT advisor, with an empty initial candidate set.
     *
     * @param db
     *      the dbms where wfit will run on
     * @param workload
     *      workload that the advisor will be observing
     * @param initialSet
     *      initial candidate set
     * @param isPaused
     *      whether the advisor goes on automatically after instantiating or pauses
     * @param maxNumberOfStates
     *      maximum number of states per partition
     * @param maxHotSetSize
     *      maximum number of candidates to keep in the hot set
     * @param indexStatisticsWindowSize
     *      size of the sliding window of interaction-related measurements
     * @param numberOfPartitionIterations
     *      number of attempts that the repartitioning algorithm executes to stabilize the candidate
     *      set partition
     */
    public WFIT(
        DatabaseSystem db,
        Workload workload,
        Set<Index> initialSet,
        boolean isPaused,
        int maxNumberOfStates,
        int maxHotSetSize,
        int indexStatisticsWindowSize,
        int numberOfPartitionIterations)
    {
        this(db, workload, initialSet, isPaused, new IBGDoiFinder(), maxNumberOfStates, 
                maxHotSetSize, indexStatisticsWindowSize, numberOfPartitionIterations);
    }

    /**
     * Creates a WFIT advisor, with an empty initial candidate set.
     *
     * @param db
     *      the dbms where wfit will run on
     * @param maxNumberOfStates
     *      maximum number of states per partition
     * @param maxHotSetSize
     *      maximum number of candidates to keep in the hot set
     * @param indexStatisticsWindowSize
     *      size of the sliding window of interaction-related measurements
     * @param numberOfPartitionIterations
     *      number of attempts that the repartitioning algorithm executes to stabilize the candidate
     *      set partition
     */
    public WFIT(
        DatabaseSystem db,
        int maxNumberOfStates,
        int maxHotSetSize,
        int indexStatisticsWindowSize,
        int numberOfPartitionIterations)
    {
        this(db, new TreeSet<Index>(), maxHotSetSize, indexStatisticsWindowSize,
                maxNumberOfStates, numberOfPartitionIterations);

        this.isCandidateSetFixed = false;
    }

    /**
     * Creates a WFIT advisor, with the given candidate set as the initial candidate set.
     *
     * @param db
     *      the dbms where wfit will run on
     * @param initialSet
     *      initial candidate set
     * @param maxNumberOfStates
     *      maximum number of states per partition
     * @param maxHotSetSize
     *      maximum number of candidates to keep in the hot set
     * @param indexStatisticsWindowSize
     *      size of the sliding window of interaction-related measurements
     * @param numberOfPartitionIterations
     *      number of attempts that the repartitioning algorithm executes to stabilize the candidate
     *      set partition
     */
    public WFIT(
            DatabaseSystem db,
            Set<Index> initialSet,
            int maxNumberOfStates,
            int maxHotSetSize,
            int indexStatisticsWindowSize,
            int numberOfPartitionIterations)
    {
        this(db, NO_WORKLOAD, initialSet, true, new IBGDoiFinder(), maxNumberOfStates, 
                maxHotSetSize, indexStatisticsWindowSize, numberOfPartitionIterations);
    }

    /**
     * Creates a WFIT advisor with the given initial candidate set, DoI finder and components
     * parameters.
     *
     * @param db
     *      the dbms where wfit will run on
     * @param workload
     *      workload that the advisor will be observing
     * @param initialSet
     *      initial candidate set
     * @param isPaused
     *      whether the advisor goes on automatically after instantiating or pauses
     * @param doiFinder
     *      interaction finder
     * @param maxNumberOfStates
     *      maximum number of states per partition
     * @param maxHotSetSize
     *      maximum number of candidates to keep in the hot set
     * @param indexStatisticsWindowSize
     *      size of the sliding window of interaction-related measurements
     * @param numberOfPartitionIterations
     *      number of attempts that the repartitioning algorithm executes to stabilize the candidate
     *      set partition
     */
    WFIT(
            DatabaseSystem db,
            Workload workload,
            Set<Index> initialSet,
            boolean isPaused,
            DegreeOfInteractionFinder doiFinder,
            int maxNumberOfStates,
            int maxHotSetSize,
            int indexStatisticsWindowSize,
            int numberOfPartitionIterations)
    {
        super(workload, isPaused);

        this.db = db;
        this.doiFinder = doiFinder;

        this.wfitDriver =
            new SATuningDBTuneTranslator(
                    db.getCatalog(),
                    initialSet,
                    maxNumberOfStates,
                    maxHotSetSize,
                    indexStatisticsWindowSize,
                    numberOfPartitionIterations);

        this.pool = new TreeSet<Index>(initialSet);
        this.stats = new WFITRecommendationStatistics("WFIT" + maxNumberOfStates);
        this.optStats = new WFITRecommendationStatistics("OPT");

        //if (initialSet.isEmpty())
            this.isCandidateSetFixed = false;
        //else
            //this.isCandidateSetFixed = true;
    }

    /**
     * Adds a query to the set of queries that are considered for
     * recommendation.
     *
     * @param sql
     *      sql statement
     * @throws SQLException
     *      if the given statement can't be processed
     */
    @Override
    public void processNewStatement(SQLStatement sql) throws SQLException
    {
        if (!isCandidateSetFixed)
            pool.addAll(db.getOptimizer().recommendIndexes(sql));

        PreparedSQLStatement  pStmt = db.getOptimizer().prepareExplain(sql);
        ExplainedSQLStatement eStmt = pStmt.explain(pool);
        InteractionBank       bank  = doiFinder.degreeOfInteraction(pStmt, pool);

        wfitDriver.analyzeQuery(sql.getSQL(), pStmt, eStmt, pool, bank);

        Set<Index> recommendation = getRecommendation();

        if (isCandidateSetFixed)
            getOptimalRecommendationStatistics();

        stats.addNewEntry(
            sql,
            pStmt.explain(recommendation).getTotalCost(),
            pool,
            getStablePartitioning(),
            getUsefulnessMap(),
            recommendation,
            getBenefits(pStmt, recommendation),
            wfitDriver.getWorkFunctionScores(pool));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Index> getRecommendation() throws SQLException
    {
        return wfitDriver.getRecommendation();
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
     * {@inheritDoc}
     */
    public SortedSet<Map.Entry<Set<Index>, Double>> getWorkFunctionScores()
    {
        return entriesSortedByValuesDesc(wfitDriver.getWorkFunctionScores(getPool()));
    }

    //CHECKSTYLE:OFF
    private static <K,V extends Comparable<? super V>>
        SortedSet<Map.Entry<K,V>> entriesSortedByValues(Map<K,V> map)
    {
        SortedSet<Map.Entry<K,V>> sortedEntries = new TreeSet<Map.Entry<K,V>>(
                new Comparator<Map.Entry<K,V>>() {
                    @Override
                    public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2)
                    {
                        int res = e1.getValue().compareTo(e2.getValue());
                        return res != 0 ? res : 1;
                    }
                }
            );
        sortedEntries.addAll(map.entrySet());
        return sortedEntries;
    }
    private static <K,V extends Comparable<? super V>>
        SortedSet<Map.Entry<K,V>> entriesSortedByValuesDesc(Map<K,V> map)
    {
        SortedSet<Map.Entry<K,V>> sortedEntries = new TreeSet<Map.Entry<K,V>>(
                new Comparator<Map.Entry<K,V>>() {
                    @Override
                    public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2)
                    {
                        int res = e2.getValue().compareTo(e1.getValue());
                        return res != 0 ? res : 1;
                    }
                }
            );
        sortedEntries.addAll(map.entrySet());
        return sortedEntries;
    }
    //CHECKSTYLE:ON

    /**
     * Returns the usefulness map. This map rates the usefulness of each index in the candidate set
     * by comparing it against the indexes in the next step of the OPT schedule. An index is useful
     * if gets recommended in OPT and is not useful if it gets dropped.
     *
     * @return
     *      the usefulness map, which might be empty if {@link #isCandidateSetFixed} is {@code
     *      false}
     */
    public Map<Index, Boolean> getUsefulnessMap()
    {
        if (!isCandidateSetFixed)
            return new HashMap<Index, Boolean>();

        Map<Index, Boolean> usefulness = new HashMap<Index, Boolean>();

        for (Index idx : pool)
            if (optStats.getLastEntry().getRecommendation().contains(idx))
                usefulness.put(idx, true);
            else
                usefulness.put(idx, false);

        return usefulness;
    }

    /**
     * Returns the stable partitioning of the current candidate set.
     *
     * @return
     *      set of sets of indexes, where each set corresponds to a partition
     */
    public Set<Set<Index>> getStablePartitioning()
    {
        return wfitDriver.getStablePartitioning(pool);
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
        //if (!isCandidateSetFixed)
            //throw new SQLException("Can't produce OPT without specifying an initial candidate 
            //set");

        optStats.clear();

        int i = 0;

        for (Set<Index> optRecommendation : wfitDriver.getOptimalScheduleRecommendation(pool))
            optStats.addNewEntry(
                null,
                wfitDriver.getCost(i, optRecommendation),
                pool,
                new TreeSet<Set<Index>>(),
                optRecommendation,
                new HashMap<Index, Double>());

        return optStats;
    }

    /**
     * Adds a new index into the pool.
     */
    public void add(Index index)
    {
        if (this.pool.contains(index)) {
            throw new RuntimeException("index " + index + " already in pool ");
        }

        this.pool.add(index);
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
     * @return the isCandidateSetFixed
     */
    public boolean isCandidateSetFixed()
    {
        return isCandidateSetFixed;
    }

    /**
     * {@inheritDoc}
     */
    public void voteUp(Integer id)
        throws SQLException
    {
        voteUp(findOrThrow(pool, id));
    }

    /**
     * {@inheritDoc}
     */
    public void voteDown(Integer id) throws SQLException
    {
        voteDown(findOrThrow(pool, id));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<SQLStatement> getWindow()
    {
        return wfitDriver.getWindow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void vote(Index index, boolean up)
        throws SQLException
    {
        wfitDriver.vote(index, up);

        updateLastEntry();
    }

    /**
     * updates the last entry in the statistics.
     *
     * @throws SQLException
     *      if the last entry can't be updated
     */
    private void updateLastEntry()
        throws SQLException
    {
        if (stats.size() == 0)
            return;

        stats.getLastEntry().update(getRecommendation());
    }

    /**
     * returns the db.
     *
     * @return
     *      the db
     */
    public DatabaseSystem getDatabase()
    {
        return db;
    }
}
