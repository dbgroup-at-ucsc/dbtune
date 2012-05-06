package edu.ucsc.dbtune.advisor.wfit;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.advisor.Advisor;
import edu.ucsc.dbtune.advisor.RecommendationStatistics;
import edu.ucsc.dbtune.advisor.interactions.DegreeOfInteractionFinder;
import edu.ucsc.dbtune.advisor.interactions.IBGDoiFinder;
import edu.ucsc.dbtune.advisor.interactions.InteractionBank;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.IBGOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.PreparedSQLStatement;
import edu.ucsc.dbtune.workload.SQLStatement;

import static edu.ucsc.dbtune.util.MetadataUtils.getMinimumId;
//import static edu.ucsc.dbtune.util.MetadataUtils.toSet;
//import static edu.ucsc.dbtune.advisor.wfit.WorkFunctionAlgorithm.transitionCost;

/**
 * @author Ivo Jimenez
 */
public class WFIT extends Advisor
{
    private IBGOptimizer ibgOptimizer;
    private Selector selector;
    private Set<Index> pool;
    private DegreeOfInteractionFinder doiFinder;
    private RecommendationStatistics stats;
    //private BitSet previousState;
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

        isCandidateSetFixed = false;
        selector = new Selector(Index.IN_MEMORY_ID.get());
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
        if (!(optimizer instanceof IBGOptimizer))
            throw new RuntimeException(
                    "Expecting IBGOptimizer; found: " + optimizer.getClass().getName());

        ibgOptimizer = (IBGOptimizer) optimizer;

        int minId = getMinimumId(initialSet);
        selector = new Selector(initialSet, minId);
        doiFinder = new IBGDoiFinder();
        pool = new HashSet<Index>(initialSet);
        //stats = new WFITRecommendationStatistics("WFIT");
        //previousState = new BitSet();
        isCandidateSetFixed = true;
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
            if (pool.addAll(ibgOptimizer.recommendIndexes(sql)))
                System.out.println("added new: " + pool);

        PreparedSQLStatement  pStmt = ibgOptimizer.prepareExplain(sql);
        ExplainedSQLStatement eStmt = pStmt.explain(pool);
        InteractionBank       bank  = doiFinder.degreeOfInteraction(pStmt, pool);

        System.out.println("bank:\n" + bank);

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
        BitSet newState = new BitSet();

        for (Index idx : selector.getRecommendation())
            newState.set(idx.getId());
        
        //WFITRecommendationStatistics.Entry e =
            //(WFITRecommendationStatistics.Entry)
            //stats.addNewEntry(
                //eStatement.getTotalCost(),
                //eStatement.getConfiguration(),
                //selector.getRecommendation(),
                //transitionCost(getSnapshot(pool), previousState, newState));

        //e.setCandidatePartitioning(getStablePartitioning(selector.getStablePartitioning()));

        //previousState = newState;
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
     */
    public RecommendationStatistics getOptimalRecommendationStatistics()
    {
        /*
        RecommendationStatistics optStats = new WFITRecommendationStatistics("OPT");
        BitSet[] optimalSchedule = selector.getOptimalScheduleRecommendation();

        BitSet prevState = new BitSet();

        int i = 0;

        for (BitSet bs : optimalSchedule) {

            BitSet newState = new BitSet();

            newState.set(bs);

            WFITRecommendationStatistics.Entry e =
                (WFITRecommendationStatistics.Entry)
                optStats.addNewEntry(
                    selector.getCost(i, bs),
                    pool,
                    convert(optimalSchedule[i], pool),
                    transitionCost(getSnapshot(pool), prevState, newState));

            e.setCandidatePartitioning(getStablePartitioning(selector.getStablePartitioning()));

            prevState = newState;
        }

        return optStats;
        */
        return null;
    }

    /**
     * Converts a bit array into a set partition of indexes.
     *
     * @param bitSets
     *      an array of bit set objects
     * @return
     *      a set of sets of indexes
    private Set<Set<Index>> getStablePartitioning(BitSet[] bitSets)
    {
        Set<Set<Index>> partitioning = new HashSet<Set<Index>>();

        for (BitSet bs : bitSets)
            partitioning.add(new HashSet<Index>(toSet(bs, pool)));

        return partitioning;
    }
     */
}
