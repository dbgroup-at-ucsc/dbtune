package edu.ucsc.dbtune.advisor.wfit;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.MetadataUtils;

/**
 * Transforms data structures in the format the the SATuning API expects them, i.e. mainly BitSet- 
 * and array-based ones.
 *
 * @author Ivo Jimenez
 */
public class SATuningDBTuneTranslator
{
    private final IndexStatistics idxStats;
    private final WorkFunctionAlgorithm wfa;
    private final DynamicIndexSet matSet;
    private StaticIndexSet hotSet;
    private final DynamicIndexSet userHotSet;
    private IndexPartitions hotPartitions;
    // hotSet size
    private int maxHotSetSize = Environment.getInstance().getMaxNumIndexes();
    private int maxNumStates = Environment.getInstance().getMaxNumStates();
    private List<ProfiledQuery> qinfos;
    private int queryCount;
    private int minId;
    
    /**
     * @param minId
     *      minimum id among the all the possible indexes that will be sent "down" to WFA and its 
     *      dependencies.
     */
    public SATuningDBTuneTranslator(int minId)
    {
        this(new HashSet<Index>(), minId);
    }

    /**
     * @param minId
     *      minimum id among the all the possible indexes that will be sent "down" to WFA and its 
     *      dependencies.
     * @param initialSet
     *      the initial candidate set to be used 
     */
    public SATuningDBTuneTranslator(Set<Index> initialSet, int minId)
    {
        idxStats = new IndexStatistics(minId);
        matSet = new DynamicIndexSet(minId);
        userHotSet = new DynamicIndexSet(minId);
        qinfos = new ArrayList<ProfiledQuery>();
        queryCount = 0;
        this.minId = minId;

        hotSet = new StaticIndexSet(initialSet.toArray(new Index[0]));
        hotPartitions = new IndexPartitions(hotSet, minId);
        wfa = new WorkFunctionAlgorithm(hotPartitions, true, minId);
    }

    /**
     * Perform the per-query tasks that are done after profiling. *
     * @param qinfo
     *      a profiled query
     * @return
     *      the WFA analysis, contained in the given analyzed query
     */
    public AnalyzedQuery analyzeQuery(ProfiledQuery qinfo)
    {
        // add the query to the statistics repository
        idxStats.addQuery(qinfo, matSet);

        reorganizeCandidates(qinfo.candidateSet);
        
        wfa.newTask(qinfo);

        queryCount++;
        qinfos.add(qinfo);

        return new AnalyzedQuery(qinfo, hotPartitions.bitSetArray());
    }

    /**
     * Returns the cost for a query using the given configuration. Looks at the list of {@link 
     * ProfiledQuery profiled} queries and uses the one corresponding to the given query id.
     *
     * @param queryId
     *      id of the query to explain
     * @param conf
     *      configuration used to explain the corresponding statement.
     * @return
     *      cost of the query
     * @throws IndexOutOfBoundsException
     *      if {@code queryId} is negative or greater than the total number of statements seen so 
     *      far.
     */
    public double getCost(int queryId, Set<Index> conf)
    {
        return qinfos.get(queryId).cost(conf);
    }
    
    /**
     * Returns the current candidate set partitioning.
     *
     * @param pool
     *      pool of all the candidate indexes referenced in any step
     * @return
     *      a set of sets of indexes
     */
    public Set<Set<Index>> getStablePartitioning(Set<Index> pool)
    {
        Set<Set<Index>> partitioning = new HashSet<Set<Index>>();

        for (BitSet bs : hotPartitions.bitSetArray())
            partitioning.add(new HashSet<Index>(toSet(bs, pool, minId)));

        return partitioning;
    }
    
    /**
     * @return
     *      recommendation for the last seen statement
     */
    public Set<Index> getRecommendation()
    {
        return wfa.getRecommendation();
    }

    /**
     * @return
     *      the number of queries seen so far
     */
    public int getQueryCount()
    {
        return queryCount;
    }
    
    /**
     * votes an index up.
     *
     * @param isPositive
     *      whether the vote is positive or not
     * @param index
     *      an index being voted
     */
    public void vote(Index index, boolean isPositive)
    {
        wfa.vote(index, isPositive);
    }
    
    /**
     * reorganizes the internal candidate set based on a new incoming set.
     *
     * @param candSet
     *      new set of indexes
     */
    private void reorganizeCandidates(Set<Index> candSet)
    {
        // determine the hot set
        DynamicIndexSet reqIndexes = new DynamicIndexSet(minId);
        for (Index index : userHotSet) reqIndexes.add(index);
        for (Index index : matSet) reqIndexes.add(index);
        StaticIndexSet newHotSet = 
            HotSetSelector.chooseHotSet(candSet, hotSet, reqIndexes, idxStats, maxHotSetSize);
        
        // determine new partitioning
        // store into local variable, since we might reject it
        IndexPartitions newHotPartitions = InteractionSelector.choosePartitions(newHotSet, 
                hotPartitions, idxStats, maxNumStates, minId);
        
        // commit hot set
        hotSet = newHotSet;
        if (hotSet.size() > maxHotSetSize) {
            maxHotSetSize = hotSet.size();
            //Debug.logNotice("Maximum number of monitored indexes has been automatically increased 
            //to " + maxHotSetSize);
        }
        
        // commit new partitioning
        if (!newHotPartitions.equals(hotPartitions)) {
            hotPartitions = newHotPartitions;
            wfa.repartition(hotPartitions);
        }
    }

    /**
     * Converts a bitSet to a set of indexes.
     *
     * @param bs
     *      bitset containing ids of indexes being looked for
     * @param indexes
     *      set of indexes where one with the given id is being looked for
     * @param minimumId
     *      minimum id to consider when indexing arrays
     * @return
     *      the index with the given id; {@code null} if not found
     */
    public static Set<Index> toSet(BitSet bs, Set<Index> indexes, int minimumId)
    {
        Set<Index> indexSet = new HashSet<Index>();

        for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1))
            indexSet.add(MetadataUtils.findOrThrow(indexes, i + minimumId));

        return indexSet;
    }

    /**
     * Converts a bitSet to a set of indexes.
     *
     * @param bs
     *      bitset containing ids of indexes being looked for
     * @param indexes
     *      set of indexes where one with the given id is being looked for
     * @return
     *      the index with the given id; {@code null} if not found
     */
    public static Set<Index> toSet(BitSet bs, Set<Index> indexes)
    {
        return toSet(bs, indexes, WFIT.getMinimumId(indexes));
    }
    
    /**
     * Returns the recommendation corresponding to {@code OPT}.
     *
     * @param pool
     *      pool of all the candidate indexes referenced in any step
     * @return
     *      a list containing a recommendation in each element. Each element in the list corresponds 
     *      to a query, in the order they've been passed to the {@code Selector}
     */
    public List<Set<Index>> getOptimalScheduleRecommendation(Set<Index> pool)
    {
        BitSet[] optimalSchedule =
            wfa.getTrace().optimalSchedule(
                hotPartitions, queryCount, qinfos.toArray(new ProfiledQuery[0]));
        List<Set<Index>> optimalRecs = new ArrayList<Set<Index>>();

        for (BitSet bs : optimalSchedule)
            optimalRecs.add(toSet(bs, pool));

        return optimalRecs;
    }
}
