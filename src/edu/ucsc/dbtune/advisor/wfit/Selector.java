package edu.ucsc.dbtune.advisor.wfit;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.MetadataUtils;

//CHECKSTYLE:OFF
public class Selector {
    private final IndexStatistics idxStats;
    private final WorkFunctionAlgorithm wfa;
    private final DynamicIndexSet matSet;
    private StaticIndexSet hotSet;
    private final DynamicIndexSet userHotSet;
    private IndexPartitions hotPartitions;
    private int maxHotSetSize = Environment.getInstance().getMaxNumIndexes(); // hotSet size
    private int maxNumStates = Environment.getInstance().getMaxNumStates();
    private List<ProfiledQuery> qinfos;
    private int queryCount;
    private int minId;
    
    public Selector(int minId)
    {
        this(new HashSet<Index>(), minId);
    }

    public Selector(Set<Index> initialSet, int minId)
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

    /*
     * Perform the per-query tasks that are done after profiling
     */
    public AnalyzedQuery analyzeQuery(ProfiledQuery qinfo) {
        // add the query to the statistics repository
        idxStats.addQuery(qinfo, matSet);

        reorganizeCandidates(qinfo.candidateSet);
        
        wfa.newTask(qinfo);

        queryCount++;
        qinfos.add(qinfo);

        return new AnalyzedQuery(qinfo, hotPartitions.bitSetArray());
    }

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
            partitioning.add(new HashSet<Index>(toSet(bs, pool)));

        return partitioning;
    }
    
    /*
     * Called by main thread to get a recommendation
     */
    public Set<Index> getRecommendation() {
        return wfa.getRecommendation();
    }
    
    public void positiveVote(Index index, Set<Index> candSet) {
        // get it in the hot set
        if (!userHotSet.contains(index)) {
            userHotSet.add(index);
            
            // ensure that userHotSet is a subset of HotSet
            if (!hotSet.contains(index)) {
                reorganizeCandidates(candSet);
            }
        }
        
        // Now the index is being monitored by WFA
        // Just need to bias the statistics in its favor
        wfa.vote(index, true);
    }
    
    public void negativeVote(Index index) {     
        // Check if the index is hot before doing anything.
        //
        // If the index is not being tracked by WFA, we have nothing to do.
        // Note that this check skips indexes that are not in 
        // the overall candidate pool.
        if (hotSet.contains(index)) {
            // ensure that the index is no longer forced in the hot set
            userHotSet.remove(index);
            
            // don't remove from the hot set necessarily
            
            // bias the statistics against the index
            wfa.vote(index, false);
        }
    }

    public double drop(Index index) {
        matSet.remove(index);
        return 0; // XXX: assuming no cost to drop
    }

    public double create(Index index) {
        if (!matSet.contains(index)) {
            matSet.add(index);
            return index.getCreationCost();
        }
        return 0;
    }

    public int getQueryCount()
    {
        return queryCount;
    }
    
    /* 
     * common code between positiveVote and processQuery 
     */
    private void reorganizeCandidates(Set<Index> candSet) {
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
     * @return
     *      the index with the given id; {@code null} if not found
     */
    public static Set<Index> toSet(BitSet bs, Set<Index> indexes)
    {
        Set<Index> indexSet = new HashSet<Index>();

        int minId = WFIT.getMinimumId(indexes);

        for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1)) {
            Index index = MetadataUtils.find(indexes, i + minId);

            if (index == null)
                throw new RuntimeException("Can't find index with id " + (i + minId));

            indexSet.add(index);
        }

        return indexSet;
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
//CHECKSTYLE:ON
