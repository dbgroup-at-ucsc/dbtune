package edu.ucsc.dbtune.advisor.wfit;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.advisor.wfit.CandidatePool.Snapshot;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Environment;

//CHECKSTYLE:OFF
public class Selector {
    private final IndexStatistics idxStats;
    private final WorkFunctionAlgorithm wfa;
    private final DynamicIndexSet matSet;
    private StaticIndexSet hotSet;
    private final DynamicIndexSet userHotSet;
    private IndexPartitions hotPartitions;
    private List<ProfiledQuery> qinfos;
    private int maxHotSetSize = Environment.getInstance().getMaxNumIndexes(); // hotSet size
    private int maxNumStates = Environment.getInstance().getMaxNumStates();
    private int queryCount;
    
    public Selector() {
        idxStats = new IndexStatistics();
        userHotSet = new DynamicIndexSet();
        matSet = new DynamicIndexSet();
        qinfos = new ArrayList<ProfiledQuery>();
        queryCount = 0;

        hotSet = new StaticIndexSet();
        hotPartitions = new IndexPartitions(hotSet);
        wfa = new WorkFunctionAlgorithm();
    }

    public Selector(Set<Index> initialSet) {
        idxStats = new IndexStatistics();
        matSet = new DynamicIndexSet();
        userHotSet = new DynamicIndexSet();
        qinfos = new ArrayList<ProfiledQuery>();
        queryCount = 0;

        hotSet = new StaticIndexSet(initialSet.toArray(new Index[0]));
        hotPartitions = new IndexPartitions(hotSet);
        wfa = new WorkFunctionAlgorithm(hotPartitions, true);
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

    public double getCost(int queryId, BitSet bs)
    {
        return qinfos.get(queryId).cost(bs);
    }
    
    /*
     * Called by main thread to get a recommendation
     */
    public List<Index> getRecommendation() {
        return wfa.getRecommendation();
    }
    
    /*
     * Called by main thread to get a recommendation
     */
    public BitSet[] getOptimalScheduleRecommendation() {
        return wfa.getTrace().optimalSchedule(
                hotPartitions, queryCount, qinfos.toArray(new ProfiledQuery[0]));
    }
    
    public void positiveVote(Index index, Snapshot candSet) {
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

    public double currentCost(ProfiledQuery qinfo) {
        return qinfo.cost(matSet.bitSet());
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
    private void reorganizeCandidates(Snapshot candSet) {
        // determine the hot set
        DynamicIndexSet reqIndexes = new DynamicIndexSet();
        for (Index index : userHotSet) reqIndexes.add(index);
        for (Index index : matSet) reqIndexes.add(index);
        StaticIndexSet newHotSet = 
            HotSetSelector.chooseHotSet(candSet, hotSet, reqIndexes, idxStats, maxHotSetSize);
        
        // determine new partitioning
        // store into local variable, since we might reject it
        IndexPartitions newHotPartitions = 
            InteractionSelector.choosePartitions(newHotSet, hotPartitions, idxStats, maxNumStates);
        
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
}
//CHECKSTYLE:ON
