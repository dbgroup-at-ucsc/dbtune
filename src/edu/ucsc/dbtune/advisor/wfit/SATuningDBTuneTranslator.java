package edu.ucsc.dbtune.advisor.wfit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import edu.ucsc.dbtune.advisor.interactions.InteractionBank;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;

import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.PreparedSQLStatement;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.MetadataUtils;

import static edu.ucsc.dbtune.util.MetadataUtils.getDisplayList;

/**
 * Transforms data structures in the format the the SATuning API expects them, i.e. mainly BitSet- 
 * and array-based ones.
 *
 * @author Ivo Jimenez
 */
public class SATuningDBTuneTranslator
{
    private IndexStatistics idxStats;
    private WorkFunctionAlgorithm wfa;
    private DynamicIndexSet matSet;
    private StaticIndexSet hotSet;
    private DynamicIndexSet userHotSet;
    private IndexPartitions hotPartitions;

    // hotSet size
    private int maxHotSetSize = Environment.getInstance().getMaxNumIndexes();
    private int maxNumStates = Environment.getInstance().getMaxNumStates();
    private List<ProfiledQuery> qinfos;
    private int queryCount;
    private int idOffset;
    
    /**
     * @param cat
     *      when no initial set is used, the catalog must be provided
     * @param initialSet
     *      the initial candidate set to be used 
     */
    public SATuningDBTuneTranslator(Catalog cat, Set<Index> initialSet)
    {
        if (initialSet.isEmpty())
            init(cat.indexes(), true);
        else
            init(initialSet, false);
    }

    /**
     * @param initialSet
     *      the initial candidate set to be used
     * @param fromCatalog
     *      whether the initialSet was provided by the caller or was obtained from the catalog
     */
    private void init(Set<Index> initialSet, boolean fromCatalog)
    {
        idxStats = new IndexStatistics(idOffset);
        matSet = new DynamicIndexSet(idOffset);
        userHotSet = new DynamicIndexSet(idOffset);
        qinfos = new ArrayList<ProfiledQuery>();
        queryCount = 0;

        if (!isInitialSetValid(initialSet))
            throw new RuntimeException(
                "Initial set should contain indexes with consecutive IDs, the given has " + 
                getDisplayList(new TreeSet<Index>(initialSet), "   "));

        if (fromCatalog) {
            idOffset = getMaximumId(initialSet) + 1;
            hotSet = new StaticIndexSet();
        } else {
            hotSet = new StaticIndexSet(initialSet.toArray(new Index[0]));
            idOffset = getMinimumId(initialSet);
        }

        hotPartitions = new IndexPartitions(hotSet, idOffset);
        wfa = new WorkFunctionAlgorithm(hotPartitions, true, idOffset);
    }

    /**
     * Perform the per-query tasks that are done after profiling.
     *
     * @param sql
     *      string of statement
     * @param pStmt
     *      prepared statement
     * @param eStmt
     *      explained statement
     * @param candidateSet
     *      candidate set
     * @param bank
     *      interaction bank
     */
    public void analyzeQuery(
            String sql,
            PreparedSQLStatement pStmt,
            ExplainedSQLStatement eStmt,
            Set<Index> candidateSet,
            InteractionBank bank)
    {
        analyzeQuery(new ProfiledQuery(sql, pStmt, eStmt, candidateSet, bank, idOffset));
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
            partitioning.add(new HashSet<Index>(toSet(bs, pool, idOffset)));

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
        DynamicIndexSet reqIndexes = new DynamicIndexSet(idOffset);
        for (Index index : userHotSet) reqIndexes.add(index);
        for (Index index : matSet) reqIndexes.add(index);
        StaticIndexSet newHotSet = 
            HotSetSelector.chooseHotSet(candSet, hotSet, reqIndexes, idxStats, maxHotSetSize);
        
        // determine new partitioning
        // store into local variable, since we might reject it
        IndexPartitions newHotPartitions = InteractionSelector.choosePartitions(newHotSet, 
                hotPartitions, idxStats, maxNumStates, idOffset);
        
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
     * @param idOffset
     *      offset to subtract when mapping index IDs to {@link Index} objects
     * @return
     *      the index with the given id; {@code null} if not found
     */
    public static Set<Index> toSet(BitSet bs, Set<Index> indexes, int idOffset)
    {
        Set<Index> indexSet = new HashSet<Index>();

        for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1))
            indexSet.add(MetadataUtils.findOrThrow(indexes, i + idOffset));

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
            optimalRecs.add(toSet(bs, pool, idOffset));

        return optimalRecs;
    }

    /**
     * Returns the minimum id.
     *
     * @param indexes
     *      a collection of indexes
     * @return
     *      the minimum id. Zero if the collection is empty
     */
    public static int getMinimumId(Collection<Index> indexes)
    {
        if (indexes.isEmpty())
            return 0;

        int minId = Integer.MAX_VALUE;

        for (Index i : indexes)
            if (i.getId() < minId)
                minId = i.getId();

        return minId;
    }

    /**
     * Returns the minimum id.
     *
     * @param indexes
     *      a collection of indexes
     * @return
     *      the minimum id. Zero if the collection is empty
     */
    public static int getMaximumId(Collection<Index> indexes)
    {
        if (indexes.isEmpty())
            return 0;

        int maxId = Integer.MIN_VALUE;

        for (Index i : indexes)
            if (i.getId() > maxId)
                maxId = i.getId();

        return maxId;
    }

    /**
     * Checks if the initial set contains consecutive IDs.
     *
     * @param initialSet
     *      the set to be checked
     * @return
     *      {@code true} if there are no "gaps" in the IDs of the given set; {@code false} 
     *      otherwise.
     */
    private static boolean isInitialSetValid(Set<Index> initialSet)
    {
        if (initialSet.isEmpty())
            return true;

        Set<Index> set = new TreeSet<Index>(initialSet);

        int previousId = -1;

        for (Index idx : set)
            if (previousId == -1)
                previousId = idx.getId();
            else if (idx.getId() != previousId + 1)
                return false;
            else
                previousId++;

        return true;
    }
}
