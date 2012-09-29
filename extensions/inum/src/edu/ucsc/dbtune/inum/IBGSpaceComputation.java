package edu.ucsc.dbtune.inum;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.seq.utils.RTimer;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;

import static com.google.common.collect.Sets.cartesianProduct;

import static edu.ucsc.dbtune.optimizer.plan.Operator.NLJ;
import static edu.ucsc.dbtune.util.MetadataUtils.getIndexesPerTable;

/**
 * IBG-based INUM space computation.
 * 
 * @author Rui Wang
 * @author Ivo Jimenez
 */
public class IBGSpaceComputation extends AbstractSpaceComputation 
{
    /**
     * number of what if call. In the worst case, the complexity of
     * IBG can went to the order of permutation. This keep a track
     * of the whatif calls made.
     */
    public int whatIfCount = 0;
    
    /**
     * If the number of what if calls exceed the maximum possible index combinations,
     * the algorithm will switch to use combination.
     */
    public int maxAllowedWhatIfCount = 0;
    
    /**
     * Tested configurations
     */
    private HashSet<BitSet> tested;
    
    /**
     * Give each index an unique id
     */
    private Hashtable<Index,Integer> indexToId;
    
    private RTimer timer;
    public static int maxTime=0;

    /**
     * TODO: complete.
     * 
     * @param statement
     *            todo
     * @param delegate
     *            todo
     * @param indexes
     *            todo
     * @param inumSpace
     *            todo
     * @throws SQLException
     *             todo
     */
    public void ibg(SQLStatement statement, Optimizer delegate,
            Set<Index> indexes, Set<InumPlan> inumSpace, String path)
            throws SQLException {
        if (indexes.isEmpty())
            return;
        if (whatIfCount > maxAllowedWhatIfCount)
            return;

        long timePassed=timer.get();
        if (maxTime > 0 && timePassed > maxTime)
            throw new SQLException("IBG timeout");
        timer.next("index=" + indexes.size() + " space=" + inumSpace.size()
                + " whatif=" + whatIfCount + " " + path);
        BitSet bitset=new BitSet();
        for (Index index : indexes) {
            int id= indexToId.get(index);
            bitset.set(id);
        }
        if ( tested.contains(bitset))
            return;
        tested.add(bitset);
        ExplainedSQLStatement estmt;
        try {
            estmt = delegate.explain(statement, indexes);
        } catch (SQLException e) {
            if ("Invalid plan, too many invalid nodes".equals(e.getMessage()))
                return;
            else
                throw e;
        }
        whatIfCount++;

        //Can't remember why I disabled the following code
        /*
        List<Set<Index>> intersectedIndexes = new ArrayList<Set<Index>>();
        Set<Index> notIntersectedIndexes = new HashSet<Index>();

        for (Set<Index> indexesForTable : getIndexesPerTable(estmt.getPlan().getIndexes()).values()) 
        {
            if (indexesForTable.size() > 1)
                intersectedIndexes.add(indexesForTable);
            else
                notIntersectedIndexes.addAll(indexesForTable);
        }

        
        if (!intersectedIndexes.isEmpty()) {
            for (List<Index> atomic : cartesianProduct(intersectedIndexes)) {
                Set<Index> conf = new HashSet<Index>();

                conf.addAll(notIntersectedIndexes);
                conf.addAll(atomic);

                ibg(statement, delegate, conf, inumSpace, path);
            }
        } else {
        */
            InumPlan inumPlan=new InumPlan(delegate, estmt);
            inumPlan.fromIndexes=new Vector<Index>();
            inumPlan.fromIndexes.addAll(indexes);
            inumSpace.add(inumPlan);

            List<Index> usedIndexes = estmt.getPlan().getIndexes();
            // check
            Set<Index> set2 = new HashSet<Index>();
            set2.addAll(usedIndexes);
            for (Index usedIndex : set2) {
                if (!indexes.contains(usedIndex))
                    throw new Error("index " + usedIndex
                            + " was not in input index set");
            }
            int id = 0;
            for (Index usedIndex : set2) {
                Set<Index> conf = new HashSet<Index>();

                conf.addAll(indexes);
                conf.remove(usedIndex);
                if (conf.size() == indexes.size())
                    throw new Error("Failed to remove index");

                String path2 = path + "," + id + "/" + set2.size();
                ibg(statement, delegate, conf, inumSpace, path2);
                id++;
            }
        //}
    }

    public void combination(SQLStatement statement, Optimizer delegate,
            Index[] allIndexes, Set<Index> indexes, Set<InumPlan> inumSpace,
            int pos) throws SQLException {
        if (pos >= allIndexes.length) {
//            System.out.println(pos + "/" + allIndexes.length + " space=" + inumSpace.size()
//                    + " whatif=" + whatIfCount);
            ExplainedSQLStatement estmt = delegate.explain(statement, indexes);
            inumSpace.add(new InumPlan(delegate, estmt));
            whatIfCount++;
            return;
        }
        combination(statement, delegate, allIndexes, indexes, inumSpace,
                pos + 1);
        indexes.add(allIndexes[pos]);
        combination(statement, delegate, allIndexes, indexes, inumSpace,
                pos + 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void computeWithCompleteConfiguration(Set<InumPlan> space,
            Set<? extends Index> indexes, SQLStatement statement,
            Optimizer delegate) throws SQLException {
        whatIfCount = 0;
        maxAllowedWhatIfCount = (int) Math.pow(2, indexes.size());
        tested=new HashSet<BitSet>();
        indexToId=new Hashtable<Index, Integer>();
        int id=0;
        for (Index index : indexes) {
//            Rt.p(index);
            indexToId.put(index, id++);
        }
        timer=new RTimer();
        timer.interval=10000;
        ibg(statement, delegate, new HashSet<Index>(indexes), space, "");
        if (whatIfCount > maxAllowedWhatIfCount) {
            // use powerset instead
            space.clear();
            combination(statement, delegate, indexes.toArray(new Index[indexes
                    .size()]), new HashSet<Index>(), space, 0);
        }
    }
}