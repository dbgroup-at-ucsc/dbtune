package edu.ucsc.dbtune.advisor.bc;

import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.ToStringBuilder;

public class BcTuner
{
    private final Configuration   hotSet;
    private final BcIndexPool    pool;
    private final Configuration  snapshot;
    private final IndexBitSet    currentRecommendation;

    /**
     * Construct a {@code BcTuner} object.
     *
     * @param snapshot
     *      a {@code snapshot} of the {@link edu.ucsc.dbtune.metadata.Configuration} candidate pool 
     *      of indexes.
     * @param hotSet
     *      a {@code hotSet} of indexes.
     */
    public BcTuner(Configuration snapshot, Configuration hotSet)
    {
        this.snapshot               = snapshot;
        this.hotSet                 = hotSet;
        this.pool                   = new BcIndexPool(snapshot,this.hotSet);
        this.currentRecommendation  = new IndexBitSet();
    }

    /**
     * @return the {@code index} to create.
     */
    public Index chooseIndexToCreate()
    {
        Index indexToCreate = null;
        double maxBenefit = 0;

        for (Index idx : hotSet) {
            BcIndexInfo stats = pool.get(snapshot.getOrdinalPosition(idx));
            if (stats.state == BcIndexInfo.State.HYPOTHETICAL) {
                double benefit = stats.benefit(idx.getCreationCost());
                if (benefit >= 0 && (indexToCreate == null || benefit > maxBenefit)) {
                    indexToCreate = idx;
                    maxBenefit = benefit;
                }
            }
        }
        
        return indexToCreate;
    }

    /**
     * @return the {@code index} to drop.
     */
    public Index chooseIndexToDrop()
    {
        Index indexToDrop = null;
        double minResidual = 0;

        for (Index idx : hotSet) {
            BcIndexInfo stats = pool.get(snapshot.getOrdinalPosition(idx));
            if (stats.state == BcIndexInfo.State.MATERIALIZED) {
                double residual = stats.residual(idx.getCreationCost());
                if (residual <= 0 && (indexToDrop == null || residual < minResidual)) {
                    indexToDrop = idx;
                    minResidual = residual;
                }
            }
        }
        
        return indexToDrop;
    }

    /**
     * @return the recommended indexes configuration.
     */
    public IndexBitSet getRecommendation()
    {
        IndexBitSet bs = new IndexBitSet();
        for (Index index : hotSet) {
            if (pool.get(snapshot.getOrdinalPosition(index)).state == BcIndexInfo.State.MATERIALIZED){
                bs.set(snapshot.getOrdinalPosition(index));
            }
        }
        return bs;
    }

    /*
    private int inferUseLevel(Index i1, Index i2, boolean prefix)
    {
        if (prefix) {
            return 2;
        } else if (i1.get(0).equals(i2.get(0))) {
            return 1;
        } else {
            return 0;
        }
    }

     * Process a {@code profiled query} with the whole purpose determining its benefit info. This
     * includes the updating of indexes' statistics.
     * @param profiledQuery
     *      a {@code profiled query} object.
     * @throws SQLException
     *      an unexpected error occurred.
    public void processQuery(IBGPreparedSQLStatement profiledQuery) throws SQLException
    {
        BcBenefitInfo qinfo = new BcBenefitInfo(snapshot,hotSet,currentRecommendation,profiledQuery)
        
        // update statistics
        for (Index idx : hotSet) {
            int id = snapshot.getOrdinalPosition(idx);
            BcIndexInfo stats = pool.get(id);
            
            if (qinfo.origCost(id) != qinfo.newCost(id)) // kludge to check if the index was used
                stats.addCosts(qinfo.reqLevel(id), qinfo.origCost(id), qinfo.newCost(id));
            
            stats.addUpdateCosts(qinfo.overhead(id));
            stats.updateDeltaMinMax();
        }
        
        // iteratively drop indices 
        while (true) {
            // choose the worst index to drop
            Index indexToDrop = chooseIndexToDrop();
            if (indexToDrop == null) 
                break;
            
            BcIndexInfo indexToDropStats = pool.get(snapshot.getOrdinalPosition(indexToDrop));
            
            // record the drop
            indexToDropStats.state = BcIndexInfo.State.HYPOTHETICAL;
            indexToDropStats.initDeltaMin();
            
            // record interactions
            double[] beta = new double[3];
            for (int level = 0; level <= 2; level++)
            {
                double costO = indexToDropStats.origCost(level);
                double costN = indexToDropStats.newCost(level);
                if (costN == 0 && costO == 0)
                    beta[level] = 1;
                else
                    beta[level] = costO / costN;
            }
            for (Index ij : hotSet) {
                if (ij == indexToDrop)
                    continue;
                BcIndexInfo ijStats = pool.get(snapshot.getOrdinalPosition(ij));
                int useLevel = useLevel(indexToDrop, ij);
                for (int level = 0; level <= useLevel; level++) {
                    double costO = ijStats.origCost(level);
                    double costN = ijStats.newCost(level);
                    ijStats.setCost(level, costO * beta[level], costN);
                }
                ijStats.updateDeltaMinMax();
            }
            
        } // done dropping indices
        
        // iteratively create indices
        while (true) {
            // choose the best index to create
            Index indexToCreate = chooseIndexToCreate();
            if (indexToCreate == null) 
                break;
            
            BcIndexInfo indexToCreateStats = pool.get(snapshot.getOrdinalPosition(indexToCreate));
            
            // record the create
            indexToCreateStats.state = BcIndexInfo.State.MATERIALIZED;
            indexToCreateStats.initDeltaMax();
            
            // record interactions
            double indexToCreateSize = indexToCreate.getBytes();
            for (Index ij : hotSet) {
                if (ij == indexToCreate)
                    continue;
                BcIndexInfo ijStats = pool.get(snapshot.getOrdinalPosition(ij));
                int useLevel = useLevel(indexToCreate, ij);
                double alpha = ij.getBytes() / indexToCreateSize;
                for (int level = 0; level <= useLevel; level++) {
                    double costO = ijStats.origCost(level);
                    double costN = ijStats.newCost(level);
                    ijStats.setCost(level, Math.min(costO, alpha * costN), costN);
                }
                ijStats.updateDeltaMinMax();
            }
        } // done creating indices
    }
    
    private int useLevel(Index i1, Index i2)
    {
        // Shortcut if different relations
        if (!i1.getTable().equals(i2.getTable()))
            return -1;
        
        int n1 = i1.size();
        int n2 = i2.size();
        
        // Shortcut if I1 has fewer columns than I2
        if (n1 < n2){
            return -1;
        }
        
        // Set isPrefix true until we find a counterexample
        ColumnChecker columnChecker = new ColumnChecker(i1, i2, n1, n2).get();
        boolean isPrefix = columnChecker.isPrefix();


        // Now we know that I1 contains the columns of I2
        return inferUseLevel(i1, i2, isPrefix);
    }
     */

    @Override
    public String toString()
    {
        return new ToStringBuilder<BcTuner>(this)
               .add("snapshot", snapshot)
               .add("hotSet", hotSet)
               .add("indexPool", pool)
               .add("currentRecommendation", currentRecommendation)
           .toString();
    }
}
