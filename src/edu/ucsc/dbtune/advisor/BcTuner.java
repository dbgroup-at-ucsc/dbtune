/*
 * ****************************************************************************
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 *  ****************************************************************************
 */

package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.connectivity.DatabaseConnection;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.IBGPreparedSQLStatement;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.spi.core.Supplier;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.sql.SQLException;

public class BcTuner {
    private final DatabaseConnection    connection;
    private final StaticIndexSet     hotSet;
    private final BcIndexPool        pool;
    private final Snapshot           snapshot;
    private final IndexBitSet           currentRecommendation;
    private final Console console = Console.streaming();

    /**
     * Construct a {@code BcTuner} object.
     * @param databaseConnection
     *      a {@link DatabaseConnection} object.
     * @param snapshot
     *      a {@code snapshot} of the {@link edu.ucsc.dbtune.ibg.CandidatePool candidate pool of
     *      indexes}.
     * @param hotSet
     *      a {@code hotSet} of indexes.
     */
    public BcTuner(DatabaseConnection databaseConnection, Snapshot snapshot, StaticIndexSet hotSet) {
        this.connection             = databaseConnection;
        this.snapshot               = snapshot;
        this.hotSet                 = hotSet;
        this.pool                   = new BcIndexPool(this.hotSet);
        this.currentRecommendation  = new IndexBitSet();
    }

    /**
     * @return the {@code index} to create.
     */
    Index chooseIndexToCreate() {
        Index indexToCreate = null;
        double maxBenefit = 0;

        for (Index idx : hotSet) {
            BcIndexInfo stats = pool.get(idx.getId());
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
    Index chooseIndexToDrop() {
        Index indexToDrop = null;
        double minResidual = 0;

        for (Index idx : hotSet) {
            BcIndexInfo stats = pool.get(idx.getId());
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
     * dump to index pool on the screen (print it on screen).
     */
    public void dumpIndexPool() {
        for (Index idx : hotSet) {
            int id = idx.getId();
            BcIndexInfo stats = pool.get(id);
            
            console.log(idx.getCreateStatement());
            console.log(stats.toString(idx));
            console.skip();
        }
    }

    /**
     * @return the recommended indexes configuration.
     */
    public IndexBitSet getRecommendation() {
        IndexBitSet bs = new IndexBitSet();
        for (Index index : hotSet) {
            if (pool.get(index.getId()).state == BcIndexInfo.State.MATERIALIZED){
                bs.set(index.getId());
            }
        }
        return bs;
    }

    private int inferUseLevel(Index i1, Index i2, boolean prefix) {
        if (prefix) {
            return 2;
        } else if (i1.getColumn(0).equals(i2.getColumn(0))) {
            return 1;
        } else {
            return 0;
        }
    }

    /**
     * Process a {@code profiled query} with the whole purpose determining its benefit info. This
     * includes the updating of indexes' statistics.
     * @param profiledQuery
     *      a {@code profiled query} object.
     * @throws SQLException
     *      an unexpected error occurred.
     */
    public void processQuery(IBGPreparedSQLStatement profiledQuery) throws SQLException {
        final BenefitInfoInput input = new BenefitInfoInput.StrictBuilder(connection)
                .snapshot(snapshot)
                .hotSet(hotSet)
                .recommendedIndexes(currentRecommendation)
                .profiledQuery(profiledQuery)
            .get();
        @SuppressWarnings({"RedundantTypeArguments"})
        BcBenefitInfo qinfo = BcBenefitInfo.makeBcBenefitInfo(input);
        
        // update statistics
        for (Index idx : hotSet) {
            int id = idx.getId();
            BcIndexInfo stats = pool.get(id);
            
            if (qinfo.origCost(id) != qinfo.newCost(id)) // kludge to check if the index was used
                stats.addCosts(qinfo.reqLevel(id), qinfo.origCost(id), qinfo.newCost(id));
            
            stats.addUpdateCosts(qinfo.overhead(id));
            stats.updateDeltaMinMax();
        }
        
        console.log("*** UPDATED STATS");
        dumpIndexPool();
        
        // iteratively drop indices 
        while (true) {
            // choose the worst index to drop
            Index indexToDrop = chooseIndexToDrop();
            if (indexToDrop == null) 
                break;
            
            BcIndexInfo indexToDropStats = pool.get(indexToDrop.getId());
            
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
                BcIndexInfo ijStats = pool.get(ij.getId());
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
            
            BcIndexInfo indexToCreateStats = pool.get(indexToCreate.getId());
            
            // record the create
            indexToCreateStats.state = BcIndexInfo.State.MATERIALIZED;
            indexToCreateStats.initDeltaMax();
            
            // record interactions
            double indexToCreateSize = indexToCreate.getMegaBytes();
            for (Index ij : hotSet) {
                if (ij == indexToCreate)
                    continue;
                BcIndexInfo ijStats = pool.get(ij.getId());
                int useLevel = useLevel(indexToCreate, ij);
                double alpha = ij.getMegaBytes() / indexToCreateSize;
                for (int level = 0; level <= useLevel; level++) {
                    double costO = ijStats.origCost(level);
                    double costN = ijStats.newCost(level);
                    ijStats.setCost(level, Math.min(costO, alpha * costN), costN);
                }
                ijStats.updateDeltaMinMax();
            }
        } // done creating indices
    }
    
    private int useLevel(Index i1, Index i2) {
        /* Shortcut if different relations */
        if (!i1.getTable().equals(i2.getTable()))
            return -1;
        
        int n1 = i1.size();
        int n2 = i2.size();
        
        /* Shortcut if I1 has fewer columns than I2 */
        if (n1 < n2){
            return -1;
        }
        
        /* Set isPrefix true until we find a counterexample */      
        ColumnChecker columnChecker = new ColumnChecker(i1, i2, n1, n2).get();
        boolean isPrefix = columnChecker.isPrefix();


        /* Now we know that I1 contains the columns of I2 */
        return inferUseLevel(i1, i2, isPrefix);
    }


    @Override
    public String toString() {
        return new ToStringBuilder<BcTuner>(this)
               .add("connection", connection)
               .add("snapshot", snapshot)
               .add("hotSet", hotSet)
               .add("indexPool", pool)
               .add("currentRecommendation", currentRecommendation)
           .toString();
    }

    /**
     * This class checks that I1 contains the columns of I2, and if so, it will
     * set isPrefix to false if it finds one of I2's columns in a different 
     * position within I1
     */    
    private static class ColumnChecker implements Supplier<ColumnChecker> {
        private Index       i1;
        private Index       i2;
        private int     n1;
        private int     n2;
        private boolean prefix;

        public ColumnChecker(Index i1, Index i2, int n1, int n2) {
            this.i1 = i1;
            this.i2 = i2;
            this.n1 = n1;
            this.n2 = n2;
        }

        public boolean isPrefix() {
            return prefix;
        }

        @Override
        public ColumnChecker get() {
            for (int j2 = 0; j2 < n2; j2++){
                Column col2 = i2.getColumn(j2);
                /* check for col2 in the same position */
                if (i1.getColumn(j2).equals(col2)) continue;

                /* it's not in the same position */
                prefix = false;
                for (int j1 = 0; j1 < n1; j1++) {
                    if (i1.getColumn(j1).equals(col2)) {
                        break;
                    } else if (j1 == n1 -1) {
                        return this;
                    }
                }
            }

            return this;
        }
    }
}
