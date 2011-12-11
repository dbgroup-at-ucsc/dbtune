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

package edu.ucsc.dbtune.advisor.bc;

import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.sql.SQLException;
import java.util.Arrays;

public class BcBenefitInfo {
    private final double[]              origCosts;
    private final double[]              newCosts;
    private final int[]                 reqLevels;
    private final double[]              overheads;
    private final ExplainedSQLStatement  profiledQuery;

    private BcBenefitInfo(double[] origCosts, double[] newCosts,
                          int[] reqLevels, double[] overheads,
                          ExplainedSQLStatement profiledQuery
    ) {
        this.origCosts      = origCosts;
        this.newCosts       = newCosts;
        this.reqLevels      = reqLevels;
        this.overheads      = overheads;
        this.profiledQuery  = profiledQuery;
    }

    static BcBenefitInfo genBcBenefitInfo(
            Configuration snapshot,
            Configuration hotSet,
            IndexBitSet config,
            ExplainedSQLStatement profiledQuery )
        throws SQLException
    {
        /*
        // XXX: issue #99
        // estimateCost(
        //    String sql, IndexBitSet configuration, IndexBitSet used, Index profiledIndex)
        // 
        // this method throws a runtime exception in AbstractIBGWhatIfOptimizer
        //
        // so we're throwing it here 
        
        String sql = profiledQuery.getSQL();
        double[] origCosts = new double[snapshot.maxInternalId()+1];
        double[] newCosts = new double[snapshot.maxInternalId()+1];
        int[] reqLevels = new int[snapshot.maxInternalId()+1];
        double[] overheads = new double[snapshot.maxInternalId()+1];
        
        IndexBitSet tempBitSet = new IndexBitSet();
        IndexBitSet usedColumns = new IndexBitSet();
        for (Index idx : hotSet) {
            int id = idx.getId();
            
            // reset tempBitSet
            tempBitSet.set(config);
            
            // get original cost
            tempBitSet.clear(id);
            final IBGWhatIfOptimizer optimizer = conn.getIBGWhatIfOptimizer();
            origCosts[id] = optimizer.estimateCost(sql, tempBitSet, Instances.newBitSet(), null);
            
            // get new cost
            tempBitSet.set(id);
            usedColumns.clear();
            newCosts[id] = optimizer.estimateCost(sql, tempBitSet, usedColumns, idx);
            
            // get request level
            reqLevels[id] = usedColumns.get(0)
                          ? (usedColumns.get(1) ? 2 : 1)
                          : 0;
                         
            // get maintenance
            ExplainInfo explainInfo = profiledQuery.getExplainInfo();
            overheads[id] = explainInfo.isDML() ? explainInfo.getIndexMaintenanceCost(idx) : 0;
        }
        
        return new BcBenefitInfo(origCosts, newCosts, reqLevels, overheads, profiledQuery);
        */
        throw new RuntimeException("Not implemented yet");
    }

    /**
     * Returns the new cost of a given index (identified by its id)
     * @param id
     *      index's id
     * @return the new cost of an index.
     */
    public double newCost(int id) {
        return newCosts[id];
    }

    /**
     * Returns the original cost of a given index (identified by its id)
     * @param id
     *      index's id
     * @return the original cost of an index.
     */
    public double origCost(int id) {
        return origCosts[id];
    }

    /**
     * Returns the required level of a given index (identified by its id)
     * @param id
     *      index's id
     * @return the required level of an index.
     */
    public int reqLevel(int id) {
        return reqLevels[id];
    }

    /**
     * Returns the overhead of a given index (identified by its id)
     * @param id
     *      index's id
     * @return the overhead of an index.
     */
    public double overhead(int id) {
        return overheads[id];
    }

    /**
     * @return the {@link ExplainedSQLStatement} used in this {@code BcBenefitInfo} object.
     */
    public ExplainedSQLStatement getProfiledQuery(){
        return profiledQuery;
    }

    @Override
    public String toString() {
        return new ToStringBuilder<BcBenefitInfo>(this)
               .add("origCosts", Arrays.toString(origCosts))
               .add("newCosts", Arrays.toString(newCosts))
               .add("reqLevels", Arrays.toString(reqLevels))
               .add("overheads", Arrays.toString(overheads))
               .add("profiledQuery", getProfiledQuery())
            .toString();
    }
}
