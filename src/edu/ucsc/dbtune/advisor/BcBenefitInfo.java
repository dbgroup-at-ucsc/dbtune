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

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.core.ExplainInfo;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.spi.ibg.ProfiledQuery;
import edu.ucsc.dbtune.util.BitSet;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.sql.SQLException;
import java.util.Arrays;

public class BcBenefitInfo<I extends DBIndex<I>> {
	private final double[]          origCosts;
	private final double[]          newCosts;
	private final int[]             reqLevels;
	private final double[]          overheads;
    private final ProfiledQuery<I>  profiledQuery;

    private BcBenefitInfo(double[] origCosts, double[] newCosts, int[] reqLevels, double[] overheads, ProfiledQuery<I> profiledQuery) {
		this.origCosts      = origCosts;
		this.newCosts       = newCosts;
		this.reqLevels      = reqLevels;
		this.overheads      = overheads;
        this.profiledQuery  = profiledQuery;
    }

    /**
     * construct a new {@code BcBenefitInfo} given some {@code benefitInfo input}, which consist of
     * a {@code db connection, a snapshot of indexes, a hotset, an indexes configuration, and a
     * profiled query}.
     * @param arg
     *      a {@link BenefitInfoInput} object.
     * @param <I>
     *      a {@link DBIndex} object.
     * @return
     *      a new {@link BcBenefitInfo} object.
     * @throws SQLException
     *      unexpected error has occurred.
     */
    public static <I extends DBIndex<I>> BcBenefitInfo<I> makeBcBenefitInfo(BenefitInfoInput<I> arg) throws SQLException {
        return genBcBenefitInfo(
                arg.getDatabaseConnection(),
                arg.getSnapshot(),
                arg.getHotSet(),
                arg.getRecommendedIndexes(),
                arg.getProfiledQuery()
        );
    }
	
	static <I extends DBIndex<I>> BcBenefitInfo<I> genBcBenefitInfo(DatabaseConnection<I> conn,
                                                                    Snapshot<I> snapshot,
                                                                    StaticIndexSet<I> hotSet,
                                                                    BitSet config,
                                                                    ProfiledQuery<I> profiledQuery
    ) throws SQLException {
		String sql = profiledQuery.getSQL();
		double[] origCosts = new double[snapshot.maxInternalId()+1];
		double[] newCosts = new double[snapshot.maxInternalId()+1];
		int[] reqLevels = new int[snapshot.maxInternalId()+1];
		double[] overheads = new double[snapshot.maxInternalId()+1];
		
		conn.getWhatIfOptimizer().fixCandidates(snapshot);
		BitSet tempBitSet = new BitSet();
		BitSet usedColumns = new BitSet();
		for (I idx : hotSet) {
			int id = idx.internalId();
			
			// reset tempBitSet
			tempBitSet.set(config);
			
			// get original cost
			tempBitSet.clear(id);
			origCosts[id] = conn.getWhatIfOptimizer().whatIfOptimize(sql).using(tempBitSet, null, null).toGetCost();
			
			// get new cost
			tempBitSet.set(id);
			usedColumns.clear();
			newCosts[id] = conn.getWhatIfOptimizer().whatIfOptimize(sql).using(tempBitSet, idx, usedColumns).toGetCost();
			
			// get request level
			reqLevels[id] = usedColumns.get(0)
			              ? (usedColumns.get(1) ? 2 : 1)
			              : 0;
			             
			// get maintenance
			ExplainInfo<I> explainInfo = profiledQuery.getExplainInfo();
			overheads[id] = explainInfo.isDML() ? explainInfo.maintenanceCost(idx) : 0;
		}
		
		return new BcBenefitInfo<I>(origCosts, newCosts, reqLevels, overheads, profiledQuery);
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
     * @return the {@link ProfiledQuery} used in this {@code BcBenefitInfo} object.
     */
    public ProfiledQuery<I> getProfiledQuery(){
        return profiledQuery;
    }

    @Override
    public String toString() {
        return new ToStringBuilder<BcBenefitInfo<I>>(this)
               .add("origCosts", Arrays.toString(origCosts))
               .add("newCosts", Arrays.toString(newCosts))
               .add("reqLevels", Arrays.toString(reqLevels))
               .add("overheads", Arrays.toString(overheads))
               .add("profiledQuery", getProfiledQuery())
            .toString();
    }
}
