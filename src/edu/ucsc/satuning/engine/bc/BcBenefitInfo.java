package edu.ucsc.satuning.engine.bc;

import java.sql.SQLException;

import edu.ucsc.satuning.db.DBIndex;
import edu.ucsc.satuning.db.DatabaseConnection;
import edu.ucsc.satuning.db.ExplainInfo;
import edu.ucsc.satuning.engine.ProfiledQuery;
import edu.ucsc.satuning.engine.CandidatePool.Snapshot;
import edu.ucsc.satuning.engine.selection.StaticIndexSet;
import edu.ucsc.satuning.util.BitSet;

public class BcBenefitInfo<I extends DBIndex<I>> {
	private final double[] origCosts;
	private final double[] newCosts;
	private final int[] reqLevels;
	private final double[] overheads;
	
	private BcBenefitInfo(double[] origCosts0, double[] newCosts0, int[] reqLevels0, double[] overheads0) {
		origCosts = origCosts0;
		newCosts = newCosts0;
		reqLevels = reqLevels0;
		overheads = overheads0;
	}
	
	static <I extends DBIndex<I>> BcBenefitInfo<I> 
	genBcBenefitInfo(DatabaseConnection<I> conn, Snapshot<I> snapshot, StaticIndexSet<I> hotSet, BitSet config, ProfiledQuery<I> profiledQuery) throws SQLException {
		String sql = profiledQuery.sql;
		double[] origCosts = new double[snapshot.maxInternalId()+1];
		double[] newCosts = new double[snapshot.maxInternalId()+1];
		int[] reqLevels = new int[snapshot.maxInternalId()+1];
		double[] overheads = new double[snapshot.maxInternalId()+1];
		
		conn.getIndexExtractor().fixCandidates(snapshot);
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
			ExplainInfo<I> explainInfo = profiledQuery.explainInfo;
			overheads[id] = explainInfo.isDML() ? explainInfo.maintenanceCost(idx) : 0;
		}
		
		return new BcBenefitInfo<I>(origCosts, newCosts, reqLevels, overheads);
	}

	public double newCost(int id) { return newCosts[id]; }
	public double origCost(int id) { return origCosts[id]; }
	public int reqLevel(int id) { return reqLevels[id]; }
	public double overhead(int id) { return overheads[id]; }
}
