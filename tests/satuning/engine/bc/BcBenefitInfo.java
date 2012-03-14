package satuning.engine.bc;

import java.sql.SQLException;

import satuning.db.DB2Index;
import satuning.db.DBConnection;
import satuning.db.ExplainInfo;
import satuning.engine.ProfiledQuery;
import satuning.engine.CandidatePool.Snapshot;
import satuning.engine.selection.StaticIndexSet;
import satuning.util.BitSet;
import satuning.util.Debug;

public class BcBenefitInfo {
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
	
	static BcBenefitInfo genBcBenefitInfo(DBConnection conn, Snapshot snapshot, StaticIndexSet hotSet, BitSet config, ProfiledQuery profiledQuery) throws SQLException {
		String sql = profiledQuery.sql;
		double[] origCosts = new double[snapshot.maxInternalId()+1];
		double[] newCosts = new double[snapshot.maxInternalId()+1];
		int[] reqLevels = new int[snapshot.maxInternalId()+1];
		double[] overheads = new double[snapshot.maxInternalId()+1];

		Debug.println("Generating benefit info for: " + sql);
		
		conn.fixCandidates(snapshot);
		BitSet tempBitSet = new BitSet();
		BitSet usedColumns = new BitSet();
		for (DB2Index idx : hotSet) {
			int id = idx.internalId();
			
			// reset tempBitSet
			tempBitSet.set(config);
			
			// get original cost
			tempBitSet.clear(id);
			origCosts[id] = conn.whatifOptimize(sql, tempBitSet, null, null);
			
			// get new cost
			tempBitSet.set(id);
			usedColumns.clear();
			newCosts[id] = conn.whatifOptimize(sql, tempBitSet, idx, usedColumns);
			
			// get request level
			reqLevels[id] = usedColumns.get(0)
			              ? (usedColumns.get(1) ? 2 : 1)
			              : 0;
			             
			// get maintenance
			ExplainInfo explainInfo = profiledQuery.explainInfo;
			overheads[id] = explainInfo.isDML() ? explainInfo.maintenanceCost(idx) : 0;
		}
		
		return new BcBenefitInfo(origCosts, newCosts, reqLevels, overheads);
	}

	public double newCost(int id) { return newCosts[id]; }
	public double origCost(int id) { return origCosts[id]; }
	public int reqLevel(int id) { return reqLevels[id]; }
	public double overhead(int id) { return overheads[id]; }
}
