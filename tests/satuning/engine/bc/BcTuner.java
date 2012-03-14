package satuning.engine.bc;

import java.sql.SQLException;

import satuning.db.DB2Index;
import satuning.db.DBConnection;
import satuning.engine.ProfiledQuery;
import satuning.engine.CandidatePool.Snapshot;
import satuning.engine.selection.StaticIndexSet;
import satuning.util.BitSet;
import satuning.util.Debug;

public class BcTuner {
	private DBConnection conn;
	private StaticIndexSet hotSet;
	private BcIndexPool pool;
	private Snapshot snapshot;
	private BitSet currentRecommendation;
	
	public BcTuner(DBConnection conn0, Snapshot snapshot0, StaticIndexSet hotSet0) {
		conn = conn0;
		snapshot = snapshot0;
		hotSet = hotSet0;
		pool = new BcIndexPool(hotSet);
		currentRecommendation = new BitSet();
	}
	
	public void dumpIndexPool() {
		for (DB2Index idx : hotSet) {
			int id = idx.internalId();
			BcIndexInfo stats = pool.get(id);
			
			Debug.println(idx.creationText());
			Debug.println(stats.toString(idx));
			Debug.println();
		}
	}

	public void processQuery(ProfiledQuery profiledQuery) throws SQLException {
		BcBenefitInfo qinfo = BcBenefitInfo.genBcBenefitInfo(conn, snapshot, hotSet, currentRecommendation, profiledQuery);
		
		// update statistics
		for (DB2Index idx : hotSet) {
			int id = idx.internalId();
			BcIndexInfo stats = pool.get(id);
			
			if (qinfo.origCost(id) != qinfo.newCost(id)) // kludge to check if the index was used
				stats.addCosts(qinfo.reqLevel(id), qinfo.origCost(id), qinfo.newCost(id));
			
			stats.addUpdateCosts(qinfo.overhead(id));
			stats.updateDeltaMinMax();
		}
		
		Debug.println("*** UPDATED STATS");
		dumpIndexPool();
		
		// iteratively drop indices 
		while (true) {
			// choose the worst index to drop
			DB2Index indexToDrop = chooseIndexToDrop();
			if (indexToDrop == null) 
				break;
			
			BcIndexInfo indexToDropStats = pool.get(indexToDrop.internalId());
			
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
			for (DB2Index ij : hotSet) {
				if (ij == indexToDrop)
					continue;
				BcIndexInfo ijStats = pool.get(ij.internalId());
				int useLevel = useLevel(indexToDrop, ij);
				for (int level = 0; level <= useLevel; level++) {
					double costO = ijStats.origCost(level);
					double costN = ijStats.newCost(level);
					ijStats.setCost(level, costO * beta[level], costN);
				}
				ijStats.updateDeltaMinMax();
			}
			
//			Debug.println("*** DROPPED " + indexToDrop);
//			dumpIndexPool();
		} // done dropping indices
		
		// iteratively create indices
		while (true) {
			// choose the best index to create
			DB2Index indexToCreate = chooseIndexToCreate();
			if (indexToCreate == null) 
				break;
			
			BcIndexInfo indexToCreateStats = pool.get(indexToCreate.internalId());
			
			// record the create
			indexToCreateStats.state = BcIndexInfo.State.MATERIALIZED;
			indexToCreateStats.initDeltaMax();
			
			// record interactions
			double indexToCreateSize = indexToCreate.megabytes();
			for (DB2Index ij : hotSet) {
				if (ij == indexToCreate)
					continue;
				BcIndexInfo ijStats = pool.get(ij.internalId());
				int useLevel = useLevel(indexToCreate, ij);
				double alpha = ij.megabytes() / indexToCreateSize;
				for (int level = 0; level <= useLevel; level++) {
					double costO = ijStats.origCost(level);
					double costN = ijStats.newCost(level);
					ijStats.setCost(level, Math.min(costO, alpha * costN), costN);
				}
				ijStats.updateDeltaMinMax();
			}
			
//			Debug.println("*** CREATED " + indexToCreate);
//			dumpIndexPool();
		} // done creating indices
	}
	
	private int useLevel(DB2Index i1, DB2Index i2) {
		/* Shortcut if different relations */
		if (!i1.tableName().equals(i2.tableName()))
			return -1;
		if (!i1.tableSchemaName().equals(i2.tableSchemaName()))
			return -1;
		
		int n1 = i1.columnCount();
		int n2 = i2.columnCount();
		
		/* Shortcut if I1 has fewer columns than I2 */
	    if (n1 < n2)
	        return -1;
	    
	    /* Set isPrefix true until we find a counterexample */
	    boolean isPrefix = true;
	    
	    /* 
	     * This loop checks that I1 contains the columns of I2, and if so, it will
	     * set isPrefix to false if it finds one of I2's columns in a different 
	     * position within I1
	     */
	    for (int j2 = 0; j2 < n2; j2++)
	    {
	    	String col2 = i2.columnName(j2);
	        /* check for col2 in the same position */
	        if (i1.columnName(j2).equals(col2))
	            continue;

	        /* it's not in the same position */
	        isPrefix = false;
	        for (int j1 = 0; j1 < n1; j1++) {
	            if (i1.columnName(j1).equals(col2))
	                break;
	            else if (j1 == n1-1)
	            	return -1; // I1 doesn't contain col2
	        }
	    }

	    /* Now we know that I1 contains the columns of I2 */
	    if (isPrefix)
	        return 2;
	    else if (i1.columnName(0).equals(i2.columnName(0)))
	        return 1;
	    else
	        return 0;
	}

	DB2Index chooseIndexToDrop() {
		DB2Index indexToDrop = null;
		double minResidual = 0;

		for (DB2Index idx : hotSet) {
			BcIndexInfo stats = pool.get(idx.internalId());
			if (stats.state == BcIndexInfo.State.MATERIALIZED) {
				double residual = stats.residual(idx.creationCost());
				if (residual <= 0 && (indexToDrop == null || residual < minResidual)) {
					indexToDrop = idx;
					minResidual = residual;
				}
			}
		}
		
		return indexToDrop;
	}

	
	DB2Index chooseIndexToCreate() {
		DB2Index indexToCreate = null;
		double maxBenefit = 0;

		for (DB2Index idx : hotSet) {
			BcIndexInfo stats = pool.get(idx.internalId());
			if (stats.state == BcIndexInfo.State.HYPOTHETICAL) {
				double benefit = stats.benefit(idx.creationCost());
				if (benefit >= 0 && (indexToCreate == null || benefit > maxBenefit)) {
					indexToCreate = idx;
					maxBenefit = benefit;
				}
			}
		}
		
		return indexToCreate;
	}

	public BitSet getRecommendation() {
		BitSet bs = new BitSet();
		for (DB2Index index : hotSet) {
			if (pool.get(index.internalId()).state == BcIndexInfo.State.MATERIALIZED)
				bs.set(index.internalId());
		}
		return bs;
	}
}
