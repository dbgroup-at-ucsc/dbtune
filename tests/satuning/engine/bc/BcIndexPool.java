package satuning.engine.bc;

import satuning.db.DB2Index;
import satuning.engine.selection.StaticIndexSet;

public class BcIndexPool {
	BcIndexInfo[] arr;
	
	public BcIndexPool(StaticIndexSet hotSet) {
		int maxInternalId = 0;
		for (DB2Index idx : hotSet) 
			maxInternalId = Math.max(maxInternalId, idx.internalId());
		arr = new BcIndexInfo[maxInternalId+1];
		for (DB2Index idx : hotSet) {
			arr[idx.internalId()] = new BcIndexInfo();
		}
	}
	
	public BcIndexInfo get(int id) {
		return arr[id];
	}
}
