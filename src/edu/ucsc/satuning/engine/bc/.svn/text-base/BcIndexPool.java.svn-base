package edu.ucsc.satuning.engine.bc;

import edu.ucsc.satuning.db.DBIndex;
import edu.ucsc.satuning.engine.selection.StaticIndexSet;

public class BcIndexPool<I extends DBIndex<I>> {
	java.util.HashMap<Integer,BcIndexInfo<I>> map;
	
	public BcIndexPool(StaticIndexSet<I> hotSet) {
		map = new java.util.HashMap<Integer,BcIndexInfo<I>>(hotSet.size());
		for (I idx : hotSet) {
			map.put(idx.internalId(), new BcIndexInfo<I>());
		}
	}
	
	public BcIndexInfo<I> get(int id) {
		return map.get(id);
	}
}
