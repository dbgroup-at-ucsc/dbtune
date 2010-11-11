package edu.ucsc.satuning.engine.selection;

import java.util.Iterator;

import edu.ucsc.satuning.db.DBIndex;
import edu.ucsc.satuning.util.BitSet;

public class DynamicIndexSet<I extends DBIndex<I>> implements Iterable<I> {
	private java.util.Set<I> set = new java.util.HashSet<I>();
	private BitSet bs = new BitSet();
	
	public void add(I index) {
		set.add(index);
		bs.set(index.internalId());
	}
	
	public boolean contains(I index) {
		return bs.get(index.internalId());
	}
	
	public void remove(I index) {
		set.remove(index);
		bs.clear(index.internalId());
	}

	public Iterator<I> iterator() {
		return set.iterator();
	}

	public int size() {
		return set.size();
	}

	public DBIndex<?>[] toArray() {
		int i = 0;
		DBIndex<?>[] arr = new DBIndex<?>[set.size()];
		for (I index : set)
			arr[i++] = index;
		return arr;
	}

	public BitSet bitSet() {
		// need to clone since we modify it in place
		return bs.clone();
	}
}
