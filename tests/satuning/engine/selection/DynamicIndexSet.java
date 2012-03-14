package satuning.engine.selection;

import java.util.Iterator;

import satuning.db.DB2Index;
import satuning.util.BitSet;

public class DynamicIndexSet implements Iterable<DB2Index> {
	private java.util.Set<DB2Index> set = new java.util.HashSet<DB2Index>();
	private BitSet bs = new BitSet();
	
	public void add(DB2Index index) {
		set.add(index);
		bs.set(index.internalId());
	}
	
	public boolean contains(DB2Index index) {
		return bs.get(index.internalId());
	}
	
	public void remove(DB2Index index) {
		set.remove(index);
		bs.clear(index.internalId());
	}

	public Iterator<DB2Index> iterator() {
		return set.iterator();
	}

	public int size() {
		return set.size();
	}

	public DB2Index[] toArray() {
		int i = 0;
		DB2Index[] arr = new DB2Index[set.size()];
		for (DB2Index index : set)
			arr[i++] = index;
		return arr;
	}

	public BitSet bitSet() {
		// need to clone since we modify it in place
		return bs.clone();
	}
}
