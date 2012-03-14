package satuning.db;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import satuning.util.BitSet;

public class DB2IndexSet implements Iterable<DB2Index>, Serializable {
	
	private BitSet bs;
	private Set<DB2Index> set;
	private List<DB2Index> list;
	private int maxInternalId;

	/* serialization support */
	private static final long serialVersionUID = 1L;
	
	public DB2IndexSet() {
		clear();
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.writeInt(list.size());
		for (DB2Index idx : list)
			out.writeObject(idx);
	}
	
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    	clear();
    	int n = in.readInt();
    	for (int i = 0; i < n; i++) {
        	add((DB2Index) in.readObject());
    	}
    }
	
	public void clear() {
		set = new HashSet<DB2Index>();
		list = new ArrayList<DB2Index>();
		bs = new BitSet();
		maxInternalId = -1;
	}
	
	public void add(DB2Index idx) {
		if (set.add(idx)) {
			list.add(idx);
			bs.set(idx.meta.internalId);
			if (idx.meta.internalId > maxInternalId)
				maxInternalId = idx.meta.internalId;
		}
	}
	
	public java.util.Iterator<DB2Index> iterator() {
		return list.iterator();
	}
	
	public int maxInternalId() {
		return maxInternalId;
	}

	public int size() {
		return set.size();
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(size() + " indexes\n");
		
		for (DB2Index idx : this) {
			sb.append(idx + "\n");
		}
		
		return sb.toString();
	}

	// Renumber the indexes so they are sorted by their creation text 
	public void normalize() throws SQLException {
		DB2Index[] array = new DB2Index[list.size()];
		
		array = list.<DB2Index>toArray(array);
		Arrays.sort(array, DB2Index.schemaComparator);
		
		// start from scratch to be safe
		clear();
		for (int i = 0; i < array.length; i++) {
			DB2Index old = array[i];
			DB2Index idx = new DB2Index(DB2IndexMetadata.consDuplicate(old.meta, i), old.creationCost);
			add(idx);
		}
	}

	public BitSet bitSet() {
		return bs.clone();
	}
}
