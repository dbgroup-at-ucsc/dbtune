package edu.ucsc.satuning.db;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsc.satuning.util.BitSet;
import edu.ucsc.satuning.util.DBUtilities;

public class DBIndexSet<I extends DBIndex<I>> implements Iterable<I>, Serializable {
	
	private BitSet bs;
	private Set<I> set;
	private List<I> list;
	private int maxInternalId;

	/* serialization support */
	public static final long serialVersionUID = 1L;
	
	public DBIndexSet() {
		clear();
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.writeInt(list.size());
		for (I idx : list)
			out.writeObject(idx);
	}
	
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    	clear();
    	int n = in.readInt();
    	for (int i = 0; i < n; i++) {
    		I readObject = DBUtilities.<I>readObject(in);
			add(readObject);
    	}
    }
	
	public void clear() {
		set = new HashSet<I>();
		list = new ArrayList<I>();
		bs = new BitSet();
		maxInternalId = -1;
	}
	
	public void add(I idx) {
		if (set.add(idx)) {
			list.add(idx);
			bs.set(idx.internalId());
			if (idx.internalId() > maxInternalId)
				maxInternalId = idx.internalId();
		}
	}
	
	public java.util.Iterator<I> iterator() {
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
		
		for (I idx : this) {
			sb.append(idx + "\n");
		}
		
		return sb.toString();
	}

	// Renumber the indexes so they are sorted by their creation text 
	@SuppressWarnings("unchecked")
	public void normalize() throws SQLException {
		DBIndex<?>[] array = new DBIndex<?>[list.size()];
		array = list.<DBIndex<?>>toArray(array);
		Arrays.sort(array, schemaComparator);
		
		// start from scratch to be safe
		clear();
		for (int i = 0; i < array.length; i++) {
			I idx = (I) array[i].consDuplicate(i);
			add(idx);
		}
	}

	public BitSet bitSet() {
		return bs.clone();
	}
	
	/*
	 * java.util.Comparator for displaying indexes in an easy to read format
	 */
	private static Comparator<DBIndex<?>> schemaComparator = new Comparator<DBIndex<?>>() {
		public int compare(DBIndex<?> o1, DBIndex<?> o2) {
			return o1.creationText().compareTo(o2.creationText());
		}
	};
}
