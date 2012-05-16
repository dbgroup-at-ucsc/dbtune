package interaction.db;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import interaction.util.BitSet;

public class DB2IndexSet implements Iterable<DB2Index>, Serializable {
	
	private BitSet bs;
	private Set<DB2Index> set;
	private List<DB2Index> list;
	private int maxInternalID;

	/* serialization support */
	private static final long serialVersionUID = -3728805538556087582L;
	
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
		maxInternalID = -1;
	}
	
	public void add(DB2Index idx) {
		if (set.add(idx)) {
			list.add(idx);
			bs.set(idx.internalID);
			if (idx.internalID > maxInternalID)
				maxInternalID = idx.internalID;
		}
	}
	
	@Override
	public java.util.Iterator<DB2Index> iterator() {
		return list.iterator();
	}
	
	public int maxInternalID() {
		return maxInternalID;
	}

	public int size() {
		return set.size();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(size() + " indexes\n");
		
		for (DB2Index idx : this) {
			sb.append(idx.creationText + "\n");
			//sb.append("Hash code: " + idx.schema.hashCode());
		}
		
		return sb.toString();
	}

	// Renumber the indexes so they are sorted by their creation text 
	public void normalize() throws SQLException {
		DB2Index[] array = new DB2Index[list.size()];
		
		array = list.<DB2Index>toArray(array);
		try {
			Arrays.sort(array, new Comparator<DB2Index>() {
				@Override
				public int compare(DB2Index o1, DB2Index o2) {
					try {
						return o1.schema.creationText("a").compareTo(o2.schema.creationText("a"));
					} catch (SQLException e) {
						throw new Error(e);
					}
				} } );
		} catch (Error e) {
			if (e.getCause() instanceof SQLException) 
				throw (SQLException) e.getCause();
			else throw e;
		}
		
		// start from scratch to be safe
		clear();
		for (int i = 0; i < array.length; i++) {
			DB2Index old = array[i], idx;
			idx = new DB2Index(old.schema, i, DB2Index.indexNameBase + i, old.indexOwner, 
					old.tableOwner, old.indexExists, old.systemRequired);
			add(idx);
		}
	}

	public BitSet bitSet() {
		return bs.clone();
	}
}
