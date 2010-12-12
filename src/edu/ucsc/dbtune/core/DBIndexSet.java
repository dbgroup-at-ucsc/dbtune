/*
 * ****************************************************************************
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 *  ****************************************************************************
 */

package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.util.DefaultBitSet;
import edu.ucsc.dbtune.util.DBUtilities;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DBIndexSet<I extends DBIndex<I>> implements Iterable<I>, Serializable {
	
	private DefaultBitSet bs;
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
		bs = new DefaultBitSet();
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

	public DefaultBitSet bitSet() {
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
