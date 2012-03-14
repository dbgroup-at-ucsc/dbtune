package satuning.engine;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.HashSet;

import satuning.db.DB2Index;
import satuning.db.DB2IndexSet;
import satuning.util.BitSet;

public class CandidatePool implements Serializable {
	private static final long serialVersionUID = 1L;
	
	/* serializable fields */
	Node firstNode;
	HashSet<DB2Index> indexSet;
	int maxInternalId;
	
	public CandidatePool() {
		firstNode = null;
		indexSet = new HashSet<DB2Index>();
		maxInternalId = -1;
	}

	public final void addIndex(DB2Index index) throws SQLException {
		if (!indexSet.contains(index)) {
			++maxInternalId;
			
			DB2Index indexCopy = DB2Index.consDuplicate(index, maxInternalId);
			firstNode = new Node(indexCopy, firstNode);
			indexSet.add(index);
		}
	}

	public void addIndexes(Iterable<DB2Index> newIndexes) throws SQLException {
		for (DB2Index index : newIndexes)
			addIndex(index);
	}
	
	public final boolean isEmpty() {
		return firstNode == null;
	}

	public final boolean contains(DB2Index index) {
		return indexSet.contains(index);
	}

	public Snapshot getSnapshot() {
		return new Snapshot(firstNode);
	}
	
	public java.util.Iterator<DB2Index> iterator() {
		return new Iterator(firstNode);
	}
	
	private class Node implements Serializable {
		/* serializable fields */
		DB2Index index;
		Node next;

		/* serialization support */
		private static final long serialVersionUID = CandidatePool.serialVersionUID;
		protected Node() { } 
		
		Node(DB2Index index0, Node next0) {
			index = index0;
			next = next0;
		}
	}
	
	/*
	 * A snapshot of the candidate set (immutable set of indexes)
	 */
	public static class Snapshot implements Iterable<DB2Index>, Serializable {
		/* serializable fields */
		int maxId;
		Node first;
		BitSet bs;
		
		/* serialization support */
		private static final long serialVersionUID = CandidatePool.serialVersionUID;
		protected Snapshot() { }
		
		private Snapshot(Node first0) {
			maxId = (first0 == null) ? -1 : first0.index.internalId();
			first = first0;
			bs = new BitSet();
			bs.set(0, maxId+1);
		}
		
		public java.util.Iterator<DB2Index> iterator() {
			return new Iterator(first);
		}
		
		public int maxInternalId() {
			return maxId;
		}
		
		public BitSet bitSet() {
			return bs; // no need to clone -- this set is immutable
		}

		public DB2Index findIndexId(int i) {
			for (DB2Index idx : this) if (idx.internalId() == i) return idx;
			return null;
		}
	}
	
	/*
	 * Iterator for a snapshot of the candidate set
	 */
	private static class Iterator implements java.util.Iterator<DB2Index> {
		Node next;
		
		Iterator(Node start) {
			next = start;
		}

		public boolean hasNext() {
			return next != null;
		}

		public DB2Index next() {
			if (next == null)
				throw new java.util.NoSuchElementException();
			DB2Index current = next.index;
			next = next.next;
			return current;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	public static Snapshot emptySnapshot() {
		return new Snapshot(null);
	}

	public DB2IndexSet getDB2IndexSet() {
		DB2IndexSet retval = new DB2IndexSet();
		for (DB2Index idx : indexSet)
			retval.add(idx);
		return retval;
	}
}
