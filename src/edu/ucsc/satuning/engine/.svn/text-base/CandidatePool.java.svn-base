package edu.ucsc.satuning.engine;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;

import edu.ucsc.satuning.db.DBIndex;
import edu.ucsc.satuning.db.DBIndexSet;
import edu.ucsc.satuning.util.BitSet;

public class CandidatePool<I extends DBIndex<I>> implements Serializable {
	private static final long serialVersionUID = 1L;
	
	/* serializable fields */
	Node firstNode;
	HashSet<I> indexSet;
	int maxInternalId;
	
	public CandidatePool() {
		firstNode = null;
		indexSet = new HashSet<I>();
		maxInternalId = -1;
	}

	public final void addIndex(I index) throws SQLException {
		if (!indexSet.contains(index)) {
			++maxInternalId;
			
			I indexCopy = index.consDuplicate(maxInternalId);
			firstNode = new Node(indexCopy, firstNode);
			indexSet.add(index);
		}
	}

	public void addIndexes(Iterable<I> newIndexes) throws SQLException {
		for (I index : newIndexes)
			addIndex(index);
	}
	
	public final boolean isEmpty() {
		return firstNode == null;
	}

	public final boolean contains(I index) {
		return indexSet.contains(index);
	}

	public Snapshot<I> getSnapshot() {
		return new Snapshot<I>(firstNode);
	}
	
	public java.util.Iterator<I> iterator() {
		return new SnapshotIterator<I>(firstNode);
	}
	
	private class Node implements Serializable {
		/* serializable fields */
		I index;
		Node next;

		/* serialization support */
		private static final long serialVersionUID = CandidatePool.serialVersionUID;
		protected Node() { } 
		
		Node(I index, Node next) {
			this.index = index;
			this.next = next;
		}
	}
	
	/*
	 * A snapshot of the candidate set (immutable set of indexes)
	 */
	public static class Snapshot<I extends DBIndex<I>> implements Iterable<I>, Serializable {
		/* serializable fields */
		int maxId;
		CandidatePool<I>.Node first;
		BitSet bs;
		
		/* serialization support */
		private static final long serialVersionUID = CandidatePool.serialVersionUID;
		protected Snapshot() { }
		
		private Snapshot(CandidatePool<I>.Node first) {
			maxId = (first == null) ? -1 : first.index.internalId();
			this.first = first;
			bs = new BitSet();
			bs.set(0, maxId+1);
		}
		
		public Iterator<I> iterator() {
			return new SnapshotIterator<I>(first);
		}
		
		public int maxInternalId() {
			return maxId;
		}
		
		public BitSet bitSet() {
			return bs; // no need to clone -- this set is immutable
		}

		public I findIndexId(int i) {
			for (I idx : this) if (idx.internalId() == i) return idx;
			return null;
		}
	}
	
	/*
	 * Iterator for a snapshot of the candidate set
	 */
	private static class SnapshotIterator<I extends DBIndex<I>> implements Iterator<I> {
		CandidatePool<I>.Node next;
		
		SnapshotIterator(CandidatePool<I>.Node start) {
			next = start;
		}

		public boolean hasNext() {
			return next != null;
		}

		public I next() {
			if (next == null)
				throw new java.util.NoSuchElementException();
			I current = next.index;
			next = next.next;
			return current;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	public static <I extends DBIndex<I>> Snapshot<I> emptySnapshot() {
		return new Snapshot<I>(null);
	}

	public DBIndexSet<I> getDB2IndexSet() {
		DBIndexSet<I> retval = new DBIndexSet<I>();
		for (I idx : indexSet)
			retval.add(idx);
		return retval;
	}
}
