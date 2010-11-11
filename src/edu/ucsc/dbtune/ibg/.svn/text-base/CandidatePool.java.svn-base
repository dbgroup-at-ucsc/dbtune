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
package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DBIndexSet;
import edu.ucsc.dbtune.util.BitSet;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public class CandidatePool<I extends DBIndex<I>> implements Serializable {
	private static final long serialVersionUID = 1L;

	/* serializable fields */
	Node<I> firstNode;
	Set<I>  indexSet;
	int maxInternalId;

    /**
     * construct a pool of candidate (indexes) object.
     * @param firstNode
     *      first node in the pool.
     * @param indexSet
     *      a set of indexes to be include in the pool.
     * @param maxInternalId
     *      the max internal id of a index node.
     */
    public CandidatePool(Node<I> firstNode, Set<I> indexSet, int maxInternalId){
        this.firstNode      = firstNode;
        this.indexSet       = indexSet;
        this.maxInternalId  = maxInternalId;
    }

    /**
     * construct a pool of candidates (indexes) object. This pool will have
     * the first node set to null, an empty index set, and a maxInternalId set
     * to -1.
     */
	public CandidatePool() {
        this(null, new HashSet<I>(), -1);
	}

	public final void addIndex(I index) throws SQLException {
		if (!indexSet.contains(index)) {
			++maxInternalId;

			I indexCopy = index.consDuplicate(maxInternalId);
			firstNode = new Node<I>(indexCopy, firstNode);
			indexSet.add(index);
		}
	}

	public void addIndexes(Iterable<I> newIndexes) throws SQLException {
		for (I index : newIndexes){
            addIndex(index);
        }
	}

	public final boolean contains(I index) {
		return indexSet.contains(index);
	}

    /**
     * Returns an empty snapshot of the pool of candidates.
     * @param <I>
     *      the type of {@link DBIndex}.
     * @return an empty snapshot.
     */
	public static <I extends DBIndex<I>> Snapshot<I> emptySnapshot() {
		return new Snapshot<I>(null);
	}

	public Snapshot<I> getSnapshot() {
		return new Snapshot<I>(firstNode);
	}

    /**
     * @return an updated {@link DBIndexSet} object.
     */
	public DBIndexSet<I> getDB2IndexSet() {
		DBIndexSet<I> retval = new DBIndexSet<I>();
		for (I idx : indexSet){
            retval.add(idx);
        }
		return retval;
	}

	public final boolean isEmpty() {
		return firstNode == null;
	}

	public Iterator<I> iterator() {
		return new SnapshotIterator<I>(firstNode);
	}

    /**
     * A node in the candidate pool, which wraps a given index.
     * @param <I> the type of {@link DBIndex}.
     */
	private static class Node<I extends DBIndex<I>> implements Serializable {
		/* serializable fields */
		I       index;
		Node<I> next;

		/* serialization support */
		private static final long serialVersionUID = CandidatePool.serialVersionUID;
        Node(I index, Node<I> next) {
			this.index = index;
			this.next = next;
		}

        @Override
        public String toString() {
            return new ToStringBuilder<Node<I>>(this)
                   .add("index", index)
                   .add("next node", next)
                   .toString();
        }
    }

	/**
	 * A snapshot of the candidate set (immutable set of indexes)
	 */
	public static class Snapshot<I extends DBIndex<I>> implements Iterable<I>, Serializable {
		/* serializable fields */
		int maxId;
		Node<I> first;
		BitSet bs;

		/* serialization support */
		private static final long serialVersionUID = CandidatePool.serialVersionUID;
		protected Snapshot() { }

		private Snapshot(Node<I> first) {
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
			for (I idx : this) {
                if (idx.internalId() == i) {
                    return idx;
                }
            }
			return null;
		}

        @Override
        public String toString() {
            return new ToStringBuilder<Snapshot<I>>(this)
                   .add("maxInternalId", maxInternalId())
                   .add("first node", first)
                   .add("indexes configuration", bitSet())
                   .toString();
        }
    }

	/*
	 * Iterator for a snapshot of the candidate set
	 */
	private static class SnapshotIterator<I extends DBIndex<I>> implements Iterator<I> {
		Node<I> next;

		SnapshotIterator(Node<I> start) {
			next = start;
		}

		public boolean hasNext() {
			return next != null;
		}

		public I next() {
			if (next == null)
				throw new NoSuchElementException();
			I current = next.index;
			next = next.next;
			return current;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
