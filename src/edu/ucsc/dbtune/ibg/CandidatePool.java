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
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public class CandidatePool implements Serializable {
	private static final long serialVersionUID = 1L;

	/* serializable fields */
	Node<DBIndex> firstNode;
	Set<DBIndex>  indexSet;
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
    public CandidatePool(Node<DBIndex> firstNode, Set<DBIndex> indexSet, int maxInternalId){
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
        this(null, new HashSet<DBIndex>(), -1);
	}

	public final void addIndex(DBIndex index) throws SQLException {
		if (!indexSet.contains(index)) {
			++maxInternalId;
            // todo(Huascar) test this.
            final DBIndex duplicate = index.consDuplicate(maxInternalId);
			firstNode = new Node<DBIndex>(duplicate, firstNode);
			indexSet.add(index);
		}
	}

    Node<DBIndex> getFirstNode() {
        return firstNode;
    }

    public void addIndexes(Iterable<DBIndex> newIndexes) throws SQLException {
		for (DBIndex index : newIndexes){
            addIndex(index);
        }
	}

	public final boolean contains(DBIndex index) {
		return indexSet.contains(index);
	}

    /**
     * Returns an empty snapshot of the pool of candidates.
     * @param <I>
     *      the type of {@link DBIndex}.
     * @return an empty snapshot.
     */
	public static Snapshot emptySnapshot() {
		return new Snapshot(null);
	}

	public Snapshot getSnapshot() {
		return new Snapshot(getFirstNode());
	}

    /**
     * @return an updated {@link DBIndexSet} object.
     */
	public DBIndexSet getDB2IndexSet() {
		DBIndexSet retval = new DBIndexSet();
		for (DBIndex idx : indexSet){
            retval.add(idx);
        }
		return retval;
	}

	public final boolean isEmpty() {
		return firstNode == null;
	}

	public Iterator<DBIndex> iterator() {
		return new SnapshotIterator(firstNode);
	}

    /**
     * A node in the candidate pool, which wraps a given index.
     * @param <I> the type of {@link DBIndex}.
     */
	static class Node<I extends DBIndex> implements Serializable {
		/* serializable fields */
		I       index;
		Node<I> next;

		/* serialization support */
		private static final long serialVersionUID = CandidatePool.serialVersionUID;
        Node(I index, Node<I> next) {
			this.setIndex(index);
			this.setNext(next);
		}

        I getIndex() {
            return index;
        }

        Node<I> getNext() {
            return next;
        }

        void setIndex(I index) {
            this.index = index;
        }

        void setNext(Node<I> next) {
            this.next = next;
        }

        @Override
        public String toString() {
            return new ToStringBuilder<Node<I>>(this)
                   .add("index", getIndex())
                   .add("next node", getNext())
                   .toString();
        }
    }

	/**
	 * A snapshot of the candidate set (immutable set of indexes)
	 */
	public static class Snapshot implements Iterable<DBIndex>, Serializable {
		/* serializable fields */
		int maxId;
		Node<DBIndex> first;
		IndexBitSet bs;

		/* serialization support */
		private static final long serialVersionUID = CandidatePool.serialVersionUID;
		protected Snapshot() { }

		private Snapshot(Node<DBIndex> first) {
			maxId = (first == null) ? -1 : first.getIndex().internalId();
			this.first = first;
			bs = new IndexBitSet();
			bs.set(0, maxId+1);
		}

		public Iterator<DBIndex> iterator() {
			return new SnapshotIterator(first);
		}

		public int maxInternalId() {
			return maxId;
		}

		public IndexBitSet bitSet() {
			return bs; // no need to clone -- this set is immutable
		}

		public DBIndex findIndexId(int i) {
			for (DBIndex idx : this) {
                if (idx.internalId() == i) {
                    return idx;
                }
            }
			return null;
		}

        @Override
        public String toString() {
            return new ToStringBuilder<Snapshot>(this)
                   .add("maxInternalId", maxInternalId())
                   .add("first node", first)
                   .add("indexes configuration", bitSet())
                   .toString();
        }
    }

	/*
	 * Iterator for a snapshot of the candidate set
	 */
	private static class SnapshotIterator implements Iterator<DBIndex> {
		Node<DBIndex> next;

		SnapshotIterator(Node<DBIndex> start) {
			next = start;
		}

		public boolean hasNext() {
			return next != null;
		}

		public DBIndex next() {
			if (next == null)
				throw new NoSuchElementException();
			DBIndex current = next.getIndex();
			next = next.getNext();
			return current;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
