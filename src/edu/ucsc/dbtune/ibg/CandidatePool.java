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

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.IndexSet;
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
    Node<Index> firstNode;
    Set<Index>  indexSet;
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
    public CandidatePool(Node<Index> firstNode, Set<Index> indexSet, int maxInternalId){
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
        this(null, new HashSet<Index>(), -1);
    }

    public final void addIndex(Index index) throws SQLException {
        if (!indexSet.contains(index)) {
            ++maxInternalId;
            // todo(Huascar) test this.
            Index duplicate = new Index(index);
            duplicate.setId(maxInternalId);
            firstNode = new Node<Index>(duplicate, firstNode);
            indexSet.add(index);
        }
    }

    Node<Index> getFirstNode() {
        return firstNode;
    }

    public void addIndexes(Iterable<Index> newIndexes) throws SQLException {
        for (Index index : newIndexes){
            addIndex(index);
        }
    }

    public final boolean contains(Index index) {
        return indexSet.contains(index);
    }

    /**
     * Returns an empty snapshot of the pool of candidates.
     * @return an empty snapshot.
     */
    public static Snapshot emptySnapshot() {
        return new Snapshot(null);
    }

    public Snapshot getSnapshot() {
        return new Snapshot(getFirstNode());
    }

    /**
     * @return an updated {@link IndexSet} object.
     */
    public IndexSet getDB2IndexSet() {
        IndexSet retval = new IndexSet();
        for (Index idx : indexSet){
            retval.add(idx);
        }
        return retval;
    }

    public final boolean isEmpty() {
        return firstNode == null;
    }

    public Iterator<Index> iterator() {
        return new SnapshotIterator(firstNode);
    }

    /**
     * A node in the candidate pool, which wraps a given index.
     * @param <I> the type of {@link Index}.
     */
    static class Node<I extends Index> implements Serializable {
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
    public static class Snapshot implements Iterable<Index>, Serializable {
        /* serializable fields */
        int maxId;
        Node<Index> first;
        IndexBitSet bs;

        /* serialization support */
        private static final long serialVersionUID = CandidatePool.serialVersionUID;
        protected Snapshot() { }

        private Snapshot(Node<Index> first) {
            maxId = (first == null) ? -1 : first.getIndex().getId();
            this.first = first;
            bs = new IndexBitSet();
            bs.set(0, maxId+1);
        }

        public Iterator<Index> iterator() {
            return new SnapshotIterator(first);
        }

        public int maxInternalId() {
            return maxId;
        }

        public IndexBitSet bitSet() {
            return bs; // no need to clone -- this set is immutable
        }

        public Index findIndexId(int i) {
            for (Index idx : this) {
                if (idx.getId() == i) {
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
    private static class SnapshotIterator implements Iterator<Index> {
        Node<Index> next;

        SnapshotIterator(Node<Index> start) {
            next = start;
        }

        public boolean hasNext() {
            return next != null;
        }

        public Index next() {
            if (next == null)
                throw new NoSuchElementException();
            Index current = next.getIndex();
            next = next.getNext();
            return current;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
