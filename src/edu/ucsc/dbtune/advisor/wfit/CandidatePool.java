package edu.ucsc.dbtune.advisor.wfit;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.HashSet;

import edu.ucsc.dbtune.metadata.Index;

public class CandidatePool implements Serializable {
    private static final long serialVersionUID = 1L;
    
    /* serializable fields */
    Node firstNode;
    HashSet<Index> indexSet;
    int maxInternalId;
    
    public CandidatePool() {
        firstNode = null;
        indexSet = new HashSet<Index>();
        maxInternalId = -1;
    }

    public final void addIndex(Index index) throws SQLException {
        if (!indexSet.contains(index)) {
            ++maxInternalId;
            
            Index indexCopy = new Index(index);
            firstNode = new Node(indexCopy, firstNode);
            indexSet.add(index);
        }
    }

    public void addIndexes(Iterable<Index> newIndexes) throws SQLException {
        for (Index index : newIndexes)
            addIndex(index);
    }
    
    public final boolean isEmpty() {
        return firstNode == null;
    }

    public final boolean contains(Index index) {
        return indexSet.contains(index);
    }

    public Snapshot getSnapshot() {
        return new Snapshot(firstNode);
    }
    
    public java.util.Iterator<Index> iterator() {
        return new Iterator(firstNode);
    }
    
    private class Node implements Serializable {
        /* serializable fields */
        Index index;
        Node next;

        /* serialization support */
        private static final long serialVersionUID = CandidatePool.serialVersionUID;
        
        Node(Index index0, Node next0) {
            index = index0;
            next = next0;
        }
    }
    
    /*
     * A snapshot of the candidate set (immutable set of indexes)
     */
    public static class Snapshot implements Iterable<Index>, Serializable {
        /* serializable fields */
        int maxId;
        Node first;
        BitSet bs;
        
        /* serialization support */
        private static final long serialVersionUID = CandidatePool.serialVersionUID;
        protected Snapshot() { }
        
        private Snapshot(Node first0) {
            maxId = (first0 == null) ? -1 : first0.index.getId();
            first = first0;
            bs = new BitSet();
            bs.set(0, maxId+1);
        }
        
        public java.util.Iterator<Index> iterator() {
            return new Iterator(first);
        }
        
        public int maxInternalId() {
            return maxId;
        }
        
        public BitSet bitSet() {
            return bs; // no need to clone -- this set is immutable
        }

        public Index findIndexId(int i) {
            for (Index idx : this) if (idx.getId() == i) return idx;
            return null;
        }

        public String toString()
        {
            return "" + bs;
        }
    }
    
    /*
     * Iterator for a snapshot of the candidate set
     */
    private static class Iterator implements java.util.Iterator<Index> {
        Node next;
        
        Iterator(Node start) {
            next = start;
        }

        public boolean hasNext() {
            return next != null;
        }

        public Index next() {
            if (next == null)
                throw new java.util.NoSuchElementException();
            Index current = next.index;
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
}
