package edu.ucsc.dbtune.advisor.wfit;

import java.util.Iterator;

import edu.ucsc.dbtune.metadata.Index;

public class DynamicIndexSet implements Iterable<Index> {
    private java.util.Set<Index> set = new java.util.HashSet<Index>();
    private BitSet bs = new BitSet();
    
    public void add(Index index) {
        set.add(index);
        bs.set(index.getId());
    }
    
    public boolean contains(Index index) {
        return bs.get(index.getId());
    }
    
    public void remove(Index index) {
        set.remove(index);
        bs.clear(index.getId());
    }

    public Iterator<Index> iterator() {
        return set.iterator();
    }

    public int size() {
        return set.size();
    }

    public Index[] toArray() {
        int i = 0;
        Index[] arr = new Index[set.size()];
        for (Index index : set)
            arr[i++] = index;
        return arr;
    }

    public BitSet bitSet() {
        // need to clone since we modify it in place
        return bs.clone();
    }
}
