package edu.ucsc.dbtune.util;

public class UnionFind
{
    private int eltCount;
    private Elt[] elts;
    
    /*
     * A union-find structure for eltCount0 elements
     */
    public UnionFind(int eltCount0)
    {
        eltCount = eltCount0;
        elts = new Elt[eltCount0];
        for (int i = 0; i < eltCount0; i++)
            elts[i] = new Elt(i);
    }
    
    public final void clear()
    {
        for (int i = 0; i < eltCount; i++)
            elts[i].reset(i);
    }
    
    /*
     * This will print all sets that are a subset of the filter
     * It will not print sets that are disjoint from the filter.
     * Other sets might be printed.
     */
    public void print(IndexBitSet filter)
    {
        Elt[] tempElts = flip();
        
        for (int e = 0; e < eltCount; e++) {
            if (filter.contains(e) && elts[e].parent == e) {
                int i = e;
                while (true) {
                    if (tempElts[i].parent == i)
                        break;
                    i = tempElts[i].parent;
                } 
            }
        }
    }
    
    /*
     * Rearrange the parent pointers so that each set is stored linearly.
     * For each representative at pos e in the current representation, the
     * output array will store that partition starting at position e and
     * following parent pointers from there.
     */
    private Elt[] flip()
    {
        Elt[] tempElts = new Elt[eltCount];
        
        // create list roots
        for (int e = 0; e < eltCount; e++) 
            if (elts[e].parent == e)
                tempElts[e] = new Elt(e);
        
        // add other list members
        for (int e = 0; e < eltCount; e++) {
            if (elts[e].parent != e) {
                int rep = find(e);
                tempElts[e] = new Elt(e);
                
                // insert immediately after first list element (which is rep)
                if (tempElts[rep].parent != rep) 
                    tempElts[e] = new Elt(tempElts[rep].parent);
                else 
                    tempElts[e] = new Elt(e);
                tempElts[rep].parent = e;
            }
        }
        
        return tempElts;
    }
    
    public final void union(int e1, int e2)
    {
        assert(0 <= e1 && e1 < eltCount);
        assert(0 <= e2 && e2 < eltCount);
        
        int x = find(e1);
        int y = find(e2);
        if (x == y)
            return;

        Elt eltX = elts[x];
        Elt eltY = elts[y];
        if (eltX.rank > eltY.rank)
            eltY.parent = x;
        else {
            eltX.parent = y;
            if (eltX.rank == eltY.rank)
                ++eltY.rank;
        }
    }
    
    public final int find(int e)
    {
        assert(0 <= e && e < eltCount);
        
        int rep = e;
        while (elts[rep].parent != rep)
            rep = elts[rep].parent;
        
        int i = e;
        while (true) {
            int next = elts[i].parent;
            if (next == rep) break;
            elts[i].parent = rep;
            i = next;
        }
        
        return rep;
    }

    // return the partitioning of elements 
    public IndexBitSet[] sets()
    {
        int setCount = numSets();
        IndexBitSet[] sets = new IndexBitSet[setCount];
        for (int i = 0; i < setCount; i++)
            sets[i] = new IndexBitSet();
        
        // get the flipped representation   
        Elt[] tempElts = flip();
        
        int s = 0;
        for (int e = 0; e < eltCount; e++) {
            if (elts[e].parent == e) {
                int i = e;
                while (true) {
                    sets[s].add(i);
                    if (tempElts[i].parent == i)
                        break;
                    i = tempElts[i].parent;
                } 
                ++s;
            }
        }
            
        assert(s == setCount);
        return sets;
    }
    
    private class Elt
    {
        int parent;
        int rank;
        
        Elt(int e) {
            parent = e;
            rank = 0;
        }
        
        void reset(int e) {
            parent = e;
            rank = 0;
        }
    }

    public final int numSets()
    {
        int setCount;
        
        // count the sets we'll need and initialize them
        setCount = 0;
        for (int e = 0; e < eltCount; e++) 
            if (elts[e].parent == e)
                ++setCount;
        
        return setCount;
    }
}
