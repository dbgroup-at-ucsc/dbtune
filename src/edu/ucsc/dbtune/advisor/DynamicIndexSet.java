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
package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.ToStringBuilder;
import edu.ucsc.dbtune.util.Instances;

import java.util.Iterator;
import java.util.Set;

/**
 * A set of materialized indexes which could dynamically grow or shrink. Its implementation heavily relies on
 * a {@code set} and a {@code bitset} objects.
 *
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class DynamicIndexSet<I extends DBIndex> implements Iterable<I> {
    private final Set<I> delegate;
    private final IndexBitSet ownedBitSet;

    /**
     * construct a {@code DynamicIndexSet} object.
     */
    public DynamicIndexSet(){
       this(Instances.<I>newHashSet(), new IndexBitSet());
    }

    /**
     * construct a {@code DynamicIndexSet} object. This object takes, as a delegate,
     * a specific {@code set implementation} and an owned {@link edu.ucsc.dbtune.util.IndexBitSet} object.
     * @param delegate
     *      delegate implementation of a {@code set}.
     * @param ownedBitSet
     *      ownedBitSet which will be used for further index processing given the
     *      indexes's {@code internalId}.
     */
    DynamicIndexSet(Set<I> delegate, IndexBitSet ownedBitSet){
        this.delegate = delegate;
        this.ownedBitSet = ownedBitSet;
    }

    /**
     * adds an {@code index} to the set.
     * @param index
     *      index object to be added.
     */
    public void add(I index){
        if(!delegate.contains(index)){
            delegate.add(index);
            synchronized (delegate){
                ownedBitSet.set(index.internalId());
            }
        }
    }

    /**
     * @return a cloned version of the {@code bitset} managed by this
     *      {@code dynamic set}.
     */
    public IndexBitSet bitSet(){
        // need to clone since we modify it in place
        return ownedBitSet.clone();
    }

    /**
     * Returns whether the {@code dynamic set} contains the desired index.
     * @param index
     *      index to be checked.
     * @return
     *    {@code true} if the {@code dynamic set} contains the desired index,
     *    {@code false} otherwise.
     */
    public boolean contains(I index){
        // before it was bs.get(index.internalId()); => (Huascar) I think this is
        // broken.
        return ownedBitSet.get(index.internalId()) && delegate.contains(index);
    }

    /**
     * @return {@code true} if the set is empty. {@code false} otherwise.
     */
    public boolean isEmpty(){
        return size() == 0;
    }

    @Override
    public Iterator<I> iterator() {
        return delegate.iterator();
    }

    /**
     * removes an {@code index} from the set.
     * @param index
     *      index to be removed.
     */
    public void remove(I index){
        if(delegate.contains(index)){
            delegate.remove(index);
            ownedBitSet.clear(index.internalId());
        }
    }

    /**
     * @return the number of indexes stored in the {@code dynamic set}.
     */
    public int size(){
        return delegate.size();
    }

    /**
     * @return an array-version of the elements contained in this {@code dynamic set}.
     */
    public DBIndex[] toArray(){
        return delegate.toArray(new DBIndex[delegate.size()]);
    }

    @Override
    public String toString() {
        return new ToStringBuilder<DynamicIndexSet<I>>(this)
               .add("delegate", delegate)
               .add("ownedBitSet", ownedBitSet).toString();
    }
}
