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

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class StaticIndexSet implements Iterable<Index>, Iterator<Index> {
    private final Set<Index> delegate;

    /**
     * construct a {@code static and immutable index set} from some iterable input (e.g., list, collection, set)
     * of indexes.
     * @param input
     *      iterable input of indexes.
     */
    public StaticIndexSet(Iterable<Index> input) {
        delegate = new HashSet<Index>();
        for(Index each : input){
            delegate.add(Checks.checkNotNull(each));
        }
    }

    /**
     * construct an empty {@code static index set} (i.e., set's size == 0).
     */
    public StaticIndexSet() {
      this(Collections.<Index>emptyList());
    }

    /**
     * checks whether a given index is contained in this {@code static set}.
     * @param index
     *      a {@link Index index} object.
     * @return
     *      {@code true} if the index is in the set; {@code false} otherwise.
     */
    public boolean contains(Index index) {
        return delegate.contains(index);
    }

    @Override
    public boolean hasNext() {
        return delegate.iterator().hasNext();
    }

    /**
     * @return {@code true} if the set is empty, {@code false} otherwise.
     */
    public boolean isEmpty(){
        return delegate.isEmpty();
    }

    @Override
    public Iterator<Index> iterator() {
      return delegate.iterator();
    }

    @Override
    public Index next() {
        return delegate.iterator().next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public int size() {
      return delegate.size();
    }

    @Override
    public String toString() {
        return new ToStringBuilder<StaticIndexSet>(this)
               .add("indexes", delegate)
               .add("size", size())
               .toString();
    }
}
