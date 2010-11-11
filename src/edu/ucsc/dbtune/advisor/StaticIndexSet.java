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

import edu.ucsc.dbtune.util.ToStringBuilder;
import edu.ucsc.dbtune.core.DBIndex;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

public class StaticIndexSet<I extends DBIndex<I>> implements Iterable<I>, Iterator<I> {
    private int i = 0;
	private Object[] arr;

    /**
     * construct a {@code static index set} from some iterable input (e.g., list, collection, set)
     * of indexes.
     * @param input
     *      iterable input of indexes.
     */
	public StaticIndexSet(Iterable<I> input) {
		int count = 0;
		for (I idx : input) {
			if (idx == null) throw new IllegalArgumentException();
			++count;
		}
		arr = new Object[count];
		int i = 0;
		for (I idx : input){
            arr[i++] = idx;
        }
	}

    /**
     * construct an empty {@code static index set} (i.e., set's size == 0).
     */
	public StaticIndexSet() {
        this(Collections.<I>emptyList());
	}

    /**
     * checks whether a given index is contained in this {@code static set}.
     * @param index
     *      a {@link DBIndex index} object.
     * @return
     *      {@code true} if the index is in the set; {@code false} otherwise.
     */
	public boolean contains(I index) {
		if (arr == null)
			return false;
		
		for (Object other : arr) {
			if (index.equals(other)) return true;
		}
		
		return false;
	}

    @Override
    public boolean hasNext() {
        return i < arr.length;    
    }

    /**
     * @return {@code true} if the set is empty, {@code false} otherwise.
     */
    public boolean isEmpty(){
        return size() == 0;
    }

    @Override
	public Iterator<I> iterator() {
		return this;
	}

    @Override
    public I next() {
        if (i >= arr.length)
            throw new java.util.NoSuchElementException();
        //noinspection unchecked
        return (I) arr[i++]; // unchecked warning here...
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

	public int size() {
		return arr.length;
	}

    @Override
    public String toString() {
        return new ToStringBuilder<StaticIndexSet<I>>(this)
               .add("indexes array", Arrays.toString(arr))
               .add("size", size())
               .toString();
    }
}
