/* ************************************************************************** *
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
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied  *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
package edu.ucsc.dbtune.advisor.interactions;

import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public class IndexPartitions
{
    private List<Set<Index>> subsets;
    private int              indexCount;

    /**
     * construct an {@link IndexPartitions} object containing only one partition.
     *
     * @param configuration
     *      a configuration to be partitioned.
     */
    public IndexPartitions(Configuration conf) {
        Set<Index> set;

        indexCount = conf.size();
        subsets    = new ArrayList<Set<Index>>();
        set        = new HashSet<Index>();

        for(Index idx : conf)
            set.add(idx);

        subsets.add(set);
    }

    /**
     * Returns a {@code subset} of indexes at a given position in some linked list of
     * indexes' {@code subsets}.
     * @param i
     *      position of subset.
     * @return
     *      a {@code subset} of indexes
     */
    public final Set<Index> get(int i) {
        return subsets.get(i);
    }

    /**
     * @return the number indexes in this {@code index partitions} object.
     */
    public int indexCount() {
        return indexCount;
    }

    /**
     * merges two subsets A and B in which A contains an index <em>i1</em> and B contains an
     * index <em>i2</em>.
     * @param i1
     *      first index object.
     * @param i2
     *      second index object.
     */
    public final void merge(Index i1, Index i2) {
        Set<Index> s1 = whichSubset(i1);
        Set<Index> s2 = whichSubset(i2);
        merge(s1, s2);
    }


    private Set<Index> whichSubset(Index index) {
        for (int i = 0; i < subsets.size(); i++) {
            if (subsets.get(i).contains(index))
                return subsets.get(i);
        }

        throw new NoSuchElementException();
    }
    /**
     * merges two subsets A and B in which A is located in position <em>s1</em> and B in position
     * <em>s2</em> in the linked list of subsets of indexes.
     * @param s1
     *      position of subset A.
     * @param s2
     *      position of subset B.
     */
    public void merge(Set<Index> s1, Set<Index> s2)
    {
        if (s1 == s2)
            return;

        if (!subsets.contains(s1) || !subsets.contains(s2))
            throw new RuntimeException("Both subsets should be contained in the partitions object");

        Set<Index> newSubset = new HashSet<Index>();

        newSubset.addAll(s1);
        newSubset.addAll(s2);
        subsets.remove(s1);
        subsets.remove(s2);
        subsets.add(newSubset);
    }

    /**
     * @return the number of indexes' subsets stored in this {@code index partitions} object.
     */
    public int subsetCount() {
        return subsets.size();
    }
}
