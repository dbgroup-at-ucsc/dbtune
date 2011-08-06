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
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.Objects;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.TreeMap;

public class IndexPartitions {
    private static final int MAXIMUM_INDEX_COUNT = Integer.MAX_VALUE / 2;

    private final int                 indexCount;
    private       int                 stateCount;
    private       SubsetList          subsets;

    /**
     * construct an {@link IndexPartitions} object given a set of static indexes.
     * @param indexes
     *      a {@link StaticIndexSet set of static indexes}.
     */
    public IndexPartitions(StaticIndexSet indexes) {
        Checks.checkArgument(
                indexes.size() <= MAXIMUM_INDEX_COUNT,
                "Cannot create partitions for " + indexes.size() + "indexes"
                );

        indexCount  = indexes.size();
        stateCount  = indexes.size() * 2;
        subsets     = new SubsetList();
        for (DBIndex index : indexes) {
            subsets.add(new Subset(index));
        }
    }

    /**
     * construct an {@link IndexPartitions} object given a snapshot of candidate indexes
     * and an array of partitions of indexes represented as bitsets.
     * @param snapshot
     *      a {@link Snapshot} of candidate indexes.
     * @param partitionBitSets
     *     an array of partitions of indexes represented as bitsets.
     */
    public IndexPartitions(Snapshot snapshot, IndexBitSet[] partitionBitSets) {
        // create subsets
        int indexCount0 = 0;
        int stateCount0 = 0;
        subsets = new SubsetList();
        for (IndexBitSet eachBitSet : partitionBitSets) {
            Subset subset = null;
            for (int i = eachBitSet.nextSetBit(0); i >= 0; i = eachBitSet.nextSetBit(i+1)) {
                DBIndex idx = snapshot.findIndexId(i);
                if (idx != null) {
                    if (subset != null){
                        subset = new Subset(subset, new Subset(idx));
                    } else{
                        subset = new Subset(idx);
                    }
                }
            }
            if (subset != null) {
                subsets.add(subset);
                indexCount0 += subset.size();
                stateCount0 += subset.stateCount();
            }
        }

        if (indexCount0 > MAXIMUM_INDEX_COUNT)
            throw new IllegalArgumentException("Cannot create partitions for " + indexCount0 + "indexes");

        indexCount = indexCount0;
        stateCount = stateCount0;
    }

    /**
     * @return
     *     an array of {@link edu.ucsc.dbtune.util.IndexBitSet bitsets}. Each {@code bitset} 
     *     contains a set of indexes of interest.
     */
    public IndexBitSet[] bitSetArray() {
        IndexBitSet[] arr = new IndexBitSet[subsets.size()];
        for (int i = 0; i < subsets.size(); i++) {
            arr[i] = subsets.get(i).bitSet();
        }
        return arr;
    }

    @Override
    public boolean equals(Object o1) {
        if (!(o1 instanceof IndexPartitions)){
            return false;
        }

        IndexPartitions other = (IndexPartitions) o1;
        return !(indexCount != other.indexCount || stateCount != other.stateCount)
            && subsets.equals(other.subsets);
    }

    /**
     * Returns a {@code subset} of indexes at a given position in some linked list of
     * indexes' {@code subsets}.
     * @param i
     *      position of subset.
     * @return
     *      a {@code subset} of indexes
     */
    public final Subset get(int i) {
        return subsets.get(i);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(indexCount, stateCount, subsets);
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
    public final void merge(DBIndex i1, DBIndex i2) {
        int s1 = subsets.whichSubset(i1);
        int s2 = subsets.whichSubset(i2);
        merge(s1, s2);
    }

    /**
     * merges two subsets A and B in which A is located in position <em>s1</em> and B in position
     * <em>s2</em> in the linked list of subsets of indexes.
     * @param s1
     *      position of subset A.
     * @param s2
     *      position of subset B.
     */
    public void merge(int s1, int s2) {
        if (s1 == s2){
            return;
        }

        Subset subset1   = subsets.get(s1);
        Subset subset2   = subsets.get(s2);
        Subset newSubset = new Subset(subset1, subset2);

        // calculate & check new state count
        long oldStateCount = stateCount;
        long newStateCount = oldStateCount - subset1.stateCount() - subset2.stateCount() + newSubset.stateCount();
        if (newStateCount > Integer.MAX_VALUE)
            throw new IllegalArgumentException("merging subsets results in too many states");

        subsets.remove(s1);
        subsets.remove(s1 < s2 ? (s2-1) : s2);
        subsets.add(newSubset);
        stateCount = (int) newStateCount;
    }

    /**
     * @return the number of indexes' subsets stored in this {@code index partitions} object.
     */
    public int subsetCount() {
        return subsets.size();
    }

    /**
     * Returns the <em>theoretical</em> plan cost of a {@code profiled query}.
     * @param qinfo
     *      a {@link ProfiledQuery} object.
     * @param state
     *      an index configuration.
     * @param scratch
     *      a scratch bit set of indexes' internal ids.
     * @param <J>
     *      the {@link DBIndex} type.
     * @return
     *      the <em>theoretical</em> plan cost
     */
    public double theoreticalCost(ProfiledQuery qinfo, IndexBitSet state, IndexBitSet scratch) {
        // Let's override the nonsense for now
        return qinfo.planCost(state);
    }

    /**
     *
     * @return the state count of the {@code the WorkFunctionAlgorithm}.
     */
    public int wfaStateCount() {
        return stateCount;
    }

    @Override
    public String toString() {
        return new ToStringBuilder<IndexPartitions>(this)
            .add("indexCount", indexCount())
            .add("stateCount", wfaStateCount())
            .add("subsets count", subsetCount())
            .add("subsets", subsets)
            .toString();
    }

    /**
     * A subset of indexes.
     */
    public static class Subset implements Iterable<DBIndex> {
        private final int sumIndexIds;
        private final int minIndexIds;

        private int[] indexIds;

        private TreeMap<Integer,DBIndex> map = new TreeMap<Integer,DBIndex>();

        /**
         * construct a subset of indexes. This object will maintain a set of attributes describing
         * an array of stored indexes' ids: e.g., sum of indexes ids, the min of indexes Ids.
         * @param index
         *      the first index to be stored in the subset object.
         */
        Subset(DBIndex index) {
            map.put(index.internalId(), index);
            indexIds = new int[] { index.internalId() };
            sumIndexIds = minIndexIds = index.internalId();
        }

        /**
         * construct a subset of indexes from two subsets of indexes.
         * @param s1
         *      first subset.
         * @param s2
         *      second subset.
         */
        Subset(Subset s1, Subset s2) {
            for (DBIndex idx : s1)
                map.put(idx.internalId(), idx);
            for (DBIndex idx : s2)
                map.put(idx.internalId(), idx);

            indexIds = new int[map.size()];
            { int i = 0; for (DBIndex idx : map.values()) indexIds[i++] = idx.internalId(); }

            sumIndexIds = s1.sumIndexIds + s2.sumIndexIds;
            minIndexIds = Math.min(s1.minIndexIds, s2.minIndexIds);
        }

        /**
         * Checks whether some index is in this subset of indexes.
         * @param index
         *      index object.
         * @return {@code true} if some index is contained in this subset,
         *      {@code false} otherwise.
         */
        public final boolean contains(DBIndex index) {
            return map.containsKey(index.internalId());
        }

        /**
         * Checks whether some index is in this subset of indexes.
         * @param id
         *      index's internal id.
         * @return {@code true} if some index (using its internalId) is contained in this subset,
         *      {@code false} otherwise.
         */
        public final boolean contains(int id) {
            return map.containsKey(id);
        }

        /**        
         * @return the number of indexes in this {@link Subset this subset}.
         */
        public final int size() {
            return indexIds.length;
        }

        /**
         *
         * @return the number of states in the {@link Subset this subset} after being merged with
         * {@link Subset another subset}..
         */
        public final long stateCount() {
            return 1L << size();
        }

        public Iterator<DBIndex> iterator() {
            return map.values().iterator();
        }

        /**
         * Determines if {@link Subset this set} overlaps with {@link Subset another subset}.
         * @param other
         *      the {@link Subset other subset}.
         * @return
         *      {@code true} if both subsets overlaps, {@code false} otherwise.
         */
        public boolean overlaps(Subset other) {
            for (DBIndex x : map.values()) {
                if (other.contains(x)){
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof IndexPartitions.Subset))
                return false;
            Subset other = (Subset) o;
            if (other.size() != size())
                return false;

            // iterate over both subsets in sorted order
            Iterator<Integer> iter1 = map.keySet().iterator();
            Iterator<Integer> iter2 = other.map.keySet().iterator();
            while (iter1.hasNext()) {
                if (!iter1.next().equals(iter2.next()))
                    return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(size(), map.keySet());
        }

        /**         
         * @return a {@link edu.ucsc.dbtune.util.IndexBitSet} of indexes' internal ids.
         */
        public IndexBitSet bitSet() {
            IndexBitSet bs = new IndexBitSet();
            for (DBIndex x : this) bs.set(x.internalId());
            return bs;
        }

        /**
         * @return an array of indexes' ids.
         */
        public int[] indexIds() {
            return indexIds;
        }

        /**
         * @return the average of total number of indexes in the subset and its size.
         */
        private float avgIndexId() {
            return sumIndexIds / (float) size();
        }

        /**
         * @return the min internal id out of all indexes' internal ids available in the subset.
         */
        private int minIndexIds() {
            return minIndexIds;
        }

        @Override
        public String toString() {
            return new ToStringBuilder<Subset>(this)
                .add("sumIndex", sumIndexIds)
                .add("minIndexIds", minIndexIds())
                .add("averageIndexIds", avgIndexId())
                .add("idToIndexMap", map)
                .toString();
        }
    }

    /**
     * a linked list of indexes subsets.
     */
    private static class SubsetList {
        LinkedList<Subset> list;

        SubsetList() {
            list = new LinkedList<Subset>();
        }

        final Subset get(int i) {
            return list.get(i);
        }

        @Override
        public final boolean equals(Object o) {
            if (!(o instanceof IndexPartitions.SubsetList))
                return false;

            SubsetList other = (SubsetList) o;
            return list.equals(other.list);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(list);
        }

        final void add(Subset subset) {
            ListIterator<Subset> iter = list.listIterator();

            while (iter.hasNext()) {
                Subset n = iter.next();
                if (subset.avgIndexId() < n.avgIndexId()
                        || (subset.avgIndexId() == n.avgIndexId() && subset.minIndexIds() < n.minIndexIds())) {
                    iter.previous();
                    iter.add(subset);
                    return;
                }
            }

            // add to the end of the list
            list.add(subset);
        }

        final int whichSubset(DBIndex index) {
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i).contains(index))
                    return i;
            }
            throw new NoSuchElementException();
        }

        final void remove(int i) {
            list.remove(i);
        }

        final int size() {
            return list.size();
        }

        @Override
        public String toString() {
            return list.toString();
        }
    }
}
