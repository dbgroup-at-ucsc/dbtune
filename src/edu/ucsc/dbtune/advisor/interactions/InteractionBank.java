package edu.ucsc.dbtune.advisor.interactions;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.BitArraySet;
import edu.ucsc.dbtune.util.UnionFind;

/**
 * Stores information about interactions of a set of indexes.
 *
 * @author Karl Schnaitter
 * @author Ivo Jimenez
 */
public class InteractionBank
{
    private double[] bestBenefit;
    private double[][] lowerBounds;
    private List<Index> candidates;
    
    /**
     * Creates a bank that will manage interaction information for the given candidate set.
     *
     * @param candidateSet
     *      set of indexes for which interactions are stored.
     */
    public InteractionBank(Set<Index> candidateSet)
    {
        int maxId = 0;

        for (Index i : candidateSet)
            if (i.getId() > maxId)
                maxId = i.getId();

        candidates = new ArrayList<Index>(candidateSet);
        bestBenefit = new double[maxId];
        lowerBounds = new double[maxId][];

        for (int i = 0; i < maxId; i++)
            lowerBounds[i] = new double[i];
    }
    
    /**
     * Assigns interaction with an exact value.
     *
     * @param i1
     *      first index on the interaction
     * @param i2
     *      second index on the interaction
     * @param newValue
     *      the interaction level
     * @throws RuntimeException
     *      if {@code newValue} is less than zero; if {@code i1.equals(i2)} is {@code true}.
     */
    final void assignInteraction(Index i1, Index i2, double newValue)
    {
        assert newValue >= 0;
        assert !i1.equals(i2);

        int id1 = i1.getId();
        int id2 = i2.getId();

        // doi is a symmetric relation, thus we use the upper part of the matrix only. We accomplish 
        // this by ordering the id's of the given indexes in descending order, i.e. first we use the 
        // largest id first, then the smallest
        if (id1 < id2) {
            int t = id1;
            id1 = id2;
            id2 = t;
        }
        
        lowerBounds[id1][id2] = Math.max(newValue, lowerBounds[id1][id2]);
    }

    /**
     * Returns the interaction value for a given pair of indexes.
     *
     * @param i1
     *      first index on the pair
     * @param i2
     *      second index on the pair
     * @return
     *      the interaction level assigned to the pair
     */
    public final double interactionLevel(Index i1, Index i2)
    {
        assert !i1.equals(i2);

        if (i1.getId() < i2.getId())
            return lowerBounds[i2.getId()][i1.getId()];
        else
            return lowerBounds[i1.getId()][i2.getId()];
    }

    /**
     * Assigns the benefit of an index.
     *
     * @param i
     *      index being assigned
     * @param newValue
     *      value of the benefit
     */
    void assignBenefit(Index i, double newValue)
    {
        bestBenefit[i.getId()] = Math.max(newValue, bestBenefit[i.getId()]);
    }
    
    /**
     * Returns the best benefit of an index. The best benefit is the smallest value of all the ones 
     * that have been assigned to {@code i} through {@link #assignBenefit}.
     *
     * @param i
     *      index whose benefit is being retrieved
     * @return
     *      the best benefit assigned so far to {@code i}
     */
    public final double bestBenefit(Index i)
    {
        return bestBenefit[i.getId()];
    }
    
    /**
     * Returns the stable partitioning of the candidateSet.
     *
     * @param threshold
     *      parameter used to take into account to accomplish the partitioning of the candidate set.
     * @return
     *      the partitioned set
     */
    public final Set<Set<Index>> stablePartitioning(double threshold)
    {
        UnionFind uf = new UnionFind(candidates.size());

        for (int a = 0; a < candidates.size(); a++)
            for (int b = 0; b < a; b++)
                if (lowerBounds[a][b] > threshold)
                    uf.union(a, b);

        Set<Set<Index>> partitioning = new HashSet<Set<Index>>();

        for (BitSet bs : uf.sets()) {

            Set<Index> s = new BitArraySet<Index>();

            for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1)) {
                s.add(candidates.get(i));
            }

            partitioning.add(s);
        }

        return partitioning;
    }
}
