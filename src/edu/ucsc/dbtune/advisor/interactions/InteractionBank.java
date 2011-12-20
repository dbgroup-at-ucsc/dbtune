package edu.ucsc.dbtune.advisor.interactions;

import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.BitArraySet;

public class InteractionBank
{
    public final int indexCount;
    private double[] bestBenefit;
    private double[][] lowerBounds; 

    public InteractionBank(Set<Index> candidateSet)
    {
        indexCount = candidateSet.size() + 1;
        bestBenefit = new double[indexCount];
        lowerBounds = new double[indexCount][];
        for (int i = 0; i < indexCount; i++)
            lowerBounds[i] = new double[i];
    }

    /*
     * Assign interaction with an exact value
     */
    public final void assignInteraction(Index i1, Index i2, double newValue)
    {
        int id1 = i1.getId();
        int id2 = i2.getId();

        assert (newValue >= 0);
        assert (id1 != id2);

        if (id1 < id2) {
            int t = id1;
            id1 = id2;
            id2 = t;
        }

        lowerBounds[id1][id2] = Math.max(newValue, lowerBounds[id1][id2]);
    }

    public void assignBenefit(Index i, double newValue)
    {
        bestBenefit[i.getId()] = Math.max(newValue, bestBenefit[i.getId()]);
    }

    /*
     * Get an interaction value
     */
    public final double interactionLevel(int id1, int id2)
    {
        //      if (id1 >= indexCount || id2 >= indexCount)
        //          return 0;

        assert (id1 != id2);    
        if (id1 < id2) 
            return lowerBounds[id2][id1];
        else
            return lowerBounds[id1][id2];
    }

    /*
     * Get the best benefit of an index
     */
    public final double bestBenefit(int id)
    {
        //      if (id > indexCount)
        //          return 0;
        return bestBenefit[id];
    }

    public final BitArraySet<Index>[] stablePartitioning(double threshold)
    {
        //UnionFind uf = new UnionFind(indexCount);
        //for (int a = 0; a < indexCount; a++) 
            //for (int b = 0; b < a; b++) 
                //if (lowerBounds[a][b] > threshold)
                    //uf.union(a,b);
        //return uf.sets();
        return (BitArraySet<Index>[]) null;
    }
}
