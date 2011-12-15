package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.util.IndexBitSet;

public class IBGBestBenefitFinder
{
    private final IndexBitSet visited = new IndexBitSet();
    private final IndexBitSet bitset_Ya = new IndexBitSet();
    private final IBGNodeQueue pending = new IBGNodeQueue();
    private final IBGCoveringNodeFinder finder = new IBGCoveringNodeFinder();
    
    public double bestBenefit(IndexBenefitGraph ibg, int indexId, IndexBitSet M)
    {
        visited.clear();
        pending.clear();
        
        double bestValue = 0;
        
        pending.addNode(ibg.rootNode());
        while (pending.hasNext()) {
            IBGNode Y = pending.next();

            if (visited.contains(Y.getID()))
                continue;
            visited.add(Y.getID());

            if (!Y.getConfiguration().contains(indexId) && 
                    Y.getConfiguration().contains(Y.getConfiguration())) {
                bitset_Ya.clear();
                bitset_Ya.addAll(Y.getConfiguration());
                bitset_Ya.add(indexId);
                IBGNode Ya = finder.findFast(ibg.rootNode(), bitset_Ya, null);
                double value = Y.cost() - Ya.cost();
                bestValue = Math.max(value, bestValue);
            }
            pending.addChildren(Y.firstChild());
        }
        
        return bestValue;
    }
}
