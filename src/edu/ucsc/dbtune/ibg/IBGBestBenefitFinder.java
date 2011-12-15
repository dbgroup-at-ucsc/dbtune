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

            if (visited.get(Y.getID()))
                continue;
            visited.set(Y.getID());

            if (!Y.getConfiguration().get(indexId) && M.subsetOf(Y.getConfiguration())) {
                bitset_Ya.set(Y.getConfiguration());
                bitset_Ya.set(indexId);
                IBGNode Ya = finder.findFast(ibg.rootNode(), bitset_Ya, null);
                double value = Y.cost() - Ya.cost();
                bestValue = Math.max(value, bestValue);
                //printExpanded(ibg, node);
            }
            pending.addChildren(Y.firstChild());
        }
        
        return bestValue;
    }
}
