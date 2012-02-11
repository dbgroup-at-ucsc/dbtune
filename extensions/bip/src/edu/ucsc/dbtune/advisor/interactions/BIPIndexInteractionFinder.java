package edu.ucsc.dbtune.advisor.interactions;

import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.bip.interactions.InteractionBIP;
import edu.ucsc.dbtune.bip.interactions.InteractionOutput;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.workload.Workload;

public class BIPIndexInteractionFinder implements IndexInteractionFinder 
{
    private InumOptimizer  inumOptimizer;
    
    public void setOptimizer(InumOptimizer optimizer)
    {
        this.inumOptimizer = optimizer;
    }
    
    @Override
    public List<IndexInteraction> getInteractingIndexes(Workload w, Set<Index> c, double delta) 
    {
        InteractionOutput out = new InteractionOutput();
        InteractionBIP bip = new InteractionBIP(delta);
        bip.setCandidateIndexes(c);
        bip.setWorkload(w);        
        
        try {
            bip.setOptimizer(inumOptimizer);                        
            out = (InteractionOutput) bip.solve();
        } catch (Exception e) {
            e.printStackTrace();
        } 
        
        return out.get();
    }
}
