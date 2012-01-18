package edu.ucsc.dbtune.advisor.interactions;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.bip.interactions.InteractionBIP;
import edu.ucsc.dbtune.bip.interactions.InteractionOutput;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
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
        InteractionBIP bip = new InteractionBIP(delta);
        bip.setCandidateIndexes(c);
        bip.setSchemaToWorkloadMapping(w.getSchemaToWorkloadMapping());
        bip.setWorkloadName("wl.sql");
        bip.setInumOptimizer(this.inumOptimizer);
        InteractionOutput out = new InteractionOutput();
        try {
            out = (InteractionOutput) bip.solve();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return out.getListInteractions();
    }
}
