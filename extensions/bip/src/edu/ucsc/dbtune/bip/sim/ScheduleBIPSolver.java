package edu.ucsc.dbtune.bip.sim;

import java.util.Set;

import edu.ucsc.dbtune.bip.core.BIPSolver;
import edu.ucsc.dbtune.metadata.Index;

public interface ScheduleBIPSolver extends BIPSolver 
{
    /**
     * Set the initial and materialized configurations 
     * @param Sinit
     *      The set of indexes that are currently stored in the system
     * @param Smat
     *      The set of indexes that are going to be materialized      
     */
    void setConfigurations(Set<Index> Sinit, Set<Index> Smat);
    
    
    /**
     * Set the number of maximum windows to materialize indexes
     * @param W
     *      The number of windows
     */
    void setNumberWindows(int W);
    
    /**
     * Set the maximum number of indexes to be materialized at each window.
     * This method is optional. 
     * @param n
     */
    void setNumberofIndexesEachWindow (int n);
    
    /**
     * Set the maximum time allowed for each window.
     * This method is optional.
     * @param T
     *      The time limit
     */
    void setCreationCostWindow (int T);
}
