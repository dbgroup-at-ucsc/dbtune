package edu.ucsc.dbtune.advisor.interactions;

import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.Workload;

/**
 * @author Quoc Trung Tran
 */
public interface IndexInteractionFinder 
{
    /**
     * 
     * @param w
     *      the workload
     * @param c
     *      the configuration
     * @param delta
     *      the threshold
     * @return
     *      the interactions that were found
     */
    List<IndexInteraction> getInteractingIndexes(Workload w, Set<Index> c, double delta);
}
