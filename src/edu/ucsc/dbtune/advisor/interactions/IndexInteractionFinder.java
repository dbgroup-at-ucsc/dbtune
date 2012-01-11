package edu.ucsc.dbtune.advisor.interactions;

import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.Workload;

/**
 * XXX for issue #146 an implementation of this interface should port the InteractionSelector class 
 * and its dependencies from Karl's repository
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
