package edu.ucsc.dbtune.advisor.interactions;

import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * XXX for issue #146 an implementation of this interface should port the InteractionSelector class 
 * and its dependencies from Karl's repository
 * @author Quoc Trung Tran
 */
public interface IndexInteractionFinder 
{
    /**
     * 
     * @param wl
     *      the workload
     * @param c
     *      the configuration
     * @param delta
     *      the threshold
     * @return
     *      the interactions that were found
     */
    List<IndexInteraction> getInteractingIndexes(List<SQLStatement> wl, Set<Index> c, double delta);
}
