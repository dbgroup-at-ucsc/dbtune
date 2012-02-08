package edu.ucsc.dbtune.bip.core;


import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.workload.Workload;

/**
 * This class abstracts the many different ways of generating candidate configurations, like Optimal, 1C, PowerSet, etc
 * 
 * @author Quoc Trung Tran
 *
 */
public interface CandidateGenerator 
{   
    /**
     * Set the input workload, which we generate the set of candidate indexes for
     * @param wl
     *      The workload
     *
     */
    void setWorkload(Workload wl);
    
    
    /**
     * Set the optimizer is used to generate candidate indexes
     * 
     * @param optimizer
     *      An optimizer
     */
    void setOptimizer(Optimizer optimizer);
    
    /**
     * Generate the optimal candidate set; i.e., invoke the optimizer to generate candidate indexes
     * for each statement in the workload and then union the set of candidate for each statement.
     * The repeated indexes (with the same content) are discarded.
     * 
     * @return 
     *      The set of candidate indexes
     * @throws SQLException
     *      when there's an error in invoking the optimizer 
     *       
     */
    Set<Index> optimalCandidateSet() throws SQLException;
    
    /**
     * Generate the set of candidate indexes that contain only one column; i.e., invoke the optimizer
     * to generate {@code optimalCandidateSet()} and keep the index that has one column. 
     * 
     * @return 
     *      The set of candidate indexes
     * 
     * @throws SQLException
     *      when there's an error in invoking the optimizer 
     * 
     */
    Set<Index> oneColumnCandidateSet() throws SQLException;
    
    /**
     * Generate the set of candidate indexes that contains the powerset of all columns referenced
     * in the query.
     * 
     * @return 
     *      The set of candidate indexes
     * 
     * @throws SQLException
     *      when there's an error in invoking the optimizer 
     * 
     */
    Set<Index> powerSetCandidateSet() throws SQLException;
}
