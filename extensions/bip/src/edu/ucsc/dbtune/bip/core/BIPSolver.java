package edu.ucsc.dbtune.bip.core;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * It serves as the entry interface for the approach that formulates 
 * Binary Integer Program to solve index tuning related problems
 * 
 * @author Quoc Trung Tran
 *
 */
public interface BIPSolver 
{    
    /**
     * Set the list of candidate indexes as a part of the inputs 
     * @param candidateIndexes
     *      The set of candidate indexes
     */
    void setCandidateIndexes(Set<Index> candidateIndexes);
    
    /**
     * Set the workload as a part of the inputs
     * 
     * @param workload
     *      The input workload
     * 
     */
    void setWorkload(List<SQLStatement> wl);
    
    
    /**
     * Set the optimizer that will be used by the solver
     * 
     * @param optimizer
     *      An optimizer
     *  
     * @throw Exception
     *      If the given optimizer is not expected by a particular method      
     */
    void setOptimizer(Optimizer optimizer) throws Exception;
    
    /**
     * 
     * The method communicates with INUM to populate INUM's space, builds a Binary Integer Program,
     * asks CPLEX to solve the formulated BIP, and finally derives the output from the result of 
     * CPLEX solver
     * 
     * @return
     *      The result derived from the output of CPLEX
     *      
     * @throws SQLException
     *      when there is error in connecting with the optimizer
     * @throws IOException
     *      when there is I/O error
     * @throws Exception 
     */
    IndexTuningOutput solve() throws Exception;
}
