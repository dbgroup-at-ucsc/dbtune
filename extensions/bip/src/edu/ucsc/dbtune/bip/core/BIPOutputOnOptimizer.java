package edu.ucsc.dbtune.bip.core;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * This interface takes the result from BIP (e.g., a schedule) and re-compute the cost on 
 * the actual optimizer (e.g, DB2). For instance, in interaction problem, for every pair 
 * of indexes that is reported as interaction, this interface invokes the DB2 optimizer to 
 * re-compute the degree of interaction in order to verify the interacting of this pair. 
 *
 * @author Quoc Trung Tran
 *
 */
public interface BIPOutputOnOptimizer 
{   
    /**
     * Verify the result from BIP. The semantic of this method depends heavily on each problem
     * instance. 
     * 
     * @param optimizer
     *     The actual optimizer that will be use to verify the BIP's result 
     * 
     * @param bip
     *     The output result from BIP 
     *
     * @throws SQLException
     *      when there is error in communicating with the optimizer
     */
    void verify(Optimizer optimizer, IndexTuningOutput bip, Set<SQLStatement> workload)
    throws SQLException;
}
