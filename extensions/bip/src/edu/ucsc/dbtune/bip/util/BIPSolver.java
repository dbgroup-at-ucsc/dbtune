package edu.ucsc.dbtune.bip.util;


import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.workload.Workload;

/**
 * It serves as the entry interface for the approach that formulates 
 * Binary Integer Program to solve index tuning related problems
 * 
 * @author tqtrung@soe.ucsc.edu
 *
 */
public interface BIPSolver 
{    
    /**
     * Set the input list of candidate indexes 
     * @param candidateIndexes
     *      The set of candidate indexes
     */
    void setCandidateIndexes(List<Index> candidateIndexes);
    
    /**
     * Set the input workload
     * 
     * @param mapSchemaToWorkload
     *      Each entry of this map is a list of SQL statements that belong to a same schema
     * 
     */
    void setMapSchemaToWorkload(Map<Schema, Workload> mapSchemaToWorkload);
    
    
    /**
     * Set the input workload name 
     * 
     * @param name
     *      The name of the workload
     *      
     * {\bf Note:} The workload name is used to name the file on which the BIP is stored      
     */
    void setWorkloadName(String name);
    
    /**
     * 
     * The method communicates with INUM to populate INUM's space, builds a Binary Integer Program,
     * asks CPLEX to solve the formulated BIP, and finally derives the output from the result of CPLEX
     * 
     * @return
     *      The result derived from the output of CPLEX
     *      
     * @throws SQLException
     */
    BIPOutput solve() throws SQLException;
    
}
