package edu.ucsc.dbtune.bip.util;


import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.workload.Workload;

/**
 * The main interface to use the BIP for an index tuning related problems
 * 
 * @author tqtrung@soe.ucsc.edu
 *
 */
public interface BIPSolver 
{    
    /**
     * The main entry of using BIP to solve index tuning related problem
     * The method communicates with INUM to populate INUM's space, formulates a Binary Integer Prgoram
     * and construct the output from the result of the BIP solver
     * 
     * @return
     *      The result derived from the result of BIP solver
     *      
     * @throws SQLException
     */
    BIPOutput solve() throws SQLException;
    
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
     * Set the workload name. 
     * This function uses the given name to creates files that will contain the formulated BIP
     * The file stored in  
     * 
     * @param name
     *      The name of the workload
     */
    void setWorkloadName(String name);
}
