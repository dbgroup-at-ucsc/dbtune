package edu.ucsc.dbtune.bip.util;

import edu.ucsc.dbtune.inum.InumSpace;
import edu.ucsc.dbtune.workload.SQLStatement;

public class InumCommunicator 
{
    // TODO: the methods of this class must be implemented in somewhere in INUM package
    /**
     * Interact with INUM to get the INUM's search space for the given {@code stmt} statement
     * 
     * @param stmt
     *     A SQL statement
     * 
     * @return
     *      The inum space of the given SQL statement
     */
    public InumSpace populateInumSpace(SQLStatement stmt)
    {
        // TODO: interact with INUM to get the INUM space 
        throw new RuntimeException("NOT IMPLEMENTED YET");
    }
}
