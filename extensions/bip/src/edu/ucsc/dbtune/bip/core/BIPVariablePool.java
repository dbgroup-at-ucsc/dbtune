package edu.ucsc.dbtune.bip.core;

import edu.ucsc.dbtune.bip.util.BIPVariable;

/**
 * This interface represents a container that stores all variables constructed in a BIP
 * 
 * @author tqtrung@soe.ucsc.edu
 */
public interface BIPVariablePool 
{
    /**
     * Add a variable into this pool
     * @param var
     *      A BIP variable
     */
    void add(BIPVariable var);

    /**
     * Retrieve a {@code BIPVariable} object given a name
     * @param name
     *      The name of the variable
     * @return
     *      The variable that has the name exactly matches with the given name 
     *      or NULL if the given name does not match with any variables stored in the pool
     */
    BIPVariable getVariable(String name);

    /**
     * Enumerate list of variables in multiple lines, 
     * Each line contains at most {@code NUM_VAR_PER_LINE} variables
     * 
     * @param NUM_VAR_PER_LINE
     *      The maximum number of variables that are enumerated in one line
     * @return
     *      The string (with multiple lines) that lists all variables stored in the pool
     *      
     *  {\bf Note}: This function is usually used to enumerate binary variables     
     */
    String enumerateListVariables(final int NUM_VAR_PER_LINE);
}
