package edu.ucsc.dbtune.bip.core;

import java.util.Map;

public interface CPlexSolver 
{
    /**
     * Reads the BIP in the given {@code inputFile}, solve the BIP, and returns the result,
     * which is a mapping of variable names to their assigned values
     * 
     * @param inputFile
     *      The input file name that contains the BIP to solve
     * @return
     *      A mapping from variables to their assigned value
     *      or NULL if there has no solution
     */
    Map<String, Integer> solve(String inputFile);
    
    /**
     * Retrieve the objective value of the solution
     * 
     * @return
     *      The object value. 
     */
    public double getObjectiveValue();
    
    /**
     * Retrieve the number of variables constructed in the BIP
     * (i.e., the number of columns in the matrix used inside the BIP)
     * 
     * @return
     *     The number of variables
     */
    public int getNumberOfVariables();
    
    /**
     * Retrieve the number of constraints constructed in the BIP
     * (i.e., the number of rows in the matrix used inside the BIP)
     * 
     * @return
     *      The number of constraints
     */
    public int getNumberOfConstraints();
}
