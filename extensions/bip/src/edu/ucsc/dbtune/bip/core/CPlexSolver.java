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
}
