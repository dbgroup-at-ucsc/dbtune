package edu.ucsc.dbtune.bip.core;

import java.util.Map;

public interface CPlexSolver 
{
    /**
     * Reads the BIP in the given {@code} inputFile name, solve the BIP, and returns the result,
     * which is a mapping of variable name to their assigned value
     * 
     * @param inputFile
     *      The input file name that contains the BIP to solve
     * @return
     *      A mapping from variables to their assigned value
     *      or NULL if there has no solution
     */
    Map<String, Integer> solve(String inputFile);
}
