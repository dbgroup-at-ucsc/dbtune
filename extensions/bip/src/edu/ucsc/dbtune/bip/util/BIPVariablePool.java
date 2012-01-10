package edu.ucsc.dbtune.bip.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A container that stores all variables constructed in a BIP
 * 
 * @author tqtrung@soe.ucsc.edu
 */
public abstract class BIPVariablePool 
{
    protected List<BIPVariable> listVar;
    protected Map<String, BIPVariable> mapNameVar;
    
    public BIPVariablePool()
    {
        listVar = new ArrayList<BIPVariable>();
        mapNameVar = new HashMap<String, BIPVariable>();
    }
    
    
    /**
     * Add a variable into this pool
     * @param var
     *      A BIP variable
     */
    public void addVariable(BIPVariable var)
    {
        listVar.add(var);
    }
    
    /**
     * Retrieve a {@code BIPVariable} object given a name
     * @param name
     *      The name of the variable
     * @return
     *      The variable that has the name exactly matches with the given name 
     *      or NULL if the given name does not match with any variables stored in the pool
     */
    public BIPVariable getVariable(String name)
    {   
        Object found = mapNameVar.get(name);
        BIPVariable var = null;
        if (found != null) {
            var = (BIPVariable) found;
        }
        
        return var;
    }
    
    /**
     * Enumerate list of variables in multiple lines, 
     * Each line contains at most {@code NUM_VAR_PER_LINE} variables
     * 
     * @param NUM_VAR_PER_LINE
     *      The maximum number of variables that are enumerated in one line
     * @return
     *      The string (with multiple lines) that lists all variables stored in the pool
     */ 
    public String enumerateListVariables(final int NUM_VAR_PER_LINE)
    {
        String lineVars = "", result = "";
        int countVar = 0;
       
        for (BIPVariable var : listVar) {
            lineVars += var.getName();
            lineVars += " ";
            countVar++;
            if (countVar >= NUM_VAR_PER_LINE) {
                countVar = 0;
                result += lineVars;
                result += "\n";
                lineVars = "";                  
            }
        }
       
        if (countVar > 0) {
            result += lineVars;
            result += "\n";
        }     
        return result;
    }
}
