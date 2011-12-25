package edu.ucsc.dbtune.bip.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A container that stores all variables constructed in a BIP
 * @author tqtrung
 *
 */
public class BIPVariablePool 
{
    protected List<BIPVariable> listVar;
    protected Map<String, BIPVariable> mapNameVar;
    
    public BIPVariablePool()
    {
        listVar = new ArrayList<BIPVariable>();
        mapNameVar = new HashMap<String, BIPVariable>();
    }
    
    public void addVariable(BIPVariable var)
    {
        listVar.add(var);
    }
    
   
    /**
     * Enumerate list of variables in multiple lines, each line contains at most {@code NUM_VAR_PER_LINE} variable
     * 
     * @param NUM_VAR_PER_LINE
     *      The maximum number of variables to be in one line
     * @return
     *      The string (with multiple lines) that lists all varaibles stored in this pool
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
    
    /**
     * Retrieve a BIPVariable given a name
     * @param name
     *      The name of the variable
     * @return
     *      The object or NULL if it is not found
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
}
