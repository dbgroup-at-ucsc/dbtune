package edu.ucsc.dbtune.bip.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public abstract class AbstractBIPVariablePool  
{
    protected List<BIPVariable>        listVar;
    protected Map<String, BIPVariable> mapNameVar;
    
    public AbstractBIPVariablePool()
    {
        listVar    = new ArrayList<BIPVariable>();
        mapNameVar = new HashMap<String, BIPVariable>();
    }
    
    
    /**
     * Add a variable into this pool
     * @param var
     *      A BIP variable
     */
    public void add(BIPVariable var)
    {
        listVar.add(var);
        mapNameVar.put(var.getName(), var);
    }
    
    /**
     * Retrieve a {@code BIPVariable} object given a name
     * @param name
     *      The name of the variable
     * @return
     *      The variable that has the name exactly matches with the given name 
     *      or NULL if the given name does not match with any variables stored in the pool
     */
    public BIPVariable get(String name)
    {   
        return mapNameVar.get(name);
    }
    
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
    public String enumerateList(final int NUM_VAR_PER_LINE)
    {   
        int countVar = 0;
        StringBuilder result = new StringBuilder();
        for (BIPVariable var : listVar) {
            result.append(var.getName() + " ");
            countVar++;
            if (countVar >= NUM_VAR_PER_LINE) {
                result.append("\n");
                countVar = 0;
            }
        }
        if (countVar > 0) 
            result.append("\n");
        
        return result.toString();
    }
    
    /**
     * Retrieve the list of variables stored in this pool
     * 
     * @return
     *      A list of variables.
     */
    public List<BIPVariable> variables()
    {
        return listVar; 
    }
}
