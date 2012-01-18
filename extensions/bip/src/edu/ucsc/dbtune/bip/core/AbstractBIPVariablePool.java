package edu.ucsc.dbtune.bip.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.bip.util.BIPVariable;


public abstract class AbstractBIPVariablePool implements BIPVariablePool 
{
    protected List<BIPVariable> listVar;
    protected Map<String, BIPVariable> mapNameVar;
    
    public AbstractBIPVariablePool()
    {
        listVar = new ArrayList<BIPVariable>();
        mapNameVar = new HashMap<String, BIPVariable>();
    }
    
    
    /* (non-Javadoc)
     * @see edu.ucsc.dbtune.bip.util.BIPVariablePool#addVariable(edu.ucsc.dbtune.bip.util.BIPVariable)
     */
    @Override
    public void add(BIPVariable var)
    {
        listVar.add(var);
        mapNameVar.put(var.getName(), var);
    }
    
    /* (non-Javadoc)
     * @see edu.ucsc.dbtune.bip.util.BIPVariablePool#getVariable(java.lang.String)
     */
    @Override
    public BIPVariable get(String name)
    {   
        Object found = mapNameVar.get(name);
        BIPVariable var = null;
        if (found != null) {
            var = (BIPVariable) found;
        }
        
        return var;
    }
    
    /* (non-Javadoc)
     * @see edu.ucsc.dbtune.bip.util.BIPVariablePool#enumerateListVariables(int)
     */ 
    @Override
    public String enumerateList(final int NUM_VAR_PER_LINE)
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
