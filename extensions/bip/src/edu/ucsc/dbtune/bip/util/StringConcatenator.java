package edu.ucsc.dbtune.bip.util;

import java.util.List;

public class StringConcatenator
{
    /**
     * Concatenate a list of input strings using the given connector and return one output string.
     * Eg: join("+", (("a"), ("bc")) = "a + bc"
     * 
     * @param connector
     *      The input connector to link element (e.g., "+", "-")
     * @param listElement
     *      The input list of elements to connect    * 
     * @return
     *      The output string
     */ 
    public static String concatenate(String connector, List<String> listElement)
    {   
        String result = "";
        boolean is_first = true;
        
        for (String var : listElement) {            
            if (is_first == false){
                result = result.concat(connector);
            }
            result = result.concat(var);
            is_first = false;
        }
        
        return result;
    }
}
