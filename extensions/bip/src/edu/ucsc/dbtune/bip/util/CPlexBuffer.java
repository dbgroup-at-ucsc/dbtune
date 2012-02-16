package edu.ucsc.dbtune.bip.util;

import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * File managements for a CPlex class
 * 
 */
public class CPlexBuffer 
{
    private List<String> obj;
    private List<String> cons;
    private List<String> bin;   
    private List<String> bound;
    
    private String lpFileName;

    public CPlexBuffer(String prefix) throws IOException 
    {   
        lpFileName = prefix + ".lp";
        
        obj   = new ArrayList<String>();
        cons  = new ArrayList<String>();
        bin   = new ArrayList<String>();
        bound = new ArrayList<String>();
        
        cons.add("Subject To");
        bin.add("Binary");

        obj.add("minimize");
        obj.add("obj: ");
        
        bound.add("bounds");
    }

    /**
     * Retrieve the file name that stores the formulated BIP.
     * 
     * @return
     *      The file name
     */
    public String getLpFileName()
    {
        return lpFileName;
    }
    
    /**
     * Retrieve the list that stores the objective formula
     * 
     * @return
     *      A list of string
     */
    public List<String> getObj() 
    {
        return obj;
    }

    /**
     * Retrieve the list that stores the objective formula
     * 
     * @return
     *      A list of string
     */
    public List<String> getBound() 
    {
        return bound;
    }
    
    /**
     * Retrieve the list that will store constraints formulated for the BIP. Each constraint
     * is stored as an element (a string) in this list.
     * 
     * @return
     *      The list that stores constraints.
     */
    public List<String> getCons() 
    {
        return cons;
    }

    /**
     * Retrieve the list that will store binary variable constraints
     * 
     * @return
     *      The list that stores binary variable constraints.
     */
    public List<String> getBin() 
    {
        return bin;
    }
    
	/**
	 * Write the formulated BIP (constraints, objective function, etc.) into a text file for 
	 * the CPLEX solver to read.
	 * 
	 * @throws IOException
	 *     when there is I/O error.
	 */
	public void writeToLpFile() throws IOException 
    {   
        PrintWriter writer = new PrintWriter (
                                 new BufferedWriter(new FileWriter(lpFileName), 65536));
        bin.add("End \n");
        
        for (String str : obj) 
            writer.println(str);
        
        for (String str : cons)
            writer.println(str);
            
        // if we have some bound constraint
        if (bound.size() > 1) {
            for (String str : bound)
                writer.println(str);
        }
        
        for (String str : bin)
            writer.println(str);
        
        writer.close();
        obj.clear();
        cons.clear();
        bin.clear();
    }
}

