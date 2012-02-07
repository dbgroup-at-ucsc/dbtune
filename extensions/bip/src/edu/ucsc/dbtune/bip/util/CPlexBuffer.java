package edu.ucsc.dbtune.bip.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * File managements for a CPlex class
 * 
 */
public class CPlexBuffer 
{
    private PrintWriter obj, cons, bin;
    private String objFileName, consFileName, binFileName, lpFileName;

    public CPlexBuffer(String prefix) throws IOException 
    {
        objFileName = prefix+".obj";
        consFileName = prefix+".cons";
        binFileName = prefix+".bin";
        lpFileName = prefix+".lp";
        
        // erase the content stored in the current file
        obj  = new PrintWriter(new FileWriter(objFileName), false);
        cons = new PrintWriter(new FileWriter(consFileName), false);
        bin  = new PrintWriter(new FileWriter(binFileName), false);

        cons.println();
        cons.println("Subject To");

        bin.println();
        bin.println("Binary");
        obj.println("minimize");
        obj.print("obj: ");
    }

    public void close() 
    {
        bin.println("End");
        obj.close();
        cons.close();
        bin.close();
    }

    public String getObjFileName()
    {
        return this.objFileName;
    }
    
    public String getBinFileName()
    {
        return this.binFileName;
    }
    
    public String getConsFileName()
    {
        return this.consFileName;
    }
    
    public String getLpFileName()
    {
        return this.lpFileName;
    }
    public PrintWriter getObj() 
    {
        return obj;
    }

    public PrintWriter getCons() 
    {
        return cons;
    }

    public PrintWriter getBin() 
    {
        return bin;
    }
    
    /**
	 * Concatenate the contents from multiple input files into an output file:
	 *    Placing each row in each input file into the output file
	 * 
	 * @param target
	 * 	    The name of the output file
	 * @param files
	 * 		The array of strings representing the name of the input files
	 * @throws IOException
	 */
	public static void concat(String target, String... files) throws IOException {
        PrintWriter writer = new PrintWriter(new FileWriter(target));
        for (int i = 0; i < files.length; i++) {
            String file = files[i];
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = null;
            while((line = reader.readLine()) != null) {
                writer.println(line);
            }
            reader.close();
        }
        writer.close();
	}
}

