package edu.ucsc.dbtune.bip.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Mar 16, 2007
 * Time: 6:04:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class CPlexBuffer 
{
    private PrintWriter obj, cons, bin;

    public CPlexBuffer(String prefix) throws IOException 
    {
        obj  = new PrintWriter(new FileWriter(prefix+".obj"));
        cons = new PrintWriter(new FileWriter(prefix+".cons"));
        bin  = new PrintWriter(new FileWriter(prefix+".bin"));

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
	 *    Placing row by row in each input file into the output one
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

