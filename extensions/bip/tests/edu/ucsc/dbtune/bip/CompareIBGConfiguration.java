package edu.ucsc.dbtune.bip;

import edu.ucsc.dbtune.advisor.candidategeneration.Generation;
import java.io.File;

public abstract class CompareIBGConfiguration 
{
    public static String subdir = "tpch"; 
    
    private static File root = 
        new File("/home/tqtrung/previous_source_code/interaction/src/interaction/results");
    
    public static File logInteractionFile(Generation.Strategy s, double t) 
    {
        return new File(outputDir(), "interaction-" + s.nickname + "-SERIAL-" + t + ".txt");
    }
    
    private static File resultsDir() 
    {
        return new File (root, subdir);
    }
    
    private static File outputDir() 
    {
        return new File(resultsDir(), "output");
    }
}
