package edu.ucsc.dbtune.bip;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Test;
import edu.ucsc.dbtune.bip.div.RobustDivBIP;

public class FailureDivBIPTest extends DIVPaper 
{   
    @Test
    public void testFailureDiv() throws Exception
    {   
        // 1. Set common parameters
        initialize();
        getEnvironmentParameters();
        setParameters();
        
        List<Double> failureFactors = new ArrayList<Double>();
        try {   
            failureFactors = en.getFailureImbalanceFactors();
        }
        catch (NoSuchElementException e){
            ;
        }
        
        testFailure(failureFactors);
        
        // not to draw graph
        resetParameterNotDrawingGraph();
    }
    
    /**
     * Test with failure only
     * 
     * @param failureFactors
     * @throws Exception
     */
    protected static void testFailure(List<Double> failureFactors) throws Exception
    {
        List<RobustPaperEntry> entries = new ArrayList<RobustPaperEntry>();
        RobustDivBIP robustDiv;
        
        for (int i = 0; i < failureFactors.size(); i++) {
            // initialize object
            robustDiv = new RobustDivBIP(-1, 0.0, 
                             failureFactors.get(i));
            
            failureFile = new File(rawDataDir, wlName + "_" + FAILURE_FILE);
            failureFile.delete();
            failureFile = new File(rawDataDir, wlName + "_" + FAILURE_FILE);
            failureFile.createNewFile();
            
            RobustDivBIPTest.runFullDDT(robustDiv, true, entries);
            
            // ----------------------------------
            // store to file (since it takes time)
            serializeFailureImbalanceResult(entries, failureFile);
        }
    }
}
