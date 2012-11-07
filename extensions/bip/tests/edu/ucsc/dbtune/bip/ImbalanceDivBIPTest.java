package edu.ucsc.dbtune.bip;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Test;
import edu.ucsc.dbtune.bip.div.RobustDivBIP;
import edu.ucsc.dbtune.util.Rt;

public class ImbalanceDivBIPTest extends DIVPaper
{
    @Test
    public void testImbalanceDiv() throws Exception
    {   
        // 1. Set common parameters
        initialize();
        getEnvironmentParameters();
        setParameters();
        
        List<Double> imbalanceFactors = new ArrayList<Double>();
        try {   
            imbalanceFactors = en.getNodeImbalanceFactors();
        }
        catch (NoSuchElementException e){
            ;
        }
        // run test
        testImbalance(imbalanceFactors);
        
        // not to draw graph
        resetParameterNotDrawingGraph();
    }
    
    /**
     * Test with failure only
     * 
     * @param failureFactors
     * @throws Exception
     */
    protected static void testImbalance(List<Double> imbalanceFactors) throws Exception
    {
        List<RobustPaperEntry> entries = new ArrayList<RobustPaperEntry>();
        RobustDivBIP robustDiv;
        double optimalTotalCost;

        // get optimal total cost value
        optimalTotalCost = DivBIPFunctionalTest.testDiv(nReplicas, B, false);
        div.clear();
        Rt.p(" optimal total cost = " + optimalTotalCost);
        
        entries = new ArrayList<RobustPaperEntry>();
        String name = IMBALANCE_GREEDY_FILE;
        
        for (int i = 0; i < imbalanceFactors.size(); i++){
            
            Rt.p("---- IMBALANCE factor = " + imbalanceFactors.get(i));
            robustDiv = new RobustDivBIP(optimalTotalCost, imbalanceFactors.get(i), 
                             0.0);
            
            imbalanceFile = new File(rawDataDir, wlName + "_" + name);
            imbalanceFile.delete();
            imbalanceFile = new File(rawDataDir, wlName + "_" + name);
            imbalanceFile.createNewFile();
            
            RobustDivBIPTest.runFullDDT(robustDiv, true, entries);
            Rt.p(" The divergent design is the divConf object");
            Rt.p(" Details = " + divConf);
            // ----------------------------------
            // store to file (since it takes time)
            serializeFailureImbalanceResult(entries, imbalanceFile);
        }
    }
}
