package edu.ucsc.dbtune.bip;

import org.junit.Test;

import edu.ucsc.dbtune.util.Rt;

public class UNIFDB2UpdateChangeWeightTest extends DIVPaper 
{
    @Test
    public void main() throws Exception
    {
        // 1. initialize data structures, like database names
        // wl names
        initialize();
        
        // 2. test UNIF
        testUNIF();
        
        // not to draw graph
        resetParameterNotDrawingGraph();
    }
    
    /**
     * Test UNIF with 
     * @throws Exception
     */
    protected static void testUNIF() throws Exception
    {   
        for (double w : updateRatios) {
            isLoadEnvironmentParameter = false;
            updateRatio = w;
            getEnvironmentParameters();
            setParameters();
            UNIFDB2AdvisorTest.fileName = 
                    wlName + "_" + Double.toString(updateRatio) + "_" 
                    + UNIF_DETAIL_DB2_FILE;
            UNIFDB2AdvisorTest.testUNIFDB2();
        }
    }
}
