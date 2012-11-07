package edu.ucsc.dbtune.bip;

import org.junit.Test;

public class DIVDB2UpdateChangeWeightTest extends DIVPaper 
{
    @Test
    public void main() throws Exception
    {
        // 1. initialize data structures, like database names
        // wl names
        initialize();
        
        // 2. test UNIF
        testDIV();
        
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
            updateRatio = w;
            isLoadEnvironmentParameter = false;
            getEnvironmentParameters();
            setParameters();
            UNIFDB2AdvisorTest.fileName = 
                    wlName + "_" + Double.toString(updateRatio) + "_" 
                    + UNIF_DETAIL_DB2_FILE;
            UNIFDB2AdvisorTest.testUNIFDB2();   
        }
    }
    
    /**
     * Test divergent with different values of weights
     * for updates
     * 
     * @throws Exception
     */
    protected static void testDIV() throws Exception
    {
        for (double w : updateRatios){
            updateRatio = w;
            isLoadEnvironmentParameter = false;
            getEnvironmentParameters();        
            setParameters();
            DIVBIPEquivalentTest.fileName = 
                wlName + "_" + Double.toString(updateRatio) + "_"
                + DIV_DETAIL_DB2_FILE;
            
            // experiment for DIV equivalent to BIP    
            DIVBIPEquivalentTest.runDivBIP();
        }
    }
}
