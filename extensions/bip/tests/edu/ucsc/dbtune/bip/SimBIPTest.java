package edu.ucsc.dbtune.bip;

import java.util.ArrayList;
import java.util.List;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import edu.ucsc.dbtune.metadata.Index; 
import edu.ucsc.dbtune.bip.sim.MatIndex;
import edu.ucsc.dbtune.bip.sim.SimBIP;
import edu.ucsc.dbtune.bip.util.*;


/**
 * Test SimBIP
 * @author tqtrung
 *
 */
public class SimBIPTest  extends BIPTestConfiguration
{   
    /*
    @Test
    public void testScheduling() throws Exception
    {   
        SimBIP sim = new SimBIP();
        List<Index> Sinit = new ArrayList<Index>();
        List<Index> Smat = new ArrayList<Index>();
        int W = 4;
        List<Double> B = new ArrayList<Double>();
        double space = 140;
        for (int w = 0; w < W; w++) {
            B.add(new Double((w+1) * space));
        }
        
        // Case 1: set Sinit = {}       
        Smat = candidateIndexes;
        agent = mock(BIPAgentPerSchema.class);
        when(agent.populateInumSpace()).thenReturn(listInum);
        List<MatIndex> listIndex = sim.schedule(Sinit, Smat, agent, W, B);
        String strSchedule = sim.printSchedule(listIndex, W);
        System.out.println(strSchedule);
    }
    */
}
