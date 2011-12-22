package edu.ucsc.dbtune.bip;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

import edu.ucsc.dbtune.metadata.Index; 
import edu.ucsc.dbtune.bip.sim.MatIndex;
import edu.ucsc.dbtune.bip.sim.SimBIP;


/**
 * Test SimBIP
 * @author tqtrung
 *
 */
public class SimBIPTest  extends BIPTestConfiguration
{   
    
    @Test
    public void testScheduling() throws Exception
    {   
        SimBIP sim = new SimBIP();
        List<Index> Sinit = new ArrayList<Index>();
        List<Index> Smat = new ArrayList<Index>();
        int W = 4;
        double B = 300;
     
        // Sinit = \emptyset
        // Smat = indexes relevant to the queries
        for (List<Index> listIndexQuery : listIndexQueries) {
            for (Index idx : listIndexQuery) {
                Smat.add(idx);
            }
        }
        
        List<MatIndex> listIndex = sim.schedule(Sinit, Smat, listWorkload, listAgent, W, B);
        String strSchedule = sim.printSchedule(listIndex, W);
        System.out.println(strSchedule);
    }
}
