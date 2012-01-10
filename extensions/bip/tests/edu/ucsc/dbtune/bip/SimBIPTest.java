package edu.ucsc.dbtune.bip;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.bip.sim.SimBIP;
import edu.ucsc.dbtune.bip.util.BIPOutput;


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
        List<Index> Sinit = new ArrayList<Index>();
        List<Index> Smat = new ArrayList<Index>();
        int W = 4;
        double timeLimit = 300;
     
        // Sinit = \emptyset
        // Smat = indexes relevant to the queries
        for (List<Index> listIndexQuery : listIndexQueries) {
            for (Index idx : listIndexQuery) {
                Smat.add(idx);
            }
        }
        SimBIP sim = new SimBIP(Sinit, Smat, communicator, W, timeLimit);
        sim.setMapSchemaToWorkload(mapSchemaToWorkload);
        sim.setWorkloadName(workloadName);
        BIPOutput schedule = sim.solve();
        System.out.println("Result: " + schedule.toString());
    }
}
