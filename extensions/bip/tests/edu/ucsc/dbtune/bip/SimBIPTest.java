package edu.ucsc.dbtune.bip;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.bip.core.BIPOutput;
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
        Set<Index> Sinit = new HashSet<Index>();
        Set<Index> Smat = new HashSet<Index>();
        int W = 4;
        double timeLimit = 300;
     
        // Sinit = \emptyset
        // Smat = indexes relevant to the queries
        for (List<Index> listIndexQuery : listIndexQueries) {
            for (Index idx : listIndexQuery) {
                Smat.add(idx);
            }
        }
        SimBIP sim = new SimBIP(Sinit, Smat, W, timeLimit);
        sim.setMapSchemaToWorkload(mapSchemaToWorkload);
        sim.setWorkloadName(workloadName);
        BIPOutput schedule = sim.solve();
        System.out.println("Result: " + schedule.toString());
    }
}
