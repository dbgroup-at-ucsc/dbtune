package edu.ucsc.dbtune.bip;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import edu.ucsc.dbtune.bip.div.DivBIP;
import edu.ucsc.dbtune.bip.util.BIPOutput;
import edu.ucsc.dbtune.metadata.Index;

public class DivBIPTest extends BIPTestConfiguration 
{
    @Test
    public void testDivergentDesign() throws Exception
    {  
        int Nreplicas = 3;
        int loadfactor = 2;
        double B = 300;
        DivBIP div = new DivBIP(Nreplicas, loadfactor, B);
       
        List<Index> divCandidateIndexes = new ArrayList<Index>();
        for (List<Index> listIndexQuery : listIndexQueries) {
            for (Index idx : listIndexQuery) {
                divCandidateIndexes.add(idx);
            }
        }
        div.setCandidateIndexes(divCandidateIndexes);
        div.setMapSchemaToWorkload(mapSchemaToWorkload);
        div.setCommunicator(communicator);
        div.setWorkloadName("testwl"); 
        
        BIPOutput result = div.solve();
        System.out.println("In test, result: " + result.toString());
    }
}
