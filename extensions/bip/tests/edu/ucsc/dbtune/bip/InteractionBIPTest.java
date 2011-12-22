package edu.ucsc.dbtune.bip;

import java.sql.SQLException;
import java.util.List;
import org.junit.Test;

import edu.ucsc.dbtune.advisor.interactions.IndexInteraction;
import edu.ucsc.dbtune.bip.interactions.InteractionBIP;


/**
 * Test for the InteractionBIP class
 */
public class InteractionBIPTest extends BIPTestConfiguration
{  
    @Test
    public void testInteraction() throws Exception
    {
        try {
            InteractionBIP bip = new InteractionBIP();
                    
            double delta = 0.35;
            List<IndexInteraction> listInteractions =
                bip.computeInteractionIndexes(listWorkload, listAgent, candidateIndexes, delta);
            System.out.println(bip.printListInteraction(listInteractions));
        } catch (SQLException e){
            System.out.println(" error " + e.getMessage());
        }
    }
}
