package edu.ucsc.dbtune.bip;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

import edu.ucsc.dbtune.metadata.Index; 
import edu.ucsc.dbtune.advisor.interactions.IndexInteraction;
import edu.ucsc.dbtune.bip.util.*;
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
    		
    		agent = mock(BIPAgent.class);
    		when(agent.populateInumSpace()).thenReturn(listInum);    		
    		double delta = 0.35;
            List<Index> C = new ArrayList<Index>(candidateIndexes);
            List<IndexInteraction> listInteractions =
                bip.computeInteractionIndexes(agent, C, delta);
    		System.out.println(bip.printListInteraction(listInteractions));
    	} catch (SQLException e){
    		System.out.println(" error " + e.getMessage());
    	}
    }
 
}
