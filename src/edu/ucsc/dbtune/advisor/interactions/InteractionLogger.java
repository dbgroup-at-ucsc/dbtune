package edu.ucsc.dbtune.advisor.interactions;

import edu.ucsc.dbtune.metadata.Configuration;

public class InteractionLogger
{
	private InteractionBank bank; 
	
	public InteractionLogger(Configuration candidateSet0)
{
		bank = new InteractionBank(candidateSet0);
	}
	
	/*
	 * Assign interaction with an exact value
	 */
	public final void assignInteraction(int id1, int id2, double newValue)
{		
		bank.assignInteraction(id1, id2, newValue);
	}

	public void assignBenefit(int id, double newValue)
{
		bank.assignBenefit(id, newValue);
	}

	public final InteractionBank getInteractionBank()
{
		return bank;
	}
}
