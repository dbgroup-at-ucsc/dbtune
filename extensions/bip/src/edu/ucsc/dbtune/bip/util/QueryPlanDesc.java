package edu.ucsc.dbtune.bip.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.bip.sim.MatIndex;
import edu.ucsc.dbtune.bip.sim.MatIndexPool;
import edu.ucsc.dbtune.inum.InumSpace;
import edu.ucsc.dbtune.inum.InumStatementPlan;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;


public class QueryPlanDesc {
	
	private int Kq; // number of template plans	
	private int n; // number of relations used in the query
	private int numIndex; // number of candidate indexes
	private ArrayList<Integer> S; // size of each slot		
	private ArrayList<Double> beta; // internal plan cost
	private ArrayList< ArrayList< ArrayList <Double> > > gamma;	 
	// access cost gamma[k][i][a]
	// sort in the increasing order of their value	
	private ArrayList< ArrayList< Index> > candidateIndexes;
	
	
	// Number of template plans
	public int getNumPlans()
	{
		return Kq;
	}
	public void setNumPlans(int Kq)
	{
		this.Kq = Kq;
	}
	
	// Number of relations in the query schema
	public int getNumRels()
	{
		return n;
	}
	public void setNumRels(int n)
	{
		this.n = n;
	}
	
	// Number of candidate indexes
	public int getNumCandidateIndexes()
	{
		return numIndex;
	}
	public void setNumCandidateIndexes(int numIndex)
	{
		this.numIndex = numIndex;
	}
	
	// Number of candidate indexes at each slot
	public int getNumIndexEachSlot(int i)
	{
		return S.get(i);
	}
	public void setNumIndexEachSlot(ArrayList<Integer> S)
	{
		this.S = S;		
	}
	
	// internal plan cost
	public double getInternalPlanCost(int i)
	{
		return beta.get(i);
	}
	public void setInternalPlanCost(ArrayList<Double> beta)
	{
		this.beta = beta;		
	}
	
	
	// index access cost
	public double getIndexAccessCost(int k, int i, int a)
	{
		return gamma.get(k).get(i).get(a);
	}
	
	public void setIndexAccessCost(ArrayList< ArrayList< ArrayList <Double> > > gamma)
	{
		this.gamma = gamma;
	}
	
	/**
	 * Retrieve the index in the corresponding slot
	 * @param i 
	 * 		The position of the relation
	 * @param a
	 * 		The position of this index in the list of indexes belonging to this relation
	 * @return
	 * 		Index
	 */
	public Index getIndex(int i, int a)
	{
		return candidateIndexes.get(i).get(a);
	}
	
	public void setCandidateIndexes(ArrayList<ArrayList<Index>> candidateIndexes)
	{
		this.candidateIndexes = candidateIndexes;
	}

	/**
	 * Populate query plan description (like number of template plans, internal cost, 
	 * index access cost, etc. )
	 * 
	 * @param inum
	 * 		Inum space corresponding to the given query
	 * 
	 * @param globaCandidateIndexes
	 * 		The given list of candidate indexes (globally)	
	 */	
	public void generateQueryPlanDesc(InumSpace inum, List<Index> globalCandidateIndexes)
	{
		int i, a, numIndexEachSlot;
		String relName;
		
		S = new ArrayList<Integer>();
		beta = new ArrayList<Double>();
		gamma = new ArrayList< ArrayList< ArrayList< Double> >>(); 
		candidateIndexes = new ArrayList< ArrayList<Index> >();		
		Set<InumStatementPlan> templatePlans = inum.getTemplatePlans();
		ArrayList<String> listRelName = new ArrayList<String>(); 
		
		// TODO: replace with the new interface ----------------------
		List<Table> listTables = new ArrayList<Table>();   
		for (InumStatementPlan plan : templatePlans)
		{
			listTables = plan.getReferencedTables();
		}
		// ------------------------------------------------------------
		// Number of relations
		n = 0;
		for (Iterator<Table> iter = listTables.iterator(); iter.hasNext(); )		
		{	
			listRelName.add(iter.next().getName()); 			
			n++;			
		}
				 
		// Number of indexes in each slot
		numIndex = 0;				
		for (i = 0; i < n; i++)
		{
			relName = listRelName.get(i);
			numIndexEachSlot = 0;
			ArrayList<Index> listIndex = new ArrayList<Index>();
			
			for (Iterator<Index> iter = globalCandidateIndexes.iterator(); iter.hasNext(); )			
			{					
				Index index = iter.next();
				if (index.getName().contains(relName))
				{
					numIndexEachSlot++;
					listIndex.add(index);
					numIndex++;
				}
			}
			S.add(new Integer(numIndexEachSlot));
			candidateIndexes.add(listIndex);			 
		}
		
		Kq = 0;
		double sizeMatIndex = 0.0;
		for (InumStatementPlan plan : templatePlans)
		{
			beta.add(new Double(plan.getInternalCost()));
			ArrayList< ArrayList<Double> > gammaPlan = new ArrayList< ArrayList<Double> >(); 
			for (i = 0; i < n; i++)
			{
				relName = listRelName.get(i);				
				ArrayList<Double> gammaRel = new ArrayList<Double>(); 
				 
				for (a = 0; a < getNumIndexEachSlot(i); a++)
				{
					Index index = getIndex(i, a);
					gammaRel.add(new Double(plan.getAccessCost(index)));
					
					// update materialize index
					// MIGHT BE WRONG for @IIP
					Object found = MatIndexPool.getMatIndex(index);
					if (found != null)
					{
						sizeMatIndex = plan.getMaterializedIndexSize(index);
						MatIndex matIdx = (MatIndex) found;
						matIdx.setMatSize(sizeMatIndex);
					}
				}		
				
				gammaPlan.add(gammaRel);							
			}
			
			gamma.add(gammaPlan);
			Kq++;
		}
		
	}

	
	/**
	 * Return the global position of the given index at position a in the slot i (i.e., S[i])
	 * @param index
	 * 		The position of relation S[index]
	 * @param a
	 * 		The position of the given index in S[index]
	 * 
	 * @return 
	 * 		Global index 
	 */
	public int globalIndex(int index, int a){
		int result = 0, i;
		for (i = 0; i < index; i++){
			result += S.get(i);
		}
		
		result += a;		
		return result;
	}
}
