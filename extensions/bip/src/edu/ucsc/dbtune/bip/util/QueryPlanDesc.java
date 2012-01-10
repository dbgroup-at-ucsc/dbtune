package edu.ucsc.dbtune.bip.util;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import edu.ucsc.dbtune.inum.InumSpace;
import edu.ucsc.dbtune.inum.InumStatementPlan;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * This class stores the constants relating to INUM's template plans 
 * such as: internal plan cost, index access costs for a given set
 * of candidate indexes and a particular statement
 *  
 * @author tqtrung@soe.ucsc.edu
 *
 */
public class QueryPlanDesc 
{	
	private int Kq;
	private int n; 
	private int numIndexes;
	private List<Integer> S;
	private List<Double> beta;
	private List< List< List <Double> > > gamma;
	private List< List< Index> > listIndexesEachSlot;
	private List<Table> listSchemaTables;
	private Map<Integer, Integer> mapReferencedSlotID;
	private InumSpace inum;
	private int stmtID;	
    static AtomicInteger STMT_ID = new AtomicInteger(0); /** used to uniquely identify each instances of the class. */
    
	/**
	 * Number of template plans	 
	 */
	public int getNumberOfTemplatePlans()
	{
		return Kq;
	}
	public void setNumberOfTemplatePlans(int Kq)
	{
		this.Kq = Kq;
	}
	
	/**
	 * Number of relations in the query schema 
	 */
	public int getNumberOfSlots()
	{
		return n;
	}
	
	public void setNumberOfSlots(int n)
	{
		this.n = n;
	}
	
	/**
	 *  Number of candidate indexes
	 */
	public int getNumberOfCandidateIndexes()
	{
		return numIndexes;
	}
	public void setNumberOfCandidateIndexes(int numIndex)
	{
		this.numIndexes = numIndex;
	}
	
	/**
	 * Number of candidate indexes at each slot 
	 */
	public int getNumberOfIndexesEachSlot(int i)
	{
		return S.get(i);
	}
	public void setNumberOfIndexesEachSlot(List<Integer> S)
	{
		this.S = S;		
	}
	
	/**
	 * The internal plan cost 
	 */
	public double getInternalPlanCost(int i)
	{
		return beta.get(i);
	}
	public void setInternalPlanCost(ArrayList<Double> beta)
	{
		this.beta = beta;		
	}
	
	/**
	 * Index access cost
	 */
	public double getIndexAccessCost(int k, int i, int a)
	{
		return gamma.get(k).get(i).get(a);
	}
	
	public void setIndexAccessCost(List< List< List <Double> > > gamma)
	{
		this.gamma = gamma;
	}
	
	/**
	 * Statement ID
	 */
	public int getStatementID()
    {
        return this.stmtID;
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
		return listIndexesEachSlot.get(i).get(a);
	}
	
	public void setCandidateIndexes(List<List<Index>> candidateIndexes)
	{
		listIndexesEachSlot = candidateIndexes;
	}
	
	/**
     * Populate query plan description: the number of template plans, internal cost, 
     * index access cost, etc. )
     * 
     * @param preparator
     *      The class to communicate with INUM to get InumSpace
     * @param stmt
     *      A SQL statement
     * @param globaCandidateIndexes
     *      The given list of candidate indexes   
     * 
     * {\b Note}: 
     *     - There does not contain the empty index (table scan) in {@code globalCandidateIndexes}
     *     - The index full table scan is always assigned the last position in the list of indexes
     *     at each slot
     * @throws SQLException 
     */ 
    public void generateQueryPlanDesc(BIPPreparatorSchema preparator, SQLStatement stmt,
                                      BIPIndexPool poolIndexes) throws SQLException
    {
        S = new ArrayList<Integer>();
        beta = new ArrayList<Double>();
        gamma = new ArrayList<List<List<Double>>>(); 
        listIndexesEachSlot = new ArrayList<List<Index>>();
        
        this.inum = preparator.populateInumSpace(stmt);
        this.stmtID = QueryPlanDesc.STMT_ID.getAndIncrement();     
        List<IndexFullTableScan> listFullTableScanIndexes = preparator.getListFullTableScanIndexes();  
        listSchemaTables = preparator.getListSchemaTables();
        Set<InumStatementPlan> templatePlans = inum.getTemplatePlans();
                    
        // TODO: replace with the new interface ----------------------
        // Note that list tables is derived from the schema
        // @listSchemaTables and @listReferencedTable is different      
        List<Table> listReferencedTables = new ArrayList<Table>();   
        for (InumStatementPlan plan : templatePlans) {
            listReferencedTables = plan.getReferencedTables();
            break;
        }
        // ------------------------------------------------------------
        
        // 1. Set up the number of slots & number of indexes in each slot
        n = 0;
        numIndexes = 0;     
        
        for (Table table : listSchemaTables) {          
            int numIndexEachSlot = 0;
            List<Index> listIndex = new ArrayList<Index>();         
            
            for (Index index : poolIndexes.indexes()) {
                if (index.getTable().equals(table)){                
                    numIndexEachSlot++;
                    numIndexes++;
                    listIndex.add(index);
                }
            }
            
            // find the full table scan index corresponding to the slot
            for (IndexFullTableScan scanIdx : listFullTableScanIndexes) {
                if (scanIdx.getTable().equals(table) == true) {
                    numIndexEachSlot++;
                    numIndexes++;
                    listIndex.add(scanIdx);
                    break;
                }
            }
            
            S.add(new Integer(numIndexEachSlot));
            listIndexesEachSlot.add(listIndex);
            n++;            
        }
        
        Map<Table, Table> mapReferenceTable = new HashMap<Table, Table>();
        for (Table referencedTable : listReferencedTables){
            mapReferenceTable.put(referencedTable, referencedTable);
        }
        
        mapReferencedSlotID = new HashMap<Integer, Integer>();
        for (int i = 0; i < listSchemaTables.size(); i++){
            Object found = mapReferenceTable.get(listSchemaTables.get(i));
            if (found != null){
                mapReferencedSlotID.put(new Integer(i), new Integer(1));
            }
        }
        
        Kq = 0;
        for (InumStatementPlan plan : templatePlans) {
            beta.add(new Double(plan.getInternalCost()));
            List<List<Double>> gammaPlan = new ArrayList<List<Double>>();
            
            for (int i = 0; i < n; i++) {
                List<Double> gammaRel = new ArrayList<Double>(); 
                
                // If the table is not reference then assigned gamma = 0
                Object found = mapReferenceTable.get(listSchemaTables.get(i));
                if (found == null){
                    for (int a = 0; a < getNumberOfIndexesEachSlot(i); a++) {
                        gammaRel.add(new Double(0.0));
                    }
                } else {
                    Table table = (Table) found;
                    for (int a = 0; a < getNumberOfIndexesEachSlot(i); a++) {
                        Index index = getIndex(i, a);
                        // Full table scan index 
                        if (a == getNumberOfIndexesEachSlot(i) - 1){                     
                            gammaRel.add(new Double(plan.getFullTableScanCost(table)));
                        } else {
                            gammaRel.add(new Double(plan.getAccessCost(index)));
                        }
                    }       
                }
                gammaPlan.add(gammaRel);                            
            }
            
            gamma.add(gammaPlan);
            Kq++;
        }
    }
	
    /**
     * Map the position in each slot of every index in the pool {@code poolIndex} to its identifier
     * @param poolIndex
     *      The pool that stores candidate indexes
     */
    public void mapIndexInSlotToPoolID(BIPIndexPool poolIndex)
    {
        int q = getStatementID();
        for (int i = 0; i < n; i++) {
            for (int a = 0; a < getNumberOfIndexesEachSlot(i); a++) {
                Index index = getIndex(i, a);
                IndexInSlot iis = new IndexInSlot(q,i,a);
                poolIndex.mapIndexInSlot(iis, index);
            }
        }
    }
    
	/**
	 * Check if the relation at the position {@code idSlot} is referenced by the query
	 * 
	 * @param idSlot
	 *     The ID of the slot
	 * @return
	 *     {@code boolean}: true if the relation at the position {@code idSlot} is referenced by the query
	 */
	public boolean isReferenced(int idSlot)
	{
	    Object found = this.mapReferencedSlotID.get(new Integer(idSlot));
	    if (found == null){
	        return false;
	    } else {
	        return true;
	    }
	}
}
