package edu.ucsc.dbtune.bip.core;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;


import edu.ucsc.dbtune.bip.util.IndexInSlot;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.workload.SQLStatement;

import static edu.ucsc.dbtune.metadata.FullTableScanIndex.getFullTableScanIndexInstance;

public class InumQueryPlanDesc implements QueryPlanDesc {	private int Kq;
    private int n; private List<Integer> S;
	private List<Double> beta;
	private List< List< List <Double> > > gamma;
	private List< List< Index> > listIndexesEachSlot;	
	private Map<Integer, Integer> mapReferencedSlotID;
	
	static AtomicInteger STMT_ID = new AtomicInteger(0); 
    /** used to uniquely identify each instances of the class. */
	private int stmtID;	
	SQLStatement stmt;
	Set<InumPlan> templatePlans;
	List<Table> listReferencedTables;
    
	/**
	 * The constructor. This object is automatically assigned an ID, increasing from 0
	 */
    public InumQueryPlanDesc()
    {
        this.stmtID = InumQueryPlanDesc.STMT_ID.getAndIncrement();
    }
    
    @Override
    public void setStatement(SQLStatement stmt)
    {
        this.stmt = stmt;
    }
    
    @Override
    public void populateInumSpace(InumOptimizer optimizer)
    {   
        try {
            InumPreparedSQLStatement preparedStmt = (InumPreparedSQLStatement) optimizer.prepareExplain(stmt);
            preparedStmt.explain(new HashSet<Index>());
            templatePlans = preparedStmt.getTemplatePlans();
        } catch (SQLException e) {
            e.printStackTrace();
        }
                
        listReferencedTables = new ArrayList<Table>();             
        for (InumPlan plan : templatePlans) {
            listReferencedTables = plan.getReferencedTables();
            break;
        }
    }
    
    /* (non-Javadoc)
     * @see edu.ucsc.dbtune.bip.util.QueryPlanDesc#generateQueryPlanDesc(SQLStatement stmt, Schema schema, IndexPool poolIndexes)
     */ 
    @Override
    public void generateQueryPlanDesc(List<Table> listSchemaTables, IndexPool poolIndexes) throws SQLException
    {
        S = new ArrayList<Integer>();
        beta = new ArrayList<Double>();
        gamma = new ArrayList<List<List<Double>>>(); 
        listIndexesEachSlot = new ArrayList<List<Index>>();
        
        // 1. Set up the number of slots & number of indexes in each slot
        n = listSchemaTables.size();
       
        for (Table table : listSchemaTables) {          
            int numIndexEachSlot = 0;
            List<Index> listIndex = new ArrayList<Index>();         
            
            for (Index index : poolIndexes.indexes()) {
                // normal index (not the full table scan index)
                if (index.getTable().equals(table)){                
                    numIndexEachSlot++;                  
                    listIndex.add(index);
                }
            }
            // add the index full table scan at the last position in this slot
            FullTableScanIndex scanIdx = getFullTableScanIndexInstance(table);
            numIndexEachSlot++;
            listIndex.add(scanIdx);
            
            S.add(new Integer(numIndexEachSlot));
            listIndexesEachSlot.add(listIndex);
        }
        
        Map<Table, Table> mapReferencedTable = new HashMap<Table, Table>();
        for (Table referencedTable : listReferencedTables){
            mapReferencedTable.put(referencedTable, referencedTable);
        }
        
        mapReferencedSlotID = new HashMap<Integer, Integer>();
        for (int i = 0; i < listSchemaTables.size(); i++){
            Object found = mapReferencedTable.get(listSchemaTables.get(i));
            if (found != null){
                mapReferencedSlotID.put(new Integer(i), new Integer(1));
            }
        }
        
        Kq = 0;
        for (InumPlan plan : templatePlans) {
            beta.add(new Double(plan.getInternalCost()));
            List<List<Double>> gammaPlan = new ArrayList<List<Double>>();
            
            for (int i = 0; i < n; i++) {
                List<Double> gammaRel = new ArrayList<Double>(); 
                
                // If the table is not reference then assigned gamma = 0
                Object found = mapReferencedTable.get(listSchemaTables.get(i));
                if (found == null){
                    for (int a = 0; a < getNumberOfIndexesEachSlot(i); a++) {
                        gammaRel.add(new Double(0.0));
                    }
                } else {
                    for (int a = 0; a < getNumberOfIndexesEachSlot(i); a++) {
                        Index index = getIndex(i, a);
                        gammaRel.add(new Double(plan.plug(index)));
                    }       
                }
                gammaPlan.add(gammaRel);                            
            }
            gamma.add(gammaPlan);
            Kq++;
        }
    }
    
    
    @Override
    public void mapIndexInSlotToPoolID(IndexPool poolIndex)
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
    
    
    @Override
    public boolean isSlotReferenced(int idSlot)
    {
        Object found = this.mapReferencedSlotID.get(new Integer(idSlot));
        if (found == null){
            return false;
        } else {
            return true;
        }
    }
    
	/* (non-Javadoc)
     * @see edu.ucsc.dbtune.bip.util.QueryPlanDesc#getNumberOfTemplatePlans()
     */
    @Override
	public int getNumberOfTemplatePlans()
	{
		return Kq;
	}
	
	/* (non-Javadoc)
     * @see edu.ucsc.dbtune.bip.util.QueryPlanDesc#getNumberOfSlots()
     */
    @Override
	public int getNumberOfGlobalSlots()
	{
		return n;
	}		
	
	/* (non-Javadoc)
     * @see edu.ucsc.dbtune.bip.util.QueryPlanDesc#getNumberOfIndexesEachSlot(int)
     */
    @Override
	public int getNumberOfIndexesEachSlot(int i)
	{
		return S.get(i);
	}
	
	/* (non-Javadoc)
     * @see edu.ucsc.dbtune.bip.util.QueryPlanDesc#getInternalPlanCost(int)
     */
    @Override
	public double getInternalPlanCost(int i)
	{
		return beta.get(i);
	}
	
	
	/* (non-Javadoc)
     * @see edu.ucsc.dbtune.bip.util.QueryPlanDesc#getIndexAccessCost(int, int, int)
     */
    @Override
	public double getIndexAccessCost(int k, int i, int a)
	{
		return gamma.get(k).get(i).get(a);
	}
	
	
	/* (non-Javadoc)
     * @see edu.ucsc.dbtune.bip.util.QueryPlanDesc#getStatementID()
     */
    @Override
	public int getStatementID()
    {
        return this.stmtID;
    }
	
    @Override
	public Index getIndex(int i, int a)
	{
		return listIndexesEachSlot.get(i).get(a);
	}
    
    @Override 
    public List<Table> getReferencedTables()
    {
        return this.listReferencedTables;
    }
	
	
}
