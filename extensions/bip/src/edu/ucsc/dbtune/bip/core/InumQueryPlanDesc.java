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
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.workload.SQLStatement;

import static edu.ucsc.dbtune.metadata.FullTableScanIndex.getFullTableScanIndexInstance;

/**
 * An implementation of {@link QueryPlanDesc} interface.
 * There's only one single object of this type per statement, 
 * i.e., this can be viewed as a singleton class but instead of 
 * creating one object throughout the JVM, this class creates one for every 
 * distinct statement contained in a {@link Workload}.
 * 
 * @author tqtrung@soe.ucsc.edu.sg
 *
 */
public class InumQueryPlanDesc implements QueryPlanDesc 
{	
    public static double BIP_MAX_VALUE = 999999999; 
    private int Kq;
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
	List<Table> listTables;
    
	private static Map<SQLStatement, QueryPlanDesc> instances = new HashMap<SQLStatement, QueryPlanDesc>();
	
	/**
	 * The constructor, each object of this class corresponds to a {@code SQLSatement} object
	 * 
	 * @param stmt
	 *      The statement that this object corresponds to
	 *      
	 * {\bf Note}: A new object is associated with an ID, that is incremented starting from 0     
	 */
    private InumQueryPlanDesc(SQLStatement stmt)
    {
        this.stmtID = InumQueryPlanDesc.STMT_ID.getAndIncrement();
        this.stmt = stmt;
        this.templatePlans = null;
        this.listTables = null;
    }
    
    /**
     * Returns the single instance of this class corresponding to the given statement.
     *
     * @param stmt
     *      SQL statement for which the corresponding instance is being retrieved
     * @return
     *      The query plan description object corresponding to the given statement      
     */
    public static QueryPlanDesc getQueryPlanDescInstance(SQLStatement stmt) 
    {
        QueryPlanDesc desc = (QueryPlanDesc) instances.get(stmt);

        if (desc == null) {
            desc = new InumQueryPlanDesc(stmt);
            instances.put(stmt, desc);
        }

        return desc;
    }
    
    @Override
    public void populateInumSpace(InumOptimizer optimizer)
    {   
        // only populate the INUM space if it has not been done before
        if (templatePlans == null) {
            try {
                InumPreparedSQLStatement preparedStmt = (InumPreparedSQLStatement) optimizer.prepareExplain(stmt);
                preparedStmt.explain(new HashSet<Index>());
                templatePlans = preparedStmt.getTemplatePlans();
            } catch (SQLException e) {
                e.printStackTrace();
            }
                    
            listTables = new ArrayList<Table>();             
            for (InumPlan plan : templatePlans) {
                listTables = plan.getTables();
                break;
            }
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
        for (Table referencedTable : listTables){
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
            System.out.println("L119, internal plan cost: " + plan.getInternalCost());
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
                        double cost = plan.plug(index);
                        if (cost == Double.POSITIVE_INFINITY) {
                            cost = InumQueryPlanDesc.BIP_MAX_VALUE;
                        }
                        gammaRel.add(new Double(cost));
                        System.out.println("L136, index: " + index.getFullyQualifiedName()
                                            + " cost: " + cost);
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
    public List<Table> getTables()
    {
        return this.listTables;
    }
	
	
}
