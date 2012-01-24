package edu.ucsc.dbtune.bip.core;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.workload.SQLStatement;

import static edu.ucsc.dbtune.inum.FullTableScanIndex.getFullTableScanIndexInstance;

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
    private int n; 
	private List<Double> beta;
	private List<List<Index>> listIndexesEachSlot;
	private List<List<Index>> listIndexesWithoutFTSEachSlot;
	private List<Map<Index, Double>> listAccessCostPerPlan;
	
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
    
    /* (non-Javadoc)
     * @see edu.ucsc.dbtune.bip.util.QueryPlanDesc#generateQueryPlanDesc(SQLStatement stmt, Schema schema, IndexPool poolIndexes)
     */ 
    @Override
    public void generateQueryPlanDesc(InumOptimizer optimizer, Set<Index> candidateIndexes) throws SQLException
    {   
        beta = new ArrayList<Double>();
        listIndexesEachSlot = new ArrayList<List<Index>>();
        
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
        
        // 1. Set up the number of slots & list of indexes in each slot
        n = listTables.size();
       
        for (Table table : listTables) {    
            List<Index> listIndex = new ArrayList<Index>();         
            
            for (Index index : candidateIndexes) {
                // normal index (not the full table scan index)
                if (index.getTable().equals(table)){     
                    listIndex.add(index);
                }
            }
            this.listIndexesWithoutFTSEachSlot.add(listIndex);
            // add the index full table scan at the last position in this slot
            FullTableScanIndex scanIdx = getFullTableScanIndexInstance(table);
            listIndex.add(scanIdx);
            listIndexesEachSlot.add(listIndex);
        }
                
        Kq = templatePlans.size();
        listAccessCostPerPlan = new ArrayList<Map<Index, Double>>();
        for (InumPlan plan : templatePlans) {
            System.out.println("L119, internal plan cost: " + plan.getInternalCost());
            beta.add(new Double(plan.getInternalCost()));
            Map<Index, Double> mapIndexAccessCost = new HashMap<Index, Double>();
            for (int i = 0; i < n; i++) {
                for (Index index : this.listIndexesEachSlot.get(i)) {
                    double cost = plan.plug(index);
                    if (cost == Double.POSITIVE_INFINITY) {
                        cost = InumQueryPlanDesc.BIP_MAX_VALUE;
                    }
                    mapIndexAccessCost.put(index, new Double(cost));
                    System.out.println("L136, index: " + index.getFullyQualifiedName()
                                            + " cost: " + cost);
                }                            
            }
            this.listAccessCostPerPlan.add(mapIndexAccessCost);
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
	public int getNumberOfSlots()
	{
		return n;
	}		
	
	/* (non-Javadoc)
     */
    @Override
	public List<Index> getListIndexesAtSlot(int i)
	{
		return this.listIndexesEachSlot.get(i);
	}
	
    /* (non-Javadoc)
     */
    @Override
    public List<Index> getListIndexesWithoutFTSAtSlot(int i)
    {
        return this.listIndexesWithoutFTSEachSlot.get(i);
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
     */
    @Override
	public double getAccessCost(int k, Index index)
	{
        assert (k < Kq);
		Object found = listAccessCostPerPlan.get(k).get(index);
		if (found != null) {
		    return (Double) found;
		}
        return -1;
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
    public List<Table> getTables()
    {
        return this.listTables;
    }
}
