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
 * An implementation of {@link QueryPlanDesc} interface. There's only one single object of 
 * this type per statement, i.e., this can be viewed as a singleton class but instead of  
 * creating one object throughout the JVM, this class creates one for every distinct statement 
 * contained in a {@link Workload}.
 * 
 * @author Quoc Trung Tran
 *
 */
public class InumQueryPlanDesc implements QueryPlanDesc 
{	
    public static double BIP_MAX_VALUE = 99999999;
    
    /** The corresponding SQL statement of this object */
    SQLStatement stmt;
    
    /** The number of template plans */
    private int Kq;
    
    /** The number of slots */
    private int n;
    
    /** The array of internal plan costs */
	private List<Double> beta;
	
	/** List of indexes (including FTS) at each slot */
	private List<List<Index>> indexesEachSlot;
	
	/** List of indexes (excluding FTS) at each slot */
	private List<List<Index>> indexesWithoutFTSEachSlot;
	
	/** List of indexes that can be used at at least one slot of the template plans */
	private List<Set<Index>> activeIndexesEachSlot;
	 
	/** List of index access cost in each plan */
	private List<Map<Integer, Double>> accessCostPerPlan;
	
	/** used to uniquely identify each instances of the class. */
	static AtomicInteger STMT_ID = new AtomicInteger(0);
	private int stmtID;
	
	/** List of referenced tables */
	List<Table> listTables;
		
    /** A map to manage each statement corresponding to one instance of this class*/
	private static Map<SQLStatement, QueryPlanDesc> instances = new 
	                                            HashMap<SQLStatement, QueryPlanDesc>();
	
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
        QueryPlanDesc desc = instances.get(stmt);

        if (desc == null) {
            desc = new InumQueryPlanDesc(stmt);
            instances.put(stmt, desc);
        }

        return desc;
    }
    
	/**
	 * The constructor, each object of this class corresponds to a {@code SQLSatement} object
	 * A new object is associated with an ID that is incremented starting from value {@code 0}.
	 * 
	 * @param stmt
	 *      The statement that this object corresponds to
	 */
    private InumQueryPlanDesc(SQLStatement stmt)
    {
        this.stmtID = InumQueryPlanDesc.STMT_ID.getAndIncrement();
        this.stmt = stmt;
        this.listTables = null;
    }
    
     
    @Override
    public void generateQueryPlanDesc(InumOptimizer optimizer, Set<Index> candidateIndexes) 
                                      throws SQLException
    {   
        beta                      = new ArrayList<Double>();
        indexesEachSlot           = new ArrayList<List<Index>>();
        indexesWithoutFTSEachSlot = new ArrayList<List<Index>>();
        activeIndexesEachSlot     = new ArrayList<Set<Index>>();
        listTables                = new ArrayList<Table>();
        accessCostPerPlan         = new ArrayList<Map<Integer, Double>>();
        
        InumPreparedSQLStatement preparedStmt = (InumPreparedSQLStatement) 
                                                 optimizer.prepareExplain(stmt);
        Set<InumPlan> templatePlans   = preparedStmt.getTemplatePlans();
                     
        for (InumPlan plan : templatePlans) {
            
            listTables = plan.getTables();
            break;
            
        }
        
        // 1. Set up the number of slots & list of indexes in each slot
        n = listTables.size();    
        
        for (Table table : listTables) {    
            
            List<Index> listIndex           = new ArrayList<Index>();         
            List<Index> listIndexWithoutFTS = new ArrayList<Index>();
            Set<Index>   setActiveIndexes   = new HashSet<Index>();
            
            // normal index (not the full table scan index)
            for (Index index : candidateIndexes) {
                
                if (index.getTable().equals(table)){     
                    listIndex.add(index);
                    listIndexWithoutFTS.add(index);
                }
                
            }
            
            indexesWithoutFTSEachSlot.add(listIndexWithoutFTS);
            activeIndexesEachSlot.add(setActiveIndexes);
            
            // add the Full Table Scan Index at the last position in this slot
            FullTableScanIndex scanIdx = getFullTableScanIndexInstance(table);
            listIndex.add(scanIdx);
            indexesEachSlot.add(listIndex);
        }
                
        Kq = 0;
        double cost, costFTS = 0.0;
        Index index;
        int numIndex;
        
        for (InumPlan plan : templatePlans) {
            
            beta.add(plan.getInternalCost());
            Map<Integer, Double> mapIndexAccessCost = new HashMap<Integer, Double>();
            
            for (int i = 0; i < n; i++) {
                
                numIndex = indexesEachSlot.get(i).size();
                
                for (int j = numIndex - 1; j > -1; j--){
                    
                    index = indexesEachSlot.get(i).get(j);                    
                    cost = plan.plug(index);                    
                    
                    if (cost == Double.POSITIVE_INFINITY)
                        cost = InumQueryPlanDesc.BIP_MAX_VALUE;
             
                    if (j == numIndex - 1) 
                        costFTS = cost;
                    else if (cost < costFTS) 
                        activeIndexesEachSlot.get(i).add(index);
                    
                    mapIndexAccessCost.put(index.getId(), cost);                    
                }
                
            }
            
            accessCostPerPlan.add(mapIndexAccessCost);
            Kq++;
        }
    }
     
	
    @Override
	public int getNumberOfTemplatePlans()
	{
		return Kq;
	}
	
	
    @Override
	public int getNumberOfSlots()
	{
		return n;
	}		
	
	
    @Override
	public List<Index> getListIndexesAtSlot(int i)
	{
		return indexesEachSlot.get(i);
	}
	
    
    @Override
    public List<Index> getListIndexesWithoutFTSAtSlot(int i)
    {
        return indexesWithoutFTSEachSlot.get(i);
    }
	
    
    @Override
	public double getInternalPlanCost(int k)
	{
		return beta.get(k);
	}
	
	
    @Override
	public double getAccessCost(int k, Index index)
	{
		return accessCostPerPlan.get(k).get(index.getId());
	}
	
	
    @Override
	public int getStatementID()
    {
        return stmtID;
    }
	
    
    @Override 
    public List<Table> getTables()
    {
        return listTables;
    }

    @Override
    public Set<Index> getActiveIndexsAtSlot(int i) 
    {
        return activeIndexesEachSlot.get(i);
    }   
}
