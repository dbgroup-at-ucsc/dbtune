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
	private List<List<Index>> indexesSlot;
	
	/** List of indexes (excluding FTS) at each slot */
	private List<List<Index>> indexesWithoutFTSSlot;
	
	/** List of active indexes at each slot*/
	private List<Set<Index>> activeIndexesWithouFTSSlot;
	 
	/** List of index access cost in each plan */
	private List<Map<Integer, Double>> accessCostPerPlan;
	
	/** used to uniquely identify each instances of the class. */
	static AtomicInteger STMT_ID = new AtomicInteger(0);
	private int stmtID;
	
	/** List of referenced tables */
	List<Table> tables;
		
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
        this.tables = null;
    }
    
     
    @Override
    public void generateQueryPlanDesc(InumOptimizer optimizer, Set<Index> candidateIndexes) 
                                      throws SQLException
    {   
        InumPreparedSQLStatement preparedStmt;
        Set<InumPlan>            templatePlans;
        
        beta   = new ArrayList<Double>();
        tables = new ArrayList<Table>();
        
        indexesSlot           = new ArrayList<List<Index>>();
        indexesWithoutFTSSlot = new ArrayList<List<Index>>();
        accessCostPerPlan     = new ArrayList<Map<Integer, Double>>();
        
        activeIndexesWithouFTSSlot = new ArrayList<Set<Index>>();
        
        preparedStmt  = (InumPreparedSQLStatement) optimizer.prepareExplain(stmt);
        templatePlans = preparedStmt.getTemplatePlans();
                     
        for (InumPlan plan : templatePlans) {
            tables = plan.getTables();
            break;
        }
        
        // 1. Set up the number of slots & list of indexes in each slot
        n = tables.size();    
        
        for (Table table : tables) {    
            
            List<Index> indexes           = new ArrayList<Index>();         
            List<Index> indexesWithoutFTS = new ArrayList<Index>();
            Set<Index>  activeIndexes     = new HashSet<Index>();
            
            // normal index (not the full table scan index)
            for (Index index : candidateIndexes) 
                if (index.getTable().equals(table) && !(index instanceof FullTableScanIndex)){     
                    indexes.add(index);
                    indexesWithoutFTS.add(index);
                }
            
            indexesWithoutFTSSlot.add(indexesWithoutFTS);
            activeIndexesWithouFTSSlot.add(activeIndexes);
            
            // add the Full Table Scan Index at the last position in this slot
            FullTableScanIndex scanIdx = getFullTableScanIndexInstance(table);
            indexes.add(scanIdx);
            indexesSlot.add(indexes);
        }
                
        Kq = 0;
        double cost, costFTS = 0.0;
        Index index;
        int numIndex;
        
        for (InumPlan plan : templatePlans) {
            
            beta.add(plan.getInternalCost());
            Map<Integer, Double> mapIndexAccessCost = new HashMap<Integer, Double>();
            
            for (int i = 0; i < n; i++) {
                
                numIndex = indexesSlot.get(i).size();
                
                for (int j = numIndex - 1; j > -1; j--) {
                    index = indexesSlot.get(i).get(j);                    
                    cost = plan.plug(index);                    

                    if (cost == Double.POSITIVE_INFINITY)
                        cost = InumQueryPlanDesc.BIP_MAX_VALUE;
             
                    if (j == numIndex - 1) 
                        costFTS = cost;
                    else if (cost < costFTS) 
                        activeIndexesWithouFTSSlot.get(i).add(index);
                    
                    mapIndexAccessCost.put(index.getId(), cost);
                }                
            }
            
            accessCostPerPlan.add(mapIndexAccessCost);
            Kq++;
        }
        
        // Update indexEachSlot and indexWithoutFTSEachSlot
        // Remove inactive index
        for (int i = 0; i < n; i++) {
            indexesWithoutFTSSlot.set(i, new ArrayList<Index>
                                             (activeIndexesWithouFTSSlot.get(i)));
            numIndex  = indexesSlot.get(i).size();
            Index fts = indexesSlot.get(i).get(numIndex - 1);
            List<Index> active = new ArrayList<Index>
                                     (activeIndexesWithouFTSSlot.get(i));
            active.add(fts);
            indexesSlot.set(i, active);     
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
	public List<Index> getIndexesAtSlot(int i)
	{
		return indexesSlot.get(i);
	}
	
    
    @Override
    public List<Index> getIndexesWithoutFTSAtSlot(int i)
    {
        return indexesWithoutFTSSlot.get(i);
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
        return tables;
    }

    @Override
    public Set<Index> getActiveIndexesAtSlot(int i) 
    {
        return activeIndexesWithouFTSSlot.get(i);
    }   
}
