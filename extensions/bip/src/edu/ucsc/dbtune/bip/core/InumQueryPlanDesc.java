package edu.ucsc.dbtune.bip.core;

import java.io.Serializable;
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
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.workload.SQLCategory;
import edu.ucsc.dbtune.workload.SQLStatement;

import static edu.ucsc.dbtune.inum.FullTableScanIndex.getFullTableScanIndexInstance;
import static edu.ucsc.dbtune.workload.SQLCategory.NOT_SELECT;
import static edu.ucsc.dbtune.workload.SQLCategory.UPDATE;
import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;
import static edu.ucsc.dbtune.workload.SQLCategory.INSERT;
import static edu.ucsc.dbtune.workload.SQLCategory.DELETE;

/**
 * An implementation of {@link QueryPlanDesc} interface. There's only one single object of 
 * this type per statement, i.e., this can be viewed as a singleton class but instead of  
 * creating one object throughout the JVM, this class creates one for every distinct statement 
 * contained in a {@link Workload}.
 * 
 * @author Quoc Trung Tran
 *
 */
public class InumQueryPlanDesc implements QueryPlanDesc, Serializable 
{	
    /**
     * Default value
     */
    private static final long serialVersionUID = 1L;
    

    /**
     * The maximum value that corresponds to INFTY
     * In order for CPLEX solver to run 
     */
    public static double BIP_MAX_VALUE = 99999999;
    
    /** The corresponding SQL statement of this object */
    private SQLStatement stmt;
    
    /** The number of template plans */
    private int Kq;
    
    /** The number of slots */
    private int n;
    	
	/** List of indexes (including FTS) at each slot */
	private List<List<Index>> indexSlot;
	
	/** List of indexes (excluding FTS) at each slot */
	private List<List<Index>> indexWithoutFTSSlot;
	
	/** List of active indexes at each slot*/
	private List<Set<Index>> activeIndexWithouFTSSlot;
	
	/** The array of internal plan costs */
    private List<Double> beta;
    
	private Map<Integer, Double> indexUpdateCosts;
	
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
	
	/** A map in order not to populate template plans if it has been done before */
	public static Map<SQLStatement, InumPreparedSQLStatement> preparedStmts = new
	                                            HashMap<SQLStatement, InumPreparedSQLStatement>();
	
	private double baseTableUpdateCost;
	
	/** An index update cost = this_factor * base_table_cost (for INSERT/DELETE) */
	public static double INDEX_UPDATE_COST_FACTOR = 1.0;
	
	
	@Override
	public void setStatement(SQLStatement stmt)
	{
	    this.stmt = stmt;
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
        
        // initialize default value
        Kq = 0;
        n = 0;
    }
    
    @Override
    public double getStatementWeight()
    {
        return stmt.getStatementWeight();
    }
    
    public long populateTime;
    public long pluginTime;

    @Override 
    public double getBaseTableUpdateCost()
    {
        // UPDATE, INSERT, DELETE all have base table update cost
        if (stmt.getSQLCategory().isSame(NOT_SELECT))
            return baseTableUpdateCost;
        else 
            return 0.0;
    }
    
    
    @Override
    public void generateQueryPlanDesc(InumOptimizer optimizer, Set<Index> candidates) 
                                      throws SQLException
    {   
        InumPreparedSQLStatement preparedStmt;
        Set<InumPlan> templatePlans;
        
        // 1. Get the template plans from INUM
        preparedStmt = preparedStmts.get(stmt);
        if (preparedStmt == null) {
            preparedStmt = (InumPreparedSQLStatement) optimizer.prepareExplain(stmt);
            preparedStmts.put(stmt, preparedStmt);
        }
        
        templatePlans = preparedStmt.getTemplatePlans();
                
        // 2. Number of slots and indexes in each slot
        // 3. Template plan cost
        // only for SELECT and UPDATE statements
        if (stmt.getSQLCategory().isSame(SELECT) 
                  || stmt.getSQLCategory().isSame(UPDATE)) {
            assignIndexesToSlot(candidates, templatePlans);
            getPlanCostsFromInum(templatePlans);
        }
        
        // 4. Get the update costs if the statement is not SELECT statement
        // including base table & index update cost
        if (stmt.getSQLCategory().isSame(NOT_SELECT))
            getUpdateCostsFromInum(preparedStmt, candidates);
        
        // 5. Optimization: remove inactive indexes
        removeInactiveIndexes();
    }
     
    /**
     * Assign indexes into the slot. 
     * 
     * @param candidates
     *      The set of candidate indexes
     * @param templatePlans
     *      The set of template plans returned by INUM 
     *      
     * @throws SQLException
     *      When there is an error in creating a Full Table Scan index     
     * 
     */
	private void assignIndexesToSlot(Set<Index> candidates, Set<InumPlan> templatePlans) 
	                                throws SQLException
	{
	    indexSlot           = new ArrayList<List<Index>>();
        indexWithoutFTSSlot = new ArrayList<List<Index>>();
        activeIndexWithouFTSSlot = new ArrayList<Set<Index>>();
        
        // Get table in each slot
        for (InumPlan plan : templatePlans) {
            tables = plan.getTables();
            break;
        }
        
        n = tables.size();        
	    for (Table table : tables) {
	        
	        List<Index> indexes           = new ArrayList<Index>();         
	        List<Index> withoutFTSIndexes = new ArrayList<Index>();
	        Set<Index>  activeIndexes     = new HashSet<Index>();
            
            // normal index (not the full table scan index)
            for (Index index : candidates) 
                if (index.getTable().equals(table) && !(index instanceof FullTableScanIndex)){     
                    indexes.add(index);
                    withoutFTSIndexes.add(index);
                }
            
            indexWithoutFTSSlot.add(withoutFTSIndexes);
            activeIndexWithouFTSSlot.add(activeIndexes);
            
            // add the Full Table Scan Index at the last position in this slot
            FullTableScanIndex scanIdx = getFullTableScanIndexInstance(table);
            indexes.add(scanIdx);
            indexSlot.add(indexes);
        }
	}
    
	/**
	 * Retrieve the internal plan costs and index access costs.
	 * 
	 * @param templatePlans
	 *     A set of template plans returned by INUM
	 *     
	 * @throws SQLException
	 *     When there is error in communicating with INUM.
	 */
	private void getPlanCostsFromInum(Set<InumPlan> templatePlans) throws SQLException
	{
	    double cost;
        double costFTS;
        int    numIndex;
        Index  index;
        
        beta = new ArrayList<Double>();
        accessCostPerPlan = new ArrayList<Map<Integer, Double>>();
        
        Kq = 0;
        costFTS = 0.0;
        
        for (InumPlan plan : templatePlans) {
            
            beta.add(plan.getInternalCost());
            Map<Integer, Double> mapIndexAccessCost = new HashMap<Integer, Double>();
            
            for (int i = 0; i < n; i++) {
                
                numIndex = indexSlot.get(i).size();
                
                for (int j = numIndex - 1; j > -1; j--) {
                    index = indexSlot.get(i).get(j);                    
                    cost = plan.plug(index);                    
                    
                    if (cost == Double.POSITIVE_INFINITY)
                        cost = InumQueryPlanDesc.BIP_MAX_VALUE;
                    
                    if (j == numIndex - 1) 
                        costFTS = cost;
                    else if (cost < costFTS) 
                        activeIndexWithouFTSSlot.get(i).add(index);
                    
                    mapIndexAccessCost.put(index.getId(), cost);
                }                
            }
            
            accessCostPerPlan.add(mapIndexAccessCost);
            Kq++;
        }    
	}
	
	/**
	 * Retrieve the update costs of the indexes that are relevant to the statement
	 * 
	 * @param preparedStmt
	 *      The INUM prepared statement
	 * @param candidates
	 *       The set of candidate indexes
	 *            
	 * @throws SQLException 
	 */
	private void getUpdateCostsFromInum(InumPreparedSQLStatement preparedStmt, Set<Index> candidates) 
	             throws SQLException
	{
	    indexUpdateCosts = new HashMap<Integer, Double>();
	    ExplainedSQLStatement inumExplain = preparedStmt.explain(candidates);
	    
	    double cost;
	    
	    // since we don't have slot in INSERT/DELETE statement,
	    // we will get indexes from the given set of candidate indexes
	    if (stmt.getSQLCategory().isSame(INSERT) || 
	            stmt.getSQLCategory().isSame(DELETE)) {
	        
	        for (Index index : candidates) {
	            cost = inumExplain.getUpdateCost(index);
	            indexUpdateCosts.put(index.getId(), cost);
	        }
	        
	    } else {
	        for (int i = 0; i < n; i++)
	            for (Index index : indexSlot.get(i)) 
	                indexUpdateCosts.put(index.getId(), inumExplain.getUpdateCost(index));
	    }
	    
	    baseTableUpdateCost = inumExplain.getBaseTableUpdateCost();
	}
	
	
	/**
	 * An index is inactive iff its cost is greater than the cost of the Full Table Scan index.
	 * An inactive index will never be used in the optimal plan derived by INUM.
	 * 
	 */
	private void removeInactiveIndexes()
	{
	    int numIndex;
	    
	    // Update indexEachSlot and indexWithoutFTSEachSlot
        // Remove inactive index
        for (int i = 0; i < n; i++) {
            indexWithoutFTSSlot.set(i, new ArrayList<Index>
                                             (activeIndexWithouFTSSlot.get(i)));
            numIndex  = indexSlot.get(i).size();
            Index fts = indexSlot.get(i).get(numIndex - 1);
            List<Index> active = new ArrayList<Index>
                                     (activeIndexWithouFTSSlot.get(i));
            active.add(fts);
            indexSlot.set(i, active);     
        }
	}  
	
	/**
	 * Retrieve the list of full table scan indexes referenced in this plan description 
	 */
	@Override
	public Set<FullTableScanIndex> getFullTableScanIndexes()
	{
	    Set<FullTableScanIndex> fts = new HashSet<FullTableScanIndex>();
	    int pos;
	    
	    for (int i = 0; i < n; i++) {
	        pos = indexSlot.get(i).size() - 1;
	        fts.add((FullTableScanIndex) indexSlot.get(i).get(pos));
	    }
	    
	    return fts;
	}
	
	
	@Override
    public Set<Index> getIndexes()
    {
	    Set<Index> candidates = new HashSet<Index>();
	    
        for (int i = 0; i < n; i++) 
            candidates.addAll(indexSlot.get(i));
        
        return candidates;
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
		return indexSlot.get(i);
	}
	
    
    @Override
    public List<Index> getIndexesWithoutFTSAtSlot(int i)
    {
        return indexWithoutFTSSlot.get(i);
    }
	
    
    @Override
	public double getInternalPlanCost(int k)
	{
		return beta.get(k);
	}
	
	
    @Override
	public double getAccessCost(int k, Index index)
	{
        // if the index has not been seen in this slot
        // return INFINITY
        if (!accessCostPerPlan.get(k).containsKey(index.getId()))
            return BIP_MAX_VALUE;
        
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
        return activeIndexWithouFTSSlot.get(i);
    }

    @Override
    public SQLCategory getSQLCategory() 
    {
        return stmt.getSQLCategory();
    }       
    
    @Override
    public double getUpdateCost(Index index)
    {   
        if (!stmt.getSQLCategory().isSame(NOT_SELECT) 
                || indexUpdateCosts.get(index.getId()) == null)
            return 0.0;
     
        return indexUpdateCosts.get(index.getId());
    }

    @Override
    public String toString() 
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append("InumQueryPlanDesc \n" + "Number of template plans:" + Kq + "\n");
        sb.append(" Weight = " + getStatementWeight() + "\n");
        sb.append("Number of slots: " + n + "\n");
        sb.append("Internal plan costs: " + beta + "\n");
        sb.append("Index access costs: " + accessCostPerPlan + "\n");
        sb.append("Index update costs: " + indexUpdateCosts + "\n");
        
        return sb.toString();
    }
    

    @Override
    public boolean equals(Object obj) 
    {   
        double tolerance;
        InumQueryPlanDesc other;
        
        other = (InumQueryPlanDesc) obj;
        tolerance = 1.05;
                
        if (Kq != other.Kq) {
            System.out.println(" QueryPlanDesc NOT COMPABTILE because of: Number of template plans");
            return false;
        }
        
        // number of relations
        if (this.n != other.n) {
            System.out.println(" QueryPlanDesc NOT COMPABTILE because of: " +
            		"Number of slots");
            return false;
        }
        
        if (!checkInternalPlan(other, tolerance))
            return false;
        
        if (!checkIndexAccessCost(other, tolerance))
            return false;
        
        if (!checkIndexUpdateCost(other, tolerance))
            return false;
        
        
        return true;
    }
    
    
    /**
     * todo
     * @param other
     * @param tolerance
     * @return
     */
    private boolean checkInternalPlan(InumQueryPlanDesc other, double tolerance)
    {
        boolean matched;
        
        if (beta == null) {
            if (other.beta != null)
                return false;
        } else {
            if (beta.size() != other.beta.size()) {
                System.out.println(" QueryPlanDesc NOT COMPABTILE because of: Number of internal plan costs");
                return false;
            }
        
            // internal plan cost
            for (int i = 0; i < beta.size(); i++) {
                matched =false;
                for (int j = 0; j < other.beta.size(); j++) {
            
                    matched = matchedDouble(beta.get(i), other.beta.get(j), tolerance);
                    if (matched)
                        break;
                }
                
                if (!matched) {
                    System.out.println(" QueryPlanDesc NOT COMPABTILE because of: " +
                            "INTERNAL PLAN COST");
                    return false;
                }
            }
            
        }
        
        return true;
    }
    
    /**
     * todo
     * @return
     */
    private boolean checkIndexAccessCost(InumQueryPlanDesc other, double tolerance)
    {
        boolean matched;
        
        Set<Index> sourceIndexes;
        Set<Index> destIndexes;
        
        // index access cost
        sourceIndexes = getIndexes();
        destIndexes = other.getIndexes();
        
        if (sourceIndexes.size() != destIndexes.size()) {
            System.out.println(" QueryPlanDesc NOT COMPABTILE because of: " +
                    "Number of candidate indexes");
            return false;
        }
        
        // check the index access cost
        if (accessCostPerPlan == null){
            if (other.accessCostPerPlan != null)
                return false;
        } else {
            for (int kSource = 0; kSource < Kq; kSource++) {
                matched = false;
                for (Index index : sourceIndexes) {
                    
                    // iterate over all plans
                    for (int kDest = 0; kDest < Kq; kDest++) {
                        
                        matched = matchedDouble(getAccessCost(kSource, index),
                                                other.getAccessCost(kDest, index),
                                                tolerance);
                        if (matched)
                            break; 
                        
                    }
                    if (!matched) {
                        System.out.println("FIRST, QueryPlanDesc NOT COMPABTILE because of: " +
                                "INDEX ACCESS COST"
                                + " Index: " + index + " " + index.getId()
                                + " cost: " + getAccessCost(kSource, index));
                        return false;
                    }
                }   
            }
             
            // dest
            for (int kDest = 0; kDest < Kq; kDest++) {
                matched = false;
                for (Index index : destIndexes) {
                    for (int kSource = 0; kSource < Kq; kSource++) {
                        matched = matchedDouble(getAccessCost(kSource, index),
                                                other.getAccessCost(kDest, index), tolerance);
                        if (matched)
                            break;
                    }
                        
                    if (!matched) {
                        System.out.println("SECOND, QueryPlanDesc NOT COMPABTILE because of: " +
                        "INDEX ACCESS COST"
                                + " Index: " + index + " " + index.getId()
                                + " cost: " + getAccessCost(kDest, index));
                
                        return false;
                    }
                }   
            }
        }
        
        return true;
    }
    
    /**
     * todo
     * @param other
     * @param tolerance
     * @return
     */
    private boolean checkIndexUpdateCost(InumQueryPlanDesc other, double tolerance)
    {
        Set<Index> sourceIndexes;
        Set<Index> destIndexes;
        
        // index access cost
        sourceIndexes = getIndexes();
        destIndexes = other.getIndexes();
        // index update cost semantic
        if (indexUpdateCosts == null) {
            if (other.indexUpdateCosts != null)
                return false;
        } else {
            // index update cost
            for (Index index : sourceIndexes)
                if (!matchedDouble(getUpdateCost(index), other.getUpdateCost(index), tolerance)) {
                    System.out.println(" QueryPlanDesc NOT COMPABTILE because of: " +
                    "INDEX UPDATE COST");
                    return false;
                }
                
            
            // index update cost
            for (Index index : destIndexes)
                if (!matchedDouble(getUpdateCost(index), other.getUpdateCost(index), tolerance)) {
                    System.out.println(" QueryPlanDesc NOT COMPABTILE because of: " +
                    "INDEX UPDATE COST");
                    return false;
                }
        }
    
        return true;
    }
    /**
     * todo
     * @param first
     * @param second
     * @param tolerance
     * @return
     */
    private boolean matchedDouble(double first, double second, double tolerance)
    {
        if (first == 0.0 && second == 0.0)
            return true;
        
        double ratio = first / second;
        
        if (ratio < 1.0)
            ratio = 1.0 / ratio;
        
        return (ratio <= tolerance) ? true : false;
    }
}
