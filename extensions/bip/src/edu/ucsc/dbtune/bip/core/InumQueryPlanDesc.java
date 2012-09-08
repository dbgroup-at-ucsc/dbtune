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
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.TableAccessSlot;
import edu.ucsc.dbtune.util.Rt;
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
    private static final long serialVersionUID = 1L;    
    public static final double BIP_MAX_VALUE = Math.pow(10, 10);
    
    /** The set of template plans */
    private List<PlanInBIP> plans;
    
    /** The corresponding SQL statement of this object */
    private SQLStatement stmt;
    
    /** Index update cost */
	private Map<Index, Double> indexUpdateCosts;
	
	/** used to uniquely identify each instances of the class. */
	static AtomicInteger STMT_ID = new AtomicInteger(0);
	private int stmtID;
	
    /** A map to manage each statement corresponding to one instance of this class*/
	private static Map<SQLStatement, QueryPlanDesc> instances = new 
	                                            HashMap<SQLStatement, QueryPlanDesc>();
	
	/** A map in order not to populate template plans if it has been done before */
	public static Map<SQLStatement, InumPreparedSQLStatement> preparedStmts = new
	                                            HashMap<SQLStatement, InumPreparedSQLStatement>();
	
	private double baseTableUpdateCost;
	
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
    }
    
    @Override
    public double getStatementWeight()
    {
        return stmt.getStatementWeight();
    }
    
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
        // 2. Template plan
        // only for SELECT and UPDATE statements
        plans = new ArrayList<PlanInBIP>();
        if (stmt.getSQLCategory().isSame(SELECT) 
                  || stmt.getSQLCategory().isSame(UPDATE)) {
            
            for (InumPlan plan : templatePlans) {
                
                PlanInBIP planBIP = new PlanInBIP();
                planBIP.getPlanCostsFromInum(candidates, plan);
                
                if (!planBIP.isDead)
                    plans.add(planBIP);
            }
            
        }
        
        // 4. Get the update costs if the statement is not SELECT statement
        // including base table & index update cost
        if (stmt.getSQLCategory().isSame(NOT_SELECT))
            getUpdateCostsFromInum(preparedStmt, candidates);
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
	    indexUpdateCosts = new HashMap<Index, Double>();
	    ExplainedSQLStatement inumExplain = preparedStmt.explain(candidates);
	    
	    double cost;
	    
	    // since we don't have slot in INSERT/DELETE statement,
	    // we will get indexes from the given set of candidate indexes
	    if (stmt.getSQLCategory().isSame(INSERT) || 
	            stmt.getSQLCategory().isSame(DELETE)) {
	        
	        for (Index index : candidates) {
	            cost = inumExplain.getUpdateCost(index);
	            indexUpdateCosts.put(index, cost);
	        }
	        
	    } else {
	        // we just have one plan for update statement?
	        if (plans.size() > 1)
	            Rt.p("WATCH OUT, more than one plans for an update statement");
	        
	         for (Index index : plans.get(0).getIndexes())
	             indexUpdateCosts.put(index, inumExplain.getUpdateCost(index));
	    }
	    
	    baseTableUpdateCost = inumExplain.getBaseTableUpdateCost();
	}
		
	@Override
	public Index getFTSAtSlot(int planId, int slotId)
	{
	    return plans.get(planId).getFTSIndexAtSlot(slotId);
	}
	
	@Override
	public Set<FullTableScanIndex> getFullTableScanIndexes()
	{
	    Set<FullTableScanIndex> fts = new HashSet<FullTableScanIndex>();
	    
	    if (plans.size() > 0)
	        fts = plans.get(0).getFullTableScanIndexes();
	    
	    return fts;
	}
	
	
	@Override
    public Set<Index> getIndexes()
    {
	    Set<Index> candidates = new HashSet<Index>();
	    
	    for (PlanInBIP plan : plans)
	        candidates.addAll(plan.getIndexes());
	    
        return candidates;
    }
    
	
    @Override
	public int getNumberOfTemplatePlans()
	{
		return plans.size();
	}
	
	
    @Override
	public int getNumberOfSlots(int planId)
	{
		return plans.get(planId).getNumberOfSlots();
	}		
	
    @Override
	public List<Index> getIndexesAtSlot(int planId, int slotId)
	{
		return plans.get(planId).getIndexesAtSlot(slotId);
	}
	
    
    @Override
    public List<Index> getIndexesWithoutFTSAtSlot(int planId, int slotId)
    {
        return plans.get(planId).getIndexesWithoutFTSAtSlot(slotId);
    }
	
    
    @Override
	public double getInternalPlanCost(int planId)
	{
		return plans.get(planId).getInternalPlanCost();
	}
	
	
    @Override
	public double getAccessCost(int planId, int slotId, Index index)
	{
        return plans.get(planId).getIndexAccessCost(slotId, index);
	}
	
	
    @Override
	public int getStatementID()
    {
        return stmtID;
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
                || indexUpdateCosts.get(index) == null)
            return 0.0;
     
        return indexUpdateCosts.get(index);
    }

    @Override
    public String toString() 
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append("InumQueryPlanDesc \n" + "Number of template plans:" + plans.size() + "\n");
        sb.append(" Weight = " + getStatementWeight() + "\n");
        for (PlanInBIP plan : plans)
            sb.append(plan.toString());
        sb.append("\nIndex update costs: " + indexUpdateCosts + "\n");
        
        return sb.toString();
    }
    
    /**
     * Each plan objects containing the costs retrieved from INUM
     * 
     * @author Quoc Trung Tran
     *
     */
    class PlanInBIP implements Serializable
    {   
        private static final long serialVersionUID = 1L;
        
        /**
         * Each slot containing the cost retrieved by INUM
         * 
         * @author Quoc Trung Tran
         *
         */
        class SlotInBIP implements Serializable
        {
            private static final long serialVersionUID = 1L;
            
            /* A slot is dead if no index can fit into this slot*/
            boolean isDead;
            Table table;
            
            /* Only store indexes that have index access cost
             * less than INF (can be plug into this slot) 
             **/
            List<Index> indexes;
            List<Index> withoutFTSIndexes;
            Map<Index, Double> accessCost;
            
            @Override
            public String toString() 
            {
                StringBuilder sb = new StringBuilder();
                sb.append(accessCost);
                return sb.toString();
            }
        }
        
        /** List of table access slots */
        private List<SlotInBIP> slots;
        
        /** Internal plan cost **/
        private double internalCost;
        
        /** A plan is dead if at least one of its slot is dead */
        private boolean isDead;
               
        @Override
        public String toString() 
        {
            StringBuilder sb = new StringBuilder();
            sb.append("-------------\n");
            sb.append("Internal plan cost: " + internalCost + "\n");
            sb.append("Number of slots:" + slots.size());
            for (SlotInBIP slot : slots)
                sb.append(slot.toString());
            
            return sb.toString();
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
        public void getPlanCostsFromInum(Set<Index> candidates, InumPlan plan) throws SQLException
        {   
            double cost;
            
            internalCost = plan.getInternalCost();
            slots = new ArrayList<SlotInBIP>();
            isDead = false;
            
            for (TableAccessSlot slot : plan.getSlots()) {
                
                SlotInBIP slotBIP = new SlotInBIP();
                
                slotBIP.table = slot.getTable();
                slotBIP.indexes = new ArrayList<Index>();         
                slotBIP.withoutFTSIndexes = new ArrayList<Index>();
                slotBIP.accessCost = new HashMap<Index, Double>();
                slotBIP.isDead = true;
                
                // normal index (not the full table scan index)
                for (Index index : candidates) 
                    if (index.getTable().equals(slot.getTable()) && 
                            !(index instanceof FullTableScanIndex)){     
                        
                        cost = plan.plug(slot, index);
                        
                        // if the cost is INF ==> the index
                        // is not used in this slot of this particular
                        // template plan
                        if (!Double.isInfinite(cost)) {
                            slotBIP.indexes.add(index);
                            slotBIP.withoutFTSIndexes.add(index);
                            slotBIP.accessCost.put(index, cost);
                            slotBIP.isDead = false;
                        }
                            
                    }
                
                // add the Full Table Scan Index at the last position in this slot
                FullTableScanIndex ftsIdx = getFullTableScanIndexInstance(slot.getTable());
                cost = plan.plug(slot, ftsIdx);
                
                if (!Double.isInfinite(cost)) {
                    slotBIP.isDead = false;
                    slotBIP.indexes.add(ftsIdx);
                    slotBIP.accessCost.put(ftsIdx, cost);
                }
                
                slots.add(slotBIP);
                
                // At least one slot is dead, 
                // then the plan is dead also
                // This plan will not be used
                if (slotBIP.isDead) {
                    Rt.p(" WATCH-OUT, plan is DEAD");
                    isDead = true;
                    break;
                    
                }
            }
        }
        
        /**
         * Retrieve the internal plan cost
         * 
         * @return
         *      Internal plan cost
         */
        public double getInternalPlanCost()
        {
            return internalCost;
        }
        
        /**
         * Retrieve the index access cost
         * 
         * @param slotId
         *      Slot id
         * @param index
         *      Index
         * @return
         *      Index access cost
         */
        public double getIndexAccessCost(int slotId, Index index) 
            throws RuntimeException
        {
            if (!slots.get(slotId).accessCost.containsKey(index))
                throw new RuntimeException("This index is not activated"
                                 + " in the given slot");
            
            return slots.get(slotId).accessCost.get(index);
        }
        
        /**
         * Retrieve the number of slots
         * 
         * @return
         *      Number of slots
         */
        public int getNumberOfSlots()
        {
            return slots.size();
        }
        /**
         * Retrieve indexes (without FTS) at the given slot
         * 
         * @param slotId
         * @return
         */
        public List<Index> getIndexesWithoutFTSAtSlot(int slotId)
        {
            return slots.get(slotId).withoutFTSIndexes;
        }
        
        /**
         * Retrieve indexes (without FTS) at the given slot
         * 
         * @param slotId
         * @return
         */
        public List<Index> getIndexesAtSlot(int slotId)
        {
            return slots.get(slotId).indexes;
        }
        
        /**
         * Retrieve the set of full table scan indexes
         * 
         * @return
         */
        public Set<FullTableScanIndex> getFullTableScanIndexes()
        {
            Set<FullTableScanIndex> fts = new HashSet<FullTableScanIndex>();
            
            try {
                for (SlotInBIP slot : slots)
                    fts.add(getFullTableScanIndexInstance(slot.table));
            } catch (SQLException e) {            
                e.printStackTrace();
            }
            
            return fts;
        }
        
        /**
         * Retrieve the set of indexes 
         * 
         * @return
         */
        public Set<Index> getIndexes()
        {
            Set<Index> candidates = new HashSet<Index>();
                        
            for (SlotInBIP slot : slots)
                candidates.addAll(slot.indexes); 
            
            return candidates;
        }
        
        /**
         * Check if the plan is dead
         * @return
         */
        public boolean isPlanDead()
        {
            return this.isDead;
        }
        
        /**
         * 
         * @param slotId
         * @return
         */
        public Index getFTSIndexAtSlot(int slotId)
        {
            FullTableScanIndex ftsIdx = null;
            
            try {
                ftsIdx = getFullTableScanIndexInstance(slots.get(slotId).table);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            
            return ftsIdx;
        }
    }
}
