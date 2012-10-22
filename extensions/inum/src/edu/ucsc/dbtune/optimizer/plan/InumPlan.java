package edu.ucsc.dbtune.optimizer.plan;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Double.doubleToLongBits;

import com.google.common.collect.Sets;

import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.DatabaseObject;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.ExplainTables;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.InumUtils;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;
import edu.ucsc.dbtune.util.Tree.Entry;
import edu.ucsc.dbtune.workload.SQLStatement;

import static com.google.common.collect.Iterables.get;
import static edu.ucsc.dbtune.optimizer.plan.Operator.FETCH;
import static edu.ucsc.dbtune.optimizer.plan.Operator.INDEX_SCAN;
import static edu.ucsc.dbtune.optimizer.plan.Operator.TABLE_SCAN;
import static edu.ucsc.dbtune.util.MetadataUtils.getIndexesPerTable;
import static edu.ucsc.dbtune.util.MetadataUtils.getReferencedTables;

/**
 * Extends {@link SQLStatementPlan} class in order to add INUM-specific functionality. Specifically,
 * the capacity of returning the cost of the internal subplan, i.e. the cost of the original plan
 * minus the cost of the leafs. Also, adds the ability of obtaining the cost of a slot by plugging
 * an index into it.
 * <p>
 * For the case where the FTS is being proved, a {@link edu.ucsc.dbtune.metadata.IndexFullTableScan
 * special Index type} is used.
 *
 * @author Ivo Jimenez
 * @author Quoc Trung Tran
 * @see edu.ucsc.dbtune.metadata.IndexFullTableScan
 */
public class InumPlan extends SQLStatementPlan
{
    /** used to represent an incompatible instantiation. */
    protected static final Operator INCOMPATIBLE = new Operator("INUM_INCOMPATIBLE", -1, -1);

    /**
     * Convenience object that maps tables to slots. This is done so that the {@link
     * #getAccessSlots} and {@link #plugIntoSlots} method can run efficiently
     */
    protected Map<Table, TableAccessSlot> slots;

    /**
     * Maps id to slots.
     */
    protected Map<Integer, TableAccessSlot> slotsById;
    
    /**
     * Maps qid in explain table to slots. For example
     * Q2.SS_ITEM_SK
     */
    protected Map<String, Operator> qidToOperator;

    /**
     * The cost of the internal nodes of a plan; the paper refers to it as {@latex.inline
     * $\\beta_k$}.
     */
    protected double internalPlanCost;

    /**
     * We use this optimizer for doing what-if calls to compute index access cost.
     */
    protected Optimizer delegate;

    /**
     * For UPDATE statements, this is the cost, from the internal plan cost, that corresponds to 
     * doing the update.
     */
    protected double baseTableUpdateCost;

    /**
     * For UPDATE statements, this is the table being updated.
     */
    protected Table updatedTable;
  
    /**
     * Index set which built this plan. 
     * (for debugging purpose only)
     */
    public Vector<Index> fromIndexes;
    
    /**
     * Creates a new instance of an INUM plan.
     *
     * @param delegate
     *      to do what-if optimization later on
     * @param explainedStatement
     *      an explained statement produced by an Optimizer implementations (through the {@link
     *      edu.ucsc.dbtune.optimizer.Optimizer#explain} or methods)
     * @throws SQLException
     *      if the plan is referencing a table more than once or if one of the leafs doesn't refer 
     *      any database object.
     */
    public InumPlan(Optimizer delegate, ExplainedSQLStatement explainedStatement)
        throws SQLException
    {
        super(explainedStatement.getPlan());

        this.id = explainedStatement.getPlan().id+"/INUM template";

        this.delegate = delegate;
        this.updatedTable = explainedStatement.getUpdatedTable();
        this.baseTableUpdateCost = explainedStatement.getBaseTableUpdateCost();

        if (!explainedStatement.getPlan().contains(TABLE_SCAN) &&               
                !explainedStatement.getPlan().contains(INDEX_SCAN))
            // we need at least one data-access operator slot
            InumUtils.makeLeafATableScan(
                    this, this.updatedTable, explainedStatement.getSelectCost());

        //CHECKSTYLE:OFF
//        while (InumUtils.removeTemporaryTables(this));
        //CHECKSTYLE:ON

        double leafsCost = replaceLeafsBySlots();

        orgPlan=explainedStatement.getPlan();
//        this.internalPlanCost = explainedStatement.getSelectCost() - leafsCost;
//        calculateCoefficient(this, this.getRootElement(), 1);
        this.internalPlanCost =calculateInternalCost(this.getRootElement());
        this.internalPlanCost-= baseTableUpdateCost;
        qidToOperator=new Hashtable<String, Operator>();
        slots = new HashMap<Table, TableAccessSlot>();
        slotsById=new Hashtable<Integer, TableAccessSlot>();
        for (Operator o :   nodes()) {
            if (o.aliasInExplainTables!=null)
                qidToOperator.put(o.aliasInExplainTables, o);
            if (o.aliasInExplainTables2!=null)
                qidToOperator.put(o.aliasInExplainTables2, o);
            if (o.fetchAliasInExplainTables!=null)
                qidToOperator.put(o.fetchAliasInExplainTables, o);
            if (o instanceof TableAccessSlot) {
                slots.put(((TableAccessSlot) o).getTable(), (TableAccessSlot) o);
                slotsById.put(((TableAccessSlot) o).id, (TableAccessSlot) o);
            }
        }
    }
    public SQLStatementPlan orgPlan;
    /**
     * copy constructor.
     *
     * @param other
     *      plan being copied
     */
    protected InumPlan(InumPlan other)
    {
        super(other);

        this.fromIndexes=other.fromIndexes;
        slots = new HashMap<Table, TableAccessSlot>();
        slotsById=new Hashtable<Integer, TableAccessSlot>();
        qidToOperator=new Hashtable<String, Operator>();

       for (Operator o :   nodes()) {
            if (o.aliasInExplainTables!=null)
                qidToOperator.put(o.aliasInExplainTables, o);
            if (o.aliasInExplainTables2!=null)
                qidToOperator.put(o.aliasInExplainTables2, o);
            if (o.fetchAliasInExplainTables!=null)
                qidToOperator.put(o.fetchAliasInExplainTables, o);
            if (!(o instanceof TableAccessSlot))
                continue;
            if (!(o instanceof TableAccessSlot))
                throw new RuntimeException("Leaf should be a " + TableAccessSlot.class.getName());

            slots.put(((TableAccessSlot) o).getTable(), (TableAccessSlot) o);
            slotsById.put(((TableAccessSlot) o).id, (TableAccessSlot) o);
        }

        internalPlanCost = other.internalPlanCost;
        delegate = other.delegate;
        updatedTable = other.updatedTable;
        baseTableUpdateCost = other.baseTableUpdateCost;
        orgPlan=other.orgPlan;
    }
    
    /**
     * load plan from xml
     * @throws SQLException 
     */
    public InumPlan(Catalog catalog,Rx rx) throws SQLException {
        super(catalog,rx);
        qidToOperator=new Hashtable<String, Operator>();
        slots = new HashMap<Table, TableAccessSlot>();
        slotsById=new Hashtable<Integer, TableAccessSlot>();
        for (Operator o :   nodes()) {
            if (o.aliasInExplainTables!=null)
                qidToOperator.put(o.aliasInExplainTables, o);
            if (o.aliasInExplainTables2!=null)
                qidToOperator.put(o.aliasInExplainTables2, o);
            if (o.fetchAliasInExplainTables!=null)
                qidToOperator.put(o.fetchAliasInExplainTables, o);
            if (o instanceof TableAccessSlot) {
                slots.put(((TableAccessSlot) o).getTable(), (TableAccessSlot) o);
                slotsById.put(((TableAccessSlot) o).id, (TableAccessSlot) o);
            }
        }
    }


    /**
     * save everything to a xml node
     * @param rx
     */
    public void save(Rx rx) {
        rx.createChild("internalPlanCost", internalPlanCost);
        rx.createChild("baseTableUpdateCost", baseTableUpdateCost);
        if (updatedTable!=null)
            rx.createChild("updatedTable", updatedTable.toString());
        super.save(rx);
        Rx s=rx.createChild("slots");
        for (TableAccessSlot slot : slotsById.values()) {
            slot.save(s.createChild("slot"), null);
        }
    }
    
    /**
     * Replaces the leafs of this plan by access slots.
     *
     * @return
     *      the sum of costs of the leafs
     * @throws SQLException
     *      if a leaf is not {@link INDEX_SCAN} or {@link TABLE_SCAN}; if a leaf doesn't refer to an 
     *      object; if one or more leafs can't be assigned with a slot
     */
    private double replaceLeafsBySlots() throws SQLException
    {
        TableAccessSlot slot = null;
        double leafsCost = 0;

        this.slots = new HashMap<Table, TableAccessSlot>();
        int slotCount=0;

        Set<Operator> leafs=  nodes();
        for (Operator leaf : leafs) {
            if (!leaf.getName().equals(INDEX_SCAN) && !leaf.getName().equals(TABLE_SCAN))
//                throw new SQLException("Leaf should be " + INDEX_SCAN + " or " + TABLE_SCAN);
                continue;

            if (leaf.getDatabaseObjects().size() != 1) {
//                Rt.error("Leaf " + leaf + " doesn't refer to any object");
                continue;
            }
            if (leaf.getColumnsFetched() == null || leaf.getColumnsFetched().size() == 0) {
                /**
                └── FETCH(cost=63.33583068847656 rows=0 id=16 object=TPCH.LINEITEM fetch=[+TPCH.LINEITEM.L_QUANTITY(A)] io=8.370734214782715 cpu=280950.875 coeff=0.0)
                     └── INDEX.SCAN(cost=15.14356803894043 rows=30 id=17 object=[+TPCH.LINEITEM.L_PARTKEY(A)] fetch=NONE io=2.0 cpu=124742.6640625 coeff=0.0)
          */
//                continue;
            }

            leafsCost += extractCostOfLeafAndRemoveFetch(this, leaf,leafs);

            slot = new TableAccessSlot(leaf);

            TableAccessSlot prevSlot=slots.get(slot.getTable());

            replaceLeafBySlot(this, leaf, slot);

            if (prevSlot==null)
                slots.put(slot.getTable(), slot);
            else {
                if ( prevSlot.id==slot.id) {
                    throw new Error();
                }
                prevSlot.nextSlotId=slot.id;
            }
            slotCount++;
        }

        if (leafs().size() != slotCount) {
//            Rt.error("leafs: " + leafs().size()+" slots: " + slotCount);
//            throw new SQLException("One or more leafs haven't been assigned with a slot \n" + this);
        }

        return leafsCost;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Index> getIndexes()
    {
        List<Index> indexes = new ArrayList<Index>();

        for (TableAccessSlot s : slots.values())

            if (s.getIndex() instanceof FullTableScanIndex)
                continue;
            else
                indexes.add(s.getIndex());

        return indexes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Table> getTables()
    {
        return new ArrayList<Table>(slots.keySet());
    }
    
    /**
     * Returns the internal cost of the "template plan".
     *
     * @return
     *      the cost of the internal subplan, i.e. the cost of the original plan minus the cost of
     *      the leafs
     */
    public double getInternalCost()
    {
        return internalPlanCost;
    }

    /**
     * Returns the table that the update operates on.
     *
     * @return
     *     the updated base table. If the statement is a {@link SQLCategory#SELECT} statement, the 
     *     value returned is {@code null}.
     */
    public Table getUpdatedTable()
    {
        return updatedTable;
    }

    /**
     * Returns the update cost associated to the cost of updating the base table.
     *
     * @return
     *     the update cost of the base table. If the statement is a {@link SQLCategory#SELECT} 
     *     statement, the value returned is zero.
     */
    public double getBaseTableUpdateCost()
    {
        return baseTableUpdateCost;
    }

    /**
     * The cost for the plan, considering that the given atomic configuration is used in each slot, 
     * as well as the internal cost.
     *
     * @param atomicConfiguration
     *      a configuration where every index corresponds to a different table
     * @return
     *      {@code true} if all the indexes in the given configuration are compatible; {@code false} 
     *      otherwise.
     * @throws SQLException
     *      if two or more indexes reference the same table
     */
    public double plug(Collection<Index> atomicConfiguration) throws SQLException
    {
        SQLStatementPlan plan = instantiate(atomicConfiguration);

        if (plan == null)
            return Double.POSITIVE_INFINITY;

        return plan.getRootOperator().getAccumulatedCost();
    }

    /**
     * The cost for the corresponding {@link TableAccessSlot table slot}, considering that the given 
     * index is used to execute the operator.
     *
     * @param index
     *      used to "instantiate" the corresponding slot
     * @return
     *      cost if the given index is used to execute the corresponding {@link TableSlot table
     *      slot}. {@link Double#POSITIVE_INFINITY} if the index isn't compatible with the slot
     * @throws SQLException
     *      if the plan doesn't contain a slot for {@code index.getTable()}
     */
    public double plug(TableAccessSlot slot,Index index) throws SQLException
    {
        Operator o = instantiate2(slot,index);
        if (o== INCOMPATIBLE)
            return Double.POSITIVE_INFINITY;
        return o.getAccumulatedCost()*o.coefficient;
    }
    
    /**
     * return coefficient of a slot
     * @param slot
     * @return
     * @throws SQLException
     */
    public double getCoefficient(TableAccessSlot slot) throws SQLException
    {
        return slot.coefficient;
    }
    
    /**
     * The cost for the corresponding {@link TableAccessSlot table slot}, considering that the given 
     * index is used to execute the operator.
     *
     * @param index
     *      used to "instantiate" the corresponding slot
     * @return
     *      cost if the given index is used to execute the corresponding {@link TableSlot table
     *      slot}. {@link Double#POSITIVE_INFINITY} if the index isn't compatible with the slot
     * @throws SQLException
     *      if the plan doesn't contain a slot for {@code index.getTable()}
     */
    public double plug(Index index) throws SQLException
    {
        Operator[] os = instantiate(index);

        if (os.length==0)
            return Double.POSITIVE_INFINITY;
        double sum=0;
        for (Operator o : os) {
        	sum+=o.getAccumulatedCost();
        }
        return sum;
    }

    /**
     * Instantiates a slot by plugging the given index in the corresponding slot.
     *
     * @param index
     *      index being plugged
     * @throws SQLException
     *      if the plan doesn't contain a slot for {@code index.getTable()}
     * @return
     *      a new operator that results from plugging the index in the corresponding slot. The 
     *      operator can be a {@link Operator#TABLE_SCAN} or {@link Operator#INDEX_SCAN}, depending 
     *      on what is the interesting order that was used to build this slot. Specifically, {@link 
     *      Operator#TABLE_SCAN} is returned when the given index is an instance of {@link 
     *      FullTableScanIndex} and the slot was also built with it. Otherwise, the index is checked 
     *      to see if it's compatible with the slot and if isn't, {@link INCOMPATIBLE} is returned. 
     */
    public Operator instantiate2(TableAccessSlot slot,Index index) throws SQLException
    {
        if (slot.getIndex().equals(index)
                || slot.getIndex().equalsContent(index)) {
            // if we have the same index as when we built the template
            Operator o = makeOperatorFromSlot(slot);
            if (o.getDatabaseObjects().size() == 0)
                throw new Error();
            o.slot = slot;
            return o;
        } else if (!slot.isCreatedFromFullTableScan()
                && !slot.isCompatible(index))
            // if we DON'T have FTS, we have to check that sent index is
            // compatible
            ;

        else if (isWhatIfCallAvoidableViaFullTableScan(slot, index)) {
            // if we do a what-if call, we know the optimizer will return
            // FTS, so let's not do it
            Operator o = makeOperatorFromSlot(slot);
            if (o.getDatabaseObjects().size() == 0)
                throw new Error();
            o.slot = slot;
            return o;
        } else {
            Operator o = instantiate(slot, index);
            if (o != INCOMPATIBLE) {
                if (o.getDatabaseObjects().size() == 0)
                    throw new Error();
                o.slot = slot;
                return o;
            } else {
            }
        }
        return INCOMPATIBLE;
    }
    /**
     * Instantiates a slot by plugging the given index in the corresponding slot.
     *
     * @param index
     *      index being plugged
     * @throws SQLException
     *      if the plan doesn't contain a slot for {@code index.getTable()}
     * @return
     *      a new operator that results from plugging the index in the corresponding slot. The 
     *      operator can be a {@link Operator#TABLE_SCAN} or {@link Operator#INDEX_SCAN}, depending 
     *      on what is the interesting order that was used to build this slot. Specifically, {@link 
     *      Operator#TABLE_SCAN} is returned when the given index is an instance of {@link 
     *      FullTableScanIndex} and the slot was also built with it. Otherwise, the index is checked 
     *      to see if it's compatible with the slot and if isn't, {@link INCOMPATIBLE} is returned. 
     */
    public Operator[] instantiate(Index index) throws SQLException
    {
        TableAccessSlot slot = slots.get(index.getTable());

        if (slot == null)
            throw new SQLException("Plan doesn't contain a slot for table " + index.getTable());

		Vector<Operator> operators = new Vector<Operator>();
		for (; slot != null;) {
		    Operator o = instantiate2(slot, index);
            if (o != INCOMPATIBLE)
                operators.add(o);
			if (slot.nextSlotId<0)
			    break;
			slot= slotsById.get(slot.nextSlotId);
		}
        return operators.toArray(new Operator[operators.size()]);
    }

    /**
     * Instantiates a plan by plugging each given index in the corresponding slot.
     *
     * @param atomicConfiguration
     *      configuration used to instantiate the template
     * @throws SQLException
     *      if the plan doesn't contain a slot for one of the tables
     * @return
     *      a new plan that results from plugging the index in the corresponding slot; {@code null} 
     *      if the configuration is not compatible with the template
     */
    public SQLStatementPlan instantiate(Collection<Index> atomicConfiguration)
        throws SQLException
    {
        Set<Table> visited = new HashSet<Table>();
        List<Operator> operators = new ArrayList<Operator>();

        Map<Table, Set<Index>> indexesPerTable = getIndexesPerTable(atomicConfiguration);
        Collection<TableAccessSlot> slots=getSlots();
        for (TableAccessSlot slot : slots) {
            Set<Index> indexesForTable = indexesPerTable.get(slot.getTable());

            Index bestIndexForSlot = null;
            double bestCostForIndex = Double.POSITIVE_INFINITY;

            for (Index i : indexesForTable) {
                double costForIndex = plug(slot,i);
                if (costForIndex < bestCostForIndex) {
                    bestCostForIndex = costForIndex;
                    bestIndexForSlot = i;
                }
            }
            
            if (bestIndexForSlot==null) {
                if (slot.getIndex() instanceof FullTableScanIndex) {
                    continue;
                } else {
                    return null;
                }
            }
            Operator o = instantiate2(slot, bestIndexForSlot);
            if (o== INCOMPATIBLE)
                return null;
            operators.add(o);
        }

//        if (visited.size() != getSlots().size())
//            throw new SQLException(
//                "One or more tables missing in atomic configuration.\n" +
//                "  Tables in atomic " + getReferencedTables(atomicConfiguration) + "\n" +
//                "  Tables in stmt: " + getTables() + "\n" +
//                "  Plan: \n" + this);

        return instantiate(this, operators);
    }

    /**
     * Returns the slot corresponding to the given table.
     *
     * @param table
     *      the given table
     * @return
     *      a {@code TableAccessSlot} object or {@code null} if the table is not referenced by the
     *      statement
     */
    public TableAccessSlot getSlot(Table table)
    {
        return slots.get(table);
    }

    /**
     * Returns the slots of the template plan.
     *
     * @return
     *      a set of {@code TableAccessSlot} objects
     */
    public Collection<TableAccessSlot> getSlots()
    {
        return new ArrayList<TableAccessSlot>(slotsById.values());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return 37 * super.hashCode() + (int) doubleToLongBits(internalPlanCost);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj)
    {
        if (!super.equals(obj))
            return false;

        if (!(obj instanceof InumPlan))
            return false;
    
        InumPlan o = (InumPlan) obj;

        return Double.compare(internalPlanCost, o.internalPlanCost) == 0;
    }

    /**
     * Executes a what-if call on the given index in order to determine if the corresponding slot 
     * would use it.
     *
     * @param slot
     *      slot for which the operator is being instantiated
     * @param index
     *      the index being sent as the hypothetical configuration
     * @return
     *      an {@link Operator#INDEX_SCAN} operator; {@link INCOMPATIBLE} if the given index is not 
     *      used in the what-if call.
     * @throws SQLException
     *      if the statement can't be explained
     * @see #buildQueryForUnseenIndex
    */
    protected Operator instantiate(TableAccessSlot slot, Index index)
        throws SQLException
    {
        SQLStatement sql=buildQueryForUnseenIndex(slot,this);
        slot.costSQL=sql.getSQL();
        SQLStatementPlan plan;
        try {
            plan = delegate.explain(
                    sql, Sets.<Index>newHashSet(index)).getPlan();
        } catch (SQLException e) {
            Rt.p(this);
           throw e;
        }

        if (!plan.contains(INDEX_SCAN))
            return INCOMPATIBLE;
        
        Operator indexScan=null;
        for (Operator operator : plan.nodes()) {
            if (INDEX_SCAN.equals(operator.getName())) {
                indexScan= operator;
                break;
            }
        }
        if (indexScan==null)
            throw new Error("Can't find operator");
        
        HashSet<String> neededColumns=new HashSet<String>();
        HashSet<String> hasColumns=new HashSet<String>();
        for (String c : slot.rawColumnNames.split("\\+")) {
            int t=c.lastIndexOf('.');
            if (t>0)
                c=c.substring(t+1);
            if (c.length()>0&&!c.startsWith("$RID$")&&!c.startsWith("$C"))
                neededColumns.add(c);
        }
        while (indexScan != null) {
            if (indexScan.rawColumnNames != null) {
                hasColumns.clear();
                for (String c : indexScan.rawColumnNames.split("\\+")) {
                    int t = c.lastIndexOf('.');
                    if (t > 0)
                        c = c.substring(t + 1);
                    if (neededColumns.contains(c))
                        hasColumns.add(c);
                    t = c.indexOf('(');
                    if (t > 0) {
                        c = c.substring(0, t);
                        if (neededColumns.contains(c))
                            hasColumns.add(c);
                    }
                }
                if (hasColumns.size() == neededColumns.size())
                    break;
            }
            indexScan = plan.getParent(indexScan);
        }
        
        double cost;
        double cardinality;
        
        if ( indexScan!=null) {
            cost=indexScan.getAccumulatedCost();
            cardinality= indexScan.cardinality;
        } else {
            Rt.p(this);
            Rt.p(plan);
            Rt.p(slot.rawColumnNames);
            Rt.p("need " + neededColumns.size());
            Rt.p(neededColumns);
            cost=plan.getRootOperator().getAccumulatedCost();
            cardinality=plan.getRootOperator().cardinality;
//            throw new Error();
        }
        
/**
RETURN(cost=1470.4276123046875 rows=0 id=1 object=NONE alias= rawColumns=null rawPredicate=[] coeff=0.0)
    └── NESTED.LOOP.JOIN(cost=1470.4276123046875 rows=2874 id=2 object=NONE alias= rawColumns=+Q3.SS_ITEM_SK(A)+Q3.SS_EXT_SALES_PRICE+Q3.SS_SOLD_DATE_SK rawPredicate=[(Q2.SS_ITEM_SK = Q1.I_ITEM_SK)] coeff=0.0)
        ├── TABLE.SCAN(cost=1197.7904052734375 rows=17 id=3 object=NONE alias= rawColumns=+Q1.I_ITEM_SK(A) rawPredicate=[] coeff=0.0)
        │   └── SORT(cost=1197.7901611328125 rows=17 id=4 object=NONE alias= rawColumns=+Q1.I_ITEM_SK(A) rawPredicate=[] coeff=0.0)
        │       └── TABLE.SCAN(cost=1197.7886962890625 rows=17 id=5 object=TPCDS.ITEM alias=Q1 rawColumns=+Q1.I_ITEM_SK rawPredicate=[(Q1.I_MANUFACT_ID = 741)] coeff=0.0)
        └── INDEX.SCAN(cost=15.186594009399414 rows=160 id=6 object=[+TPCDS.STORE_SALES.SS_ITEM_SK(A)+TPCDS.STORE_SALES.SS_EXT_SALES_PRICE(A)+TPCDS.STORE_SALES.SS_SOLD_DATE_SK(A)] alias=Q2 rawColumns=+Q2.SS_ITEM_SK(A)+Q2.SS_EXT_SALES_PRICE(A)+Q2.SS_SOLD_DATE_SK(A) rawPredicate=[(Q2.SS_ITEM_SK = Q1.I_ITEM_SK), (Q2.SS_ITEM_SK = Q1.I_ITEM_SK)] coeff=0.0)
 */
        Operator op = makeOperatorFromSlot(slot);
        op.setName(INDEX_SCAN);
        op.setAccumulatedCost(cost);
        op.scanCost=cost;
        op.joinInput=slot.joinInput;
        op.cardinality=cardinality;
        op.removeDatabaseObject();
        op.add(index);
        
        return op;
    }

    /**
     * Determines whether a what-if call can be avoided, based on the content of the slot and the 
     * index being sent.
     *
     * @param slot
     *      slot
     * @param index
     *      the index that is checked against the slot
     * @return
     *      {@code true} if no what-if call is needed; {@code false} otherwise
     */
    static boolean isWhatIfCallAvoidableViaFullTableScan(TableAccessSlot slot, Index index)
    {
        if (!slot.isCreatedFromFullTableScan())
            return false;

        if (slot.getPredicates().isEmpty() &&                 
                slot.getColumnsFetched().isCoveredByIgnoreOrder(index))
            return false;

        for (Predicate p : slot.getPredicates())
            if (p.isCoveredBy(index))
                return false;

        return true;
    }

    /**
     * Instantiates an operator out of a slot.
     *
     * @param slot
     *      slot
     * @return
     *      a {@link Operator#TABLE_SCAN} operator if the slot is using a {@link 
     *      FullTableScanIndex}; a {@link Operator#INDEX_SCAN} if the slot uses an {@link Index}.
     */
    static Operator makeOperatorFromSlot(TableAccessSlot slot)
    {
        DatabaseObject dbo;
        String name;
        
        if (slot.isCreatedFromFullTableScan()) {
            name = TABLE_SCAN;
            dbo = slot.getTable();
        } else {
            name = INDEX_SCAN;
            dbo = slot.getIndex();
        }

        Operator op = new Operator(name, slot.getAccumulatedCost(), slot.getCardinality());
        op.scanCost=slot.accumulatedCost;
        op.add(dbo);
        op.addColumnsFetched(slot.getColumnsFetched());
        op.add(slot.getPredicates());
        op.aliasInExplainTables=slot.aliasInExplainTables;
        op.aliasInExplainTables2=slot.aliasInExplainTables2;
        op.fetchAliasInExplainTables=slot.fetchAliasInExplainTables;
        op.rawColumnNames=slot.rawColumnNames;
        op.rawPredicateList=slot.rawPredicateList;
        op.cardinalityNLJ=slot.cardinalityNLJ;
        op.coefficient=slot.coefficient;
        op.joinInput=slot.joinInput;

        return op;
    }

    /**
     * Builds a query for an unseen index. An unseen index is one that wasn't part of the
     * interesting order enumeration that was used to build a template plan, thus the cost of using
     * it can't be inferred from the template and has to be estimated through the DBMS' optimizer.
     * <p>
     * This method assumes that the index is compatible with the slot, i.e. that {@code
     * slot.getTable == index.getTable()} is {@code true}.
     *
     * @param slot
     *      slot that corresponds to the index
     * @return
     *      a SQL statement that can be used to retrieve the cost of using the index at the given
     *      slot
     * @throws SQLException
     *      if no columns fetched for the corresponding table
     */
    static SQLStatement buildQueryForUnseenIndex(TableAccessSlot slot)
        throws SQLException {
        return buildQueryForUnseenIndex(slot,null);
    }
    /**
     * Builds a query for an unseen index. An unseen index is one that wasn't part of the
     * interesting order enumeration that was used to build a template plan, thus the cost of using
     * it can't be inferred from the template and has to be estimated through the DBMS' optimizer.
     * <p>
     * This method assumes that the index is compatible with the slot, i.e. that {@code
     * slot.getTable == index.getTable()} is {@code true}.
     *
     * @param slot
     *      slot that corresponds to the index
     * @param plan
     *      plan that the slot belong to
     * @return
     *      a SQL statement that can be used to retrieve the cost of using the index at the given
     *      slot
     * @throws SQLException
     *      if no columns fetched for the corresponding table
     */
    static SQLStatement buildQueryForUnseenIndex(TableAccessSlot slot, InumPlan plan)
        throws SQLException
    {
        StringBuilder sb = new StringBuilder();

        if (slot.getColumnsFetched().size() == 0)
            throw new SQLException("No columns fetched; something must be wrong");

        // We build a query like:
        //
        //   SELECT
        //       slot.getColumnsFetched
        //   FROM
        //       slot.getTable
        //   WHERE
        //       slot.getPredicates
        //   ORDER BY
        //       slot.getIndex

        sb.append("SELECT ");

        for (Column c : slot.getColumnsFetched().columns())
            sb.append(c.getName()).append(", ");

        sb.delete(sb.length() - 2, sb.length() - 1);

        if (plan == null) {
            sb.append(" FROM ").append(slot.getTable().getFullyQualifiedName());
            if (slot.getPredicates().size() > 0) {

                sb.append(" WHERE ");

                for (Predicate p : slot.getPredicates())
                    sb.append(p.getText()).append(" AND ");

                sb.delete(sb.length() - 5, sb.length() - 1);
            }
        } else {
            HashSet<String> tableNames = new HashSet<String>();
            tableNames.add(slot.getTable().getFullyQualifiedName());
            StringBuilder where = new StringBuilder();
            HashSet<String> hash = new HashSet<String>();
            if (slot.rawPredicateList != null
                    && slot.rawPredicateList.size() > 0) {
                hash.addAll(slot.rawPredicateList);
            }
            if (hash.size()>0) {
                HashSet<Operator> processed = new HashSet<Operator>();
                Pattern tableReferencePattern = Pattern.compile("Q\\d+\\.");
                LinkedList<String> queue = new LinkedList<String>();
                queue.addAll(hash);
                processed.add(slot);
                nextPredicate: while (queue.size() > 0) {
                    String predicate = queue.remove().trim();
                    if (predicate.length() == 0)
                        continue;
                    if (predicate.indexOf("$C")>=0)
                        continue;
                    // remove table alias
                    predicate= predicate.replaceAll(" AS Q\\d+ ", " ");
                    predicate= predicate.replaceAll("SELECTIVITY \\d+\\.\\d+", " ");
                    // (Q1.I_MANUFACT_ID = 741)
                    // (Q2.SS_ITEM_SK = Q1.I_ITEM_SK)
                    while (true) {
                        Matcher matcher = tableReferencePattern
                                .matcher(predicate);
                        if (matcher.find()) {
                            String name = predicate.substring(matcher.start(),
                                    matcher.end() - 1);
                            Operator operator = plan.qidToOperator.get(name);
                            if (operator == null) {
                                // Rt.error("Can't find "+name+". It might be a FETCH which is removed");
                                continue nextPredicate;
                            }
                            if (Operator.TEMPORARY_TABLE_SCAN.equals(operator
                                    .getName())) {
                                // Rt
                                // .error("Don't know the content of sysibm.genrow yet. Abandon "
                                // + predicate
                                // + ", cost may be huge.");
                                continue nextPredicate;
                            }
                            Table table = null;
                            if (operator instanceof TableAccessSlot) {
                                TableAccessSlot op = (TableAccessSlot) operator;
                                table = op.getTable();
                            } else {
                                List<DatabaseObject> objects = operator
                                        .getDatabaseObjects();
                                if (objects.size() != 1)
                                    throw new SQLException(
                                            "Leaf should contain one object");
                                DatabaseObject object = objects.get(0);
                                if (object instanceof Table)
                                    table = (Table) object;
                                else if (object instanceof Index)
                                    table = ((Index) object).getTable();
                                else
                                    throw new Error();
                            }
                            predicate = predicate
                                    .replaceAll(Pattern.quote(name), table
                                            .getFullyQualifiedName());
                            if (!processed.contains(operator)) {
                                processed.add(operator);
                                tableNames
                                        .add(table.getFullyQualifiedName());
                                hash.clear();
                                if (operator.rawPredicateList!=null)
                                    hash.addAll(operator.rawPredicateList);
                                queue.addAll(hash);
                            }
                        } else
                            break;
                    }
                    if (where.length() > 0)
                        where.append(" AND ");
                    where.append(predicate);
                }
            }
            sb.append(" FROM ");
            boolean first=true;
            for (String tableName : tableNames) {
                if (first)
                    first=false;
                else
                    sb.append(",");
                sb.append(tableName);
            }
            if (where.length() > 0) {
                sb.append(" WHERE ");
                sb.append(where);
            }
        }

        if (!slot.isCreatedFromFullTableScan()) {

            sb.append(" ORDER BY ");

            for (Column c : slot.getIndex().columns())
                sb.append(c.getName())
                    .append(slot.getIndex().isAscending(c) ? " ASC, " : " DESC, ");

            sb.delete(sb.length() - 2, sb.length() - 1);
        }

        return new SQLStatement(sb.toString());
    }

    /**
     * Returns the cost of the given leaf, taking into account that if it's an {@link 
     * Operator#INDEX_SCAN}, a {@link Operator#FETCH} operator is looked for in all the ascendants 
     * of it and, if found, the cost of the {@link Operator#FETCH} operator is returned. If a {@link 
     * Operator#FETCH} operator is found and it's a parent of the {@link Operator#INDEX_SCAN}, it is 
     * removed an replaced by the index scan. For example: TODO
     *
     * @param sqlPlan
     *      plan which the leaf is contained in
     * @param leaf
     *      operator at the base of the plan
     * @return
     *      cost of the leaf
     * @throws SQLException
     *      if {@link Operator#FETCH} is found but it has no parent; if {@link Operator#FETCH} has 
     *      siblings; if an error occurs while pulling down the set of columns fetched from {@link
     *      Operator#FETCH} down to the leaf.
     */
    static double extractCostOfLeafAndRemoveFetch(SQLStatementPlan sqlPlan, Operator leaf,Set<Operator> leafs)
        throws SQLException
    {
//        if (!sqlPlan.getChildren(leaf).isEmpty()) {
//            throw new SQLException("Operator is not a leaf: " + leaf);
//            return 0; //TODO return internal cost of this node
//        }

        if (leaf.getName().equals(TABLE_SCAN))
            return leaf.getAccumulatedCost();

        Operator fetch = sqlPlan.findAncestorWithName(leaf, FETCH);

        if (fetch == null)
            // no FETCH found
            return leaf.getAccumulatedCost();
/*
            │   │   ├── FETCH(id=6 OUTER cost=57206.05 card=5315.52 coeff=1.0 object=TPCDS.STORE_SALES rawColumns=+Q4.SS_STORE_SK+Q4.SS_HDEMO_SK+Q4.SS_SOLD_TIME_SK+Q4.SS_STORE_SK+Q4.SS_HDEMO_SK+Q4.SS_SOLD_TIME_SK rawPredicate=null fetch=[+TPCDS.STORE_SALES.SS_STORE_SK(A)+TPCDS.STORE_SALES.SS_HDEMO_SK(A)+TPCDS.STORE_SALES.SS_SOLD_TIME_SK(A)] internal=25380.52)
            │   │   │   └── RID.SCAN(id=7 cost=31825.53 rows=5316 card=5315.52 coeff=1.0 object=NONE rawColumns=+Q4.$RID$(A) rawPredicate=null fetch=NONE internal=0.00)
            │   │   │       └── SORT(id=8 cost=31825.53 rows=5316 rowWidth=20 card=5315.52 coeff=1.0 object=NONE rawColumns=+Q4.$RID$(A) rawPredicate=null fetch=NONE internal=2.07)
            │   │   │           └── INDEX.AND(id=9 cost=31823.46 card=5315.52 coeff=1.0 object=NONE rawColumns=+Q4.$RID$ rawPredicate=null fetch=NONE internal=946.49)
            │   │   │               ├── NESTED.LOOP.JOIN(id=10 cost=28521.87 card=529263.75 coeff=1.0 object=NONE rawColumns=null rawPredicate=[(Q4.SS_SOLD_TIME_SK = Q2.T_TIME_SK)] fetch=NONE internal=0.00)
            │   │   │               │   ├── TABLE.SCAN(id=11 OUTER cost=1282.83 card=1800.00/1798.99 coeff=1.0 object=TPCDS.TIME_DIM rawColumns=+Q2.T_TIME_SK+Q2.$RID$+Q2.T_MINUTE+Q2.T_HOUR+Q2.T_TIME_SK rawPredicate=[(30 <= Q2.T_MINUTE), (Q2.T_HOUR = 8)] fetch=[+TPCDS.TIME_DIM.T_MINUTE(A)+TPCDS.TIME_DIM.T_HOUR(A)+TPCDS.TIME_DIM.T_TIME_SK(A)] internal=1282.83)
            │   │   │               │   └── INDEX.SCAN(id=12 INNER cost=15.14 card=294.04 coeff=1799.0 object=[+TPCDS.STORE_SALES.SS_SOLD_TIME_SK(A)] rawColumns=+Q4.SS_SOLD_TIME_SK(A)+Q4.SS_SOLD_TIME_SK(A)+Q4.$RID$ rawPredicate=[(Q4.SS_SOLD_TIME_SK = Q2.T_TIME_SK), (Q4.SS_SOLD_TIME_SK = Q2.T_TIME_SK)] fetch=[+TPCDS.STORE_SALES.SS_SOLD_TIME_SK(D)] internal=15.14)
            │   │   │               ├── NESTED.LOOP.JOIN(id=13 cost=1714.57 card=2540465.75 coeff=1.0 object=NONE rawColumns=null rawPredicate=[(Q4.SS_STORE_SK = Q1.S_STORE_SK)] fetch=NONE internal=0.00)
            │   │   │               │   ├── TABLE.SCAN(id=14 OUTER cost=45.42 card=10.20/1.27 coeff=1.0 object=TPCDS.STORE rawColumns=+Q1.S_STORE_SK+Q1.$RID$+Q1.S_STORE_NAME+Q1.S_STORE_SK rawPredicate=[(Q1.S_STORE_NAME = 'ese')] fetch=[+TPCDS.STORE.S_STORE_NAME(A)+TPCDS.STORE.S_STORE_SK(A)] internal=45.42)
            │   │   │               │   └── INDEX.SCAN(id=15 INNER cost=1318.26 card=249065.28 coeff=1.3 object=[+TPCDS.STORE_SALES.SS_STORE_SK(A)] rawColumns=+Q4.SS_STORE_SK(A)+Q4.SS_STORE_SK(A)+Q4.$RID$ rawPredicate=[(Q4.SS_STORE_SK = Q1.S_STORE_SK), (Q4.SS_STORE_SK = Q1.S_STORE_SK)] fetch=[+TPCDS.STORE_SALES.SS_STORE_SK(D)] internal=1318.26)
            │   │   │               └── NESTED.LOOP.JOIN(id=16 cost=640.54 card=2551451.25 coeff=1.0 object=NONE rawColumns=null rawPredicate=[(Q4.SS_HDEMO_SK = Q3.HD_DEMO_SK)] fetch=NONE internal=0.00)
            │   │   │                   ├── TABLE.SCAN(id=17 OUTER cost=50.47 card=720.00/22.50 coeff=1.0 object=TPCDS.HOUSEHOLD_DEMOGRAPHICS rawColumns=+Q3.HD_DEMO_SK+Q3.$RID$+Q3.HD_DEP_COUNT+Q3.HD_DEMO_SK rawPredicate=[(Q3.HD_DEP_COUNT = 2)] fetch=[+TPCDS.HOUSEHOLD_DEMOGRAPHICS.HD_DEP_COUNT(A)+TPCDS.HOUSEHOLD_DEMOGRAPHICS.HD_DEMO_SK(A)] internal=50.47)
            │   │   │                   └── INDEX.SCAN(id=18 INNER cost=26.23 card=3543.68 coeff=22.5 object=[+TPCDS.STORE_SALES.SS_HDEMO_SK(A)] rawColumns=+Q4.SS_HDEMO_SK(A)+Q4.SS_HDEMO_SK(A)+Q4.$RID$ rawPredicate=[(Q4.SS_HDEMO_SK = Q3.HD_DEMO_SK), (Q4.SS_HDEMO_SK = Q3.HD_DEMO_SK)] fetch=[+TPCDS.STORE_SALES.SS_HDEMO_SK(D)+TPCDS.STORE_SALES.SS_STORE_SK(A)+TPCDS.STORE_SALES.SS_SOLD_TIME_SK(A)] internal=26.23)
 */
        
        int numOfLeaves=countLeaves(sqlPlan,fetch,leafs);
        if ( numOfLeaves>1)
            return leaf.getAccumulatedCost();
        Index index = (Index) leaf.getDatabaseObjects().get(0);

        if (!fetch.getDatabaseObjects().get(0).equals(index.getTable()))
            // FETCH is referring to a distinct table
            return leaf.getAccumulatedCost();

        if (fetch.getColumnsFetched() == null || fetch.getColumnsFetched().size() == 0)
            throw new SQLException("Column set in FETCH is empty or null: " + fetch);

        double costOfLeafSiblings = 0;

        List<Operator> fetchChildern=sqlPlan.getChildren(fetch);
        int numOfChildrenOfFetch=fetchChildern.size();
        if (numOfChildrenOfFetch > 1) {
            // obtain the cost of children of FETCH that are distinct to leaf and remove them
            for (Operator child : fetchChildern) {
                if (!child.equals(leaf)) {
                    costOfLeafSiblings += child.getAccumulatedCost();
                    sqlPlan.remove(child);
                }
            }
//        } else if (numOfChildrenOfFetch == 1) {
//            Operator child=sqlPlan.getChildren(fetch).get(0);
//            if (sqlPlan.getChildren(child).size()>1) {
//                if ("RID.SCAN".equals(child.getName())) {
//                    return leaf.getAccumulatedCost();
//                }
//            }
        }

        Operator fetchParent = sqlPlan.getParent(fetch);

        if (fetchParent == null)
            throw new SQLException(FETCH + " should have an ancestor in " + sqlPlan);

        sqlPlan.remove(leaf);

        leaf.setAccumulatedCost(fetch.getAccumulatedCost() - costOfLeafSiblings);
        leaf.add(fetch.getPredicates());
        if (fetch.rawPredicateList!=null)
            leaf.rawPredicateList.addAll(fetch.rawPredicateList);
        if (fetch.rawColumnNames!=null) {
            if (leaf.rawColumnNames!=null) {
                leaf.rawColumnNames+=fetch.rawColumnNames;
            } else {
                leaf.rawColumnNames=fetch.rawColumnNames;
            }
        }
        leaf.fetchAliasInExplainTables=fetch.aliasInExplainTables;
        leaf.cardinalityNLJ=fetch.cardinalityNLJ;

        // we also have to pull the columns fetched down to the slot
        for (Column c : fetch.getColumnsFetched().columns())
            if (leaf.getColumnsFetched() == null)
                leaf.addColumnsFetched(
                        new InterestingOrder(c, fetch.getColumnsFetched().isAscending(c)));
            else if (!leaf.getColumnsFetched().contains(c))
                leaf.getColumnsFetched().add(c, fetch.getColumnsFetched().isAscending(c));

        sqlPlan.remove(fetch);
        sqlPlan.setChild(fetchParent, leaf);

        return leaf.getAccumulatedCost();
    }
    
    static int countLeaves(SQLStatementPlan sqlPlan,Operator fetch,Set<Operator> leafs) {
        List<Operator> fetchChildern=sqlPlan.getChildren(fetch);
        int count=0;
        for (Operator op : fetchChildern) {
            List<Operator> opChildern=sqlPlan.getChildren(op);
            if (op.getName().equals(INDEX_SCAN) || op.getName().equals(TABLE_SCAN)|| op instanceof TableAccessSlot)
                count++;
            if (opChildern.size()>0)
                 count+=countLeaves(sqlPlan, op, leafs);
        }
        return count;
    }

    /**
     * Replaces the leafs in the plan by slots.
     *
     * @param sqlPlan
     *      plan from which the given leaf will be removed and replaced by the slot
     * @param leaf
     *      leaf to be removed
     * @param slot
     *      slot to be inserted instead of the leaf
     * @throws SQLException
     *      if the leaf turns out to be the root operator
     */
    static void replaceLeafBySlot(
            SQLStatementPlan sqlPlan, Operator leaf, TableAccessSlot slot)
        throws SQLException
    {
        Operator parent = sqlPlan.getParent(leaf);

        if (parent == null)
            throw new SQLException("Leaf " + leaf + " shouldn't be the root operator");

        List<Operator> childern=sqlPlan.getChildren(leaf);
        if (childern!=null) {
            //Some plan has internal node as table scan
            SQLStatementPlan plan2=new SQLStatementPlan(sqlPlan);
            int index=sqlPlan.remove(leaf);
            sqlPlan.setChild(parent, slot,index);
            copySubTree(sqlPlan,plan2,slot,leaf);
        } else {
            int index=sqlPlan.remove(leaf);
            sqlPlan.setChild(parent, slot,index);
        }
    }
    
    static void copySubTree(SQLStatementPlan p1, SQLStatementPlan p2,
            Operator o1,
            Operator o2) {
        List<Operator> childern=p2.getChildren(o2);
        for (Operator operator : childern) {
//            Rt.p(o1.id+" "+ operator.id);
            p1.setChild(o1, operator);
            copySubTree(p1,p2,operator,operator);
        }
    }

    /**
     * Replaces the slots at the leafs by the actual operators used in the instantiated plan.
     *
     * @param templatePlan
     *      template plan being instantiated
     * @param instantiatedOperators
     *      a list containing operators obtained from ({@link #instantiate(Index)})
     * @return
     *      an instance of a plan based on the given template, where each leaf is an {@link 
     *      Operator#TABLE_SCAN} or a {@link Operator#INDEX_SCAN}
     */
    static SQLStatementPlan instantiate(
            InumPlan templatePlan,
            List<Operator> instantiatedOperators)
    {
        SQLStatementPlan plan = new SQLStatementPlan(templatePlan);
        plan.id = templatePlan.id+"/instantiated";
        Operator parent;
        TableAccessSlot slot;
        double cost = 0;
//        Rt.p(templatePlan.id);
        for (Operator newLeaf : instantiatedOperators) {
            DatabaseObject dbo = newLeaf.getDatabaseObjects().get(0);

            if (dbo instanceof Table || dbo instanceof FullTableScanIndex)
                slot = templatePlan.getSlot((Table) dbo);
            else
                slot = templatePlan.getSlot(((Index) dbo).getTable());
            
            if (newLeaf.slot!=null)
            	slot= newLeaf.slot;
            else
            	throw new Error();

            newLeaf.id=slot.id;
            parent = templatePlan.getParent(slot);
            Entry<Operator> entry=plan.elements.get(slot);
            if (entry==null) {
                if (ExplainTables.showWarnings)
                    Rt.error("Can't find slot "+slot.id);
                continue;
            }
            List<Operator> childern=plan.getChildren(slot);
            if (childern!=null) {
                //Some plan has internal node as table scan
                SQLStatementPlan plan2=new SQLStatementPlan(plan);
                int index=plan.remove(slot);
                plan.setChild(parent, newLeaf,index);
                copySubTree(plan,plan2,newLeaf,slot);
            } else {
                int index=plan.remove(slot);
                plan.setChild(parent, newLeaf,index);
            }

//            Rt.p(newLeaf);
//            Rt.p(newLeaf.scanCost*newLeaf.coefficient+" "+cost);
            cost += newLeaf.scanCost*newLeaf.coefficient;//.getAccumulatedCost();
        }

        cost += templatePlan.getInternalCost();

        calculateAccumulatedCost(plan,plan.getRootElement());
        plan.assignCost(plan.getRootElement(), cost + templatePlan.getBaseTableUpdateCost());
         
        return plan;
    }

    /**
     * Calculate internal cost of a plan
     * @param node
     * @return
     */
    private double calculateInternalCost(Operator node) {
        double cost=0;
        if (!(node instanceof TableAccessSlot))
            cost+=node.internalCost*node.coefficient;
        List<Operator> children=getChildren(node);
        for (Operator operator : children) {
            cost+= calculateInternalCost(operator);
        }
        return cost;
    }

    /**
     * Update accumulated cost of each node in a plan recursively
     * @param plan
     * @param node
     */
    private static void calculateAccumulatedCostOriginal(SQLStatementPlan plan,Operator node) {
        List<Operator> children=plan.getChildren(node);
        if (children.size()==0)
            return;
        for (Operator operator : children) {
            calculateAccumulatedCostOriginal(plan,operator);
        }
        if (node.getName().equals(Operator.NLJ)) {
            Operator left=children.get(0);
            Operator right=children.get(1);
            double rows=left.cardinalityNLJ;
            if (rows < 1)
                rows = 1;
            double cost1=left.accumulatedCost+rows*right.accumulatedCost;
            node.accumulatedCost=node.internalCost+cost1;
        } else {
            node.accumulatedCost=node.internalCost+node.scanCost;
            for (Operator operator : children) {
                node.accumulatedCost+=operator.accumulatedCost;
            }
        }
    }
    
    /**
     * Update accumulated cost of each node in a plan using coefficient
     * @param plan
     * @param node
     */
    private static void calculateAccumulatedCost(SQLStatementPlan plan,
            Operator node) {
        List<Operator> children = plan.getChildren(node);
        if (children.size() == 0)
            return;
        node.accumulatedCost = node.internalCost + node.scanCost;
        for (Operator operator : children) {
            calculateAccumulatedCost(plan, operator);
            node.accumulatedCost += operator.accumulatedCost
                    * operator.coefficient / node.coefficient;
        }
    }
    /**
     * Calculate coefficient of each node in a plan
     * (Not used, moved to DB2Optimizer
     * @param plan
     * @param node
     */
    private static void calculateCoefficient(SQLStatementPlan plan,
            Operator node, double coefficient) {
        node.coefficient = coefficient;
        List<Operator> children = plan.getChildren(node);
        if (children.size() == 0)
            return;
        if (node.getName().equals(Operator.NLJ)) {
            if (children.size() != 2)
                throw new Error("NLJ operator has " + children.size()
                        + " children");
            Operator left = children.get(0);
            Operator right = children.get(1);
            double rows = left.cardinalityNLJ;
            if (rows < 1)
                rows = 1;
            calculateCoefficient(plan, left, coefficient);
            calculateCoefficient(plan, right, coefficient * rows);
        } else {
            for (Operator operator : children) {
                calculateCoefficient(plan, operator, coefficient);
            }
        }
    }
    
     @Override
    public String toString() {
        return "From indexes: "+ fromIndexes+"\r\n"+super.toString();
    }
}
