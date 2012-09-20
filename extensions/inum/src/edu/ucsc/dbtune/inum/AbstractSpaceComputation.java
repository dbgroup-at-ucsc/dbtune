package edu.ucsc.dbtune.inum;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.DatabaseObject;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.InterestingOrder;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;

import static com.google.common.collect.Iterables.get;

import static edu.ucsc.dbtune.inum.FullTableScanIndex.getFullTableScanIndexInstance;
import static edu.ucsc.dbtune.optimizer.plan.Operator.NLJ;
import static edu.ucsc.dbtune.util.InumUtils.extractInterestingOrders;
import static edu.ucsc.dbtune.util.InumUtils.getCoveringAtomicConfiguration;
import static edu.ucsc.dbtune.util.MetadataUtils.getIndexesReferencingTable;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.workload.SQLCategory.INSERT;

/**
 * Common functionality for space computation.
 *
 * @author Ivo Jimenez
 */
public abstract class AbstractSpaceComputation implements InumSpaceComputation
{
    private static Set<Index> empty = new HashSet<Index>();


    public static Set<InumInterestingOrder> overrideInumSpacePopulateIndexSet;
    
    public static void setInumSpacePopulateIndexSet(Set<Index> indexes)
            throws SQLException {
        overrideInumSpacePopulateIndexSet = new HashSet<InumInterestingOrder>();
        for (Index index : indexes) {
            InumInterestingOrder order = new InumInterestingOrder(index
                    .columns(), index.getAscending());
            overrideInumSpacePopulateIndexSet.add(order);
        }
    }
    
    private static InumInterestingOrder getInterestingOrder(
            Map<String, Table> qidToTable,Map<String, Table> columnNameToTable, String c) throws SQLException {
        c=c.trim();
        if (c.length() == 0 || c.contains("$RID$") || c.contains("$C"))
            return null;
        if (c.startsWith("'"))
            return null;
        // Rt.np(c);
        String[] ss = c.split("\\.");
        if (ss.length!=2)
            return null;
        String columnName = ss[1];
        boolean asc = true;
        int t = columnName.indexOf("(");
        if (t > 0) {
            String order = columnName.substring(t);
            columnName = columnName.substring(0, t);
            if ("(A)".equals(order))
                asc = true;
            else if ("(D)".equals(order))
                asc = false;
            else
                throw new Error();
        }
        Table table = qidToTable.get(ss[0]);
        // Rt.p(ss[0]+" "+table);
        if (table == null) {
            //TODO temporary workaround. Need to trace where the column comes from.
/*
For example,find out where Q6.O_ORDERDATE(A) comes from
   RETURN(id=1 cost=161206.53 card=0.00 coeff=1.0 object=NONE rawColumns=null rawPredicate=null fetch=NONE internal=0.00)
    └── TABLE.SCAN(id=2 cost=161206.53 card=317674.59 coeff=1.0 object=NONE rawColumns=+Q6.REVENUE(D)+Q6.O_ORDERDATE(A)+Q6.O_SHIPPRIORITY+Q6.L_ORDERKEY rawPredicate=null fetch=NONE internal=15.00)
        └── SORT(id=3 cost=161191.53 rows=317675 rowWidth=28 card=317674.59 coeff=1.0 object=NONE rawColumns=+Q5.$C1(D)+Q5.O_ORDERDATE(A)+Q5.O_SHIPPRIORITY+Q5.L_ORDERKEY rawPredicate=null fetch=NONE internal=300.72)
            └── GRPBY(id=4 cost=160890.81 card=317674.59 coeff=1.0 object=NONE rawColumns=+Q5.O_SHIPPRIORITY+Q5.O_ORDERDATE+Q5.$C1+Q5.L_ORDERKEY rawPredicate=null fetch=NONE internal=15.00)
                └── TABLE.SCAN(id=5 cost=160875.81 card=317674.63 coeff=1.0 object=NONE rawColumns=+Q4.L_ORDERKEY(A)+Q4.O_ORDERDATE(A)+Q4.O_SHIPPRIORITY(A)+Q4.L_DISCOUNT+Q4.L_EXTENDEDPRICE rawPredicate=null fetch=NONE internal=1538.72)
                    └── SORT(id=6 cost=159337.09 rows=317675 rowWidth=32 card=317674.63 coeff=1.0 object=NONE rawColumns=+Q4.L_ORDERKEY(A)+Q4.O_ORDERDATE(A)+Q4.O_SHIPPRIORITY(A)+Q4.L_DISCOUNT+Q4.L_EXTENDEDPRICE rawPredicate=null fetch=NONE internal=10920.30)
                        └── HASH.JOIN(id=7 cost=148416.80 card=317674.63 coeff=1.0 object=NONE rawColumns=+Q4.L_DISCOUNT+Q4.L_EXTENDEDPRICE+Q4.O_SHIPPRIORITY+Q4.O_ORDERDATE+Q4.L_ORDERKEY rawPredicate=[(Q1.L_ORDERKEY = Q2.O_ORDERKEY)] fetch=NONE internal=18010.94)
                            ├── TABLE.SCAN(id=8 OUTER cost=104584.35 card=3219266.00 coeff=1.0 object=TPCH.LINEITEM rawColumns=+Q1.L_DISCOUNT+Q1.L_EXTENDEDPRICE+Q1.L_ORDERKEY+Q1.$RID$+Q1.L_DISCOUNT+Q1.L_EXTENDEDPRICE+Q1.L_SHIPDATE+Q1.L_ORDERKEY rawPredicate=[('1995-03-17' < Q1.L_SHIPDATE)] fetch=[+TPCH.LINEITEM.L_DISCOUNT(A)+TPCH.LINEITEM.L_EXTENDEDPRICE(A)+TPCH.LINEITEM.L_SHIPDATE(A)+TPCH.LINEITEM.L_ORDERKEY(A)] internal=0.00)
                            └── HASH.JOIN(id=9 INNER cost=25821.50 card=155852.95 coeff=1.0 object=NONE rawColumns=+Q2.O_SHIPPRIORITY+Q2.O_ORDERDATE+Q2.O_ORDERKEY+Q2.O_CUSTKEY+Q3.C_CUSTKEY rawPredicate=[(Q3.C_CUSTKEY = Q2.O_CUSTKEY)] fetch=NONE internal=27.89)
                                ├── TABLE.SCAN(id=10 OUTER cost=22400.94 card=760345.88 coeff=1.0 object=TPCH.ORDERS rawColumns=+Q2.O_SHIPPRIORITY+Q2.O_ORDERDATE+Q2.O_ORDERKEY+Q2.O_CUSTKEY+Q2.$RID$+Q2.O_SHIPPRIORITY+Q2.O_ORDERDATE+Q2.O_ORDERKEY+Q2.O_CUSTKEY rawPredicate=[(Q2.O_ORDERDATE < '1995-03-17')] fetch=[+TPCH.ORDERS.O_SHIPPRIORITY(A)+TPCH.ORDERS.O_ORDERDATE(A)+TPCH.ORDERS.O_ORDERKEY(A)+TPCH.ORDERS.O_CUSTKEY(A)] internal=0.00)
                                └── TABLE.SCAN(id=11 INNER cost=3392.67 card=30225.00 coeff=1.0 object=TPCH.CUSTOMER rawColumns=+Q3.C_CUSTKEY+Q3.$RID$+Q3.C_CUSTKEY+Q3.C_MKTSEGMENT rawPredicate=[(Q3.C_MKTSEGMENT = 'FURNITURE ')] fetch=[+TPCH.CUSTOMER.C_CUSTKEY(A)+TPCH.CUSTOMER.C_MKTSEGMENT(A)] internal=0.00)

 */
            table=columnNameToTable.get(columnName);
            if (table == null)
                return null;
        }
        Column column = (Column) table.find(columnName);
        if (column==null)
            return null;
        return new InumInterestingOrder(column, asc);
    }
    
    /**
     * Extract interesting order by parsing db plan
     * 
     * @param statement
     * @param delegate
     * @return
     * @throws SQLException
     */
    public static Set<InumInterestingOrder> extractInterestingOrderFromDB(
            SQLStatement statement, Optimizer delegate) throws SQLException {
        return extractInterestingOrderFromDB(statement, delegate,false);
    }
    
    /**
     * Extract interesting order by parsing db plan
     * 
     * @param statement
     * @param delegate
     * @param debug show debug information
     * @return
     * @throws SQLException
     */
    public static Set<InumInterestingOrder> extractInterestingOrderFromDB(
            SQLStatement statement, Optimizer delegate,boolean debug) throws SQLException {
        Set<InumInterestingOrder> indexes2 = new HashSet<InumInterestingOrder>();
        ExplainedSQLStatement db2plan = delegate.explain(statement);
        SQLStatementPlan plan = db2plan.getPlan();
        Map<String, Table> qidToTable = new Hashtable<String, Table>();
        Map<String, Table> columnNameToTable = new Hashtable<String, Table>();
        for (Operator op : plan.nodes()) {
            if (op.rawColumnNames == null)
                continue;
            for (String c : op.rawColumnNames.split("\\+")) {
                if (c.length() == 0 || c.contains("$RID$") || c.contains("$C"))
                    continue;
                Table table = op.getTable();
                if (table == null)
                    continue;
                String[] ss = c.split("\\.", 2);
                qidToTable.put(ss[0], table);
                if (ss[1].indexOf("(")>0)
                    ss[1]=ss[1].substring(0,ss[1].indexOf("("));
                columnNameToTable.put(ss[1], table);
//                Rt.p(ss[0]+" "+table);
            }
       }
       if (debug)
            Rt.np(plan);
        for (Operator op : plan.nodes()) {
            if (op.rawColumnNames != null) {
                for (String c : op.rawColumnNames.split("\\+")) {
                    if (!c.contains("("))
                        continue;
    //                Rt.p(column);
                    InumInterestingOrder order=getInterestingOrder(qidToTable,columnNameToTable, c);
                    if (order!=null)
                        indexes2.add(order);
                }
            }
            if (op.rawPredicateList!=null) {
                for (String predicate : op.rawPredicateList) {
                    String[] ss= predicate.split("\\(|\\)");
                    for (String s : ss) {
                        s=s.trim();
                        if (s.length()==0)
                            continue;
                        String[] ss2=s.split(" = | < | > | <= | >= | <> | LIKE ");
                        if (ss2.length!=2)
                            continue;
                        InumInterestingOrder[] orders=new InumInterestingOrder[ss2.length];
                        for (int i=0;i<ss2.length;i++) {
                            orders[i]=getInterestingOrder(qidToTable,columnNameToTable, ss2[i]);
                        }
                        if (orders[0]!=null&&orders[1]!=null) {
                            indexes2.add(orders[0]);
                            indexes2.add(orders[1]);
                        }
                    }
                }
            }
        }
        return indexes2;
    }
    
    public static int derbyFailedCount=0;
    
    /**
     * Computes the INUM space by extracting interesting orders using the {@link 
     * DerbyInterestingOrdersExtractor}. The configuration associated to each table is completed 
     * using {@link #complete}, i.e. this method ensures that each table has at least the {@link 
     * FullTableScanIndex} interesting order in the set of interesting orders. After this is done, 
     * the {@link #computeWithCompleteConfiguration} method is called.
     *
     * @param space
     *      the space to be computed. {@link Set#clear} is invoked before populating it.
     * @param statement
     *      SQL statement for which the INUM space is computed
     * @param delegate
     *      optimizer used to execute {@link Optimizer#explain(SQLStatement, Configuration) what if 
     *      calls}
     * @param catalog
     *      used to retrieve metadata for objects referenced in the statement
     * @throws SQLException
     *      if the inum space can't be populated
     */
    @Override
    public void compute(
            Set<InumPlan> space, SQLStatement statement, Optimizer delegate, Catalog catalog)
        throws SQLException
    {
        space.clear();

        // add FTS-on-all-slots template
        InumPlan templateForEmpty = new InumPlan(delegate, delegate.explain(statement, empty));

        space.add(templateForEmpty);

        if (statement.getSQLCategory().isSame(INSERT))
            // no need to explore more
            return;

        Set<? extends Index> indexes;
        if (overrideInumSpacePopulateIndexSet!=null)
            indexes=overrideInumSpacePopulateIndexSet;
        else {
            try {
                indexes=extractInterestingOrders(statement, catalog);
            } catch (Exception e) {
                derbyFailedCount++;
//                Rt.error(e.getClass().getName()+": "+e.getMessage());
                indexes=extractInterestingOrderFromDB(statement,delegate);
            }
        }
        
        // obtain plans for all the extracted interesting orders
        computeWithCompleteConfiguration(
                space, indexes, statement, delegate);

        // NLJ heuristic referred in the INUM paper
        Set<Index> covering = getCoveringAtomicConfiguration(templateForEmpty);

        ExplainedSQLStatement coveringExplainedStmt = delegate.explain(statement, covering);

        if (coveringExplainedStmt.getPlan().contains(NLJ))
            space.add(new InumPlan(delegate, coveringExplainedStmt));
    }

    /**
     * Computes the space given a complete configuration that is extracted from a set of interesting 
     * orders. A complete configuration is guaranteed to have at least one index for every table 
     * referenced in the statement, where the worst case is when the {@link FullTableScanIndex} is 
     * the only index for a particular table.
     *
     * @param space
     *      the space to be computed. {@link Set#clear} is invoked before populating it.
     * @param indexes
     *      interesting orders
     * @param statement
     *      SQL statement for which the INUM space is computed
     * @param delegate
     *      optimizer used to execute {@link Optimizer#explain(SQLStatement, Configuration) what if 
     *      calls}
     * @throws SQLException
     *      if the inum space can't be populated
     */
    public abstract void computeWithCompleteConfiguration(
            Set<InumPlan> space,
            Set<? extends Index> indexes,
            SQLStatement statement,
            Optimizer delegate)
        throws SQLException;

    /**
     * Checks if the plan actually uses the given {@code interestingOrders}. For each slot, the 
     * corresponding index is retrieved and compares it against the corresponding index contained in 
     * {@code interestingOrders}, if for any slot the order doesn't correspond to the used 
     * interesting order, the method returns {@code false}.
     * 
     * @param plan
     *      The plan returned by the optimizer
     * @param interestingOrders
     *      The given interesting order
     * @return
     *      {@code true} if the plan uses all the indexes corresponding to the given interesting 
     *      orders; {@code false} otherwise
     * @throws SQLException
     *      if a leaf of the plan corresponding to a table contains more than one index assigned to 
     *      it, i.e. if the plan is not atomic; if the plan is using a materialized index
     */
    public static boolean isUsingAllInterestingOrders(
            SQLStatementPlan plan, Collection<Index> interestingOrders)
        throws SQLException
    {
        for (Table table : plan.getTables()) {

            Index indexFromPlan = getIndexReferencingTable(plan.getIndexes(), table);
            Index indexFromOrder = getIndexReferencingTable(interestingOrders, table);

            boolean isIndexFromPlanFTS = indexFromPlan instanceof FullTableScanIndex;
            boolean isIndexFromOrderFTS = indexFromOrder instanceof FullTableScanIndex;
            
            if (isIndexFromOrderFTS && isIndexFromPlanFTS)
                // this is fine
                continue;
            
            if (isIndexFromOrderFTS && !isIndexFromPlanFTS)
                throw new SQLException(
                    "Interesting order is FTS but optimizer is using a materialized index");
            
            if (!isIndexFromOrderFTS && isIndexFromPlanFTS)
                // the given interesting order is an actual index, but the one returned by the 
                // optimizer is the FTS index, thus they're not compatible
                return false;
            
            if (!isIndexFromOrderFTS && !isIndexFromPlanFTS)
                // need to check whether the two indexes are the same
                if (!indexFromOrder.equalsContent(indexFromPlan))
                    return false;
        }
        
        return true;
    }

    /**
     * Returns the index that references the given table. If there is no index referencing the given 
     * table, the corresponding {@link FullTableScanIndex} is returned.
     *
     * @param indexes
     *      indexes that are iterated in order to look for one referencing to {@code table}
     * @param table
     *      table that should be referenced by an index
     * @return
     *      the index in {@code indexes} that refers to {@code table}; the {@link 
     *      FullTableScanIndex} if none is referring to {@code table}
     * @throws SQLException
     *      if a table is referenced by more than one index
     */
    public static Index getIndexReferencingTable(Collection<Index> indexes, Table table)
        throws SQLException
    {
        Set<Index> indexesReferncingTable = getIndexesReferencingTable(indexes, table);

        if (indexesReferncingTable.size() > 1)
            throw new SQLException("More than one index on slot for " + table);

        if (indexesReferncingTable.size() == 0)
            return getFullTableScanIndexInstance(table);

        return get(indexesReferncingTable, 0);
    }
}
