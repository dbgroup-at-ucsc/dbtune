package edu.ucsc.dbtune.inum;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.sql.compile.BinaryRelationalOperatorNode;
import org.apache.derby.impl.sql.compile.ColumnReference;
import org.apache.derby.impl.sql.compile.CursorNode;
import org.apache.derby.impl.sql.compile.FromBaseTable;
import org.apache.derby.impl.sql.compile.FromList;
import org.apache.derby.impl.sql.compile.GroupByList;
import org.apache.derby.impl.sql.compile.HalfOuterJoinNode;
import org.apache.derby.impl.sql.compile.JoinNode;
import org.apache.derby.impl.sql.compile.OrderByList;
import org.apache.derby.impl.sql.compile.QueryTreeNode;
import org.apache.derby.impl.sql.compile.SelectNode;

import static edu.ucsc.dbtune.inum.FullTableScanIndex.getFullTableScanIndexInstance;

/**
 * Extracts interesting orders by parsing a query using the Derby SQL parser. Supports any SQL
 * statement that is supported by Derby 10.8. This implementation assumes that the SQL statements
 * given to it are well written, i.e. that there are no semantic issues w.r.t. to column names that
 * reference tables from the from list. For example:
 * <pre>
 * {@code .
 * SELECT *
 *   FROM
 *      r,
 *      s
 *   WHERE
 *      r.a = s.a
 *   ORDER BY
 *      a
 * }
 * </pre>
 * <p>
 * The above is ambiguous since the parser can't identify which table the {@code ORDER BY} clause is
 * referring to.
 *
 * @author Ivo Jimenez
 * @author Quoc Trung Tran
 */
public class DerbyInterestingOrdersExtractor implements InterestingOrdersExtractor, Visitor
{
    private static final String CONNECTION_URL = "jdbc:derby:memory:dummy;create=true";
    private static final String DERBY_DEBUG_SETTING = "derby.debug.true";
    private static final String DRIVER_NAME = "org.apache.derby.jdbc.EmbeddedDriver";
    private static final String LANG_CONNECTION = "LanguageConnectionContext";
    private static final String STOP_AFTER_PARSING =  "StopAfterParsing";
    private static final String STOPPED_AFTER_PARSING = "42Z55";
    private static final Connection CON;

    protected Catalog catalog;
    protected Set<Visitable> astNodes;
    protected boolean defaultAscending;
    
    /** From, group-by, and order-by list */
    protected FromList from;
    protected GroupByList groupBy;
    protected OrderByList orderBy;
    
    /** Set of columns that correspond to interesting orders */
    private Set<Column> columns;
    private Map<Column, Boolean> ascending;
    
    /** Order by and group by list per table*/
    private Map<Table, List<Column>> orderByColumnsPerTable;
    private Map<Table, List<Column>> groupByColumnsPerTable;
    
    /** Binary operand (e.g., R.a > S.b, R.a = 10) extracted by Derby */
    private List<ColumnReference> binaryOperandNodes;    
    protected boolean leftOperand;
    protected boolean rightOperand;

    static {
        System.setProperty(DERBY_DEBUG_SETTING, STOP_AFTER_PARSING);

        try {
            Class.forName(DRIVER_NAME);
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        try {
            CON = (EmbedConnection) DriverManager.getConnection(CONNECTION_URL);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        //SanityManager.SET_DEBUG_STREAM(new PrintWriter(System.out));
    }

    /**
     * @param catalog
     *      used to retrieve database metadata
     * @param mode
     *      the default value assigned to columns appearing in {@code GROUP BY} but not in the
     *      {@code ORDER BY} clause. Possible values are {@link Index#ASCENDING} and {@link
     *      Index#DESCENDING}
     */
    public DerbyInterestingOrdersExtractor(Catalog catalog, boolean mode)
    {
        this.catalog = catalog;
        this.defaultAscending = mode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Set<Index>> extract(SQLStatement statement) throws SQLException
    {
        parse(statement);

        try {
            return extractInterestingOrders();
        }
        catch (StandardException e) {
            throw new SQLException("An error occurred while iterating the from list", e);
        }
    }

    /**
     * Determines if this instance is defaultAscending.
     *
     * @return The defaultAscending.
     */
    public boolean isDefaultAscending()
    {
        return this.defaultAscending;
    }

    /**
     * Parses the statement by invoking the Derby optimizer. More specifically, it extracts the
     * names of tables referred in the {@code FROM} clause of a {@code SELECT} statement, as well as
     * the column names referred in the {@code ORDER BY} and {@code GROUP BY} clauses.
     * <p>
     * The result is placed in {@link #from}, {@link #groupBy} and {@link #orderBy} members.
     *
     * @param stmt
     *      statement to be parsed
     * @throws SQLException
     *      if an error occurs while obtaining the statement AST; if an error occurs while walking
     *      through the statement AST
     */
    protected void parse(SQLStatement stmt) throws SQLException
    {
        ContextManager cm;
        LanguageConnectionContext lcc;
        QueryTreeNode qt;

        try {
            CON.prepareStatement(stmt.getSQL());
        }
        catch (SQLException se) {
            String sqlState = se.getSQLState();

            if (!STOPPED_AFTER_PARSING.equals(sqlState))
                throw se;
        }

        cm = ((EmbedConnection) CON).getContextManager();
        lcc = (LanguageConnectionContext) cm.getContext(LANG_CONNECTION);
        qt = (QueryTreeNode) lcc.getLastQueryTree();

        astNodes = new HashSet<Visitable>();
        from = null;
        groupBy = null;
        orderBy = null;
        binaryOperandNodes = new ArrayList<ColumnReference>();
        leftOperand = false;
        rightOperand = false;
        try {
            visit(qt);
        }
        catch (StandardException e) {
            throw new SQLException("An error occurred while walking the query AST", e);
        }

        //qt.treePrint(); // useful for debugging; prints to stdout
    }

    /**
     * Extracts the interesting orders from the previously populated {@link #from}, {@link #groupBy}
     * and {@link #orderBy} members. At the same time, this method binds database object names to
     * their in-memory representation (by consulting the {@link Catalog} object that was provided to
     * the constructor).
     * <p>
     * For columns specified in the {@code GROUP BY} but not contained in the {@code ORDER BY}, the
     * default sorting order is given by {@link #isDefaultAscending}.
     *
     * @return
     *      a list of sets of interesting orders, one per every table referenced in the statement
     * @throws StandardException
     *      if an error occurs while iterating the from list
     * @throws SQLException
     *      if a {@code SELECT} statement contains subqueries; if {@link
     *      #extractInterestingOrdersPerTable} fails;
     */
    private List<Set<Index>> extractInterestingOrders() throws StandardException, SQLException
    {
        List<String> tableNames = new ArrayList<String>();
        columns = new HashSet<Column>();
        ascending = new HashMap<Column, Boolean>();
        groupByColumnsPerTable = new HashMap<Table, List<Column>>();
        orderByColumnsPerTable = new HashMap<Table, List<Column>>();

        if (from == null || from.size() == 0)
            throw new SQLException("null or empty FROM list");

        for (int i = 0; i < from.size(); i++) {
            
            if (from.elementAt(i) instanceof FromBaseTable)
                tableNames.add(((FromBaseTable) from.elementAt(i)).getTableName().toString());
        }
        
        if (orderBy != null)
            bindOrderByColumns(orderBy, tableNames);
        
        if (groupBy != null)
            bindGroupByColumns(groupBy, tableNames);
        
        if (binaryOperandNodes.size() > 0)
            bindJoinPredicateColumns(binaryOperandNodes, tableNames);

        return extractInterestingOrdersPerTable(tableNames, columns, ascending);
    }
    
    
    /**
     * Bind columns referenced in the order-by into the corresponding database objects.
     * We only add the first column in the order-by clause
     * 
     * @param orderBy
     *      The list of order-by columns
     * @param tableNames
     *      The list of table names in the from-clause
     */
    private void bindOrderByColumns(OrderByList orderBy, List<String> tableNames)
    {
        // only consider the first column in the order-by clause
        String colName = orderBy.getOrderByColumn(0).getColumnExpression().getColumnName();
        Column col = bindColumn(tableNames, colName);
        List<Column> orderByColumns;
        
        if (col != null) {
            columns.add(col);
            ascending.put(col, orderBy.getOrderByColumn(0).isAscending());
        }

        for (int i = 0; i < orderBy.size(); i++) {
            
            colName = orderBy.getOrderByColumn(i).getColumnExpression().getColumnName();
            col = bindColumn(tableNames, colName);
            
            if (col == null)
                continue;

            ascending.put(col, isDefaultAscending());

            orderByColumns = orderByColumnsPerTable.get(col.getTable());

            if (orderByColumns == null) {
                orderByColumns = new ArrayList<Column>();
                orderByColumnsPerTable.put(col.getTable(), orderByColumns);
            }

            orderByColumns.add(col);
        }
    }

    /**
     * Bind columns in the given group-by list into the corresponding database object.
     *
     * @param groupBy
     *      The list of group-by columns
     * @param tableNames
     *      The list of table names in the from-clause
     */
    private void bindGroupByColumns(GroupByList groupBy, List<String> tableNames)
    {
        String colName;
        Column col;
        List<Column> groupByColumns;

        for (int i = 0; i < groupBy.size(); i++) {
            colName = groupBy.getGroupByColumn(i).getColumnExpression().getColumnName();
            col = bindColumn(tableNames, colName);
            if (col == null)
                continue;

            if (columns.add(col))
                ascending.put(col, isDefaultAscending());

            groupByColumns = groupByColumnsPerTable.get(col.getTable());

            if (groupByColumns == null) {
                groupByColumns = new ArrayList<Column>();
                groupByColumnsPerTable.put(col.getTable(), groupByColumns);
            }

            groupByColumns.add(col);
        }
    }


    /**
     * Bind the columns that are referenced in the join predicates
     * @param binaryOperandNodes
     *      A list of left and right columns of all the binary operators in the statement
     *      (including join predicate R.a  = S.b, selection predicate R.a > R.c, and
     *      the predicates in the sub-queries)
     * @param tableNames
     *      The list of table names that we can find the columns on
     *
     *
     */
    private void bindJoinPredicateColumns(List<ColumnReference> binaryOperandNodes, 
                                          List<String> tableNames) throws SQLException
    {
        String colName;
        Column col;
        List<Column> columnBinaryOperator = new ArrayList<Column>();

        for (ColumnReference colRef: binaryOperandNodes) {
            
            colName = colRef.getColumnName();
            col = null;
            
            // p_partkey = ps_partkey
            if (colRef.getTableName() == null)
                col = bindColumn(tableNames, colName);
            
            else {
                // table_0.column_0 = table_1.column_0
                // We need to retrieve the table name of each {@ColumnReference} object
                // to unambiguously identify which relation the column belongs to
                for (String tblName : tableNames) {
                    if (tblName.contains(colRef.getTableName()) == true) {
                        colName = tblName + "." + colName;

                        try {
                            col = catalog.<Column>findByName(colName);
                        } catch (SQLException e) {
                            // the column might belong to subqueries
                            e = null;
                        }
                        break;
                    }
                }
            }

            columnBinaryOperator.add(col);
        }

        // postprocess {@code columnBinaryOperator}
        // the two consecutive columns must be not null and belong to different relations
        for (int i = 0; i < columnBinaryOperator.size(); i += 2) {
            
            Column leftCol = columnBinaryOperator.get(i);
            Column rightCol = columnBinaryOperator.get(i + 1);

            // the predicate belongs to subqueries
            if (leftCol == null || rightCol == null)
                continue;

            // this is an actual join predicate (i.e., columns belonging to different relations) 
            if (!(leftCol.getTable()).equals(rightCol.getTable())) {
                
                if (columns.add(leftCol))
                    ascending.put(leftCol, isDefaultAscending());

                if (columns.add(rightCol))
                    ascending.put(rightCol, isDefaultAscending());
            }
        }
    }

    /**
     * Binds database object names to their in-memory representation (by consulting the {@link
     * Catalog} object that was provided to the constructor). This method assumes that the SQL text
     * is well written, i.e. that there are no ambiguities with respect to the column names referred
     * in the {@code ORDER BY} and {@code GROUP BY} clauses.
     *
     * @param tableNames
     *      each element corresponds to a table in the {@code FROM} clause
     * @param colName
     *      corresponds to a column in the {@code GROUP BY} or {@code ORDER BY} clause
     * @return
     *      A {@Column} object or NULL if the column is not a part of all the tables
     *      in @code tableNames}
     */
    protected Column bindColumn(List<String> tableNames, String colName)
    {
        Column col = null;

        for (String tblName : tableNames) {
            
            try {
                col = catalog.<Column>findByName(tblName + "." + colName);
            }
            catch (SQLException e) {
                // it's OK not to find it, as long as we find it in a subsequent iteration
                e = null;
            }
            if (col != null)
                break;
        }

        return col;
    }

    /**
     * Extract interesting orders.
     *
     * @param tableNames
     *      List of table names referenced in the from-clause
     * @param columns
     *      The set of columns that are referenced by from, order by, and group by clause
     * @param ascending
     *      The ascending order of each interesting column
     * @return
     *      A set of interesting orders
     * @throws SQLException
     *      when there is error in connecting with databases
     */
    private List<Set<Index>> extractInterestingOrdersPerTable(
            List<String> tableNames,
            Set<Column> columns,
            Map<Column, Boolean> ascending)
        throws SQLException
    {
        Map<Table, Set<Index>> interestingOrdersPerTable = new HashMap<Table, Set<Index>>();
        Set<Index> interestingOrdersForTable;
        Table table;

        // add FTS for each table in the from clause
        for (String tblName : tableNames) {
            
            table = catalog.<Table>findByName(tblName);

            if (interestingOrdersPerTable.get(table) == null)  {
                interestingOrdersForTable = new HashSet<Index>();
                interestingOrdersForTable.add(getFullTableScanIndexInstance(table));
                interestingOrdersPerTable.put(table, interestingOrdersForTable);
            }
        }

        for (Column col : columns) {
            table = col.getTable();

            if (!interestingOrdersPerTable.containsKey(table))
                throw new SQLException("Can't find " + table + " in the from list");

            interestingOrdersForTable = interestingOrdersPerTable.get(table);
            interestingOrdersForTable.add(new InumInterestingOrder(col, ascending.get(col)));
        }

        return new ArrayList<Set<Index>>(interestingOrdersPerTable.values());
    }

    /**
     * Walks the tree to extract the FROM, ORDER BY and GROUP BY lists.
     *
     * @param node
     *      a node of the tree
     * @return
     *      the passed node
     * @throws StandardException
     *      if an error occurs while visiting the node
     */
    @Override
    public Visitable visit(Visitable node) throws StandardException
    {
        if (!astNodes.contains(node)) {
            astNodes.add(node);
            
            /**
             * The algorithm to derive the columns in the join predicates work as follows:
             * Whenever we encounter {@code BinaryRelationalOperatorNode} object, we are expected
             * to receive the next two nodes are the left operand and right operand.
             * We then need to check if both the left and right operand are of type
             * {code ColumnReference}, then this operator node corresponds to a join predicate.
             */
            
            if (leftOperand == true) {
                
                // If the left operand is a column, we will investigate the right operand
                // to determine a join predicate.
                // Otherwise, we simply skip this BinaryOperator.
                if (node instanceof ColumnReference){
                    binaryOperandNodes.add((ColumnReference) node);
                    rightOperand = true;
                } else
                    rightOperand = false;

                leftOperand = false;
                
            } else if (rightOperand == true) {
                
                if (node instanceof ColumnReference) // this is matched join predicate
                    binaryOperandNodes.add((ColumnReference) node);
                else // if not, remove the left operand stored in the list
                    binaryOperandNodes.remove(binaryOperandNodes.size() - 1);

                rightOperand = false;
            }            
            
            else if (node instanceof SelectNode) {
                
                final SelectNode select = (SelectNode) node;
                if (from == null)
                    from = select.getFromList();
                else {
                    
                    for (int i = 0; i < select.getFromList().size(); i++) {
                        
                        if (select.getFromList().elementAt(i) instanceof FromBaseTable)
                            from.addFromTable((FromBaseTable) select.getFromList().elementAt(i));
                        // Extract the table from the left and right result set
                        // of a join node instance
                        else if (select.getFromList().elementAt(i) instanceof HalfOuterJoinNode) {
                            
                            HalfOuterJoinNode outerJoin = (HalfOuterJoinNode) 
                                                            select.getFromList().elementAt(i);                            
                            from.addFromTable((FromBaseTable)outerJoin.getLeftResultSet());
                            from.addFromTable((FromBaseTable)outerJoin.getRightResultSet());
                        }
                        
                        else if (select.getFromList().elementAt(i) instanceof JoinNode) {
                            
                            JoinNode outerJoin = (JoinNode) select.getFromList().elementAt(i);
                            from.addFromTable((FromBaseTable)outerJoin.getLeftResultSet());
                            from.addFromTable((FromBaseTable)outerJoin.getRightResultSet());
                        }
                    }
                }
                
                if (groupBy == null)
                    groupBy = select.getGroupByList();
                
            } else if (node instanceof CursorNode) {
                
                if (orderBy == null)
                    orderBy = ((CursorNode) node).getOrderByList();
                
            } else if (node instanceof BinaryRelationalOperatorNode) {
                
                leftOperand = true;
                rightOperand = false;
            } 
                
            node.accept(this);
        }

        return node;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean visitChildrenFirst(Visitable node)
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean stopTraversal()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean skipChildren(Visitable node) throws StandardException
    {
        return false;
    }
}
